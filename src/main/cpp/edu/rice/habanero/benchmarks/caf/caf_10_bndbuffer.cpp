#include <iostream>
#include <cstdlib>
#include <deque>
#include <fstream>

#include "benchmark_runner.hpp"

#include "pseudo_random.hpp"

using namespace std;
using namespace caf;

class config : public actor_system_config {
public:
  int buffer_size = 50;
  int num_producers = 40;
  int num_consumers = 40;
  static int prod_cost; // = 25;
  static int cons_cost; // = 25;
  int num_items_per_producer = 1000;
  int num_mailboxes = 1;
  
  config() {
    opt_group{custom_options_, "global"}
    .add(buffer_size, "bbb,b", "buffer size")
    .add(num_producers, "ppp,p", "number of producers")
    .add(num_consumers, "ccc,c", "number of consumers")
    .add(prod_cost, "xxx,x", "producer cost")
    .add(cons_cost, "yyy,y", "consumer cost")
    .add(num_items_per_producer, "iii,i", "number of items per producer")
    .add(num_mailboxes, "nm", "number of mailboxes");
  }
};
int config::prod_cost = 25;
int config::cons_cost = 25;

double process_item(double cur_term, int cost) {
  double res = cur_term;
  pseudo_random random(cost);
  if (cost > 0) {
    for (int i = 0; i < cost; i++) {
      for (int j = 0; j < 100; j++) {
        res += log(abs(random.next_double()) + 0.01);
      }
    }
  } else {
    res += log(abs(random.next_double()) + 0.01);
  }
  return res;
}

//enum class msgs_source {
  //producer, consumer
//};

struct data_item_msg {
  double data;
  actor producer;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(data_item_msg);

struct consumer_available_msg {
  actor consumer;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(consumer_available_msg);

using produce_data_msg_atom = atom_constant<atom("prodatam")>;
using producer_exit_msg_atom = atom_constant<atom("proexitm")>;
using consumer_exit_msg_atom = atom_constant<atom("conexitm")>;
struct consumer_actor_state {
  consumer_available_msg consumer_available_message; 
  double cons_item;
};

behavior consumer_actor(stateful_actor<consumer_actor_state>* self, int /*id*/, actor manager) {
  auto& s = self->state;
  s.consumer_available_message =
    consumer_available_msg{actor_cast<actor>(self)};
  s.cons_item = 0.0;
  auto consume_data_item = [=](double data_to_consume) {
    auto& s = self->state;
    s.cons_item = process_item(s.cons_item + data_to_consume, config::cons_cost);
  };
  return {
    [=](data_item_msg& dm) {
      consume_data_item(dm.data);
      self->send(manager, s.consumer_available_message);
    },
    [=](consumer_exit_msg_atom) {
      self->quit();
    }
  };
}

struct producer_actor_state {
  double prod_item = 0.0;
  int items_produced = 0;
};

behavior producer_actor(stateful_actor<producer_actor_state>* self, int /*id*/, actor manager,
                        int num_items_to_produce) {
  auto produce_data = [=]() {
    auto& s = self->state;
    s.prod_item = process_item(s.prod_item, config::prod_cost);
    self->send(manager, data_item_msg{s.prod_item, actor_cast<actor>(self)});
    ++s.items_produced;
  };
  return {
    [=](produce_data_msg_atom) {
      auto& s = self->state;
      if (s.items_produced == num_items_to_produce) {
        // onPreExit
        self->send(manager, producer_exit_msg_atom::value);
        self->quit(); 
      } else {
        produce_data(); 
      }
    }
  };
}

struct manager_actor_state {
  int adjust_buffer_size;
  deque<actor> available_producers;
  deque<actor> available_consumers;
  deque<data_item_msg> pending_data;
  int num_terminated_producers;
  vector<actor> producers;
  vector<actor> consumers;
  actor self;
};

behavior manager_actor(stateful_actor<manager_actor_state>* self, int buffer_size,
                       int num_producers, int num_consumers,
                       int num_items_per_producer) {
  auto& s = self->state;
  s.self = actor_cast<actor>(self);
  s.adjust_buffer_size = buffer_size - num_producers;
  s.num_terminated_producers = 0;
  s.producers.reserve(num_producers);
  for (int i = 0; i < num_producers; ++i) {
    s.producers.emplace_back(self->spawn(producer_actor, i, s.self, num_items_per_producer));
  }
  s.consumers.reserve(num_consumers);
  for (int i = 0; i < num_consumers; ++i) {
    s.consumers.emplace_back(self->spawn(consumer_actor, i, s.self));
  }
  // onPostStart()
  for (auto& loop_consumer : s.consumers) {
    s.available_consumers.emplace_back(loop_consumer);
  }
  for (auto& loop_producer : s.producers) {
    self->send(loop_producer, produce_data_msg_atom::value); 
  }
  // onPreExit 
  auto on_pre_exit = [=]() {
    for (auto& loop_consumer : s.consumers) {
      self->send(loop_consumer, consumer_exit_msg_atom::value);   
    }
  };
  auto try_exit = [=]() {
    auto& s = self->state;
    if (s.num_terminated_producers == num_producers
        && static_cast<int>(s.available_consumers.size()) == num_consumers) {
      on_pre_exit();
      self->quit(); 
    }
  };
  return {
    [=](data_item_msg& dm) {
      auto& s = self->state;
      auto producer = dm.producer; 
      if (s.available_consumers.empty()) {
        s.pending_data.emplace_back(move(dm)); 
      } else {
        self->send(s.available_consumers.front(), move(dm));
        s.available_consumers.pop_front();
      }
      if (static_cast<int>(s.pending_data.size()) >= s.adjust_buffer_size) {
        s.available_producers.emplace_back(move(producer)); 
      } else {
        self->send(producer, produce_data_msg_atom::value); 
      }
    },
    [=](consumer_available_msg& cm) {
      auto& s = self->state;
      auto& consumer = cm.consumer; 
      if (s.pending_data.empty()) {
        s.available_consumers.emplace_back(move(consumer));
        try_exit();
      } else {
        self->send(consumer, s.pending_data.front());
        s.pending_data.pop_front();
        if (!s.available_producers.empty()) {
          self->send(s.available_producers.front(),
                     produce_data_msg_atom::value);
          s.available_producers.pop_front();
        }
      }
    },
    [=](producer_exit_msg_atom) {
      auto& s = self->state;
      ++s.num_terminated_producers;
      try_exit();
    }
  };
}

class bench : public benchmark {
public:
  void print_arg_info() const override {
    printf(benchmark_runner::arg_output_format(), "Buffer size",
           to_string(cfg_.buffer_size).c_str());
    printf(benchmark_runner::arg_output_format(), "num producers",
           to_string(cfg_.num_producers).c_str());
    printf(benchmark_runner::arg_output_format(), "num consumers",
           to_string(cfg_.num_consumers).c_str());
    printf(benchmark_runner::arg_output_format(), "prod cost",
           to_string(cfg_.prod_cost).c_str());
    printf(benchmark_runner::arg_output_format(), "cons cost",
           to_string(cfg_.cons_cost).c_str());
    printf(benchmark_runner::arg_output_format(), "items per producer",
           to_string(cfg_.num_items_per_producer).c_str());
    printf(benchmark_runner::arg_output_format(), "num mailboxes",
           to_string(cfg_.num_mailboxes).c_str());
  }

  void initialize(message& args) override {
    std::ifstream ini{ini_file(args)};
    cfg_.parse(args, ini);
  }

  void run_iteration() override {
    actor_system system{cfg_};
    if (cfg_.buffer_size <= cfg_.num_producers) {
      cerr << "buffer_size must be larger than num_producers" << endl;
      exit(0);
    }
    actor manager =
      system.spawn(manager_actor, cfg_.buffer_size, cfg_.num_producers,
                   cfg_.num_consumers, cfg_.num_items_per_producer);
  }
protected:
  const char* current_file() const override {
    return __FILE__; 
  }

private:
  config cfg_;
};

int main(int argc, char** argv) {
  benchmark_runner br;
  br.run_benchmark(argc, argv, bench{});
}

