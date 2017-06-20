#include <iostream>
#include <stdlib.h>
#include <fstream>

#include "benchmark_runner.hpp"

using namespace std;
using namespace caf;

class config : public actor_system_config {
public:
  int n = 100;
  int r = 100000;

  config() {
    opt_group{custom_options_, "global"}
    .add(n, "nnn,n", "num of actors")
    .add(r, "rrr,r", "num of pings");
  }
};

struct ping_message {
  int pings_left;

  bool has_next() {
    return pings_left > 0; 
  }

  ping_message next() {
    return ping_message{pings_left -1}; 
  }
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(ping_message);

struct data_message {
  actor data;  
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(data_message);

struct exit_message {
  int exits_left;
  
  bool has_next() {
    return exits_left > 0; 
  }

  exit_message next() {
    return exit_message{exits_left -1};
  }
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(exit_message);

behavior thread_ring_actor(stateful_actor<actor>* self, int /*id*/, int num_actors_in_ring) {
  return {
    [=](ping_message& pm) {
      if (pm.has_next()) {
        self->send(self->state, pm.next()); 
      } else {
        self->send(self->state, exit_message{num_actors_in_ring});
      }
    },
    [=](exit_message& em) {
      if (em.has_next()) {
        self->send(self->state, em.next()); 
      }
      self->quit();
    },
    [=](data_message& dm) {
      self->state = dm.data;
    }
  };
}

void starter_actor(event_based_actor* self, const config* cfg) {
  auto num_actors_in_ring = cfg->n;
  vector<actor> ring_actors;
  ring_actors.reserve(num_actors_in_ring);
  for (int i = 0; i < num_actors_in_ring; ++i) {
    ring_actors.emplace_back(self->spawn(thread_ring_actor, i, num_actors_in_ring));
  }
  for (size_t i = 0; i < ring_actors.size(); ++i) {
    auto next_actor = ring_actors[(i + 1) % num_actors_in_ring] ;
    self->send(ring_actors[i], data_message{next_actor});
  }
  self->send(ring_actors[0], ping_message{cfg->r});
}

class bench : public benchmark {
public:
  void print_arg_info() const override {
    printf(benchmark_runner::arg_output_format(), "N (num actors)",
           to_string(cfg_.n).c_str());
    printf(benchmark_runner::arg_output_format(), "R (num rounds)",
           to_string(cfg_.r).c_str());
  }

  void initialize(message& args) override {
    std::ifstream ini{ini_file(args)};
    cfg_.parse(args, ini);
  }

  void run_iteration() override {
    actor_system system{cfg_};
    system.spawn(starter_actor, &cfg_);
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
