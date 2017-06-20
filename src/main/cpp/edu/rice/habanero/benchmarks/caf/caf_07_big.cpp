#include <iostream>
#include <random>
#include <fstream>
#include <stdlib.h>

#include "benchmark_runner.hpp"

using namespace std;
using namespace caf;

class config : public actor_system_config {
public:
  int n = 20000; 
  int w = 120;
  
  config() {
    opt_group{custom_options_, "global"}
    .add(n, "nnn,n", "number of pings")
    .add(w, "www,w", "number of actors");
  }
};

struct ping_msg {
  int sender;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(ping_msg);

struct pong_msg {
  int sender;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(pong_msg);

struct neighbor_msg {
  vector<actor> neighbors;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(neighbor_msg);

using exit_msg_atom = atom_constant<atom("exit")>;

struct sink_actor_state {
  int num_messages;
  vector<actor> neighbors;
};

behavior sink_actor_fun(stateful_actor<sink_actor_state>* self,
                        int num_workers) {
  self->state.num_messages = 0;
  return  {
    [=](exit_msg_atom) {
      auto& s = self->state;
      ++s.num_messages;
      if (s.num_messages == num_workers) {
        for (auto& loop_worker : s.neighbors) {
          self->send(loop_worker, exit_msg_atom::value);
          self->quit();
        }
      }
    },
    [=](neighbor_msg& nm) {
      self->state.neighbors = move(nm.neighbors);
    }
  };
}

struct big_actor_state {
  int num_pings;
  int exp_pinger;
  std::default_random_engine random;
  vector<actor> neighbors;
  ping_msg my_ping_msg;
  pong_msg my_pong_msg;
};

behavior big_actor_fun(stateful_actor<big_actor_state>* self, int id,
                       int num_messages, actor sink_actor) {
  auto& s = self->state;
  s.num_pings = 0; 
  s.exp_pinger = -1;
  s.random.seed(id);
  s.my_ping_msg = ping_msg{id};
  s.my_pong_msg = pong_msg{id};

  auto send_ping = [=](){
    auto& s = self->state;
    int target = s.random() % s.neighbors.size();
    auto target_actor = s.neighbors[target];
    s.exp_pinger = target;
    self->send(target_actor, s.my_ping_msg);
  };

  return {
    [=](ping_msg& pm) {
      auto& s = self->state;
      auto& sender = s.neighbors[pm.sender];
      self->send(sender, s.my_pong_msg);
    },
    [=](pong_msg& pm) {
      auto& s = self->state;
      if (pm.sender != s.exp_pinger) {
        cerr << "ERROR: Expected: " << s.exp_pinger
             << ", but received ping from " << pm.sender << endl;
      } 
      if (s.num_pings  == num_messages) {
        self->send(sink_actor, exit_msg_atom::value);
      } else {
        send_ping();
        ++s.num_pings;
      }
    },
    [=](exit_msg_atom) {
      self->quit();
    },
    [=](neighbor_msg& nm) {
      self->state.neighbors = move(nm.neighbors);
    }
  };
}

class bench : public benchmark {
public:
  void print_arg_info() const override {
    printf(benchmark_runner::arg_output_format(), "N (num pings)",
           to_string(cfg_.n).c_str());
    printf(benchmark_runner::arg_output_format(), "W (num actors)",
           to_string(cfg_.w).c_str());
  }

  void initialize(message& args) override {
    std::ifstream ini{ini_file(args)};
    cfg_.parse(args, ini);
  }

  void run_iteration() override {
    actor_system system{cfg_};
    actor sink_actor = system.spawn(sink_actor_fun, cfg_.w);
    vector<actor> big_actors;
    big_actors.reserve(cfg_.w);
    for (int i = 0; i < cfg_.w; ++i) {
      big_actors.emplace_back(
        system.spawn(big_actor_fun, i, cfg_.n, sink_actor));
    }
    anon_send(sink_actor, neighbor_msg{big_actors});
    for (auto& loop_actor : big_actors) {
      anon_send(loop_actor, neighbor_msg{big_actors});
    }
    for (auto& loop_actor : big_actors) {
      anon_send(loop_actor, pong_msg{-1});
    }
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

