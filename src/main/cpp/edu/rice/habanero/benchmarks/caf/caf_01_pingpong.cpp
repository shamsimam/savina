#include <iostream>
#include <fstream>

#include "benchmark.hpp"
#include "benchmark_runner.hpp"

using namespace std;
using namespace caf;

class config : public actor_system_config {
public:
  int n = 40000;

  config() {
    opt_group{custom_options_, "global"}.add(n, "num,n", "number of ping-pongs");
  }
};

using start_msg_atom = atom_constant<atom("start")>;
using ping_msg_atom = atom_constant<atom("ping")>;
using stop_msg_atom = atom_constant<atom("stop")>;

struct send_ping_msg {
  actor sender;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(send_ping_msg);

struct send_pong_msg {
  actor sender;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(send_pong_msg);

behavior ping_actor(stateful_actor<int>* self, int count, actor pong) {
  self->state = count;
  return {
    [=](start_msg_atom) {
        self->send(pong, send_ping_msg{actor_cast<actor>(self)});
        --self->state;
    },
    [=](ping_msg_atom) {
      self->send(pong, send_ping_msg{actor_cast<actor>(self)});
      --self->state;
    },
    [=](send_pong_msg&) {
      if (self->state > 0) {
        self->send(self, ping_msg_atom::value);
      } else {
        self->send(pong, stop_msg_atom::value);
        self->quit();
      }
    }
  };
}

behavior pong_actor(stateful_actor<int>* self) {
  self->state = 0;
  return {
    [=](send_ping_msg& msg) { 
      auto& sender = msg.sender;
      self->send(sender, send_pong_msg{actor_cast<actor>(self)}); 
      ++self->state;
    },
    [=](stop_msg_atom) { 
      self->quit(); 
    }
  };
}

void starter_actor(event_based_actor* self, const config* cfg) {
  auto pong = self->spawn(pong_actor);
  auto ping = self->spawn(ping_actor, cfg->n, pong);
  self->send(ping, start_msg_atom::value);
}


class bench : public benchmark {
public:
  void print_arg_info() const override {
    printf(benchmark_runner::arg_output_format(), "n (num pings)",
           to_string(cfg_.n).c_str());
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
