#include <iostream>
#include <stdlib.h>
#include <fstream>

#include "benchmark_runner.hpp"

using namespace std;
using namespace caf;

class config : public actor_system_config {
public:
  int n = 10000;
  int a = 60;

  config() {
    opt_group{custom_options_, "global"}
    .add(n, "nnn,n", "messages per actor")
    .add(a, "aaa,a", "num of actors");
  }
};

void perform_computation(double theta) {
  double sint = sin(theta);
  double res = sint * sint;
  if (res <= 0) {
    throw string("Benchmark exited with unrealistic res value "
                 + to_string(res));
  }
}

struct message_t {
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(message_t);

behavior throughput_actor(stateful_actor<int>* self, int total_msgs) {
  self->state = 0;
  return {
    [=](message_t) {
      ++self->state;
      perform_computation(37.2);
      if (self->state == total_msgs) {
        self->quit(); 
      }
    }
  };
}

class bench : public benchmark {
public:
  void print_arg_info() const override {
    printf(benchmark_runner::arg_output_format(), "N (messages per actor)",
           to_string(cfg_.n).c_str());
    printf(benchmark_runner::arg_output_format(), "A (num actors)",
           to_string(cfg_.a).c_str());
  }

  void initialize(message& args) override {
    std::ifstream ini{ini_file(args)};
    cfg_.parse(args, ini);
  }

  void run_iteration() override {
    actor_system system{cfg_};
    vector<actor> actors;
    actors.reserve(cfg_.a);
    for (int i = 0; i< cfg_.a; ++i) {
      actors.emplace_back(system.spawn(throughput_actor, cfg_.n));
    }
    message_t message;
    for (int m = 0; m < cfg_.n; ++m) {
      for (auto& loop_actor: actors) {
        anon_send(loop_actor, message);
      }
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
