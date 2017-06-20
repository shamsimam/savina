#include <iostream>
#include <fstream>

#include "benchmark_runner.hpp"

using namespace std;
using namespace caf;

class config : public actor_system_config {
public:
  static int n; //= 1e6;

  config() {
    opt_group{custom_options_, "global"}
    .add(n, "num,n", "number of messages");
  }
};
int config::n = 1e6;

using increment_atom = atom_constant<atom("increment")>;

struct retrieve_msg {
  actor sender;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(retrieve_msg);

struct result_msg {
  int result;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_msg);

behavior producer_actor(event_based_actor* self, actor counting) {
  return {
    [=](increment_atom) {
      for (int i = 0; i < config::n; ++i) {
        self->send(counting, increment_atom::value);
      }
      self->send(counting, retrieve_msg{actor_cast<actor>(self)});
    },
    [=](result_msg& m) {
      auto result = m.result;
      if (result != config::n) {
        cout << "ERROR: expected: " << config::n << ", found: " << result
             << endl;
      } else {
        cout << "SUCCESS! received: " << result << endl;
      }
    }
  };
}

behavior counting_actor(stateful_actor<int>* self) {
  self->state = 0;
  return {
    [=](increment_atom) { 
      ++self->state; 
    },
    [=](retrieve_msg& m) {
      self->send(m.sender, result_msg{self->state});
    }
  };
}

class bench : public benchmark {
public:
  void print_arg_info() const override {
    printf(benchmark_runner::arg_output_format(), "N (num messages)",
           to_string(cfg_.n).c_str());
  }

  void initialize(message& args) override {
    std::ifstream ini{ini_file(args)};
    cfg_.parse(args, ini);
  }

  void run_iteration() override {
    actor_system system{cfg_};
    auto counting = system.spawn(counting_actor);
    auto producer = system.spawn(producer_actor, counting);
    anon_send(producer, increment_atom::value);
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
