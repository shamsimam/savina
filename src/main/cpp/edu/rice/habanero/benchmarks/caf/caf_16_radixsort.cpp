#include <iostream>
#include <vector>
#include <cstdlib>
#include <fstream>

#include "benchmark_runner.hpp"
#include "pseudo_random.hpp"

using namespace std;
using std::chrono::seconds;
using namespace caf;

class config : public actor_system_config {
public:
  int n = 100000;
  long long m = 1l << 60;
  long long s = 2048;

  config() {
    opt_group{custom_options_, "global"}
      .add(n, "nnn,n", "data size")
      .add(m, "mmm,m", "maximum value")
      .add(s, "sss,s", "seed for random number generator");
  }

};

struct next_actor_msg {
  actor next_actor;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(next_actor_msg);

struct value_msg {
  long long value;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(value_msg);

behavior int_source_actor_fun(event_based_actor* self, int num_values,
                              long long max_value, long long seed) {
  pseudo_random random(seed);
  return {
    [=](next_actor_msg& nm) mutable {
      for (int i = 0; i < num_values; ++i) {
        auto candidate = abs(random.next_long()) % max_value;
        auto message = value_msg{candidate};
        self->send(nm.next_actor, move(message));
      }
      self->quit();
    } 
  };
}

struct sort_actor_state {
  vector<value_msg> ordering_array;
};

behavior sort_actor_fun(stateful_actor<sort_actor_state>* self, int num_values,
                        long long radix, actor next_actor) {
  auto& s = self->state;
  s.ordering_array.reserve(num_values);
  for (int i = 0; i < num_values; ++i) {
    s.ordering_array.emplace_back(value_msg());
  }
  int values_so_far = 0;  
  int j = 0; 
  return  {
    [=](value_msg& vm) mutable {
      auto& s = self->state;
      ++values_so_far;
      auto current = vm.value;
      if ((current & radix) == 0) {
        self->send(next_actor, move(vm)); 
      } else {
        s.ordering_array[j] = move(vm);
        ++j;
      }
      if (values_so_far == num_values) {
        for (int i = 0; i < j; ++i) {
          self->send(next_actor, s.ordering_array[i]);
        }
        self->quit();
      }
    } 
  };
}

behavior validation_actor_fun(event_based_actor* self, int num_values) {
  auto sum_so_far = 0.0;
  auto values_so_far = 0;
  auto prev_value = 0L;
  auto error_value = make_tuple(-1L, -1); 
  return {
    [=](value_msg& vm) mutable {
      ++values_so_far; 
      if (vm.value < prev_value && get<0>(error_value) < 0) {
        error_value = make_tuple(vm.value, values_so_far -1);
      }
      prev_value = vm.value;
      sum_so_far += prev_value;
      if (values_so_far == num_values) {
        if (get<0>(error_value) >= 0) {
          cout << "ERROR: Value out of place: " << get<0>(error_value)
               << " at index " << get<1>(error_value) << endl;
        } else {
          cout << "Elements sum: " << sum_so_far << endl;
        }
        self->quit();
      }
    } 
  };
}

class bench : public benchmark {
public:
  void print_arg_info() const override {
    printf(benchmark_runner::arg_output_format(), "N (num values)",
           to_string(cfg_.n).c_str());
    printf(benchmark_runner::arg_output_format(), "M (max value)",
           to_string(cfg_.m).c_str());
    printf(benchmark_runner::arg_output_format(), "S (seed)",
           to_string(cfg_.s).c_str());
  }

  void initialize(message& args) override {
    std::ifstream ini{ini_file(args)};
    cfg_.parse(args, ini);
  }

  void run_iteration() override {
    actor_system system{cfg_};
    auto validation_actor = system.spawn(validation_actor_fun, cfg_.n);
    auto source_actor = system.spawn(int_source_actor_fun, cfg_.n, cfg_.m, cfg_.s);
    auto radix = cfg_.m / 2;
    auto next_actor = validation_actor;
    while (radix > 0) {
      auto local_radix = radix; 
      auto local_next_actor = next_actor;
      auto sort_actor = system.spawn(sort_actor_fun, cfg_.n, local_radix, local_next_actor);
      radix /= 2;
      next_actor = sort_actor;
    }
    anon_send(source_actor, next_actor_msg{next_actor});
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
