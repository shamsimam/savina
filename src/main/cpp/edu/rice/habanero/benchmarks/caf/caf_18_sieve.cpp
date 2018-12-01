#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include <fstream>

#include "benchmark_runner.hpp"

using namespace std;
using std::chrono::seconds;
using namespace caf;

class config : public actor_system_config {
public:
  int n = 100000;
  int m = 1000;
  static bool debug; // = false;

  config() {
    opt_group{custom_options_, "global"}
      .add(n, "nnn,n", "search limit for primes") 
      .add(m, "mmm,m", "maximum primes storage capacity per actor")
      .add(debug, "ddd,d", "debug");
  }
};
bool config::debug = false;

struct long_box {
  long value;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(long_box);

bool is_locally_prime(long candidate, const vector<long>& local_primes,
                      int start_inc, int end_exc) {
  for (int i = start_inc; i < end_exc; ++i) {
    long remainder = candidate % local_primes[i];
    if (remainder == 0) {
      return false; 
    }
  }
  return true;
}

behavior number_producer_actor(event_based_actor* self, long limit) {
  return {
    [=](actor filter_actor) {
      long candidate = 3;
      while (candidate < limit) {
        self->send(filter_actor, long_box{candidate});
        candidate += 2;
      }
      self->send(filter_actor, string("EXIT"));
      self->quit();
    } 
  };
}

struct prime_filter_actor_state {
  actor next_filter_actor;
  vector<long> local_primes;
  int available_local_primes; 
};

behavior prime_filter_actor_fun(stateful_actor<prime_filter_actor_state>* self,
                                int id, long my_initial_prime,
                                int num_max_local_primes) {
  auto& s = self->state;
  s.local_primes.reserve(num_max_local_primes);
  for (int i = 0; i < num_max_local_primes; ++i) {
    s.local_primes.emplace_back(0); 
  }
  s.available_local_primes = 1;
  s.local_primes[0] = my_initial_prime;
  auto handle_new_prime = [=](long new_prime) mutable {
    auto& s = self->state;
    if (config::debug) {
      aout(self) << "Found new prime number " << new_prime << endl;
    } 
    if (s.available_local_primes < num_max_local_primes) {
      // Store locally if there is space
      s.local_primes[s.available_local_primes] = new_prime;
      ++s.available_local_primes;
    } else {
      // Create a new actor to store the new prime
      s.next_filter_actor = self->spawn(prime_filter_actor_fun, id + 1,
                                        new_prime, num_max_local_primes);
    }
  };
  return {
    [=](long_box& candidate) mutable {
      auto& s = self->state;
      auto locally_prime = is_locally_prime(candidate.value, s.local_primes, 0,
                                            s.available_local_primes);
      if (locally_prime) {
        if (s.next_filter_actor) {
          // Pass along the chain to detect for 'primeness'
          self->send(s.next_filter_actor, move(candidate)); 
        } else {
          // Found a new prime! 
          handle_new_prime(candidate.value);
        }
      }
    },
    [=](string& x) {
      auto& s = self->state;
      if (!s.next_filter_actor)  {
        // Signal next actor to terminate
        self->send(s.next_filter_actor, move(x));
      } else {
        auto total_primes =
          ((id - 1) * num_max_local_primes) + s.available_local_primes;
        aout(self) << "Total primes = " << total_primes << endl;
      }
      if (config::debug) {
        aout(self) << "Terminating prime actor for number " << my_initial_prime
                   << endl;
        ;
      }
      self->quit();
    }
  };
}

class bench : public benchmark {
public:
  void print_arg_info() const override {
    printf(benchmark_runner::arg_output_format(), "N (input size)",
           to_string(cfg_.n).c_str());
    printf(benchmark_runner::arg_output_format(), "M (M)",
           to_string(cfg_.m).c_str());
    printf(benchmark_runner::arg_output_format(), "d (debug)",
           to_string(cfg_.debug).c_str());
  }

  void initialize(message& args) override {
    std::ifstream ini{ini_file(args)};
    cfg_.parse(args, ini);
  }

  void run_iteration() override {
    actor_system system{cfg_};
    auto producer_actor = system.spawn(number_producer_actor, cfg_.n);
    auto filter_actor = system.spawn(prime_filter_actor_fun, 1, 2, cfg_.m);
    anon_send(producer_actor, filter_actor);
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
