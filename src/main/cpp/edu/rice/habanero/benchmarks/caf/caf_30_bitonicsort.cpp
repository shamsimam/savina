#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include <fstream>

#include "benchmark_runner.hpp"
#include "pseudo_random.hpp"

using namespace std;
using std::chrono::seconds;
using namespace caf;

class config : public actor_system_config {
public:
  static int n;
  static long m;
  static long s;
  static bool debug;

  config() {
    opt_group{custom_options_, "global"}
      .add(n, "nnn,n", "data size (must be power of 2)")
      .add(m, "mmm,m", "max values")
      .add(s, "sss,s", "seed for random number generator")
      .add(debug, "ddd,d", "debug");
  }
};
int config::n = 4096;
long config::m = 1l << 60;
long config::s = 2048;
bool config::debug = false;

struct next_actor_msg {
  actor next_actor;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(next_actor_msg);

struct value_msg {
  long value;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(value_msg);

struct data_msg {
  int order_id;
  long value;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(data_msg);

using start_msg_atom = atom_constant<atom("start")>;
using exit_msg_atom = atom_constant<atom("exit")>;

// forward declatrions
behavior value_data_adapter_actor_fun(event_based_actor* self, int order_id,
                                      actor next_actor);
behavior data_value_adapter_actor_fun(event_based_actor* self,
                                      actor next_actor);
struct round_robin_splitter_actor_state;
behavior round_robin_splitter_actor_fun(
  stateful_actor<round_robin_splitter_actor_state>* self,
  const string& /*name*/, int length, vector<actor>&& receivers_);
struct round_robin_joiner_actor_state;
behavior round_robin_joiner_actor_fun(
  stateful_actor<round_robin_joiner_actor_state>* self, string /*name*/,
  int length, int num_joiners, actor next_actor);
behavior compare_exchange_actor_fun(event_based_actor* self, int order_id,
                                    bool sort_direction, actor next_actor);
behavior partition_bitonic_sequence_actor_fun(event_based_actor* self,
                                              int order_id, int length,
                                              bool sort_dir, actor next_actor);
behavior step_of_merge_actor_fun(event_based_actor* self, int order_id,
                                 int length, int num_seq_partitions,
                                 int direction_counter, actor next_actor);
behavior step_of_last_merge_actor_fun(event_based_actor* self, int length,
                                      int num_seq_partitions,
                                      bool sort_direction, actor next_actor);
behavior merge_stage_actor_fun(event_based_actor* self, int p, int n,
                               actor next_actor);
behavior last_merge_stage_actor_fun(event_based_actor* self, int n,
                                    bool sort_direction, actor next_actor);
behavior bitonic_sort_kernel_actor_fun(event_based_actor* self, int n,
                                       bool sort_direction, actor next_actor);
struct init_source_actor_state;
behavior int_source_actor_fun(stateful_actor<init_source_actor_state>* self,
                              int num_values, long max_value, long seed,
                              actor next_actor);
struct validation_actor_state;
behavior validation_actor_fun(stateful_actor<validation_actor_state>* self,
                              int num_values);

behavior value_data_adapter_actor_fun(event_based_actor* self, int order_id,
                                      actor next_actor) {
  return {
    [=](value_msg& vm) {
        self->send(next_actor, data_msg{order_id, vm.value});
      },
    [=](data_msg& dm) { 
      self->send(next_actor, dm);
    },
    [=](exit_msg_atom em) {
        self->send(next_actor, em);
        self->quit();
    }
  };
}

behavior data_value_adapter_actor_fun(event_based_actor* self,
                                      actor next_actor) {
  return {
    [=](value_msg& vm) { 
      self->send(next_actor, vm); 
    },
    [=](data_msg& dm) {
      self->send(next_actor, value_msg{dm.value}); 
    },
    [=](exit_msg_atom em) {
      self->send(next_actor, em);
      self->quit();
    }
  };
}

struct round_robin_splitter_actor_state {
  int receiver_index = 0;
  int current_run = 0;
  vector<actor> receivers;
};

behavior round_robin_splitter_actor_fun(
  stateful_actor<round_robin_splitter_actor_state>* self,
  const string& /*name*/, int length, vector<actor>&& receivers_) {
  auto& s = self->state;
  s.receivers = move(receivers_);
  return {
    [=](value_msg& vm) {
      auto& s = self->state;
      self->send(s.receivers[s.receiver_index], vm);
      s.current_run += 1;
      if (s.current_run == length) {
        s.receiver_index = (s.receiver_index + 1) % s.receivers.size();
        s.current_run = 0;
      }
    },
    [=](exit_msg_atom em) {
      auto& s = self->state;
      for (auto& loop_actor : s.receivers) {
        self->send(loop_actor, em);
      }
      self->quit();
    }
  };
}

struct round_robin_joiner_actor_state {
  vector<list<data_msg>> received_data;
  int forward_index = 0;
  int current_run = 0;
  int exits_received = 0;
};

behavior round_robin_joiner_actor_fun(
  stateful_actor<round_robin_joiner_actor_state>* self, string /*name*/,
  int length, int num_joiners, actor next_actor) {
  auto& s = self->state;
  s.received_data.reserve(num_joiners);
  for (int i = 0; i < num_joiners; ++i) {
    s.received_data.emplace_back();
  }
  auto try_forward_messages = [=](const data_msg& /*dm*/) {
    auto& s = self->state;
    while (!s.received_data[s.forward_index].empty()) {
      auto dm = s.received_data[s.forward_index].front();
      s.received_data[s.forward_index].pop_front();
      auto vm = value_msg{dm.value};
      self->send(next_actor, vm);
      s.current_run += 1;
      if (s.current_run == length) {
        s.forward_index = (s.forward_index + 1) % num_joiners;
        s.current_run = 0;
      }
    }
  };
  return {
    [=](data_msg& dm) {
      auto& s = self->state;
      s.received_data[dm.order_id].emplace_back(dm);
      try_forward_messages(dm);
    },
    [=](exit_msg_atom em) {
      auto& s = self->state;
      s.exits_received += 1;
      if (s.exits_received == num_joiners) {
        self->send(next_actor, em);
        self->quit();
      }
    }
  };
}

/**
 * Compares the two input keys and exchanges their order if they are not
 * sorted.
 *
 * sortDirection determines if the sort is nondecreasing (UP) [true] or
 * nonincreasing (DOWN) [false].
 */
behavior compare_exchange_actor_fun(event_based_actor* self, int order_id,
                                    bool sort_direction, actor next_actor) {
  long k1 = 0;
  bool value_available = false;
  return {
    [=](value_msg& vm) mutable {
      if (!value_available) {
        value_available = true;
        k1 = vm.value;
      } else {
        value_available = false;
        auto k2 = vm.value;
        auto min_k = (k1 <= k2) ? k1 : k2;
        auto max_k = (k1 <= k2) ? k2 : k1;
        if (sort_direction) {
          // UP sort
          self->send(next_actor, data_msg{order_id, min_k});
          self->send(next_actor, data_msg{order_id, max_k});
        } else {
          // DOWN sort
          self->send(next_actor, data_msg{order_id, max_k});
          self->send(next_actor, data_msg{order_id, min_k});
        }
      }
    },
    [=](exit_msg_atom em) {
      self->send(next_actor, em);
      self->quit();
    }
  };
}

/**
 * Partition the input bitonic sequence of length L into two bitonic sequences
 * of length L/2,
 * with all numbers in the first sequence <= all numbers in the second
 * sequence
 * if sortdir is UP (similar case for DOWN sortdir)
 *
 * Graphically, it is a bunch of CompareExchanges with same sortdir, clustered
 * together in the sort network at a particular step (of some merge stage).
 */
behavior partition_bitonic_sequence_actor_fun(event_based_actor* self,
                                              int order_id, int length,
                                              bool sort_dir, actor next_actor) {
  auto half_length = length / 2;
  auto forward_actor =
    self->spawn(value_data_adapter_actor_fun, order_id, next_actor);
  auto joiner_actor = self->spawn(round_robin_joiner_actor_fun,
                                  string("partition-") + to_string(order_id), 1,
                                  half_length, forward_actor);
  vector<actor> worker_actors;
  worker_actors.reserve(half_length);
  for (int i = 0; i < half_length; ++i) {
    worker_actors.emplace_back(
      self->spawn(compare_exchange_actor_fun, i, sort_dir, joiner_actor));
  }
  auto splitter_actor = self->spawn(round_robin_splitter_actor_fun,
                                    string("partition-") + to_string(order_id),
                                    1, move(worker_actors));
  return {
    [=](value_msg& vm) { 
      self->send(splitter_actor, vm); 
    },
    [=](exit_msg_atom em) {
      self->send(splitter_actor, em);
      self->quit();
    }
  };
}

/**
 * One step of a particular merge stage (used by all merge stages except the
 * last)
 *
 * directionCounter determines which step we are in the current merge stage
 * (which in turn is determined by <L, numSeqPartitions>)
 */
behavior step_of_merge_actor_fun(event_based_actor* self, int order_id,
                                 int length, int num_seq_partitions,
                                 int direction_counter, actor next_actor) {
  auto forward_actor = self->spawn(data_value_adapter_actor_fun, next_actor);
  auto joiner_actor = self->spawn(round_robin_joiner_actor_fun,
                                  string("step_of_merge-") + to_string(order_id)
                                    + ":" + to_string(length),
                                  length, num_seq_partitions, forward_actor);
  vector<actor> worker_actors;
  worker_actors.reserve(num_seq_partitions);
  for (int i = 0; i < num_seq_partitions; ++i) {
    // finding out the currentDirection is a bit tricky -
    // the direction depends only on the subsequence number during the FIRST
    // step. So to determine the FIRST step subsequence to which this sequence
    // belongs, divide this sequence's number j by directionCounter
    // (bcoz 'directionCounter' tells how many subsequences of the current
    // step
    // make up one subsequence of the FIRST step). Then, test if that result
    // is
    // even or odd to determine if currentDirection is UP or DOWN
    // respectively.
    auto current_direction = (i / direction_counter) % 2 == 0;
    // The last step needs special care to avoid split-joins with just one
    // branch.
    if (length > 2) {
      worker_actors.emplace_back(
        self->spawn(partition_bitonic_sequence_actor_fun, i, length,
                    current_direction, joiner_actor));
    } else {
      // PartitionBitonicSequence of the last step (L=2) is simply a
      // CompareExchange
      worker_actors.emplace_back(self->spawn(compare_exchange_actor_fun, i,
                                             current_direction, joiner_actor));
    }
  }
  auto splitter_actor = self->spawn(
    round_robin_splitter_actor_fun,
    string("step_of_merge-") + to_string(order_id) + ":" + to_string(length),
    length, move(worker_actors));
  return {
    [=](value_msg& vm) {
      self->send(splitter_actor, vm); 
    },
    [=](exit_msg_atom em) {
      self->send(splitter_actor, em);
      self->quit();
    }
  };
}

/**
 * One step of the last merge stage
 *
 * Main difference form StepOfMerge is the direction of sort.
 * It is always in the same direction - sortdir.
 */
behavior step_of_last_merge_actor_fun(event_based_actor* self, int length,
                                      int num_seq_partitions,
                                      bool sort_direction, actor next_actor) {
  auto joiner_actor =
    self->spawn(round_robin_joiner_actor_fun,
                string("step_of_last_merge-") + to_string(length), length,
                num_seq_partitions, next_actor);
  vector<actor> worker_actors;
  worker_actors.reserve(num_seq_partitions);
  for (int i = 0; i < num_seq_partitions; ++i) {
    // The last step needs special care to avoid split-joins with just one
    // branch.
    if (length > 2) {
      worker_actors.emplace_back(
        self->spawn(partition_bitonic_sequence_actor_fun, i, length,
                    sort_direction, joiner_actor));
    } else {
      // PartitionBitonicSequence of the last step (L=2) is simply a
      // CompareExchange
      worker_actors.emplace_back(self->spawn(compare_exchange_actor_fun, i,
                                             sort_direction, joiner_actor));
    }
  }
  auto splitter_actor =
    self->spawn(round_robin_splitter_actor_fun,
                string("step_of_last_merge-") + to_string(length), length,
                move(worker_actors));
  return {
    [=](value_msg& vm) {
      self->send(splitter_actor, vm); 
    },
    [=](exit_msg_atom em) {
      self->send(splitter_actor, em);
      self->quit();
    }
  };
}

behavior merge_stage_actor_fun(event_based_actor* self, int p, int n,
                               actor next_actor) {

  actor forward_actor;
  {
    actor loop_actor = next_actor;
    // for each of the lopP steps (except the last step) of this merge stage
    auto i = p / 2;
    while (i >= 1) {
      // length of each sequence for the current step - goes like P, P/2, ...,
      // 2.
      auto l = p / i;
      // numSeqPartitions is the number of PartitionBitonicSequence-rs in this
      // step
      auto num_seq_partitions = (n / p) * i;
      auto direction_counter = i;
      auto& local_loop_actor = loop_actor;
      auto temp_actor =
        self->spawn(step_of_merge_actor_fun, i, l, num_seq_partitions,
                    direction_counter, local_loop_actor);
      loop_actor = temp_actor;
      i /= 2;
    }
    forward_actor = loop_actor;
  }
  return {
    [=](value_msg& vm) {
      self->send(forward_actor, vm); 
    },
    [=](exit_msg_atom em) {
      self->send(forward_actor, em);
      self->quit();
    }
  };
}

behavior last_merge_stage_actor_fun(event_based_actor* self, int n,
                                    bool sort_direction, actor next_actor) {
  actor forward_actor;
  {
    auto loop_actor = next_actor;
    // for each of the lopN steps (except the last step) of this merge stage
    auto i = n / 2;
    while (i >= 1) {
      // length of each sequence for the current step - goes like N, N/2, ...,
      // 2.
      auto l = n / i;
      // numSeqPartitions is the number of PartitionBitonicSequence-rs in this
      // step
      auto num_seq_partitions = i;
      auto& local_loop_actor = loop_actor;
      auto temp_actor =
        self->spawn(step_of_last_merge_actor_fun, l, num_seq_partitions,
                    sort_direction, local_loop_actor);
      loop_actor = temp_actor;
      i /= 2;
    }
    forward_actor = loop_actor;
  }
  return {
    [=](value_msg& vm) {
      self->send(forward_actor, vm); 
    },
    [=](exit_msg_atom em) {
      self->send(forward_actor, em);
      self->quit();
    }
  };
}

behavior bitonic_sort_kernel_actor_fun(event_based_actor* self, int n,
                                       bool sort_direction, actor next_actor) {
  actor forward_actor;
  {
    auto loop_actor = next_actor;
    {
      auto local_loop_actor = loop_actor;
      auto temp_actor = self->spawn(last_merge_stage_actor_fun, n,
                                    sort_direction, local_loop_actor);
      loop_actor = temp_actor;
    }
    auto i = n / 2;
    while (i >= 2) {
      auto local_loop_actor = loop_actor;
      auto temp_actor =
        self->spawn(merge_stage_actor_fun, i, n, local_loop_actor);
      loop_actor = temp_actor;
      i /= 2;
    }
    forward_actor = loop_actor;
  }
  return {
    [=](value_msg& vm) {
      self->send(forward_actor, vm);
    },
    [=](exit_msg_atom em) {
      self->send(forward_actor, em);
      self->quit();
    }
  };
}

struct init_source_actor_state {
  pseudo_random random;
  ostringstream sb;
};

behavior int_source_actor_fun(stateful_actor<init_source_actor_state>* self,
                              int num_values, long max_value, long seed,
                              actor next_actor) {
  auto& s = self->state;
  s.random.set_seed(seed);
  return {
    [=](start_msg_atom) {
      auto& s = self->state;
      auto i = 0;
      while (i < num_values) {
        auto candidate = abs(s.random.next_long()) % max_value;
        if (config::debug) {
          s.sb << candidate << " ";
        }
        auto message = value_msg{candidate};
        self->send(next_actor, message);
        i += 1;
      }
      if (config::debug) {
        cout << "  SOURCE: " << s.sb.str() << endl;
      }
      self->send(next_actor, exit_msg_atom::value);
      self->quit();
    }
  };
}

struct validation_actor_state {
  double sum_so_far = 0.0;
  int values_so_far = 0;
  long prev_value = 0l;
  tuple<long, int> error_value = make_tuple(-1l, -1);
  ostringstream sb;
};

behavior validation_actor_fun(stateful_actor<validation_actor_state>* self,
                              int num_values) {
  return {
    [=](value_msg& vm) {
      auto& s = self->state;
      s.values_so_far += 1;
      if (config::debug) {
        s.sb << vm.value << " ";
      }
      if (vm.value < s.prev_value && get<0>(s.error_value) < 0) {
        s.error_value = make_tuple(vm.value, s.values_so_far - 1);
      }
      s.prev_value = vm.value;
      s.sum_so_far += s.prev_value;
    },
    [=](exit_msg_atom) {
      auto& s = self->state;
      if (s.values_so_far == num_values) {
        if (config::debug) {
          cout << "  OUTPUT: " << s.sb.str() << endl;
        }
        if (get<0>(s.error_value) >= 0) {
          cout << "  ERROR: value out of place: " << get<0>(s.error_value)
               << " at index " << get<1>(s.error_value) << endl;
        } else {
          cout <<"  Elements sum: " << s.sum_so_far << endl;
        }
      } else {
        cout << "  ERROR: early exit triggered, received only "
             << s.values_so_far << " values!" << endl;
      }
      self->quit();
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
    auto adapter_actor =
      system.spawn(data_value_adapter_actor_fun, validation_actor);
    auto kernel_actor =
      system.spawn(bitonic_sort_kernel_actor_fun, cfg_.n, true, adapter_actor);
    auto source_actor =
      system.spawn(int_source_actor_fun, cfg_.n, cfg_.m, cfg_.s, kernel_actor);
    anon_send(source_actor, start_msg_atom::value);
    system.await_all_actors_done();
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
