#include <iostream>
#include <vector>
#include <fstream>

#include "benchmark_runner.hpp"

using namespace std;
using namespace caf;

constexpr long solutions[] = {
  1,
  0,
  0,
  2,
  10,     /* 5 */
  4,
  40,
  92,
  352,
  724,    /* 10 */
  2680,
  14200,
  73712,
  365596,
  2279184, /* 15 */
  14772512,
  95815104,
  666090624,
  4968057848L,
  39029188884L, /* 20 */
};
constexpr int max_solutions = sizeof(solutions) / sizeof(solutions[0]);

class config : public actor_system_config {
public:
  static int num_workers;
  static int size;
  static int threshold;
  static int priorities;
  static int solutions_limit;

  config() {
    opt_group{custom_options_, "global"}
      .add(size, "nnn,n", "size of the chess board")
      .add(num_workers, "www,w", "number of workers")
      .add(threshold, "ttt,t",
           "threshold for switching from parrallel processing to sequential "
           "processing")
      .add(solutions_limit, "sss,s", "solutions limit");
  }

  void initialize() const {
    size = max(1, min(size, max_solutions));
    threshold = max(1, min(threshold, max_solutions));
  }

};
int config::num_workers = 20;
int config::size = 12;
int config::threshold = 4;
int config::priorities = 10;
int config::solutions_limit = 1500000;

// a contains array of n queen positions. Returns 1
// if none of the queens conflict, and returns 0 otherwise.
bool board_valid(int n, const vector<int>& a) {
  for (int i = 0; i < n; i++) {
    int p = a[i];
    for (int j = (i + 1); j < n; j++) {
      int q = a[j];
      if (q == p || q == p - (j - i) || q == p + (j - i))
        return false;
    }
  }
  return true;
};

struct work_msg {
  int priority;
  vector<int> data;
  int depth;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(work_msg);

using done_msg_atom = atom_constant<atom("done")>;
using result_msg_atom = atom_constant<atom("result")>;
using stop_msg_atom = atom_constant<atom("stop")>;

// calc N-Queens problem sequential
// recusive function cannot be defined as lambda function
void nqueens_kernel_seq(event_based_actor* self, actor master, int size,
                        const vector<int>& a, int depth) {
  if (size == depth)
    self->send(master, result_msg_atom::value);
  else {
    for (int i = 0; i < size; ++i) {
      auto b = a;
      b.emplace_back(i);
      if (board_valid(depth + 1, b)) {
        nqueens_kernel_seq(self, master, size, b, depth + 1);
      }
    }
  }
};

behavior worker_fun(event_based_actor* self, actor master, int /*id*/) {
  auto size = config::size;
  auto threshold = config::threshold;
  // calc N-Queens problem in parallel
  auto nqueens_kernel_par = [=](work_msg&& msg) {
    auto& a = msg.data;
    auto& depth = msg.depth;
    if (size == depth)
      self->send(master, result_msg_atom::value);
    else if (depth >= threshold)
      nqueens_kernel_seq(self, master, size, a, depth);
    else {
      auto new_priority = msg.priority - 1;
      auto new_depth = depth + 1;
      for (int i = 0; i < size; ++i) {
        auto b = a;
        b.emplace_back(i);
        if (board_valid(new_depth, b)) {
          self->send(master, work_msg{new_priority, move(b), new_depth});
        }
      }
    }
  };
  return {
    [=](work_msg& msg) {
      nqueens_kernel_par(move(msg));
      self->send(master, done_msg_atom::value);
    },
    [=](stop_msg_atom) {
      self->send(master, stop_msg_atom::value);
      self->quit();
    }};
}

struct master {
  static long result_counter;
};
long master::result_counter = 0;


struct master_data {
  vector<actor> workers;
  int message_counter = 0;
  int num_worker_send = 0;
  int num_work_completed = 0;
  int num_workers_terminated = 0;
};

behavior master_fun(stateful_actor<master_data>* self, int num_workers,
                    int priorities) {
  auto send_work = [=](work_msg&& work_message) {
    auto& s = self->state;
    self->send(s.workers[self->state.message_counter], move(work_message));
    s.message_counter = (s.message_counter + 1) % num_workers;
    ++s.num_worker_send;
  };
  auto request_workers_to_terminate = [=]() {
    auto& s = self->state;
    for (auto& e : s.workers) {
      self->send(e, stop_msg_atom::value);
    }
  };
  auto& s = self->state;
  auto solutions_limit = config::solutions_limit;
  // onPostStart()
  s.workers.reserve(num_workers);
  for (int i = 0; i < num_workers; ++i) {
    self->state.workers.emplace_back(
      self->spawn(worker_fun, actor_cast<actor>(self), i));
  }
  vector<int> in_array;
  work_msg work_messge{priorities, move(in_array), 0};
  send_work(move(work_messge));
  return {
    [=](work_msg& work_message) { 
      send_work(move(work_message)); 
    },
    [=](result_msg_atom) {
      ++master::result_counter;
      if (master::result_counter == solutions_limit) {
        request_workers_to_terminate();
      }
    },
    [=](done_msg_atom) {
      auto& s = self->state;
      ++s.num_work_completed;
      if (s.num_work_completed == s.num_worker_send) {
        request_workers_to_terminate();
      }
    },
    [=](stop_msg_atom) {
      auto& s = self->state;
      ++s.num_workers_terminated;
      if (s.num_workers_terminated == num_workers) {
        self->quit();
      }
    }};
}

class bench : public benchmark {
public:
  void print_arg_info() const override {
    printf(benchmark_runner::arg_output_format(), "Num Workers",
           to_string(cfg_.num_workers).c_str());
    printf(benchmark_runner::arg_output_format(), "Size",
           to_string(cfg_.size).c_str());
    printf(benchmark_runner::arg_output_format(), "Max Solutions",
           to_string(cfg_.solutions_limit).c_str());
    printf(benchmark_runner::arg_output_format(), "Threshold",
           to_string(cfg_.threshold).c_str());
  }

  void initialize(message& args) override {
    std::ifstream ini{ini_file(args)};
    cfg_.parse(args, ini);
  }

  void run_iteration() override {
    actor_system system{cfg_};
    cfg_.initialize();
    int num_workers = cfg_.num_workers;
    int priorities = cfg_.priorities;
    vector<actor> master;
    master.emplace_back(system.spawn(master_fun, num_workers, priorities));
    system.await_all_actors_done();
    auto exp_solution = solutions[cfg_.size - 1];
    auto act_solution = master::result_counter;
    auto solutions_limit = cfg_.solutions_limit;
    auto valid = act_solution >= solutions_limit && act_solution <= exp_solution;
    cout << "Result valid: " << valid << endl;
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
