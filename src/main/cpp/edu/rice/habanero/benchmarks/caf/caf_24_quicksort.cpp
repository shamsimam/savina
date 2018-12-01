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
  static int n; // = 1000000;
  static long m; // 1L << 60;
  static long t; // = 2048;
  static long s; //= 1024;

  config() {
    opt_group{custom_options_, "global"}
      .add(n, "nnn,n", "data size")
      .add(m, "mmm,m", "max value")
      .add(t, "ttt,t", "threshold to perform sort sequentially")
      .add(s, "sss,s", "seed for random number generator");
  }
};
int config::n = 1000000;
long config::m = 1L << 60;
long config::t = 2048;
long config::s = 1024;

vector<long> filter_less_than(const vector<long>& data, long pivot) {
  int data_length = data.size();
  vector<long> result;
  result.reserve(data_length);
  copy_if(begin(data), end(data), back_inserter(result), 
    [pivot](long loop_item){
      return loop_item < pivot;
    });
  return result;
}

vector<long> filter_equals_to(const vector<long>& data, long pivot) {
  int data_length = data.size();
  vector<long> result;
  result.reserve(data_length);
  copy_if(begin(data), end(data), back_inserter(result), 
    [pivot](long loop_item){
      return loop_item == pivot;
    });
  return result;
}

vector<long> filter_between(const vector<long>& data, long left_pivot,
                            long right_pivot) {
  int data_length = data.size();
  vector<long> result;
  result.reserve(data_length);
  copy_if(begin(data), end(data), back_inserter(result), 
    [left_pivot, right_pivot](long loop_item){
      return (loop_item >= left_pivot) && (loop_item <= right_pivot);
    });
  return result;
}

vector<long> filter_greater_than(vector<long> data, long pivot) {
  int data_length = data.size();
  vector<long> result;
  result.reserve(data_length);
  copy_if(begin(data), end(data), back_inserter(result), 
    [pivot](long loop_item){
      return loop_item > pivot;
    });
  return result;
}

vector<long> quicksort_seq(const vector<long>& data) {
  size_t data_length = data.size();
  if (data_length < 2) {
      return data;
  }
  long pivot = data[(data_length / 2)];
  auto left_unsorted = filter_less_than(data, pivot);
  auto left_sorted = quicksort_seq(left_unsorted);
  auto equal_elements = filter_equals_to(data, pivot);
  auto right_unsorted = filter_greater_than(data, pivot);
  auto right_sorted = quicksort_seq(right_unsorted);
  vector<long> sorted_array; //= move(left_sorted);
  sorted_array.reserve(data_length);
  copy(begin(left_sorted), end(left_sorted), back_inserter(sorted_array));
  copy(begin(equal_elements), end(equal_elements), back_inserter(sorted_array));
  copy(begin(right_sorted), end(right_sorted), back_inserter(sorted_array));
  return sorted_array;
}

void check_sorted(const vector<long>& data) {
  int length = data.size();
  if (length != config::n) {
    cerr << "result is not correct length, expected: " << config::n
         << ", found: " << length << endl;
  }
  long loop_value = data[0];
  int next_index = 1;
  while (next_index < length) {
    long temp = data[next_index];
    if (temp < loop_value) {
      cerr << "result is not sorted, cur index: " << next_index
           << ", cur value: " << temp << ", prev value: " << loop_value << endl;
    }
    loop_value = temp;
    next_index += 1;
  }
}

vector<long> randomly_init_array() {
  vector<long> result;
  result.reserve(config::n);
  pseudo_random random(config::s);
  for (int i = 0; i < config::n; i++) {
    result.emplace_back(random.next_long() % config::m);
  }
  return result;
}

enum class position_enum { 
  right, left, initial
};

struct sort_msg {
  vector<long> data;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(sort_msg);

struct result_msg {
  vector<long> data;
  position_enum positon;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_msg);

struct quick_sort_actor_state {
  vector<long> result;
  int num_fragments;
};

behavior quick_sort_actor_fun(stateful_actor<quick_sort_actor_state>* self,
                              actor parent,
                              position_enum position_relative_to_parent) {
  self->state.num_fragments = 0;
  auto notify_parent_and_terminate = [=]() {
    auto& s = self->state;
    if (position_relative_to_parent == position_enum::initial) {
      check_sorted(s.result);
    } 
    if (parent) {
      self->send(parent,
                 result_msg{move(s.result), position_relative_to_parent});
    }
    self->quit();
  };
  return {
    [=](sort_msg& msg){
      auto& s = self->state;
      auto& data = msg.data;
      int data_length = data.size();
      if (data_length < config::t) {
        s.result = quicksort_seq(data);
        notify_parent_and_terminate();
      } else {
        auto data_length_half = data_length / 2;
        auto pivot = data[data_length_half];
        auto left_unsorted = filter_less_than(data, pivot);
        auto left_actor = self->spawn(
          quick_sort_actor_fun, actor_cast<actor>(self), position_enum::left);
        self->send(left_actor, sort_msg{move(left_unsorted)});
        auto right_unsorted = filter_greater_than(data, pivot);
        auto right_actor = self->spawn(
          quick_sort_actor_fun, actor_cast<actor>(self), position_enum::right);
        self->send(right_actor, sort_msg{move(right_unsorted)});

        s.result = filter_equals_to(data, pivot);
        ++s.num_fragments;
      }
    }, 
    [=](result_msg& msg){
      auto& data = msg.data;
      auto& position = msg.positon;
      auto& s = self->state;
      if (!data.empty()) {
        if (position == position_enum::left) {
          vector<long> temp; // = move(data);
          copy(begin(data), end(data), back_inserter(temp));
          copy(begin(s.result), end(s.result), back_inserter(temp));
          s.result = move(temp);
        } else if (position == position_enum::right) {
          vector<long> temp; // = move(s.result);
          copy(begin(s.result), end(s.result), back_inserter(temp));
          copy(begin(data), end(data), back_inserter(temp));
          s.result = move(temp);
        }
      }
      ++s.num_fragments;
      if (s.num_fragments == 3) {
        notify_parent_and_terminate();
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
    printf(benchmark_runner::arg_output_format(), "T (sequential cutoff)",
           to_string(cfg_.t).c_str());
    printf(benchmark_runner::arg_output_format(), "S (seed)",
           to_string(cfg_.s).c_str());
  }

  void initialize(message& args) override {
    std::ifstream ini{ini_file(args)};
    cfg_.parse(args, ini);
  }

  void run_iteration() override {
    actor_system system{cfg_};
    auto input = randomly_init_array();
    auto root_actor =
      system.spawn(quick_sort_actor_fun, actor(), position_enum::initial);
    anon_send(root_actor, sort_msg{move(input)});
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
