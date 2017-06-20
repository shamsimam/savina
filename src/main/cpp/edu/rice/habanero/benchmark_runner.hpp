#ifndef BENCHMARK_RUNNER_HPP
#define BENCHMARK_RUNNER_HPP

#include <iostream>
#include <chrono>
#include <algorithm>
#include <cmath>

#include "caf/all.hpp"

#include "benchmark.hpp"

namespace {
// removes leading and trailing whitespaces
  std::string trim(std::string s) {
  auto not_space = [](char c) { return isspace(c) == 0; };
  // trim left
  s.erase(s.begin(), find_if(s.begin(), s.end(), not_space));
  // trim right
  s.erase(find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
  return s;
}
};

class benchmark_runner {
public:
  void run_benchmark(int argc, char** argv, benchmark&& benchmark) {
    using namespace caf;
    using namespace std;
    parse_args(argc, argv);
    benchmark.initialize(cfg_.args_remainder);
    if (cfg_.no_evaluation) {
      for (int i = 0; i < cfg_.iterations; i++) {
        benchmark.run_iteration();
        benchmark.cleanup_iteration(i + 1 == cfg_.iterations, 0);
      }
      return;
    }
    cout << "Runtime: " + benchmark.runtime_info() << endl;
    cout << "Benchmark: " + benchmark.name() << endl;
    cout << "Args: " << endl;
    benchmark.print_arg_info();
    cout << endl;
    printf(arg_output_format(), "C++ Version", "unkown");
    printf(arg_output_format(), "o/s Version", "unkown");
    printf(arg_output_format(), "o/s Name", "unkown");
    printf(arg_output_format(), "o/s Arch", "unkown");
    vector<double> raw_exec_times;
    raw_exec_times.reserve(cfg_.iterations);
    {
      cout << "Execution - Iterations: " << endl;
      for (int i = 0; i < cfg_.iterations; i++) {
        auto start_time = chrono::high_resolution_clock::now();
        benchmark.run_iteration();
        auto end_time = chrono::high_resolution_clock::now();
        double exec_time_millis =
          std::chrono::duration_cast<std::chrono::milliseconds>(end_time
                                                                - start_time)
            .count();
        raw_exec_times.emplace_back(exec_time_millis);
        printf(exec_time_output_format(), benchmark.name().c_str(),
               (string(" iteration-") + to_string(i)).c_str(),
               exec_time_millis);
        benchmark.cleanup_iteration(i + 1 == cfg_.iterations,
                                    exec_time_millis);
      }
      cout << endl;
    }
    cout << endl;

    sort(begin(raw_exec_times), end(raw_exec_times));
    auto exec_times = sanitize(raw_exec_times, tolerance);

    string tmp;
    auto  c_str = [&tmp](size_t e) {
      tmp = to_string(e);
      return tmp.c_str();
    };

    cout << "Execution - Summary: " << endl;
    printf(arg_output_format(), "Total executions", c_str(raw_exec_times.size()));
    printf(arg_output_format(), "Filtered executions", c_str(exec_times.size()));
    printf(exec_time_output_format(), benchmark.name().c_str(), " Best Time", exec_times[0]);
    printf(exec_time_output_format(), benchmark.name().c_str(), " Worst Time", exec_times[exec_times.size() - 1]);
    printf(exec_time_output_format(), benchmark.name().c_str(), " Median", median(exec_times));
    printf(exec_time_output_format(), benchmark.name().c_str(), " Arith. Mean Time", arithmetic_mean(exec_times));
    printf(exec_time_output_format(), benchmark.name().c_str(), " Geo. Mean Time", geometric_mean(exec_times));
    printf(exec_time_output_format(), benchmark.name().c_str(), " Harmonic Mean Time", harmonic_mean(exec_times));
    printf(exec_time_output_format(), benchmark.name().c_str(), " Std. Dev Time", standard_deviation(exec_times));
    printf(exec_time_output_format(), benchmark.name().c_str(), " Lower Confidence", confidence_low(exec_times));
    printf(exec_time_output_format(), benchmark.name().c_str(), " Higher Confidence", confidence_high(exec_times));
    printf(string(trim(exec_time_output_format()) + " (%4.3f percent) \n").c_str(), benchmark.name().c_str(), " Error Window",
           confidence_high(exec_times) - arithmetic_mean(exec_times), 
           100 * (confidence_high(exec_times) - arithmetic_mean(exec_times)) / arithmetic_mean(exec_times));
    printf(stat_data_output_format(), benchmark.name().c_str(), " Coeff. of Variation", coefficient_of_variation(exec_times));
    printf(stat_data_output_format(), benchmark.name().c_str(), " Skewness", skewness(exec_times));
    cout << endl; 
  }

  std::vector<double> sanitize(std::vector<double>& raw_list, double tolerance) {
    using namespace std;
    if (raw_list.empty()) {
      return vector<double>{};
    }
    sort(begin(raw_list), end(raw_list));
    int raw_list_size = raw_list.size();
    vector<double> result_list;
    double median = raw_list[raw_list_size / 2];
    double allowed_min = (1 - tolerance) * median;
    double allowed_max = (1 + tolerance) * median;
    for (auto& loop_val : raw_list) {
      if (loop_val >= allowed_min && loop_val <= allowed_max) {
        result_list.emplace_back(loop_val);
      }
    }
    return result_list;
  }

  double arithmetic_mean(const std::vector<double>& exec_times) {
    double sum = 0;
    for (auto& exec_time : exec_times) {
      sum += exec_time;
    }
    return (sum / exec_times.size());
    }

    double median(const std::vector<double>& exec_times) {
      if (exec_times.empty()) {
        return 0;
      }
      int size = exec_times.size();
      int middle = size / 2;
      if (size % 2 == 1) {
        return exec_times[middle];
      } else {
        return (exec_times[middle - 1] + exec_times[middle]) / 2.0;
      }
    }

    double geometric_mean(const std::vector<double>& exec_times) {
      double lg_prod = 0;
      for (auto& exec_time : exec_times) {
        lg_prod += log10(exec_time);
      }
      return pow(10, lg_prod / exec_times.size());
    }

    double harmonic_mean(const std::vector<double>& exec_times) {
      double denom = 0;
      for (auto& exec_time : exec_times) {
        denom += (1 / exec_time);
      }
      return (exec_times.size() / denom);
    }

    double standard_deviation(const std::vector<double>& exec_times) {
      auto mean = arithmetic_mean(exec_times);
      double temp = 0;
      for (auto& exec_time : exec_times) {
        temp += ((mean - exec_time) * (mean - exec_time));
      }
      return sqrt(temp / exec_times.size());
    }

    double coefficient_of_variation(const std::vector<double>& exec_times) {
      auto mean = arithmetic_mean(exec_times);
      auto sd = standard_deviation(exec_times);
      return (sd / mean);
    }

    double confidence_low(const std::vector<double>& exec_times) {
      auto mean = arithmetic_mean(exec_times);
      auto sd = standard_deviation(exec_times);
      return mean - (1.96 * sd / sqrt(exec_times.size()));
    }

    double confidence_high(const std::vector<double>& exec_times) {
      auto mean = arithmetic_mean(exec_times);
      auto sd = standard_deviation(exec_times);
      return mean + (1.96 * sd / sqrt(exec_times.size()));
    }

    double skewness(const std::vector<double>& exec_times) {
      auto mean = arithmetic_mean(exec_times);
      auto sd = standard_deviation(exec_times);
      double sum = 0.0;
      int count = 0;
      if (exec_times.size() > 1) {
        for (auto& exec_time : exec_times) {
          auto current = exec_time;
          auto diff = current - mean;
          sum = sum + diff * diff * diff;
          count++;
        }
        return sum / ((count - 1) * sd * sd * sd);
      } else {
        return 0.0;
      }
    }

    static const char* stat_data_output_format() {
      return  "%20s %20s: %9.3f \n";
    }

    static const char* exec_time_output_format() {
      return "%20s %20s: %9.3f ms \n";
    }

    static const char* arg_output_format() {
      return "%25s = %-10s \n";
    }
private:
  struct config : public caf::actor_system_config {
  public:
    int iterations = 12;
    bool no_evaluation = false;

    config() {
      opt_group{custom_options_, "global"}
      .add(iterations, "iterations", "benchmark iterations")
      .add(no_evaluation, "no_evaluation", "no evaluation of consumed time");
    }
  };

  void parse_args(int argc, char** argv) {
    using namespace caf;
    cfg_.parse(argc, argv);
  }

  double tolerance = 0.20;
  config cfg_;
};

#endif // BENCHMARK_RUNNER_HPP
