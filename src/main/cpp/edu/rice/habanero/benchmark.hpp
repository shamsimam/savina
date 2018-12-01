#ifndef BENCHMARK_HPP
#define BENCHMARK_HPP

#include <string>

#include "caf/all.hpp"

class benchmark {
public:
  std::string ini_file(caf::message& args) const;

  std::string name() const;

  std::string runtime_info() const;

  virtual void print_arg_info() const = 0;

  virtual void initialize(caf::message& args) = 0;

  virtual void run_iteration() = 0;

  virtual void cleanup_iteration(bool /*last_iteration*/,
                                 double /*exec_time_millis*/){
    // nop
  };

protected:
  virtual const char* current_file() const = 0;
};

#endif // BENCHMARK_HPP
