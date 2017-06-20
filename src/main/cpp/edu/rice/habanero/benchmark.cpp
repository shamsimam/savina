#include "benchmark.hpp"

std::string benchmark::ini_file(caf::message& args) const {
  std::string config_file = "caf-application.ini";
  args.extract_opts({
    {"caf#config-file", "", config_file}
  });
  return config_file;
}

std::string benchmark::name() const {
  std::string file(current_file());
  size_t start = file.rfind("caf");
  size_t stop = file.rfind(".") - start;
  return file.substr(start, stop);
}

std::string benchmark::runtime_info() const {
#if defined(__clang__)
    return std::string("C++: Clang ") +  __VERSION__;
#elif defined(__GNUC__)
    return std::string("C++: Gcc ") +  __VERSION__;
#else
    return "C++: unkown compiler";
#endif
  }
