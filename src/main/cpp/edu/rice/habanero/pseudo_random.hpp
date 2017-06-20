#ifndef PSEUDO_RANDOM_HPP
#define PSEUDO_RANDOM_HPP

#include <vector>
#include <limits>
#include <random>

class pseudo_random {
public:
  pseudo_random(long seed)
      : value_(seed) {
    // nop
  }

  pseudo_random() = default;

  void set_seed(long seed) {
    value_ = seed;
  }

  int next_int() {
    return next_long();
  }

  int next_int(int exclusive_max) {
    return next_long() % exclusive_max;
  }

  long long next_long() {
    value_ = ((value_ * 1309) + 13849) & 65535;
    return value_;
  }

  double next_double() {
    return 1.0 / (next_long() +1);
  }

private:
  long value_ = 74755; 
};
#endif // PSEUDO_RANDOM_HPP
