#include <iostream>
#include <vector>
#include <limits>
#include <fstream>

#include "benchmark_runner.hpp"
#include "pseudo_random.hpp"
  
using namespace std;
using std::chrono::seconds;
using namespace caf;

class config : public actor_system_config {
public:
  int a = 1000;
  int n = 50000;
  static double initial_balance;

  config() {
    opt_group{custom_options_, "global"}
      .add(a, "aaa,a", "number of accounts")
      .add(n, "nnn,n", "number of transactions");
  }

  void initalize() const {
    initial_balance = ((numeric_limits<double>::max() / (a * n)) / 1000) * 1000;
  }

};
double config::initial_balance = 0;

using start_msg_atom = atom_constant<atom("start")>;
using stop_msg_atom = atom_constant<atom("stop")>;
using reply_msg_atom = atom_constant<atom("reply")>;

struct debit_msg {
  actor sender;
  double amount;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(debit_msg);

struct credit_msg {
  actor sender;
  double amount;
  actor recipient;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(credit_msg);

#ifdef REQUEST_AWAIT
struct account_state {
  double balance;
};

behavior account(stateful_actor<account_state>* self, int /*id*/, double balance_) {
  auto& s = self->state;
  s.balance = balance_;
  return {
    [=](debit_msg& dm) {
      self->state.balance += dm.amount;
      return reply_msg_atom::value;
    },
    [=](credit_msg& cm) {
      auto& s = self->state;
      s.balance -= cm.amount;
      auto& dest_account = cm.recipient;
      auto sender = actor_cast<actor>(self->current_sender());
  #ifdef INFINITE
      self->request(dest_account, infinite,
                     debit_msg{actor_cast<actor>(self), cm.amount}).await(
  #elif HIGH_TIMEOUT
      self->request(dest_account, seconds(6000),
                     debit_msg{actor_cast<actor>(self), cm.amount}).await(
  #endif
        [=](reply_msg_atom) {
          self->send(sender, reply_msg_atom::value); 
        });
    },
    [=](stop_msg_atom) {
      self->quit(); 
    }
  };
}
#elif REQUEST_THEN
struct account_state {
  double balance;
};

behavior account(stateful_actor<account_state>* self, int /*id*/, double balance_) {
  auto& s = self->state;
  s.balance = balance_;
  return {
    [=](debit_msg& dm) {
      self->state.balance += dm.amount;
      return reply_msg_atom::value;
    },
    [=](credit_msg& cm) {
      auto& s = self->state;
      s.balance -= cm.amount;
      auto& dest_account = cm.recipient;
      auto sender = actor_cast<actor>(self->current_sender());
  #ifdef INFINITE
      self->request(dest_account, infinite,
                     debit_msg{actor_cast<actor>(self), cm.amount}).then(
  #elif HIGH_TIMEOUT
      self->request(dest_account, seconds(6000),
                     debit_msg{actor_cast<actor>(self), cm.amount}).then(
  #endif
        [=](reply_msg_atom) {
          self->send(sender, reply_msg_atom::value); 
        });
    },
    [=](stop_msg_atom) {
      self->quit(); 
    }
  };
}
#elif BECOME_UNBECOME_FAST
struct account_state {
  double balance;
  behavior wait_for_result;
  actor sender;
};

behavior account(stateful_actor<account_state>* self, int /*id*/, double balance_) {
  auto& s = self->state;
  self->set_default_handler(skip);
  s.balance = balance_;
  s.wait_for_result = {
    [=](reply_msg_atom) {
      auto& s = self->state;
      self->send(s.sender, reply_msg_atom::value); 
      self->unbecome();
    }
  };
  return {
    [=](debit_msg& dm) {
      self->state.balance += dm.amount;
      return reply_msg_atom::value;
    },
    [=](credit_msg& cm) {
      auto& s = self->state;
      s.balance -= cm.amount;
      auto& dest_account = cm.recipient;
      s.sender = actor_cast<actor>(self->current_sender());
      self->send(dest_account, debit_msg{actor_cast<actor>(self), cm.amount});
      self->become(keep_behavior, s.wait_for_result);
    },
    [=](stop_msg_atom) {
      self->quit(); 
    }
  };
}
#elif BECOME_UNBECOME_SLOW
struct account_state {
  double balance;
};

behavior account(stateful_actor<account_state>* self, int /*id*/, double balance_) {
  auto& s = self->state;
  self->set_default_handler(skip);
  s.balance = balance_;
  return {
    [=](debit_msg& dm) {
      self->state.balance += dm.amount;
      return reply_msg_atom::value;
    },
    [=](credit_msg& cm) {
      auto& s = self->state;
      s.balance -= cm.amount;
      auto& dest_account = cm.recipient;
      auto sender = actor_cast<actor>(self->current_sender());
      self->send(dest_account, debit_msg{actor_cast<actor>(self), cm.amount});
      self->become(keep_behavior, 
        [=](reply_msg_atom) {
          self->send(sender, reply_msg_atom::value); 
          self->unbecome();
        }
      );
    },
    [=](stop_msg_atom) {
      self->quit(); 
    }
  };
}
#endif

struct teller_state {
  vector<actor> accounts;
  int num_completed_banks;
  pseudo_random random; 
};

behavior teller(stateful_actor<teller_state>* self, int num_accounts, int num_bankings) {
  auto& s = self->state;
  s.accounts.reserve(num_accounts);
  for (int i = 0; i < num_accounts; ++i) {
    s.accounts.emplace_back(self->spawn(account, i, config::initial_balance)); 
  }
  s.num_completed_banks = 0;
  s.random.set_seed(123456);
  auto generate_work = [=]() {
    auto& s = self->state;
    //Warning s.accounts.size() musst be 10 or higher
    auto src_account_id = s.random.next_int((s.accounts.size() / 10) * 8);
    auto loop_id = s.random.next_int(s.accounts.size() - src_account_id);
    if (loop_id == 0) {
      ++loop_id; 
    }
    auto dest_account_id = src_account_id + loop_id;

    auto& src_account = s.accounts[src_account_id];
    auto& dest_account = s.accounts[dest_account_id];
    auto amount = abs(s.random.next_double()) * 1000;
    auto sender = actor_cast<actor>(self);
    auto cm = credit_msg{move(sender), amount, dest_account};
    self->send(src_account, move(cm));
  };
  return {
    [=](start_msg_atom) {
      for (int m = 0; m < num_bankings; ++m) {
        generate_work(); 
      } 
    },
    [=](reply_msg_atom) {
      auto& s = self->state;
      ++s.num_completed_banks;
      if (s.num_completed_banks == num_bankings) {
        for (auto& loop_account : s.accounts) {
          self->send(loop_account, stop_msg_atom::value); 
        } 
        self->quit();
      }
    }
  };
}

class bench : public benchmark {
public:
  void print_arg_info() const override {
    printf(benchmark_runner::arg_output_format(), "A (num accounts)",
           to_string(cfg_.a).c_str());
    printf(benchmark_runner::arg_output_format(), "N (num transactions)",
           to_string(cfg_.n).c_str());
    printf(benchmark_runner::arg_output_format(), "Initial Balance",
           to_string(cfg_.initial_balance).c_str());
  }

  void initialize(message& args) override {
    std::ifstream ini{ini_file(args)};
    cfg_.parse(args, ini);
  }

  void run_iteration() override {
    actor_system system{cfg_};
    cfg_.initalize();
    auto master = system.spawn(teller, cfg_.a, cfg_.n);
    anon_send(master, start_msg_atom::value);
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
