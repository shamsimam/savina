#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include <limits>
#include <fstream>

#include "benchmark_runner.hpp"
#include "pseudo_random.hpp"

using namespace std;
using std::chrono::seconds;
using namespace caf;

class config : public actor_system_config {
public:
  static int num_points;
  static double grid_size;
  static double f;
  static double alpha;
  static int cutoff_depth;
  static long seed;
  static int n;
  static int c;
  //static bool debug;

  config() {
    opt_group{custom_options_, "global"}
      .add(num_points, "nnn,n", "number of points") 
      .add(grid_size, "ggg,g", "grid size") 
      .add(alpha, "aaa,a", "alpha") 
      .add(seed, "sss,s", "seed") 
      .add(cutoff_depth, "ccc,c", "cutoff depth");
      //.add(debug, "ddd,d", "debug");
  }

  void initalize() const {
    f = sqrt(2) * grid_size; 
  }
};
int config::num_points = 100000;
double config::grid_size = 500.0;
double config::f = sqrt(2) * grid_size;
double config::alpha = 2.0;
int config::cutoff_depth = 3;
long config::seed = 123456l;
int config::n = 40000;
int config::c = 1;
//bool config::debug = false;

class point {
private:
  static pseudo_random r;

public:
  static void set_seed(long seed) {
    r = pseudo_random(seed);
  }

  static point random(double grid_size) {
    double x = r.next_double() * grid_size;
    double y = r.next_double() * grid_size;
    return point{x, y};
  }

  //template<class T> // collection<point>
  //static point find_center(T& points) {
    //double sum_x = 0; 
    //double sum_y = 0;
    //for (point p : points) {
      //sum_x += p.x;
      //sum_y += p.y;
    //}
    //int num_points = points.size();
    //double avg_x = sum_x / num_points;
    //double avg_y = sum_y / num_points;
    //return point{avg_x, avg_y};
  //}

  point(double x, double y)
      :  x(x)
      ,  y(y) {
    // nop
  }

  point() = default;
  point(const point&) = default;

  double get_distance(const point& p) const {
    double x_diff = p.x - x;
    double y_diff = p.y - y;
    double distance = sqrt((x_diff * x_diff) + (y_diff * y_diff));
    return distance;
  }

  string to_string() const {
    return string("(") + std::to_string(x) + "," + std::to_string(y) + ")";
  }

  double x;
  double y;
};
pseudo_random point::r;

using point_collection = vector<point>;

class box {
public:
  box(double x1, double y1, double x2, double y2)
      : x1(x1)
      , y1(y1)
      , x2(x2)
      , y2(y2) {
    // nop
  }

  bool contains(const point& p) const {
    return (x1 <= p.x && y1 <= p.y && p.x <= x2 && p.y <= y2);
  }

  point mid_point() const {
    return point((x1 + x2) / 2, (y1 + y2) / 2);
  }

  double x1;
  double y1;
  double x2;
  double y2;
};

enum class position : int {
  unknown = -2,
  root = -1,
  top_left = 0,
  top_right = 1,
  bot_left = 2,
  bot_right = 3
};

struct facility_msg {
  position position_relative_to_parent;
  int depth;  
  point point_value;
  bool from_child;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(facility_msg);

using next_customer_msg_atom = atom_constant<atom("next")>;

struct customer_msg {
  actor producer;
  point point_value;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(customer_msg);

using request_exit_msg_atom = atom_constant<atom("rexit")>;

struct confirm_exit_msg {
  int facilities;
  int support_customers;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(confirm_exit_msg);

struct producer_actor_state {
  int items_produced;
};

behavior producer_actor_fun(stateful_actor<producer_actor_state>* self,
                            actor consumer) {
  self->state.items_produced = 0;
  auto produce_customer = [=]() {
    self->send(consumer, customer_msg{actor_cast<actor>(self),
                                      point::random(config::grid_size)});
    ++self->state.items_produced;
  };
  // onPoststart()
  produce_customer();
  return {
    [=](next_customer_msg_atom) {
      if (self->state.items_produced < config::num_points) {
        produce_customer();
      } else {
        self->send(consumer, request_exit_msg_atom::value);
        self->quit();
      }
    }
  };
}

struct quadrant_actor_state {
  point facility;
  // all the local facilities from corner ancestors
  point_collection local_facilities;
  int known_facilities;
  int max_depth_of_known_open_facility;
  int terminated_child_count;
  // the support customers for this Quadrant
  point_collection support_customers;
  int children_facilities;
  int facility_customers;
  // null when closed, non-null when open
  vector<actor>children;
  vector<box> children_boundaries;
  // the cost so far
  double total_cost;
};

behavior quadrant_actor_fun(stateful_actor<quadrant_actor_state>* self,
                            actor parent, 
                            position position_relative_to_parent, 
                            box boundary, 
                            double threshold, 
                            int depth, 
                            const point_collection& init_local_facilities,
                            int init_known_facilities,
                            int init_max_depth_of_known_open_facility, 
                            const point_collection& init_customers) 
{
  auto& s = self->state;
  // the facility associated with this quadrant if it were to open
  s.facility = boundary.mid_point();
  // all the local facilities from corner ancestors
  s.local_facilities = init_local_facilities;
  s.local_facilities.emplace_back(s.facility);
  s.known_facilities = init_known_facilities;
  s.max_depth_of_known_open_facility = init_max_depth_of_known_open_facility;
  s.terminated_child_count = 0;
  // the support customers for this Quadrant
  s.children_facilities = 0;
  s.facility_customers = 0;
  s.total_cost = 0.0;
  // null when closed, non-null when open
  auto is_child_col_open = [=]() {
    return self->state.children.size() > 0;
  };
  //auto is_box_col_open = [=]() {
    //return self->state.children_boundaries.size() > 0;
  //};
  auto find_cost = [=](const point& point_value) {
    auto& s = self->state; 
    double result = numeric_limits<double>::max();
    // there will be at least one facility
    for (auto& loop_point : s.local_facilities) {
      auto distance = loop_point.get_distance(point_value);
      if (distance < result) {
        result = distance;
      }
    } 
    return result;
  };
  auto add_customer = [=](const point& point_value) {
    auto& s = self->state;
    s.support_customers.emplace_back(point_value);
    auto min_cost = find_cost(point_value);
    s.total_cost += min_cost;
  };
  auto notify_parent_of_facility = [=](const point& p, int depth) {
    if (parent) {
      self->send(parent,
                 facility_msg{position_relative_to_parent, depth, p, true});
    }
  };
  auto partition = [=]() {
    auto& s = self->state;
    // notify parent that opened a new facility
    notify_parent_of_facility(s.facility, depth);
    s.max_depth_of_known_open_facility =
      max(s.max_depth_of_known_open_facility, depth);
    // create children and propagate their share of customers to them
    box first_boundary{boundary.x1, s.facility.y, s.facility.x, boundary.y2};
    box second_boundary{s.facility.x, s.facility.y, boundary.x2, boundary.y2};
    box third_boundary{boundary.x1, boundary.y1, s.facility.x, s.facility.y};
    box fourth_boundary{s.facility.x, boundary.y1, boundary.x2, s.facility.y};
    auto my_self = actor_cast<actor>(self);
    auto child_generator = [&](position p, box& b) {
      return self->spawn(
        quadrant_actor_fun, my_self, p, b,
        threshold, depth + 1, s.local_facilities, s.known_facilities,
        s.max_depth_of_known_open_facility, s.support_customers);
    };
    auto first_child = child_generator(position::top_left, first_boundary);
    auto second_child = child_generator(position::top_right, second_boundary);
    auto third_child = child_generator(position::bot_left, third_boundary);
    auto fourth_child = child_generator(position::bot_right, fourth_boundary);
    s.children =
      vector<actor>{first_child, second_child, third_child, fourth_child};
    s.children_boundaries = vector<box>{first_boundary, second_boundary,
                                      third_boundary, fourth_boundary};
    // support customers have been distributed to the children
    s.support_customers.clear();
  };
  auto safely_exit = [=]() {
    auto& s = self->state;
    if (parent) {
      auto num_facilities = is_child_col_open() ?
                              s.children_facilities + 1 :
                              s.children_facilities;
      int num_customers = s.facility_customers + s.support_customers.size();
      self->send(parent, confirm_exit_msg{num_facilities, num_customers});
    } else {
      auto num_facilities = s.children_facilities + 1;
      cout << "  num facilities: " << num_facilities
           << ", num customers: " << s.facility_customers << endl;
    }
    self->quit();
  };
  for (auto& loop_point : init_customers) {
    if (boundary.contains(loop_point)) {
      add_customer(loop_point);
    }
  }
  return {
    [=](customer_msg& customer) {
      auto& s = self->state;
      auto& point_value = customer.point_value;
      if (!is_child_col_open()) {
        // no open facility
        add_customer(point_value);
        if (s.total_cost > threshold) {
          partition();
        }
      } else {
        // a facility is already open, propagate customer to correct child
        int index = 0;
        while(index <= 4) {
          auto& loop_child_boundary = s.children_boundaries[index];
          if (loop_child_boundary.contains(point_value)) {
            self->send(s.children[index], customer);
            index = 5;
          } else {
            ++index;
          }
        }
      }
      if (!parent) {
        // request next customer
        self->send(customer.producer, next_customer_msg_atom::value);
      }
    },
    [=](facility_msg& facility) {
      auto& s = self->state;
      auto& point_value = facility.point_value;
      auto from_child = facility.from_child;
      ++s.known_facilities;
      s.local_facilities.emplace_back(point_value);
      if (from_child) {
        notify_parent_of_facility(point_value, facility.depth);
        if (facility.depth > s.max_depth_of_known_open_facility) {
          s.max_depth_of_known_open_facility = facility.depth;
        }
        // notify sibling
        auto child_pos = facility.position_relative_to_parent;
        position sibling_pos;
        if (child_pos == position::top_left) {
          sibling_pos = position::bot_right;
        } else if (child_pos == position::top_right) {
          sibling_pos = position::bot_left;
        } else if (child_pos == position::bot_right) {
          sibling_pos = position::top_left;
        } else {
          sibling_pos = position::top_right;
        }
        self->send(s.children[static_cast<int>(sibling_pos)], facility_msg{position::unknown, depth, point_value, false});
      } else {
        // notify all children
        if (is_child_col_open()) {
          for (auto& loop_child : s.children) {
            self->send(loop_child, facility_msg{position::unknown, depth, point_value, false});
          }
        }
      }
    },
    [=](request_exit_msg_atom exit_msg) {
      auto& s = self->state;
      if (is_child_col_open()) {
        for (auto& loop_child : s.children) {
          self->send(loop_child, exit_msg);
        }
      } else {
        // No children, notify parent and safely exit
        safely_exit();
      }
    },
    [=](confirm_exit_msg& exit_msg) {
      auto& s = self->state;
      // child has sent a confirmation that it has exited
      ++s.terminated_child_count;
      s.children_facilities += exit_msg.facilities;
      s.facility_customers += exit_msg.support_customers;
      if (s.terminated_child_count == 4) {
        // all children terminated
        safely_exit();
      }
    }
  };
}

class bench : public benchmark {
public:
  void print_arg_info() const override {
    printf(benchmark_runner::arg_output_format(), "Num points",
           to_string(cfg_.num_points).c_str());
    printf(benchmark_runner::arg_output_format(), "Grid size",
           to_string(cfg_.grid_size).c_str());
    printf(benchmark_runner::arg_output_format(), "F",
           to_string(cfg_.f).c_str());
    printf(benchmark_runner::arg_output_format(), "Alpha",
           to_string(cfg_.alpha).c_str());
    printf(benchmark_runner::arg_output_format(), "Cut-off depth",
           to_string(cfg_.cutoff_depth).c_str());
    printf(benchmark_runner::arg_output_format(), "Seed",
           to_string(cfg_.seed).c_str());
  }

  void initialize(message& args) override {
    std::ifstream ini{ini_file(args)};
    cfg_.parse(args, ini);
  }

  void run_iteration() override {
    actor_system system{cfg_};
    cfg_.initalize();
    auto threshold = cfg_.alpha * cfg_.f;
    box bounding_box(0, 0, cfg_.grid_size, cfg_.grid_size);
    auto root_quadrant =
      system.spawn(quadrant_actor_fun, actor(), position::root, bounding_box,
                   threshold, 0, point_collection(), 1, -1, point_collection());
    auto producer = system.spawn(producer_actor_fun, root_quadrant);
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
