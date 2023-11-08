#ifndef OPERATORS_HPP
#define OPERATORS_HPP
#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>
#include <DataFlow/Table.hpp>
#include "Profile/Profiler.hpp"

using namespace std;
class Operator {
 public:
  Table *op_result = nullptr;

  std::string operator_name;
  std::vector<Operator*> children;
  Operator* parent; // parent may be useless.
  int padding;
  std::string extended_info;
  std::vector<std::string> output_cols;
  Profiler profiler;

  virtual std::string toString();

  virtual std::string toString(int level);

  virtual void profilerToString();

  virtual void execute_test();

  virtual void execute();

  virtual void parseExtended();

};

#endif
