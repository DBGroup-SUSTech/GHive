#ifndef REDUCESINK_HPP
#define REDUCESINK_HPP
#include <map>
#include <regex>
#include "Util/Util.hpp"
#include "Operator/SelectPredicate.hpp"

#include "Operator/Operator.hpp"

/*
    is terminate: return data via JNI
    not terminate: small table parsed via JNI
*/


class SinkOperator : public Operator {
 public:
  vector<std::string> partition_cols;
  std::string vertex_name;
  std::vector<std::string> key_expressions;
  std::vector<std::string> original_output_cols;
  bool is_input;  // true: input; false: output
  static map<std::string, Table *> table_map;
  //std::vector<SelectPredicate> predicates;

  std::vector<uint32_t> key_offsets;
  std::string sort_orders;
  bool require_sort = false;  // only root op needs sort

  static map<std::string, Table *> create_table_map() {
    map<std::string, Table *> mmp;
    return mmp;
  }

  SinkOperator(std::string name, std::string vertex_name,
               vector<string> partitions);

  std::string toString() override;

  std::string toString(int level) override;

  void execute() override;

  void parseExtended() override;

};

#endif