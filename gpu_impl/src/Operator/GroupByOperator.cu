#include <iostream>
#include <vector>
#include "Operator/GroupByOperator.hpp"
#include "Util/Util.hpp"

GroupByOperator::GroupByOperator(std::string name,
                                 std::vector<std::string> cols,
                                 std::vector<std::string> aggs,
                                 std::vector<std::string> keys) {
  this->operator_name = name;
  this->output_cols = cols;
  this->aggs = aggs;
  this->keys = keys;
}

void GroupByOperator::parseExtended() {
  std::vector<uint32_t> offset_keys;
  std::smatch match_result;
  for (std::string str_key: keys) {
    if (std::regex_match(children[0]->operator_name, std::regex("RS_[0-9]+")) ||
        std::regex_match(children[0]->operator_name, std::regex("Union [0-9]+"))
        ) {
      if (std::regex_match(str_key, match_result,
                           std::regex("KEY.*([0-9]+)"))) {
        int key_offset = std::stoi(match_result[1]);
        offset_keys.push_back(key_offset);
      } else if (std::regex_match(str_key, match_result,
                                  std::regex("VALUE.*([0-9]+)"))) {
        int value_offset = std::stoi(match_result[1]);
        offset_keys.push_back(value_offset +
            keys.size());
      }
    } else {
      for (uint32_t i = 0; i < children[0]->output_cols.size(); i++) {
        if (str_key == children[0]->output_cols[i]) {
          offset_keys.push_back(i);
          break;
        }
      }
    }
  }

  GroupByPredicate group_by_desc;
  group_by_desc.keys = offset_keys;
  this->predicate = group_by_desc;

  for (std::string agg_str: aggs) {
    std::cout << "GHive-CPP [GroupByOperator-parseExtended]: aggstr: aggs: = " << agg_str << std::endl;
    if (std::regex_match(agg_str, match_result, std::regex("(.*)\\((.*)\\)"))) {
      std::string agg_type_str = match_result[1];
      std::string agg_col = match_result[2];
      std::cout << "GHive-CPP [GroupByOperator-parseExtended]: agg_str=" << agg_str
                << " agg_type_str=" << agg_type_str
                << " agg_col=" << agg_col << std::endl;
      AggregationType agg_type = UNKNOWN;
      uint32_t aggregation_col = 0xffffffff;

      // TODO: More aggregation types to be added.
      // TODO: literal details should be justified.
      if (agg_type_str == "sum") {
        agg_type = SUM;
      } else if (agg_type_str == "max") {
        agg_type = MAX;
      } else if (agg_type_str == "min") {
        agg_type = MIN;
      } else if (agg_type_str == "avg") {
        agg_type = AVG;
      } else if (agg_type_str == "count") {
        agg_type = CNT;
      }

      if (std::regex_match(agg_col, match_result,
                           std::regex("KEY.*([0-9]+)"))) {
        int key_offset = std::stoi(match_result[1]);

        aggregation_col = key_offset;
      } else if (std::regex_match(agg_col, match_result,
                                  std::regex("VALUE.*([0-9]+)"))) {
        int value_offset = std::stoi(match_result[1]);
        std::cout << "GHive-CPP [GroupByOperator-parseExtended]: value_offset: " << value_offset << endl;
        aggregation_col = value_offset + children[0]->op_result->key_num;
      } else {
        for (uint32_t i = 0; i < children[0]->output_cols.size(); i++) {
          std::cout << "GHive-CPP [GroupByOperator-parseExtended]: children[0]'s output_col[i]="
                    << children[0]->output_cols[i] << std::endl;
          if (agg_col == children[0]->output_cols[i]) {
            aggregation_col = i;
            break;
          }
        }
      }
      if (aggregation_col == 0xffffffff) { // empty key
        aggregation_col = 0; // default: column 0
      }
      AggregationDesc aggregation_desc(agg_type, aggregation_col);
      this->predicate.aggregation_descs.push_back(aggregation_desc);
    }
  }
}

void GroupByOperator::execute() {
  std::cout << "GHive-CPP [GroupByOperator-execute]: inside operator_name: "
            << operator_name << std::endl;
  if (children.size() != 0) {
    for (auto child: this->children) {
      child->execute();
    }
  }
  long start = profiler_millis_timestamp();
  profiler.start_op();
  std::cout << "Operator [" + operator_name + "] starts at: " << profiler_millis_timestamp() << std::endl;
  assert(this->children.size() == 1);
  Table *input_tbl = children[0]->op_result;
  std::cout << "GHive-CPP [GroupByOperator-execute]: " << operator_name << " starts to execute with input "
            << input_tbl->toString() << std::endl;

  op_result = predicate.gpu_execute(input_tbl, profiler);

  std::cout << "GHive-CPP [GroupByOperator-execute] " << operator_name << " ends to execute, with result: "
            << op_result->toString() << std::endl;
  std::cout << "Operator [" + operator_name + "] ends at: " << profiler_millis_timestamp() << std::endl;
  long end = profiler_millis_timestamp();
  std::cout << "Operator [" + operator_name + "] takes time: " << (end - start) << std::endl;
  std::cout << "Operator [" + operator_name + "-pcie_h2d" + "] takes time: " << profiler.total_pcie_h2d << std::endl;
  std::cout << "Operator [" + operator_name + "-exe" + "] takes time: " << profiler.gpu_exec_total << std::endl;
  std::cout << "Operator [" + operator_name + "-pcie_d2h" + "] takes time: " << profiler.total_pcie_d2h << std::endl;
  std::cout << "Operator [" + operator_name + "-recover" + "] takes time: " << profiler.data_recover_total << std::endl;
  profiler.end_op();
  std::cout << profiler.toString() << std::endl;
}

std::string GroupByOperator::toString() {
  return "[" + operator_name + "]" + "; " +
      "aggs: " + vector_to_string(aggs, "") +
      "keys: " + vector_to_string(keys, "") +
      "output_cols: " + vector_to_string(output_cols, "");
}

std::string GroupByOperator::toString(int level) {
  std::string ret = "";
  for (int i = 0; i < level; i++) {
    ret += "  ";
  }
  ret += this->toString() + "\n";
  for (auto child: children) {
    ret += child->toString(level + 1);
  }
  return ret;
}

