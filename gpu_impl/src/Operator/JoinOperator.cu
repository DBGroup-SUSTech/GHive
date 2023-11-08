#include <cstdint>
#include <regex>
#include <Operator/BaseJoinPredicate.hpp>

#include "Operator/JoinOperator.hpp"
#include "Operator/JoinPredicate.hpp"
#include "Operator/SinkOperator.hpp"
#include "Util/Util.hpp"
#include "Profile/Profiler.hpp"

map<std::string, std::map<int, std::vector<std::string>>>
    JoinOperator::maintain_cols_map = JoinOperator::create_maintain_cols_map();

JoinOperator::JoinOperator(std::string operator_name, std::string conds,
                           std::vector<std::string> outputs) {
  this->operator_name = operator_name;
  this->conds = conds;
  this->output_cols = outputs;
}

void JoinOperator::parseExtended() {

  std::vector<std::string> children_names;

  // Split conds, get the index for each operator.
  std::cout << "GHive-CPP [JoinOperator-parseExtended]: conds: " << conds << std::endl;
  std::regex split_re("[\\.=]");
  std::vector<std::string> cond_splits(std::sregex_token_iterator(conds.begin(), conds.end(), split_re, -1),
                                       std::sregex_token_iterator());
  split(conds, cond_splits, ".");
  std::cout << "GHive-CPP [JoinOperator-parseExtended]: cond_splits: " << vector_to_string<std::string>(cond_splits)
            << std::endl;
  for (std::string cond_split: cond_splits) {
    if (std::regex_match(cond_split, std::regex("[A-Z]+_[0-9]+"))) {
      if (find(children_names.begin(), children_names.end(), cond_split) != children_names.end()) {
        break;
      }
      children_names.push_back(cond_split);
    }
  }

  uint32_t join_tbl_num = children_names.size();
  std::cout << "GHive-CPP [JoinOperator-parseExtended]: join table number: "
            << join_tbl_num << std::endl;
  std::cout << "GHive-CPP [JoinOperator-parseExtended]: children_names: "
            << vector_to_string<std::string>(children_names) << std::endl;

  for (std::string child_name: children_names) {
    for (Operator *op: children) {
      if (op->operator_name == child_name) {
        join_ops.push_back(op);
      }
    }
  }
  std::cout << "GHive-CPP [JoinOperator-parseExtended]: join_ops: ";
  for (Operator *op: join_ops) {
    std::cout << op->operator_name << " ";
  }
  std::cout << std::endl;

  std::vector<std::vector<uint32_t>> maintain_cols_index(join_tbl_num);

  std::vector<std::pair<BaseJoinType, std::pair<uint32_t, uint32_t>>> join_types;

  std::vector<std::vector<uint32_t>> join_cols(join_tbl_num);

  std::vector<std::string> lines;
  split(extended_info, lines, "\n");
  std::smatch match_result;
  bool get_keys = false; // keys between line "keys:" and "outputColumnName:"
  for (std::string line: lines) {
    std::cout << "line: " << line << std::endl;
    if (std::regex_search(line, match_result, std::regex("outputColumnNames: (.*)"))){
      std::vector<std::string> outputs;

      split(match_result[1], outputs, ", ");
      std::cout << "match_result[1] = " << match_result[1] << std::endl
                << "outputs = " << vector_to_string(outputs) << std::endl;
      this->output_cols = outputs;
    } else if (std::regex_search(line, match_result, std::regex("Left Semi Join ([0-9]+) to ([0-9]+)"))) {
      uint32_t tbl_left = stoi(match_result[1]);
      uint32_t tbl_right = stoi(match_result[2]);
      join_types.push_back({LEFT_SEMI_JOIN, {tbl_left, tbl_right}});
    } else if (std::regex_search(line, match_result, std::regex("Inner Join ([0-9]+) to ([0-9]+)"))) {
      uint32_t tbl_left = stoi(match_result[1]);
      uint32_t tbl_right = stoi(match_result[2]);
      join_types.push_back({INNER_JOIN, {tbl_left, tbl_right}});
    } else if (std::regex_search(line, match_result, std::regex("Left Outer Join ([0-9]+) to ([0-9]+)"))) {
      uint32_t tbl_left = stoi(match_result[1]);
      uint32_t tbl_right = stoi(match_result[2]);
      join_types.push_back({LEFT_OUTER_JOIN, {tbl_left, tbl_right}});
    }
    if (line.find("keys:") != line.npos) {
      get_keys = true;
    }
    if (line.find("outputColumnNames:") != line.npos) {
      break;
    }
    if (get_keys) {
      if (std::regex_search(line, match_result, std::regex("([0-9]+) (.+)"))) {
        uint32_t tbl_idx = stoi(match_result[1]);
        Operator *op = join_ops[tbl_idx];
        std::vector<std::string> cols;
        split(match_result[2], cols, ", ");
        for (std::string col: cols) {
          if (std::regex_search(col, match_result, std::regex("(.*) \\(type: .*\\)"))) {
            std::string col_name = match_result[1];
            for (uint32_t i = 0; i < op->output_cols.size(); i++) {
              if (col_name == op->output_cols[i]) {
                join_cols[tbl_idx].push_back(i);
              }
            }
          }
        }
      } else if (std::regex_search(line, match_result, std::regex("([0-9]+)"))) {
        uint32_t tbl_idx = stoi(match_result[1]);
        join_cols[tbl_idx] = {};
      }
    }
  }
  std::cout << "GHive-CPP [JoinOperator-parseExtended]: Finish getting keys and output columns" << std::endl;
  std::map<int, std::vector<string>> maintain_cols_str_map = maintain_cols_map[this->operator_name];
  for (auto pair: maintain_cols_str_map) {
    Operator *op = join_ops[pair.first];
    std::cout << "GHive-CPP [JoinOperator-parseExtended]: maintain pair: " << pair.first << "; "
              << vector_to_string<std::string>(pair.second) << std::endl;
    if (operator_name.find("MERGEJOIN") != operator_name.npos) {
      for (std::string maintain_col: pair.second) {
        if (regex_match(maintain_col, match_result, std::regex("VALUE\\._col([0-9]+)"))) {
          int idx = stoi(match_result[1]) + 1;
          std::string col_name = "_col" + std::to_string(idx);
          std::cout << "GHive-CPP [JoinOperator-parseExtended]: maintain col_name: " << col_name << std::endl;
          SinkOperator *sink_op = (SinkOperator *) op;
          for (uint32_t i = 0; i < sink_op->original_output_cols.size(); i++) {
            if (sink_op->original_output_cols[i] == col_name) {
              maintain_cols_index[pair.first].push_back(i);
              std::cout << "GHive-CPP [JoinOperator-parseExtended]: maintain col "
                        << pair.first << " push back: " << i << std::endl;
            }
          }
        }
      }
    } else {
      for (std::string maintain_col: pair.second) {
        for (int i = 0; i < op->output_cols.size(); i++) {
          if (maintain_col == op->output_cols[i]) {
            maintain_cols_index[pair.first].push_back(i);
            std::cout << "GHive-CPP [JoinOperator-parseExtended]: maintian col "
                      << pair.first << " push back: " << i << std::endl;
          }
        }
      }
    }
  }
  for (uint32_t i = 0; i < join_cols.size(); i++) {
    std::cout << "GHive-CPP [JoinOperator-parseExtended]: join_cols[" << i << "] = "
              << vector_to_string<uint32_t>(join_cols[i]);
  }
  for (uint32_t i = 0; i < join_types.size(); i++) {
    std::cout << "GHive-CPP [JoinOperator-parseExtended]: join_types[" << i << "] = <"
              << join_types[i].first << ", <" << join_types[i].second.first << ", "
              << join_types[i].second.second << ">>" << std::endl;
  }
  predicate.join_cols = join_cols;
  predicate.maintain_cols = maintain_cols_index;
  predicate.join_types = join_types;
}
void JoinOperator::execute() {
  std::cout << "GHive-CPP [JoinOperator-execute]: inside operator_name: "
            << operator_name << std::endl;
  if (children.size() != 0) {
    for (auto child: this->children) {
      child->execute();
    }
  }
  std::cout << "GHive-CPP [JoinOperator-execute]: " << operator_name
            << " starts to execute at time: " << profiler_timestamp() << std::endl;

  std::cout << "GHive-CPP [JoinOperator-execute]: join_ops.size(): "
            << join_ops.size() << std::endl;
  std::cout << "Operator [" + operator_name + "] starts at: " << profiler_millis_timestamp() << std::endl;

  std::vector<Table *> input_tables;
  for (Operator *child: join_ops) {
    input_tables.push_back(child->op_result);
  }
  long start = profiler_millis_timestamp();
  op_result = predicate.process(input_tables);
  std::cout << predicate.profiler.toString() << std::endl;

  std::cout << "Operator [" + operator_name + "] ends at: " << profiler_millis_timestamp() << std::endl;
  long end = profiler_millis_timestamp();
  std::cout << "Operator [" + operator_name + "] takes time: " << (end - start) << std::endl;
  std::cout << "Operator [" + operator_name + "-pcie_h2d" + "] takes time: " << predicate.profiler.total_pcie_h2d << std::endl;
  std::cout << "Operator [" + operator_name + "-exe" + "] takes time: " << predicate.profiler.gpu_exec_total << std::endl;
  std::cout << "Operator [" + operator_name + "-pcie_d2h" + "] takes time: " << predicate.profiler.total_pcie_d2h << std::endl;
  std::cout << "Operator [" + operator_name + "-recover" + "] takes time: " << predicate.profiler.data_recover_total << std::endl;
  std::cout << "Operator [" + operator_name + "-merge" + "] takes time: " << predicate.profiler.merge_total << std::endl;
  std::cout << "GHive-CPP [JoinOperator-execute] " << operator_name << " ends to execute, with result: "
            << op_result->toString() << std::endl;

}

std::string JoinOperator::toString() {
  return "[" + this->operator_name + "]; " + "conds: " + conds + "; " +
      vector_to_string(output_cols, "output_cols");
}

std::string JoinOperator::toString(int level) {
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
