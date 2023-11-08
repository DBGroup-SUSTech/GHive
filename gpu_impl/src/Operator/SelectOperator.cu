#include "Operator/SelectOperator.hpp"

SelectOperator::SelectOperator(std::string op_name, vector<string> cols) {
  operator_name = op_name;
  this->output_cols = cols;
}


SelectPredicate *SelectOperator::generatePredicate(
    std::string expression, std::string expression_type) {
  // TODO: currently only support simple calculations: + - * /.
  std::regex calculation_regex("\\((.*) ([\\+-/\\*]) (.*)\\)");
  std::smatch match_result;
  SelectPredicate *ret = nullptr;
  std::cout << "GHive-CPP [SelectOperator-generatePredicate]: expression: " << expression << std::endl;
  if (std::regex_search(expression, match_result, calculation_regex)) {
    std::string sub_expression_1 = match_result[1];
    std::string expression_op = match_result[2];
    std::string sub_expression_2 = match_result[3];
    // TODO
    SelectPredicate *predicate_1 = generatePredicate(sub_expression_1, expression_type);
    SelectPredicate *predicate_2 = generatePredicate(sub_expression_2, expression_type);
    CalculationSelectPredicate *calculation_select_predicate = nullptr;
    if (expression_op == "+") {
      calculation_select_predicate =
          new CalculationSelectPredicate(predicate_1, predicate_2, ADDITION);
      calculation_select_predicate->setColType(expression_type);
    } else if (expression_op == "-") {
      calculation_select_predicate =
          new CalculationSelectPredicate(predicate_1, predicate_2, SUBTRACTION);
      calculation_select_predicate->setColType(expression_type);

    } else if (expression_op == "*") {
      calculation_select_predicate = new CalculationSelectPredicate(
          predicate_1, predicate_2, MULTIPLICATION);
      calculation_select_predicate->setColType(expression_type);

    } else if (expression_op == "/") {
      calculation_select_predicate =
          new CalculationSelectPredicate(predicate_1, predicate_2, DIVISION);
      calculation_select_predicate->setColType(expression_type);
    }
    ret = calculation_select_predicate;
  } else {
    expression = trim(expression);
    std::cout << "GHive-CPP [SelectOperator-generatePredicate]: trimmed expression "
              << expression << std::endl;
    if (std::regex_match(expression, match_result,
                         std::regex("KEY.*([0-9]+)"))) {
      int key_offset = std::stoi(match_result[1]);
      std::cout << "GHive-CPP [SelectOperator-generatePredicate]: key_offset = " << key_offset << std::endl;
      ret = new ColumnSwitchSelectPredicate(key_offset);
      ret->setColType(expression_type);
    }
    else if (std::regex_match(expression, match_result, std::regex("VALUE.*([0-9]+)"))) {
      int value_offset = std::stoi(match_result[1]);
      std::cout << "GHive-CPP [SelectOperator-generatePredicate]: value_offset = " << value_offset << std::endl;
      ret = new ColumnSwitchSelectPredicate(
          value_offset + children[0]->op_result->key_num);
      ret->setColType(expression_type);
    }
    else if (std::regex_match(expression, match_result, std::regex("([0-9]+)L"))) {
      uint64_t constant = stoll(match_result[1]);
      std::cout << "GHive-CPP [SelectOperator-generatePredicate]: long constant = " << constant << std::endl;
      ConstantSelectPredicate *predicate = new ConstantSelectPredicate();
      predicate->colType = LONG;
      predicate->longConstant = constant;
      ret = predicate;
    }
    else if (std::regex_match(expression, match_result, std::regex("([0-9\\.]+)D"))) {
      double constant = stod(match_result[1]);
      std::cout << "GHive-CPP [SelectOperator-generatePredicate]: double constant = " << constant << std::endl;
      ConstantSelectPredicate *predicate = new ConstantSelectPredicate();
      predicate->colType = DOUBLE;
      predicate->doubleConstant = constant;
      ret = predicate;
      //TODO: INT CONSTANT SHOULD BE ADDED !!
    }
    else if (std::regex_match(expression, match_result, std::regex(std::regex("\'(.+)\'")))) {
      std::cout << "FILE: " << __FILE__ << ", LINE: " << __LINE__ << std::endl;
      string constant =match_result[1];
      std::cout << "GHive-CPP [SelectOperator-generatePredicate]: string constant = " << constant << std::endl;
      ConstantSelectPredicate *predicate = new ConstantSelectPredicate();
      predicate->colType =STRING;
      predicate->stringConstant = constant;
      ret = predicate;
    }
    else if (std::regex_match(expression, match_result, std::regex(std::regex("concat\\((.+)\\)")))) {
      std::cout << "FILE: " << __FILE__ << ", LINE: " << __LINE__ << std::endl;
      std::cout << "GHive-CPP [SelectOperator-generatePredicate]: string concat predicate" << std::endl;
      ConcatSelectPredicate *predicate = new ConcatSelectPredicate();
      std::string all_cols = match_result[1];
      std::vector<std::string> cols;
      split(all_cols, cols, ",");
      for (std::string each_col:cols) {
        if (std::regex_match(expression, match_result,
                             std::regex("KEY.*([0-9]+)"))) {

        } else if (std::regex_match(expression, match_result,
                                    std::regex("VALUE.*([0-9]+)"))) {

        } else if (std::regex_match(expression, match_result, std::regex("([0-9]+)L"))) {
          expression_type = "bigint";
        } else if (std::regex_match(expression, match_result, std::regex("([0-9\\.]+)D"))) {
          expression_type = "double";
        } else if (std::regex_match(expression, match_result, std::regex(std::regex("\'(.+)\'")))) {
          expression_type = "string";
        } else {
          expression_type = "depend";
        }
        predicate->predicates.push_back(generatePredicate(each_col, expression_type));
      }
      predicate->colType = STRING;
      ret = predicate;
    }
    else {  // in form of _colxxx
      for (uint32_t i = 0; i < children[0]->output_cols.size(); i++) {
        if (expression == children[0]->output_cols[i]) {
          ret = new ColumnSwitchSelectPredicate(i);
          ret->setColType(expression_type);
          break;
        }
      }
    }
  }
  if (!ret) {
    std::cout << "GHive-CPP-ERROR [SelectOperator-generatePredicate]: no select predicate generated" << std::endl;
  }
  return ret;
}

void SelectOperator::parseExtended() {
  std::vector<std::string> lines;
  split(extended_info, lines, "\n");
  for (std::string line : lines) {
    std::smatch match_result;
    if (std::regex_search(line, match_result,
                          std::regex("expressions: (.*)"))) {
      std::string all_expressions = match_result[1];
      std::vector<std::string> raw_expressions;
      split(all_expressions, raw_expressions, ", ");
      for (int i = 0; i < raw_expressions.size(); i++) {
        std::string each_expression_trim = trim(raw_expressions[i]);
        if (each_expression_trim.find("concat") != string::npos) {
          while (true) {
            i++;
            each_expression_trim += ",";
            if (raw_expressions[i].find("type")) {
              break;
            }
            each_expression_trim += raw_expressions[i];
          }
          each_expression_trim += raw_expressions[i];
        }
        if (std::regex_search(each_expression_trim, match_result,
                              std::regex("(.*) \\(type: (.*)\\)"))) {
          std::string expression = match_result[1];
          std::string expression_type = match_result[2];
          cout << "GHive-CPP [SelectOperator-parseExtended]: " << expression << " " << expression_type << endl;
          predicates.push_back(generatePredicate(expression, expression_type));
        }
      }
    }
  }
}


void SelectOperator::execute() {
  std::cout << "GHive-CPP [SelectOperator-execute] inside operator_name: "
            << operator_name << std::endl;
  for (auto child : this->children) {
    child->execute();
  }
  profiler.start_op();
  long start = profiler_millis_timestamp();

  std::cout << "GHive-CPP [SelectOperator-execute]: " << operator_name << " starts to execute" << std::endl;
  std::cout << "GHive-CPP [SelectOperator-execute]: len(predicates) = " << predicates.size() << std::endl;
  for (SelectPredicate *predicate: predicates) {
    predicate->process(children[0]->op_result);
  }
  Table *tbl = new Table();
  for (SelectPredicate *predicate: predicates) {
    tbl->columns.push_back(predicate->column);
    tbl->types.push_back(predicate->colType);
  }
  tbl->row_num = children[0]->op_result->row_num;

  profiler.end_op();
  this->op_result = tbl;
  std::cout << "GHive-CPP [SelectOperator-execute]: result of " << this->operator_name << ": "
            << op_result;
  long end = profiler_millis_timestamp();
  std::cout << "Operator [" + operator_name + "] takes time: " << (end - start) << std::endl;
    std::cout << "Operator [" + operator_name + "-pcie_h2d" + "] takes time: " << profiler.total_pcie_h2d << std::endl;
    std::cout << "Operator [" + operator_name + "-exe" + "] takes time: " << profiler.gpu_exec_total << std::endl;
    std::cout << "Operator [" + operator_name + "-pcie_d2h" + "] takes time: " << profiler.total_pcie_d2h << std::endl;
    std::cout << "Operator [" + operator_name + "-recover" + "] takes time: " << profiler.data_recover_total << std::endl;
}

std::string SelectOperator::toString() {
  std::string output_cols_string = "output cols: " + output_cols[0];
  for (uint32_t i = 1; i < output_cols.size(); i++) {
    output_cols_string += ",";
    output_cols_string += output_cols[i];
  }
  return "[" + operator_name + "]; " + output_cols_string;
}

std::string SelectOperator::toString(int level) {
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