#include "Operator/FilterOperator.cuh"
#include "Operator/FilterOperator.hpp"
#include <Util/Util.hpp>

void FilterOperator::parsePredicates() {
  std::cout << this->predicatesLiteral << std::endl;
}

FilterOperator::FilterOperator(std::string op_name, std::string predicates) {
  operator_name = op_name;
  predicatesLiteral = predicates;
  this->parsePredicates();
}

FilterOperator::FilterOperator() {

};

std::string FilterOperator::toString() {
  return "[" + operator_name + "]; predicate: " + predicatesLiteral;
}

std::string FilterOperator::toString(int level) {
  std::string ret = "";
  for (uint32_t i = 0; i < level; i++) {
    ret += " ";
  }
  ret += this->toString() + "\n";
  for (auto child: children) {
    ret += child->toString(level + 1);
  }
  return ret;
}


Table *FilterOperator::clean_data(thrust::device_vector<int32_t>& bitmap, Table *tbl, FilterProfiler& profiler) {
  profiler.start_gpu_exec();
  int32_t cnt = thrust::reduce(bitmap.begin(), bitmap.end());
  profiler.end_gpu_exec();
  profiler.start_gpu_alloc();
  thrust::device_vector<int32_t> offset(cnt);
  profiler.end_gpu_alloc();
  thrust::counting_iterator<int32_t> iter(0);
  profiler.start_gpu_exec();
  profiler.start_copy_if();
  thrust::copy_if(iter, iter + bitmap.size(), bitmap.begin(), offset.begin(), filter_unary_eq<int32_t>(1));
  profiler.end_copy_if();
  profiler.end_gpu_exec();
  profiler.start_pci_device2host();
  thrust::host_vector<int32_t> host_offset = offset;
  profiler.end_pci_device2host();
  profiler.start_data_recover();
  for (Column *column: tbl->columns) {
    switch (column->type) {
      case LONG: {
        long *long_col = (long *) column->data_ptr;
        for (uint32_t row = 0; row < cnt; row++) {
          long_col[row] = long_col[host_offset[row]];
        }
        break;
      }
      case DOUBLE: {
        double *double_col = (double *) column->data_ptr;
        for (uint32_t row = 0; row < cnt; row++) {
          double_col[row] = double_col[host_offset[row]];
        }
        break;
      }
      case INT: {
        int32_t *int_col = (int32_t *) column->data_ptr;
        for (uint32_t row = 0; row < cnt; row++) {
          int_col[row] = int_col[host_offset[row]];
        }
        break;
      }
      case STRING: {
        int32_t *str_idx_col = (int32_t *) column->data_ptr_aux;
        for (uint32_t row = 0; row < cnt; row++) {
          str_idx_col[2 * row] = str_idx_col[2 * host_offset[row]];
          str_idx_col[2 * row + 1] = str_idx_col[2 * host_offset[row] + 1];
        }
        break;
      }
      case DEPEND: {
        break;
      }
    }
    column->row_num = cnt;
  }
  tbl->row_num = cnt;
  profiler.end_data_recover();
  return tbl;
}

Table *FilterOperator::clean_data(Table *tbl) {
  for (Column *column: tbl->columns) {
    uint32_t idx = 0;
    switch (column->type) {
      case LONG: {
        long *long_col = (long *) column->data_ptr;
        for (uint32_t row = 0; row < tbl->row_num; row++) {
          if (h_result[row]) {
            long_col[idx++] = long_col[row];
          }
        }
        break;
      }
      case DOUBLE: {
        double *double_col = (double *) column->data_ptr;
        for (uint32_t row = 0; row < tbl->row_num; row++) {
          if (h_result[row]) {
            double_col[idx++] = double_col[row];
          }
        }
        break;
      }
      case INT: {
        int32_t *int_col = (int32_t *) column->data_ptr;
        for (uint32_t row = 0; row < tbl->row_num; row++) {
          if (h_result[row]) {
            int_col[idx++] = int_col[row];
          }
        }
        break;
      }
      case STRING: {
        int32_t *str_idx_col = (int32_t *) column->data_ptr_aux;
        for (uint32_t row = 0; row < tbl->row_num; row++) {
          if (h_result[row]) {
            str_idx_col[2 * idx] = str_idx_col[2 * row];
            str_idx_col[2 * idx + 1] = str_idx_col[2 * row + 1];
            idx++;
          }
        }
        break;
      }
      case DEPEND: {
        break;
      }
    }
    column->row_num = idx;
  }
  tbl->row_num = tbl->columns[0]->row_num;
  return tbl;
}

Table *FilterOperator::clean_data(Table *tbl, thrust::host_vector<int32_t> &host_result) {
  for (Column *column: tbl->columns) {
    uint32_t idx = 0;
    switch (column->type) {
      case LONG: {
        long *long_col = (long *) column->data_ptr;
        for (uint32_t row = 0; row < tbl->row_num; row++) {
          if (host_result[row]) {
            long_col[idx++] = long_col[row];
          }
        }
        break;
      }
      case DOUBLE: {
        double *double_col = (double *) column->data_ptr;
        for (uint32_t row = 0; row < tbl->row_num; row++) {
          if (host_result[row]) {
            double_col[idx++] = double_col[row];
          }
        }
        break;
      }
      case INT: {
        int32_t *int_col = (int32_t *) column->data_ptr;
        for (uint32_t row = 0; row < tbl->row_num; row++) {
          if (host_result[row]) {
            int_col[idx++] = int_col[row];
          }
        }
        break;
      }
      case STRING: {
        int32_t *str_idx_col = (int32_t *) column->data_ptr_aux;
        for (uint32_t row = 0; row < tbl->row_num; row++) {
          if (host_result[row]) {
            str_idx_col[2 * idx] = str_idx_col[2 * row];
            str_idx_col[2 * idx + 1] = str_idx_col[2 * row + 1];
            idx++;
          }
        }
        break;
      }
      case DEPEND: {
        break;
      }
    }
    column->row_num = idx;
  }
  tbl->row_num = tbl->columns[0]->row_num;
  return tbl;
}

void FilterOperator::execute() {
  for (auto child: this->children) {
    child->execute();
  }
//  std::cout << "GHive-CPP [FilterOperator-process]: " << this->operator_name << " starts to execute, with input" <<
//            std::endl << children[0]->op_result->toString(500) << std::endl;

  long start = profiler_millis_timestamp();
  std::cout << "Operator [" + operator_name + "] starts at: " << profiler_millis_timestamp() << std::endl;
  filter_predicate->process(children[0]->op_result, profiler);
//  h_result = filter_predicate->result_bitmap;
//  for (uint32_t i = 0; /*i < 100 & */i < h_result.size(); i++) {
//    std::cout << h_result[i] << " ";
//  }
//  std::cout << std::endl;
//  op_result = clean_data(children[0]->op_result);
  op_result = clean_data(filter_predicate->result_bitmap, children[0]->op_result, profiler);
  std::cout << "Operator [" + operator_name + "] ends at: " << profiler_millis_timestamp() << std::endl;
  long end = profiler_millis_timestamp();
  std::cout << "Operator [" + operator_name + "] takes time: " << (end - start) << std::endl;
    std::cout << "Operator [" + operator_name + "-pcie_h2d" + "] takes time: " << profiler.total_pcie_h2d << std::endl;
    std::cout << "Operator [" + operator_name + "-exe" + "] takes time: " << profiler.gpu_exec_total << std::endl;
    std::cout << "Operator [" + operator_name + "-pcie_d2h" + "] takes time: " << profiler.total_pcie_d2h << std::endl;
    std::cout << "Operator [" + operator_name + "-recover" + "] takes time: " << profiler.data_recover_total << std::endl;
//  std::cout << "GHive-CPP [FilterOperator-process]: " << this->operator_name << " ends to execute, with result" <<
//            std::endl << op_result->toString(500);
}

void FilterOperator::parsePredicate(FilterPredicate *fil_predicate, std::string match_col_name, int param_num,
                                    std::string match_value_1, std::string match_value_2 = "") {
  std::smatch match_result;
  fil_predicate->paramNum = param_num;
  for (uint32_t i = 0; i < this->output_cols.size(); i++) {
    if (this->output_cols[i] == match_col_name) {
      fil_predicate->filterCol[0] = i;
      break;
    }
    if (std::regex_match(match_value_1, match_result,
                         std::regex("([0-9\\.]+)D"))) {
      double double_val = std::stod(match_result[1]);
      fil_predicate->dataType = 1;
      fil_predicate->doubleFilterParams[0] = double_val;
    } else if (std::regex_match(match_value_1, match_result,
                                std::regex("([0-9\\.]+)L"))) {
      int64_t long_val = std::stoll(match_result[1]);
      fil_predicate->dataType = 0;
      fil_predicate->longFilterParams[0] = long_val;
    } else if (std::regex_match(match_value_1, match_result,
                                std::regex("([0-9]+)"))) {
      int64_t int_val = std::stol(match_result[1]);
      fil_predicate->dataType = 2;
      fil_predicate->intFilterParams[0] = int_val;
    } else if (std::regex_match(match_value_1, match_result, std::regex("\'(.*)\'"))) {
      fil_predicate->dataType = 3;
      std::string match_result_str = match_result[1];
      fil_predicate->stringFilterParams[0] = match_result_str;
    } else {
      std::cout << "GHive-CPP-ERROR [FilterOperator-parsePredicate]: number regex not covered in file " << __FILE__
                << " line " << __LINE__ << std::endl;
    }
    if (param_num == 2) {
      int second_date_type = -1;
      if (std::regex_match(match_value_2, match_result,
                           std::regex("([0-9\\.]+)D"))) {
        double double_val = std::stod(match_result[1]);
        second_date_type = 1;
        fil_predicate->doubleFilterParams[1] = double_val;
      } else if (std::regex_match(match_value_2, match_result,
                                  std::regex("([0-9\\.]+)L"))) {
        int64_t long_val = std::stoll(match_result[1]);
        second_date_type = 0;
        fil_predicate->longFilterParams[1] = long_val;
      } else if (std::regex_match(match_value_2, match_result,
                                  std::regex("([0-9]+)"))) {
        int64_t int_val = std::stol(match_result[1]);
        second_date_type = 2;
        fil_predicate->intFilterParams[1] = int_val;
      } else if (std::regex_match(match_value_2, match_result, std::regex("\'(.*)\'"))) {
        second_date_type = 3;
        std::string match_result_str = match_result[1];
        fil_predicate->stringFilterParams[1] = match_result_str;
      } else {
        std::cout << "GHive-CPP-ERROR [FilterOperator-parsePredicate]: number regex not covered in file " << __FILE__
                  << " line " << __LINE__ << std::endl;
      }
      if (fil_predicate->dataType != second_date_type) {
        std::cout << "GHive-CPP-ERROR [FilterOperator-parsePredicate]: data type not matched " << __FILE__
                  << " line " << __LINE__ << std::endl;
      }
    }
  }
}

FilterPredicate *FilterOperator::generateBasicPredicate(std::string pred_str) {
  std::cout << "GHive-CPP: [FilterOperator-generateBasicPredicate]: " << pred_str << std::endl;
  FilterPredicate *fil_predicate = new FilterPredicate();
  std::smatch match_result;
  if (std::regex_match(pred_str, match_result, std::regex("(.*) = (.*)"))) {
    std::cout << "FILE: " << __FILE__ << " LINE: " << __LINE__ << std::endl;
    PredicateMode predicate_mode = FILTER_EQ;
    std::string filter_col_name = match_result[1];
    std::string filter_val = match_result[2];
    fil_predicate->mode = predicate_mode;
    parsePredicate(fil_predicate, filter_col_name, 1, filter_val);
  } else if (std::regex_match(pred_str, match_result,
                              std::regex("(.*) <= (.*)"))) {
    PredicateMode predicate_mode = FILTER_LE;
    std::string filter_col_name = match_result[1];
    std::string filter_val = match_result[2];

    fil_predicate->mode = predicate_mode;
    parsePredicate(fil_predicate, filter_col_name, 1, filter_val);

  } else if (std::regex_match(pred_str, match_result,
                              std::regex("(.*) < (.*)"))) {
    PredicateMode predicate_mode = FILTER_LT;
    std::string filter_col_name = match_result[1];
    std::string filter_val = match_result[2];

    fil_predicate->mode = predicate_mode;
    parsePredicate(fil_predicate, filter_col_name, 1, filter_val);
  } else if (std::regex_match(pred_str, match_result,
                              std::regex("(.*) >= (.*)"))) {
    PredicateMode predicate_mode = FILTER_GE;
    std::string filter_col_name = match_result[1];
    std::string filter_val = match_result[2];

    fil_predicate->mode = predicate_mode;
    parsePredicate(fil_predicate, filter_col_name, 1, filter_val);

  } else if (std::regex_match(pred_str, match_result,
                              std::regex("(.*) > (.*)"))) {
    PredicateMode predicate_mode = FILTER_GT;
    std::string filter_col_name = match_result[1];
    std::string filter_val = match_result[2];
    fil_predicate->mode = predicate_mode;
    parsePredicate(fil_predicate, filter_col_name, 1, filter_val);
  } else if (std::regex_match(pred_str, match_result,
                              std::regex("(.*) <> (.*)"))) {
    PredicateMode predicate_mode = FILTER_NOT_EQ;
    std::string filter_col_name = match_result[1];
    std::string filter_val = match_result[2];

    fil_predicate->mode = predicate_mode;
    parsePredicate(fil_predicate, filter_col_name, 1, filter_val);

  } else if (std::regex_match(pred_str, match_result,
                              std::regex("(.*) BETWEEN (.*) AND (.*)"))) {
    PredicateMode predicate_mode = FILTER_EQ_RANGE;
    std::string filter_col_name = match_result[1];
    std::string filter_val_1 = match_result[2];
    std::string filter_val_2 = match_result[3];
    fil_predicate->mode = predicate_mode;
    parsePredicate(fil_predicate, filter_col_name, 2, filter_val_1, filter_val_2);

  } else if (std::regex_match(pred_str, match_result,
                              std::regex("(.*) NOT BETWEEN (.*) AND (.*)"))) {
    PredicateMode predicate_mode = FILTER_NOT_RANGE;
    std::string filter_col_name = match_result[1];
    std::string filter_val_1 = match_result[2];
    std::string filter_val_2 = match_result[3];

    fil_predicate->mode = predicate_mode;
    parsePredicate(fil_predicate, filter_col_name, 2, filter_val_1, filter_val_2);

  } else if (std::regex_match(pred_str, match_result,
                              std::regex("(.*) is not null"))) {
    PredicateMode predicate_mode = FILTER_NOT_NULL;
    std::string filter_col_name = match_result[1];
    fil_predicate->mode = predicate_mode;
    fil_predicate->paramNum = 0;
    fil_predicate->predColNum = 1;
    fil_predicate->dataType = 4;
    for (uint32_t i = 0; i < this->output_cols.size(); i++) {
      if (this->output_cols[i] == filter_col_name) {
        fil_predicate->filterCol[0] = i;
        break;
      }
    }
  } else {
    std::cout << "GHive-CPP-ERROR [FilterOperator-generateBasicPredicate]: "
                 "no matched regex when generating basic predicate." << std::endl;
  }
  return fil_predicate;
}

FilterPredicate *FilterOperator::generatePredicate() {
  std::vector<std::string> string_stack;
  std::stack<FilterPredicate *> predicate_stack;
  for (uint32_t i = 0; i < predicatesLiteral.size(); i++) {
    char c = predicatesLiteral[i];
    if (c != ' ') {
      if (string_stack.empty() || string_stack.back() == "(") {
        string_stack.push_back(std::string(1, c));
      } else if (c == ')') {
        while (string_stack.back() != "(") {
          std::string pred_str = "";
          while (string_stack.back() != "(" && string_stack.back() != "and" && string_stack.back() != "or") {
            if (pred_str == "") {
              pred_str = string_stack.back();
              string_stack.pop_back();
            } else {
              pred_str = string_stack.back() + " " + pred_str;
              string_stack.pop_back();
            }
          }
          if (pred_str != "") {
            predicate_stack.push(generateBasicPredicate(pred_str));
            std::cout << predicate_stack.top()->toString();
          }

          if (string_stack.back() == "and") {
            FilterPredicate *filter_and_pred = new FilterPredicate();
            filter_and_pred->mode = FILTER_AND;
            filter_and_pred->childrenPredicate[0] = predicate_stack.top();
            predicate_stack.pop();
            filter_and_pred->childrenPredicate[1] = predicate_stack.top();
            filter_and_pred->predColNum = 0;
            filter_and_pred->paramNum = 0;
            filter_and_pred->dataType = 4;
            filter_and_pred->filterCol[0] = 0;
            predicate_stack.pop();
            predicate_stack.push(filter_and_pred);
            string_stack.pop_back();
          } else if (string_stack.back() == "or") {
            FilterPredicate *filter_or_pred = new FilterPredicate();
            filter_or_pred->mode = FILTER_OR;
            filter_or_pred->childrenPredicate[0] = predicate_stack.top();
            predicate_stack.pop();
            filter_or_pred->childrenPredicate[1] = predicate_stack.top();
            filter_or_pred->predColNum = 0;
            filter_or_pred->paramNum = 0;
            filter_or_pred->dataType = 4;
            filter_or_pred->filterCol[0] = 0;
            predicate_stack.pop();
            predicate_stack.push(filter_or_pred);
            string_stack.pop_back();
          }
        }
        if (string_stack.back() == "(") {
          string_stack.pop_back();
        } else {
          std::cout << "ERROR: string stack error in file " << __FILE__
                    << " line " << __LINE__ << std::endl;
        }

      } else {
        string_stack.back() += c;
      }
    } else {  // c == ' '
      std::string stack_top = string_stack.back();
      if (stack_top == "and" || stack_top == "or") {
        std::string pred_str = "";
        string_stack.pop_back();
        while (string_stack.back() != "(" && string_stack.back() != "and" &&
            string_stack.back() != "or") {
          if (pred_str == "") {
            pred_str = string_stack.back();
            string_stack.pop_back();
          } else {
            pred_str = string_stack.back() + " " + pred_str;
            string_stack.pop_back();
          }
        }
        // parse pred_str && add add predicates to predicate_stack
        if (pred_str != "") {
          predicate_stack.push(generateBasicPredicate(pred_str));
          std::cout << predicate_stack.top()->toString();
        }
        string_stack.push_back(stack_top);
      }
      std::string new_string;
      string_stack.push_back(new_string);
    }
  }
  if (predicate_stack.size() == 0) {
    std::string pred_str = string_stack[0];
    for (int i = 1; i < string_stack.size(); i++) {
      pred_str += " " + string_stack[i];
    }
    predicate_stack.push(generateBasicPredicate(pred_str));
  }
  assert(predicate_stack.size() == 1);
  return predicate_stack.top();
}

void FilterOperator::parseExtended() {
  this->filter_predicate = generatePredicate();
}
