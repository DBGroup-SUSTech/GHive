#include <Operator/PTFOperator.hpp>
#include <Operator/GroupByPredicate.hpp>

void PTFOperator::parseExtended() {
  std::cout << "parse: " << operator_name << std::endl;
  std::vector<std::string> extended_plan_lines;
  split(this->extended_info, extended_plan_lines, "\n");

  std::regex col_order_by("order by: (*)");
  std::regex col_partition_by("partition by: (*)");
  std::regex col_expression0("(.*) ASC NULL (.*)");
  std::regex col_expression1("(.*) DESC NULL (.*)");
  std::regex ASC("ASC");
  std::regex functions_begin("window functions:");
  std::regex window_function_definition("window function definition");
  std::regex function_alias("alias: (.*)");
  std::regex function_argument("arguments: (.*)");
  std::regex function_type("name: (.*)");
  std::regex output_col_name("(_col[0-9]+)");
  std::regex output_shape("output shape: (.*)");

  std::smatch result;
  auto *predicates = new PTFPredicate();
  std::vector<uint32_t> order_by_col;
  std::vector<SortOrder> asc;
  std::vector<bool> null_first;
  std::vector<uint32_t> partition_by_col;
  std::vector<WindowFunction> window_functions;

  int line_index = 0;
  for (; line_index < extended_plan_lines.size(); line_index++) {
    std::string line = extended_plan_lines[line_index];
    std::cout << line << std::endl;
    if (std::regex_search(line,result,output_shape)){
      std::vector<std::string> token;
      split_unquote(result[1], token , ",");
      for (auto & i : token) {
        regex_search(i,result,output_shape);
        this->output_cols.push_back(result[1]);
      }
    }

    if (regex_search(line,result,col_order_by)) {
      std::vector<std::string> token;
      split_unquote(result[1], token, ",");
      for (auto &i: token) {
        if (std::regex_search(i, result, col_expression0)) {
          std::string col_name = result[1];
          for (uint32_t j = 0; j < children[0]->output_cols.size(); j++) {
            if (col_name == children[0]->output_cols[j]) {
              order_by_col.push_back(j);
              break;
            }
          }
          if (result[2] == "FIRST")
            null_first.push_back(true);
          else null_first.push_back(false);
          asc.push_back(SortOrder::ASC);
        } else if (std::regex_search(i, result, col_expression1)) {
          std::string col_name = result[1];
          for (uint32_t j = 0; j < children[0]->output_cols.size(); j++) {
            if (col_name == children[0]->output_cols[j]) {
              order_by_col.push_back(j);
              break;
            }
          }
          if (result[2] == "FIRST")
            null_first.push_back(true);
          else null_first.push_back(false);
          asc.push_back(SortOrder::DESC);
        }
      }
      predicates->order_by_col=order_by_col;
      predicates->null_first=null_first;
      predicates->asc=asc;
    }

    if (regex_search(line,result,col_partition_by)){
      std::vector<std::string> token;
      split_unquote(result[1], token, ",");
      for (const auto &x: token) {
        for (uint32_t j = 0; j < children[0]->output_cols.size(); j++) {
          if (x == children[0]->output_cols[j]) {
            order_by_col.push_back(j);
            break;
          }
        }
      }
      predicates->partition_by_col=partition_by_col;
    }

    else if (!regex_search(line, result, functions_begin)) continue;
    else {
      line = extended_plan_lines[++line_index];
      std::cout << line << std::endl;
      if (regex_search(line, result, window_function_definition)) {
        WindowFunction windowFunction{};
        line = extended_plan_lines[++line_index];
        regex_search(line, result, function_alias);
        windowFunction.alias = result[1];
        line = extended_plan_lines[line_index++];
        regex_search(line, result, function_argument);
        std::vector<std::string> token;
        split_unquote(result[1], token, ",");
        //only need one argument.
        for (uint32_t j = 0; j < children[0]->output_cols.size(); j++) {
          if (token[0] == children[0]->output_cols[j]) {
            windowFunction.arguments=j;
            break;
          }
        }
        line = extended_plan_lines[++line_index];
        regex_search(line, result, function_type);

        if (result[1]=="sum")
          windowFunction.type =SUM;
        else if (result[1]=="max")
          windowFunction.type =MAX;
        else if (result[1]=="min")
          windowFunction.type=MIN;
        else if (result[1]=="count")
          windowFunction.type=CNT;
        else if (result[1]=="rank")
          windowFunction.type=RANK;

        predicates->window_functions.push_back(windowFunction);
      }
    }
  }

  this->predicate = predicates;

  for (const auto& x: this->predicate->window_functions) {
    this->output_cols.push_back(x.alias);
  }
}





