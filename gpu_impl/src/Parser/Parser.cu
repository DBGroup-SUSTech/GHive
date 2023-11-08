#include "Operator/FilterOperator.hpp"
#include "Operator/GroupByOperator.hpp"
#include "Operator/JoinOperator.hpp"
#include "Parser/Parser.hpp"
#include "Operator/Operator.hpp"
#include "Operator/SelectOperator.hpp"
#include "Operator/SinkOperator.hpp"
#include "Operator/LimitOperator.hpp"
#include "Operator/PTFOperator.hpp"
#include "Operator/TableScanOperator.hpp"
#include "Util/Util.hpp"
#include "Hdfs/hdfs.h"
#include "Operator/udf/RSADecrypt.h"
#include "Operator/Base64.h"

map<std::string, Table *> SinkOperator::table_map =
    SinkOperator::create_table_map();

map<std::string, Operator *> operator_map;
map<std::string, Operator *> container_map;
std::string raw_exec_plan;
std::string raw_extended_exec_plan;
static Operator *g_root_op = nullptr;            // root operator of biggest plan


void getRawExecPlanFromHDFS(std::string &execPlan,
                            const std::string &file_path) {
  hdfsBuilder *bld = hdfsNewBuilder();
  hdfsBuilderSetNameNode(bld, "default");
  hdfsFS fs = hdfsBuilderConnect(bld);
  hdfsFile readFile = hdfsOpenFile(fs, file_path.c_str(), O_RDONLY, 0, 0, 0);
  if (!readFile) {
    std::cout << "Failed to open hdfs file" << file_path << std::endl;
  }
  int MAX_BUF_LEN = 1024;
  char buffer[MAX_BUF_LEN + 1];
  while (true) {
    memset(buffer, 0, sizeof(buffer));
    int rc = hdfsRead(fs, readFile, buffer, MAX_BUF_LEN);
    if (rc <= 0) {
      break;
    } else {
      buffer[rc] = '\0';
    }
    execPlan.append(buffer);
  }
  hdfsCloseFile(fs, readFile);
}

void getRawExecPlanFromFile(std::string &execPlan,
                            const std::string &file_path) {
  std::ifstream f(file_path);
  std::stringstream buffer;
  buffer << f.rdbuf();
  execPlan = buffer.str();
}

void getBiggestPlanBlock(std::string &result, std::string &execPlan) {
  std::vector<std::string> plan_lines;
  split(execPlan, plan_lines, "\n");
  std::cout << "GHive-CPP-Debug [Parser-getBiggestPlanBlock]: plan_lines.size = " << plan_lines.size() << std::endl;
  bool plan_start = false;
  bool vertex_start = false;

  for (int i = 0; i < plan_lines.size(); i++) {
    std::string line = plan_lines[i];
    // std::cout << "GHive-CPP-Debug [Parser-getBiggestPlanBlock]: plan_lines = " << plan_lines[i] << std::endl;
    if (trim(line) == "Stage-0") {
      plan_start = true;
    }
    if (!plan_start) {
      continue;
    }
    if (line == "\n" || line.find("Plan optimized by CBO.") != std::string::npos) {
      break;
    }

    if (line.find("Reducer") == line.find_first_not_of(' ')
        || line.find("Map") == line.find_first_not_of(' ')) {
      vertex_start = true;
    }
    if (!vertex_start) {
      continue;
    }
    result += line + "\n";
  }
  // std::cout << "GHive-CPP-Debug [Parser-getBiggestPlanBlock]: result = " << result << std::endl;

}

void getExecPlanBlock(std::string &result, std::string &execPlan,
                      std::string &vertexName) {
  std::vector<std::string> plan_lines;
  split(execPlan, plan_lines, "\n");
  int padding = -1;
  bool plan_start = false;
  for (int i = 0; i < plan_lines.size(); i++) {
    std::string line = plan_lines[i];
    if (trim(line) == "Stage-0") {
      plan_start = true;
    }
    if (!plan_start) {
      continue;
    }
    std::size_t textStartIdx = line.find_first_not_of(' ');
    if (line.find(vertexName) == textStartIdx ||
        line.find("<-" + vertexName) == textStartIdx) {
      padding = textStartIdx;
      result += line + "\n";
      result += plan_lines[++i] + "\n";
    } else if (padding != -1) {
      if (line.find_first_not_of(' ') <= padding) {
        break;
      } else {
        result += line + "\n";
      }
    }
  }
  cout << "get vertex plan block;" << endl;
}

void getExtendedExecPlanBlock(std::string &result, std::string &execPlan,
                              std::string &vertexName) {
  std::vector<std::string> plan_lines;
  split(execPlan, plan_lines, "\n");
  bool vertex_plan_start = false;
  int vertices_start_padding =
      -1;  // the space before a vertex is the space before "Vertices" + 2
  for (int i = 0; i < plan_lines.size(); i++) {
    std::string line = plan_lines[i];
    if (trim(line) == "Vertices:") {
      vertices_start_padding = line.find_first_not_of(' ');
    }
    if (vertices_start_padding == -1) {
      continue;
    }

    if (line.find_first_not_of(' ') == vertices_start_padding + 2 &&
        (std::regex_match(trim(line), std::regex("(Map|Reducer) [0-9]+")))) {
      if (trim(line) == vertexName) {
        vertex_plan_start = true;
      } else {
        vertex_plan_start = false;
      }
    }

    if (!vertex_plan_start) {
      continue;
    }
    if (line.find_first_not_of(' ') < vertices_start_padding) {
      break;
    } else {
      result += line + "\n";
    }
  }
}

void getExtendedOperatorInfo(std::vector<std::string> &operators_info,
                             std::string &vertex_plan) {
  std::vector<std::string> plan_lines;
  split(vertex_plan, plan_lines, "\n");
  bool on_operator = false;
  int operator_start_padding_last = -1;
  int operator_start_padding_now = -1;
  std::string info_string;
  for (auto &plan_line: plan_lines) {
    std::string line = plan_line;
    if (std::regex_match(trim(line),
                         std::regex("[a-zA-Z]+( [a-zA-Z]+)? Operator")) ||
        trim(line) == "TableScan" || trim(line) == "Limit") {
      operator_start_padding_last = operator_start_padding_now;
      operator_start_padding_now = plan_line.find_first_not_of(' ');
      if (on_operator) {
        if (operator_start_padding_now != operator_start_padding_last) {
          operators_info.push_back(info_string);
          info_string = "";
        } else {
          on_operator = false;
          continue;
        }
      } else if (operator_start_padding_now != operator_start_padding_last)
        on_operator = true;
    }
    if (on_operator) {
      info_string += line + "\n";
    }
  }

  if (!info_string.empty()) {
    operators_info.push_back(info_string);

  }
}

Operator *getOperatorTree(std::string &container_plan) {
  std::vector<std::string> plan_lines;
  // std::cout << "GHive-CPP-Debug [Parser-getOperatorTree]: container_plan = " << container_plan << std::endl;

  split(container_plan, plan_lines, "\n");
  std::regex rs_pattern("\\[(RS_[0-9]+)\\]");  // reduce sink
  std::regex fs_pattern("\\[(FS_[0-9]+)\\]");  // reduce sink
  std::regex sink_partition_pattern("PartitionCols:(.*)");
  // PartitionCols:_col0, _col1, _col2

  std::regex sel_pattern("\\[(SEL_[0-9]+)\\]");        // select
  std::regex sel_output_pattern("Output:\\[(.*)\\]");  // select

  std::regex fil_pattern("\\[(FIL_[0-9]+)\\]");  // filter
  std::regex fil_pred_pattern("predicate:(.*)");

  std::regex ts_pattern("\\[(TS_[0-9]+)\\]");         // table scan
  std::regex ts_output_pattern("Output:\\[(.*)\\]");  // table scan

  std::regex limit_pattern("\\[(LIM_[0-9]+)\\]");         // table scan
  std::regex limit_output_pattern("Number of rows:([0-9]+)");  // table scan

  std::regex gby_pattern("\\[(GBY_[0-9]+)\\]");
  std::regex gby_output_pattern(
      R"(Output:\[(.*)\],aggregations:\[(.*)\],keys:(.*))");

  std::regex gby_output_pattern_no_key(
      R"(Output:\[(.*)\],aggregations:\[(.*)\])");

  std::regex gby_output_pattern_no_agg(
      R"(Output:\[(.*)\],keys:(.*))");
  std::regex PTF_pattern("\\[(PTF_[0-9]+)\\]");
  std::regex PTF_function_define("Function definitions:\\[(.*)\\]");

  std::regex mapjoin_pattern("\\[(MAPJOIN_[0-9]+)\\]");
  std::regex mergejoin_pattern("\\[(MERGEJOIN_[0-9]+)\\]");
  std::regex join_output_pattern(R"(Conds:(.*),.*Output:\[(.*)\])");
  std::regex join_output_pattern_with_no_output(R"(Conds:(.*),HybridGraceHashJoin:(.*))");
  std::regex multi_join_output_pattern(R"(Conds:(.*\),.*\)),.*Output:\[(.*)\])");
  std::regex each_join("(.*)\\((.*)\\)");
  //multi will satisfy single so it should be put first!!!!

  std::regex union_pattern("(Union [0-9]+)");//union

  std::regex map_container_pattern("(Map [0-9]+)");
  std::regex reduce_container_pattern("(Reducer [0-9]+)");

  std::regex map_container_refer("Please refer to the previous (Map [0-9]+)");
  std::regex reduce_container_refer("Please refer to the previous (Reducer [0-9]+)");
  std::regex operator_refer("Please refer to the previous .* \\[([A-Z]+_[0-9]+)\\]");

  std::vector<Operator *> stack;

  std::smatch result;
  SinkOperator *root_op = nullptr;
  // std::cout << "GHive-CPP-Debug [Parser-getOperatorTree]: plan_lines.size = " << plan_lines.size() << std::endl;
  for (uint64_t i = 0, len = plan_lines.size(); i < len; i++) {
    std::string line = plan_lines[i];
    int padding = line.find_first_not_of(' ');
    if (line.find("<-") == padding) {
      padding += 2;
    }

    Operator *op = nullptr;
    bool refer = false;
    // std::cout << "GHive-CPP-Debug [Parser-getOperatorTree]: plan_lines[" << i << "]=" << line << std::endl;
    if (std::regex_search(line, result, operator_refer)) {
      //todo: refer operator;
      std::cout << "GHive-CPP-Debug [Parser-getOperatorTree]: refer expression: " << line << std::endl;
      std::string operator_name = result[1];
      std::cout << "GHive-CPP-Debug [Parser-getOperatorTree]: refer operator name: " << operator_name;
      op = new Operator(*operator_map[operator_name]); // copy
      refer = true;
      std::cout << "GHive-CPP-Debug [Parser-getOperatorTree]: refer operator op pointer: " << op << std::endl;
    } else if (std::regex_search(line, result, map_container_refer)
        || std::regex_search(line, result, reduce_container_refer)) {
      std::string container_name = result[1];
      op = container_map[container_name];
    } else if (std::regex_search(line, result, limit_pattern)) {
      std::string operator_name = result[1];
      std::string next_line = plan_lines[++i];
      if (std::regex_search(next_line, result, limit_output_pattern)) {
        int limit_num = std::stoi(result[1]);
        op = new LimitOperator(operator_name, limit_num);
      }
    } else if (std::regex_search(line, result, sel_pattern)) {
      std::string operator_name = result[1];
      std::string next_line = plan_lines[i + 1];
      std::cout << "GHive-CPP-Debug [Parser-getOperatorTree]: Select operator next line: " << next_line << std::endl;
      if (std::regex_search(next_line, result, sel_output_pattern)) {
        std::vector<std::string> sel_output_cols;
        split_unquote(result[1], sel_output_cols, ",");
        //        TODO: input the string of selected cols instead of a vector.
        op = new SelectOperator(operator_name, sel_output_cols);
        i++;
      } else {

        std::cout << "GHive-CPP-Debug [Parser-getOperatorTree]: Select operator has default cols" << std::endl;
        continue;
      }
    } else if (std::regex_search(line, result, fil_pattern)) {
      std::string operator_name = result[1];
      std::string next_line = plan_lines[++i];
      if (std::regex_search(next_line, result, fil_pred_pattern)) {
        std::string predicate_str = result[1];
        op = new FilterOperator(operator_name, predicate_str);
      } else {
        std::cout << "GHive-CPP-Error [Parser-getOperatorTree]: Error on initializing filter operator" << std::endl;
      }
    } else if (std::regex_search(line, result, ts_pattern)) {
      std::string operator_name = result[1];
      std::string next_line = plan_lines[++i];
      if (std::regex_search(next_line, result, ts_output_pattern)) {
        std::vector<std::string> ts_output_cols;
        split_unquote(result[1], ts_output_cols, ",");
        op = new TableScanOperator(operator_name, ts_output_cols);
      } else {
        std::cout << "GHive-CPP-Error [Parser-getOperatorTree]: Error on initializing table scan operator" << std::endl;
      }
    } else if (std::regex_search(line, result, gby_pattern)) {
      // std::cout << line << std::endl;
      std::string operator_name = result[1];
      std::string next_line = plan_lines[++i];
      if (std::regex_search(next_line, result, gby_output_pattern)) {
        std::vector<std::string> outputs;
        std::vector<std::string> aggregations;
        std::vector<std::string> keys;
        split_unquote(result[1], outputs, ",");
        split_unquote(result[2], aggregations, ",");
        split(result[3], keys, ", ");
        op = new GroupByOperator(operator_name, outputs, aggregations, keys);
      } else if (std::regex_search(next_line, result, gby_output_pattern_no_key)) {
        std::vector<std::string> outputs;
        std::vector<std::string> aggregations;
        std::vector<std::string> keys;
        split_unquote(result[1], outputs, ",");
        split_unquote(result[2], aggregations, ",");
        op = new GroupByOperator(operator_name, outputs, aggregations, keys);
      } else if (std::regex_search(next_line, result, gby_output_pattern_no_agg)) {
        std::vector<std::string> outputs;
        std::vector<std::string> aggregations;
        std::vector<std::string> keys;
        split_unquote(result[1], outputs, ",");
        split(result[2], keys, ", ");
        op = new GroupByOperator(operator_name, outputs, aggregations, keys);
      } else {
        std::cout << "GHive-CPP-Error [Parser-getOperatorTree]: Error on initializing groupby operator" << std::endl;
      }
    } else if (std::regex_search(line, result, mapjoin_pattern) ||
        std::regex_search(line, result, mergejoin_pattern)) {
      // std::cout << line << std::endl;
      std::string operator_name = result[1];
      std::string next_line = plan_lines[++i];

      if (std::regex_search(next_line, result, join_output_pattern)) {
        std::string conds;
        std::vector<std::string> outputs;
        conds = result[1];
        split_unquote(result[2], outputs, ",");
        op = new JoinOperator(operator_name, conds, outputs);
      } else if (std::regex_search(next_line, result, join_output_pattern_with_no_output)) {
        std::string conds;
        std::vector<std::string> outputs;
        conds = result[1];
        op = new JoinOperator(operator_name, conds, outputs);
      } else {
        std::cout << "GHive-CPP-Error [Parser-getOperatorTree]: Error on initializing join operator" << std::endl;
      }
    } else if (std::regex_search(line, result, union_pattern)) {
      std::string operator_name = result[1];
      std::vector<std::string> partitions;
      op = new SinkOperator(operator_name, operator_name, partitions);
    } else if (std::regex_search(line, result, PTF_pattern)) {
      std::string operator_name = result[1];
      op = new PTFOperator(operator_name);
      i++;
    } else if (std::regex_search(line, result, map_container_pattern) ||
        std::regex_search(line, result, reduce_container_pattern)) {
      //      Why result[1]? But the implementation does not matter as long as
      //      it works.
      std::string rs_container = result[1];
      line = plan_lines[++i];
      if (std::regex_search(line, result, rs_pattern) ||
          std::regex_search(line, result, fs_pattern)) {
        std::string operator_name = result[1];
        std::string next_line = plan_lines[i + 1];
        std::vector<std::string> rs_partitions;
        // ReduceSink may not have column partitions.
        if (std::regex_search(next_line, result, sink_partition_pattern)) {
          i++;
          split(result[1], rs_partitions, ",");
        }
        op = new SinkOperator(operator_name, rs_container, rs_partitions);
        container_map[rs_container] = op;
      } else {
        std::cout << "GHive-CPP-Error [Parser-getOperatorTree]: Error on initializing sink operator" << std::endl;
      }
    } else {
      std::cout << "GHive-CPP-Error [Parser-getOperatorTree]: Error on identifying the line from plan: " << line
                << std::endl;
      return nullptr;
    }
    if (op == nullptr) {
      std::cout << "GHive-CPP-Error [Parser-getOperatorTree]: No Operator identified: " << line << std::endl;
      return nullptr;
    }
    if (!refer) {
      // reference is not allowed to overwrite operator_map
      operator_map[op->operator_name] = op;
    }
    op->padding = padding;

    // root op: the first identified operator.
    std::cout << "GHive-CPP-Debug [Parser-getOperatorTree]: root_op = " << root_op << std::endl;
    std::cout << "GHive-CPP-Debug [Parser-getOperatorTree]: op = " << op << std::endl;

    if (root_op == nullptr) {
      root_op = (SinkOperator *) op;
      root_op->is_input = false;
    }
    std::cout << "GHive-CPP-Debug [Parser-getOperatorTree]: root_op = " << root_op << std::endl;

    if (stack.empty()) {
      stack.push_back(op);
    } else {
      //todo:padding for refer operator
      if (op->padding == stack.back()->padding + 2
          || op->padding == stack.back()->padding + 3
          || op->padding == stack.back()->padding + 4) {
        if (regex_match(stack.back()->operator_name, union_pattern)) {
          std::vector<std::string> union_output_cols{"_col0"};
          stack.back()->output_cols = union_output_cols;
        } else if (typeid(*(stack.back())) == typeid(SinkOperator) ||
            typeid(*(stack.back())) == typeid(FilterOperator)
            || typeid(*(stack.back())) == typeid(LimitOperator)) {
          stack.back()->output_cols = op->output_cols;
        }

        stack.back()->children.push_back(op);
        op->parent = stack.back();
        stack.push_back(op);
      } else if (op->padding > stack.back()->padding) {
        std::cout << "GHive-CPP-Error [Parser-getOperatorTree]: Error on build operator tree" << std::endl;
      } else {  // op->padding <= stack.back
        while (op->padding <= stack.back()->padding) {
          if (stack.empty()) {
            std::cout << "GHive-CPP-Error [Parser-getOperatorTree]: Error on build operator tree" << std::endl;
          } else {
            stack.pop_back();
          }
        }
        if (stack.empty()) {
          std::cout << "GHive-CPP-Error [Parser-getOperatorTree]: Error on build operator tree" << std::endl;
        } else {
          if (regex_match(stack.back()->operator_name, union_pattern)) {
            std::vector<std::string> union_output_cols{"_col0"};
            stack.back()->output_cols = union_output_cols;

          } else if (typeid(*(stack.back())) == typeid(SinkOperator) ||
              typeid(*(stack.back())) == typeid(FilterOperator) ||
              typeid(*(stack.back())) == typeid(LimitOperator)) {
            stack.back()->output_cols = op->output_cols;
          }
          stack.back()->children.push_back(op);
          op->parent = stack.back();
          stack.push_back(op);
        }
      }
    }
  }
  std::cout << "GHive-CPP-Debug [Parser-getOperatorTree]: root_op = " << root_op << std::endl;

  if (root_op == nullptr) {
    std::cout << "GHive-CPP-Error [Parser-getOperatorTree]: Root operator is null" << std::endl;
  }
  return root_op;
}

SinkOperator *findInputSinkOperator(Operator *op) {
  auto *sink_op = dynamic_cast<SinkOperator *> (op);
  if (sink_op && sink_op->is_input) return sink_op;
  for (auto *child_op: op->children) {
    auto *target_op = findInputSinkOperator(child_op);
    if (target_op) return target_op;
  }
  return nullptr;
}

void linkOperatorTreeAndExtendedPlan(std::vector<std::string> &vector_info,
                                     Operator *op, int ith) {
  std::cout << "FILE: " << __FILE__ << ", LINE: " << __LINE__ << std::endl;
  std::cout << "GHive-CPP [Parser-linkOperatorTreeAndExtendedPlan]: ith: " << ith << endl;
  std::cout << "GHive-CPP [Parser-linkOperatorTreeAndExtendedPlan]: op->operator_name: "
            << op->operator_name << std::endl;
  op->extended_info = vector_info[vector_info.size() - 1 - ith];
  std::cout << "GHive-CPP [Parser-linkOperatorTreeAndExtendedPlan]: op->extended_info: "
            << op->extended_info << std::endl;

  for (auto child: op->children) {
    if (typeid(*child) != typeid(SinkOperator)) {
      linkOperatorTreeAndExtendedPlan(vector_info, child, ith + 1);
    } else {
      std::regex union_pattern("(Union [0-9]+)");//union
      if (!regex_search(child->operator_name, union_pattern)) {
        std::string vertex_extended_plan;
        std::string child_vertex_name = ((SinkOperator *) child)->vertex_name;
        getExtendedExecPlanBlock(vertex_extended_plan, raw_extended_exec_plan,
                                 child_vertex_name);
        std::vector<std::string> vertex_auxiliary_info;
        getExtendedOperatorInfo(vertex_auxiliary_info, vertex_extended_plan);
        child->extended_info = vertex_auxiliary_info[vertex_auxiliary_info.size() - 1];
        child->parseExtended();
      }
    }
  }
  op->parseExtended();

  // todo: parse global root FS operator order info.
  std::cout << "GHive-CPP [Parser-linkOperatorTreeAndExtendedPlan]:"
            << "global root op: " << g_root_op
            << "current op:" << op
            << std::endl;

  if (op == g_root_op) {
    auto *sink_op = dynamic_cast<SinkOperator *>(op);
    std::cout << "GHive-CPP [Parser-execute_plan]: global root op, origin sort order: "
              << sink_op->sort_orders << std::endl;
    // find previous reducer
    auto *input_op = findInputSinkOperator(op);

    std::string last_vertex_name = input_op->vertex_name;
    std::string last_vertex_extended_plan;
    std::vector<std::string> last_vertex_auxiliary_info;

    getExtendedExecPlanBlock(last_vertex_extended_plan, raw_extended_exec_plan, last_vertex_name);
    getExtendedOperatorInfo(last_vertex_auxiliary_info, last_vertex_extended_plan);
    input_op->extended_info = last_vertex_auxiliary_info[last_vertex_auxiliary_info.size() - 1];
    input_op->parseExtended();

    sink_op->require_sort = true;
    sink_op->sort_orders = input_op->sort_orders;
    std::cout << "GHive-CPP [Parser-execute_plan]: global root op, sort order from last reducer: "
              << sink_op->sort_orders << std::endl;
  }

}



Table *execute_rsa_udf() {
    Table *table = SinkOperator::table_map["TableScan"];


    char *new_str_col = new char[128 * table->row_num];
    int *new_idx_col = new int[2 * table->row_num];
    char *data_ptr = (char *)table->columns[1]->data_ptr;
    int *data_idx_ptr = (int *)table->columns[1]->data_ptr_aux;
    for (int i = 0; i < table->row_num; i ++) {
        int i_start = data_idx_ptr[2 * i];
        int i_len = data_idx_ptr[2 * i + 1] - data_idx_ptr[2 * i];
        macaron::Base64::Decode(std::string(data_ptr + i_start, i_len), new_str_col + (128 * i));
        char buffer[128];
        for (int j = 0; j < 128; j++) {
            buffer[j] = new_str_col[128 * i + 127 - j];
        }
        for (int j = 0; j < 128; j++) {
            new_str_col[128 * i + j] = buffer[j];
        }
        new_idx_col[2 * i] = 128 * i;
        new_idx_col[2 * i + 1] = 128 * i + 128;
    }

    std::cout << "table row number: " << table->row_num  << std::endl;
    std::cout << "table col row number: " << table->columns[1]->row_num << std::endl;
    auto *col = new Column(STRING, table->row_num, new_str_col, new_idx_col, 128 * table->row_num);
    table->columns[1] = col;
    std::cout << "Decoded" << std::endl;

    FunctorSelectPredicate predicate(rsa_decrypt_functor(), 1);
    predicate.process(table);
    table->columns[1] = predicate.column;



    substr_functor f(0, 3);
    FunctorSelectPredicate substr_predicate(f, 1);
    substr_predicate.process(table);
    FilterOperator filter_operator;
    FilterPredicate str_predicate;
    str_predicate.paramNum = 1;
    str_predicate.mode = FILTER_EQ;
    str_predicate.filterCol[0] = 1;
    str_predicate.stringFilterParams[0] = "136";
    str_predicate.dataType = 3;
    FilterProfiler profiler;
    profiler.start_op();
    str_predicate.process(table, profiler);
//  filter_operator.h_result = str_predicate->result_bitmap;
//  filter_operator.clean_data(tbl);
    filter_operator.clean_data(str_predicate.result_bitmap, table, profiler);
    profiler.end_op();


    Table *new_tbl = new Table();
    new_tbl->row_num = table->row_num;
    new_tbl->columns.push_back(table->columns[0]);

//    long *id = new long[table->row_num];
//    int *tbl_id = (int *)table->columns[0]->data_ptr;
//    for (int i = 0; i < table->row_num; i ++) {
//        id[i] = tbl_id[i];
//    }
//    Column *column = new Column(LONG, table->row_num, id);
//    new_tbl->columns.push_back(column);

    return new_tbl;


};

Table *execute_plan(std::string vertex_name) {
//#define UDF_TEST
//#ifdef UDF_TEST
//  if (vertex_name == "Map 1") {
//      return execute_rsa_udf();
//  }
//#endif
  std::cout << "GHive-CPP [Parser-execute_plan]: parser invoked" << std::endl;

  std::string hdfs_file_path = "/tmp/plan/explain.txt";
  std::string hdfs_extended_plan_file_path = "/tmp/plan/explain_extended.txt";

  getRawExecPlanFromHDFS(raw_exec_plan, hdfs_file_path);
  getRawExecPlanFromHDFS(raw_extended_exec_plan, hdfs_extended_plan_file_path);

  // std::cout << "GHive-CPP [Parser-execute_plan]: vertex_name = " << vertex_name << " raw explained plan: "
  //           << raw_exec_plan << std::endl;
  // std::cout << "GHive-CPP [Parser-execute_plan]: vertex_name = " << vertex_name << " raw extended explained plan: "
  //           << raw_extended_exec_plan << std::endl;

  std::string biggest_plan;
  getBiggestPlanBlock(biggest_plan, raw_exec_plan);
  std::cout << "GHive-CPP-Debug [Parser-execute_plan]: getBiggestPlanBlock.result = " << biggest_plan << std::endl;

  g_root_op = getOperatorTree(biggest_plan);
  // std::cout << "GHive-CPP [Parser-execute_plan]: vertex_name = " << vertex_name
  //           << " g_root_op = " << g_root_op
  //           << "after getBiggestPlanBlock: \n"
  //           << biggest_plan
  //           << std::endl;

  std::string vertex_plan;
  getExecPlanBlock(vertex_plan, raw_exec_plan, vertex_name);
//    std::cout << "vertex plan: " << vertex_plan << std::endl;


  std::string vertex_extended_plan;
  getExtendedExecPlanBlock(vertex_extended_plan, raw_extended_exec_plan,
                           vertex_name);
//    std::cout << "extended vertex plan: " << vertex_extended_plan << std::endl;

  std::vector<std::string> vertex_auxiliary_info;
  getExtendedOperatorInfo(vertex_auxiliary_info, vertex_extended_plan);

  Operator *root_op = container_map[vertex_name]; // this root_op
  std::cout << "debug-point :" << (root_op) << std::endl;
  std::cout << "GHive-CPP [Parser-execute_plan]: Operator Tree of vertex: " << vertex_name <<
            ":" << std::endl << root_op->toString(0) << std::endl;

  std::cout << "GHive-CPP [Parser-execute_plan]: vertex_name = " << vertex_name
            << " root_op = " << root_op
            << " g_root_op = " << g_root_op
            << std::endl;
  std::cout << "GHive-CPP [Parser-execute_plan]: Linking Operator Tree and Extended Plan starts." << std::endl;
  linkOperatorTreeAndExtendedPlan(vertex_auxiliary_info, root_op, 0);
  std::cout << "GHive-CPP [Parser-execute_plan]: Linking Operator Tree and Extended Plan ends." << std::endl;

  std::cout << "GHive-CPP [Parser-execute_plan]: The executing vertex name: " << vertex_name << std::endl;

  std::cout << "GHive-CPP [Parser-execute_plan]: Operator Tree: " << root_op->toString(0);
  root_op->execute();
  return root_op->op_result;
}