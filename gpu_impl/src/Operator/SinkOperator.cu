#include "Operator/SinkOperator.hpp"

#include "Operator/GroupByPredicate.hpp"

SinkOperator::SinkOperator(std::string name, std::string vertex_name, vector<string> partitions) {
  operator_name = name;
  this->vertex_name = vertex_name;
  partition_cols = partitions;
  if (table_map.find(vertex_name) != table_map.end()) {
    this->op_result = table_map[vertex_name];
    is_input = true;
  } else {
    is_input = false;
  }
}

void SinkOperator::parseExtended() {
  std::vector<std::string> lines;
  split(extended_info, lines, "\n");
  for (std::string line: lines) { // todo: parse
    std::smatch match_result;
    if (std::regex_search(line, match_result,
                          std::regex("key expressions: (.*)"))) {
      std::string all_expressions = match_result[1];
      std::cout << "GHive-CPP [SinkOperator-parseExtended]: all_expression: " << match_result[1] << std::endl;
      std::vector<std::string> raw_expressions;
      split(all_expressions, raw_expressions, ", ");
      for (std::string each_expression: raw_expressions) {
        std::cout << "GHive-CPP [SinkOperator-parseExtended]: each_raw_expression: " << each_expression << std::endl;
        std::string each_expression_trim = trim(each_expression);
        std::cout << "GHive-CPP [SinkOperator-parseExtended]: each_trim_expression: " << each_expression_trim
                  << std::endl;
        if (std::regex_search(each_expression_trim, match_result, std::regex("(.*) \\(type: (.*)\\)"))) {
          std::string expression = match_result[1];
          this->key_expressions.push_back(expression);
          std::cout << "GHive-CPP [SinkOperator-parseExtended]: expression: " << expression << std::endl;
          //std::string expression_type = match_result[2];
          for (uint32_t i = 0; i < children[0]->output_cols.size(); i++) {
            std::cout << "GHive-CPP [SinkOperator-parseExtended]: children[0]->output_cols[" << i << "]: "
                      << children[0]->output_cols[i]
                      << std::endl;
            std::cout << vector_to_string(children[0]->output_cols) << std::endl;
            if (expression == children[0]->output_cols[i]) {
              key_offsets.push_back(i);
              break;
            }
          }
        }
      }

    } else if (std::regex_search(line, match_result,
                                 std::regex("null sort order: (.*)"))) { // todo: null sort order
      std::cout << "GHive-CPP [SinkOperator-parseExtended]: null sort order: " << match_result[1] << std::endl;

    } else if (std::regex_search(line, match_result,
                                 std::regex("sort order: (.*)"))) {
      this->sort_orders = match_result[1];
      std::cout << "GHive-CPP [SinkOperator-parseExtended]: sort order: " << match_result[1] << std::endl;
      // std::cout << "GHive-CPP [SinkOperator-parseExtended]: debug: ";
      // for (char c: this->sort_orders) {
      //   switch (SortOrder(c)) {
      //     case SortOrder::ASC: std::cout << "ASC" << std::endl;
      //       break;
      //     case SortOrder::DESC: std::cout << "DESC" << std::endl;
      //       break;
      //     default:break;
      //   }
      // }
      // std::cout << std::endl;
    }

  }
  std::vector<std::string> new_output_cols;

  // push key expressions first
  for (std::string key: key_expressions) {
    new_output_cols.push_back(key);
  }
  for (std::string col: output_cols) {
    bool find = false;
    for (std::string s: key_expressions) {
      if (s == col) {
        find = true;
      }
    }
    if (!find) {
      new_output_cols.push_back(col);
    }
  }
  for (std::string col: output_cols) {
    std::cout << "original pushback: " << col << std::endl;
    original_output_cols.push_back(col);
  }
  this->output_cols = new_output_cols;
  std::cout << "new output cols: " << vector_to_string(this->output_cols) << std::endl;
}

void SinkOperator::execute() {
  std::cout << "GHive-CPP [SinkOperator-execute]: inside operator_name: " << operator_name << std::endl;
  if (!is_input) {
    for (auto op: children) {
      op->execute();
    }
    Table *input_tbl = children[0]->op_result;
    std::cout << "GHive-CPP [SinkOperator-execute]: " << operator_name
              << " starts to execute with input: " << input_tbl->toString(100) << std::endl;

    Profiler profiler1;
    profiler1.start_op();
    profiler1.start_pci_host2device();
    std::vector<Column *> new_columns;
    for (uint32_t key_offset: key_offsets) {
      new_columns.push_back(input_tbl->columns[key_offset]);
    }
    for (uint32_t i = 0; i < input_tbl->columns.size(); i++) {
      bool is_key = false;
      for (uint32_t offset: key_offsets) {
        if (offset == i) {
          is_key = true;
          break;
        }
      }
      if (!is_key) {
        new_columns.push_back(input_tbl->columns[i]);
      }
    }


    auto millisecondsUTC =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    std::cout << "START_PCIE1: " << millisecondsUTC << std::endl;
    // sort new_columns -----------------------------------------------------------------------
    if (this->require_sort) {
      std::cout << "GHive-CPP [SinkOperator-execute]: Collect data and prepare sorting, order="
                << this->sort_orders
                << std::endl;

      auto key_num = this->sort_orders.size();
      auto row_num = input_tbl->rowNum();

      auto key_vec_size = key_num;
      for (uint32_t k_idx = 0; k_idx < key_num; ++k_idx) {
        if (new_columns[k_idx]->type == ColumnType::STRING) {
          key_vec_size++;
        }
      }
      thrust::device_vector<const void *> d_keys_vec;
      thrust::device_vector<int> d_keys_type;
      thrust::device_vector<SortOrder> d_sort_orders(sort_orders.size());

      for (uint32_t k_idx = 0; k_idx < key_num; ++k_idx) {
        d_sort_orders[k_idx] = SortOrder(sort_orders[k_idx]);
      }
      for (uint32_t k_idx = 0; k_idx < key_num; k_idx++) {
        auto *column = new_columns[k_idx];
        auto col_row_num = column->row_num;
        switch (column->type) {
          case LONG: {
            void *d_data_ptr = nullptr;
            cudaMalloc((void **) &d_data_ptr, sizeof(long) * col_row_num);
            cudaMemcpy(d_data_ptr, column->data_ptr, sizeof(long) * col_row_num, cudaMemcpyHostToDevice);
            d_keys_vec.push_back(d_data_ptr);
            d_keys_type.push_back(LONG);
            break;
          }
          case DOUBLE: {
            void *d_data_ptr = nullptr;
            cudaMalloc((void **) &d_data_ptr, sizeof(double) * col_row_num);
            cudaMemcpy(d_data_ptr, column->data_ptr, sizeof(double) * col_row_num, cudaMemcpyHostToDevice);
            d_keys_vec.push_back(d_data_ptr);
            d_keys_type.push_back(DOUBLE);
            break;
          }
          case INT: {
            void *d_data_ptr = nullptr;
            cudaMalloc((void **) &d_data_ptr, sizeof(int32_t) * col_row_num);
            cudaMemcpy(d_data_ptr, column->data_ptr, sizeof(int32_t) * col_row_num, cudaMemcpyHostToDevice);
            d_keys_vec.push_back(d_data_ptr);
            d_keys_type.push_back(INT);
            break;
          }
          case STRING: {
            void *d_data_ptr = nullptr;
            void *d_data_ptr_aux = nullptr;
            int32_t size_char = 0;
            for (int x = 0; x < col_row_num; x++) {
              size_char = ((int32_t *) column->data_ptr_aux)[2 * x] > size_char ?
                          ((int32_t *) column->data_ptr_aux)[2 * x] : size_char;
              size_char = ((int32_t *) column->data_ptr_aux)[2 * x + 1] > size_char ?
                          ((int32_t *) column->data_ptr_aux)[2 * x + 1] : size_char;
            }
            cudaMalloc((void **) &d_data_ptr, sizeof(char) * size_char);
            cudaMalloc((void **) &d_data_ptr_aux, sizeof(int32_t) * col_row_num * 2);
            cudaMemcpy(d_data_ptr, column->data_ptr, sizeof(char) * size_char, cudaMemcpyHostToDevice);
            cudaMemcpy(d_data_ptr_aux, column->data_ptr_aux, sizeof(int32_t) * col_row_num * 2, cudaMemcpyHostToDevice);
            d_keys_vec.push_back(d_data_ptr);
            d_keys_vec.push_back(d_data_ptr_aux);
            d_keys_type.push_back(STRING);
            break;
          }
          default: break;
        }
      }
      profiler1.end_pci_host2device();

      auto millisecondsUTC2 =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch()
          ).count();
      std::cout << "START_EXE: " << millisecondsUTC2 << std::endl;
      profiler1.start_gpu_exec();
      const void **d_keys_vec_ptr = thrust::raw_pointer_cast(d_keys_vec.data());
      const int *d_keys_type_ptr = thrust::raw_pointer_cast(d_keys_type.data());
      const SortOrder *d_sort_orders_ptr = thrust::raw_pointer_cast(d_sort_orders.data());

      thrust::device_vector<uint32_t> d_result_idx(row_num);
      thrust::sequence(d_result_idx.begin(), d_result_idx.end());
      std::cout << "GHive-CPP [SinkOperator-execute]: Keys number =" << key_num << std::endl;
      thrust::sort(d_result_idx.begin(), d_result_idx.end(),
                   sort_comparator(d_keys_vec_ptr, d_keys_type_ptr, static_cast<int>(key_num), d_sort_orders_ptr));
      std::cout << "GHive-CPP [SinkOperator-execute]: Finish gpu sorting" << std::endl;

      auto millisecondsUTC3 =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch()
          ).count();
      std::cout << "START_PCIE2: " << millisecondsUTC3 << std::endl;

      profiler1.end_gpu_exec();
      thrust::host_vector<uint32_t> h_result_idx(d_result_idx.begin(), d_result_idx.end());


      auto millisecondsUTC4 = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()
      ).count();
      std::cout << "END_PCIE2: " << millisecondsUTC4 << std::endl;


      for (auto &col: new_columns) {
        switch (col->type) {
          case LONG: {
            auto *old_data_ptr = static_cast<long *>(col->data_ptr);
            auto *new_data_ptr = new long[row_num];
            for (uint32_t r = 0; r < row_num; ++r) {
              new_data_ptr[r] = old_data_ptr[h_result_idx[r]];
            }
            col->data_ptr = new_data_ptr;
            // delete[]old_data_ptr;
            break;
          }
          case DOUBLE: {
            auto *old_data_ptr = static_cast<double *>(col->data_ptr);
            auto *new_data_ptr = new double[row_num];
            for (uint32_t r = 0; r < row_num; ++r) {
              new_data_ptr[r] = old_data_ptr[h_result_idx[r]];
            }
            col->data_ptr = new_data_ptr;
            // delete[]old_data_ptr;
            break;
          }
          case INT: {
            auto *old_data_ptr = static_cast<int32_t *>(col->data_ptr);
            auto *new_data_ptr = new int32_t[row_num];
            for (uint32_t r = 0; r < row_num; ++r) {
              new_data_ptr[r] = old_data_ptr[h_result_idx[r]];
            }
            col->data_ptr = new_data_ptr;
            // delete[]old_data_ptr;
            break;
          }
          case STRING: {
            auto *old_data_ptr_aux = static_cast<int32_t *>(col->data_ptr_aux);
            auto *new_data_ptr_aux = new int32_t[row_num * 2];
            for (uint32_t r = 0; r < row_num; ++r) {
              new_data_ptr_aux[r * 2] = old_data_ptr_aux[h_result_idx[r] * 2];
              new_data_ptr_aux[r * 2 + 1] = old_data_ptr_aux[h_result_idx[r] * 2 + 1];
            }
            col->data_ptr_aux = new_data_ptr_aux;
            // delete[]old_data_ptr;
            break;
          }
          default:break;
        }
      }
      std::cout << "GHive-CPP [SinkOperator-execute]: Finish cpu attaching" << std::endl;
    }

     auto millisecondsUTC5 = std::chrono::duration_cast<std::chrono::milliseconds>(
         std::chrono::system_clock::now().time_since_epoch()
     ).count();
     std::cout << "END_DATA_RECOVER: " << millisecondsUTC5 << std::endl;

    // ---------------------------------------------------------------------------------------------
    profiler1.end_op();
    std::cout << profiler1.toString() << std::endl;

    input_tbl->columns = new_columns;
    op_result = input_tbl;
  }
  std::cout << "GHive-CPP [SinkOperator-execute]: " << operator_name
            << " ends to execute, with result: " << op_result->toString(100) << std::endl;
  std::cout << "GHive-CPP [SinkOperator-execute]: " << operator_name
            << " ends to execute, with result.... " << std::endl;
}

std::string SinkOperator::toString() {
  std::string partition_string = "partition cols: ";
  if (!partition_cols.empty()) {
    partition_string += partition_cols[0];
    for (uint32_t i = 1; i < partition_cols.size(); i++) {
      partition_string += ",";
      partition_string += partition_cols[i];
    }
  }
  return "[" + operator_name + "]; " + "vertex name: " + vertex_name + "; " +
      partition_string + ";";
}

std::string SinkOperator::toString(int level) {
  std::string ret = "";
  for (int i = 0; i < level; i++) {
    ret += "  ";
  }
  ret += this->toString() + "\n";
  if (!is_input) {
    for (auto op: children) {
      ret += op->toString(level + 1);
    }
  }
  return ret;
}
