#include <Util/Util.hpp>
#include "Operator/BaseJoinPredicate.hpp"

Table *BaseJoinPredicate::generate_result(std::vector<Table *> tables) {

  std::cout << "GHive-CPP [BaseJoinPredicate-generate_result]: generating results." << std::endl;
//  std::cout << "GHive-CPP [BaseJoinPredicate-generate_result]: result_to_tbl: " <<
//            vector_to_string<uint32_t>(result_to_tbl) << std::endl;
//  std::cout << "GHive-CPP [BaseJoinPredicate-generate_result]: result_offset: " <<
//            vector_to_string<int32_t>(result_offsets) << std::endl;
  Table *new_table = new Table();

  for (uint32_t each_tbl = 0; each_tbl < result_to_tbl.size(); each_tbl++) {
    uint32_t tbl_idx = result_to_tbl[each_tbl];
    Table *old_tbl = tables[tbl_idx];
    std::vector<uint32_t> tbl_maintain_col = maintain_cols[tbl_idx];
    if (tbl_maintain_col.size() == 0 && each_tbl == 0) {
      tbl_maintain_col.push_back(0);
    }
    std::cout << "GHive-CPP [BaseJoinPredicate-generate_result]: "
              << "tbl [" << tbl_idx << "]; tbl_maintain_col: "
              << vector_to_string<uint32_t>(tbl_maintain_col) << std::endl;

    for (uint32_t maintain_col: tbl_maintain_col) {
      std::cout << "GHive-CPP [BaseJoinPredicate-generate_result]: "
                << "tbl [" << tbl_idx << "]; tbl_maintain_col[i] " << maintain_col << std::endl;

      Column *old_column = old_tbl->columns[maintain_col];

      switch (old_column->type) {
        case LONG: {
          long *data = (long *) old_column->data_ptr;
          long *new_data = new long[cardinality];
          for (uint32_t i = 0; i < cardinality; i++) {
            auto idx = result_offsets[i * result_to_tbl.size() + each_tbl];
            if (idx < 0) new_data[i] = Table::null_value<long>();
            else new_data[i] = data[idx];
          }
          Column *new_column = new Column(LONG, cardinality, new_data);
          new_table->columns.push_back(new_column);
          break;
        }
        case DOUBLE: {
          double *data = (double *) old_column->data_ptr;
          double *new_data = new double[cardinality];
          for (uint32_t i = 0; i < cardinality; i++) {
            auto idx = result_offsets[i * result_to_tbl.size() + each_tbl];
            if (idx < 0) new_data[i] = Table::null_value<double>();
            else new_data[i] = data[idx];
          }
          Column *new_column = new Column(DOUBLE, cardinality, new_data);
          new_table->columns.push_back(new_column);
          break;
        }
        case INT: {
          int32_t *data = (int32_t *) old_column->data_ptr;
          int32_t *new_data = new int32_t[cardinality];
          for (uint32_t i = 0; i < cardinality; i++) {
            auto idx = result_offsets[i * result_to_tbl.size() + each_tbl];
            if (idx < 0) new_data[i] = Table::null_value<int>();
            else new_data[i] = data[idx];
          }
          Column *new_column = new Column(INT, cardinality, new_data);
          new_table->columns.push_back(new_column);
          break;
        }
        case STRING: {
          char *str_col = (char *) old_column->data_ptr;
          int32_t *old_str_idx_col = (int32_t *) old_column->data_ptr_aux;
          int32_t *str_idx_col = new int32_t[cardinality * 2];
          for (uint32_t i = 0; i < cardinality; i++) {
            auto idx = result_offsets[i * result_to_tbl.size() + each_tbl];
            if (idx < 0) {
              str_idx_col[2 * i] = -1;
              str_idx_col[2 * i + 1] = -1;
            } else {
              str_idx_col[2 * i] = old_str_idx_col[2 * idx];
              str_idx_col[2 * i + 1] = old_str_idx_col[2 * idx + 1];
            }
          }
          Column *new_column = new Column(STRING, cardinality, str_col, str_idx_col, old_column->char_size);
          new_table->columns.push_back(new_column);
          break;
        }
        case DEPEND: {
          break;
        }
      }
    }

    std::cout << "GHive-CPP [BaseJoinPredicate-generate_result]: "
              << "tbl [" << each_tbl << "]; tbl_maintain_col: "
              << vector_to_string<uint32_t>(tbl_maintain_col) << std::endl;
  }
  new_table->row_num = cardinality;
  return new_table;
}
