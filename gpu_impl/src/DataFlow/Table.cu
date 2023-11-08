#include <string>
#include <sstream>
#include <Util/Util.hpp>
#include "DataFlow/Table.hpp"
#include "../../inc/DataFlow/Table.hpp"

std::string Table::toString(uint32_t print_row_num, bool raw) {
  print_row_num = print_row_num < row_num ? print_row_num : row_num;
  std::stringstream ss;
  for (uint32_t i = 0; i < print_row_num; i++) {
    ss << "row[" << std::to_string(i) << "]: [";
    for (Column *column: columns) {
      switch (column->type) {
        case LONG: {
          ss << ((long *) column->data_ptr)[i];
          break;
        }
        case DOUBLE: {
          ss << ((double *) column->data_ptr)[i];
          break;
        }
        case INT: {
          ss << ((int32_t *) column->data_ptr)[i];
          break;
        }
        case STRING: {
          char *chars = (char *) column->data_ptr;
          int32_t *indices = (int32_t *) column->data_ptr_aux;
          if (raw) {
            for (int j = indices[2 * i]; j < indices[2 * i + 1]; j ++) {
              ss << (int)chars[j] << ";";
            }
          } else {
            ss << std::string(chars + indices[2 * i], indices[2 * i + 1] - indices[2 * i]);
          }
          break;
        }
        case DEPEND:break;
      }
      if (column != columns.back()) {
        ss << ", ";
      }
    }
    ss << "]\n";
  }
  return ss.str();
}

std::string Table::toString() {
  std::stringstream ss;
  ss << "row number: " << row_num << "\n";
  ss << "column types: ";
  for (uint32_t i = 0; i < columns.size(); i ++) {
    ss << columns[i]->type;
    if (i != columns.size() - 1) {
      ss << "; ";
    }
  }
  ss << std::endl;
  ss << toString(min(20, row_num));
  return ss.str();
}

Table::Table(std::vector<ColumnType> _types, uint32_t _row_num) : types(_types), row_num(_row_num) {

}

Table::Table() {}

std::string Table::rowToString(uint32_t row_idx) {
  std::stringstream ss;
  ss << "row[" << std::to_string(row_idx) << "]: [";
  for (Column *column: columns) {
    switch (column->type) {
      case LONG: {
        ss << std::to_string(((long *) column->data_ptr)[row_idx]);
        break;
      }
      case DOUBLE: {
        ss << std::to_string(((double *) column->data_ptr)[row_idx]);
        break;
      }
      case INT: {
        ss << std::to_string(((int32_t *) column->data_ptr)[row_idx]);
        break;
      }
      case STRING: {
        char *chars = (char *) column->data_ptr;
        int32_t *indices = (int32_t *) column->data_ptr_aux;
        ss << std::string(chars + indices[2 * row_idx],
                          indices[2 * row_idx + 1] - indices[2 * row_idx]);
        break;
      }
      case DEPEND:break;
    }
    if (column != columns.back()) {
      ss << ", ";
    }
  }
  ss << "]\n";
  return ss.str();
}
