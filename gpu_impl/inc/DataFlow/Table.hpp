#ifndef TABLE_HPP
#define TABLE_HPP

#include <vector>
#include "Column.hpp"

class Table {
 public:
  std::vector<Column *> columns;
  std::vector<ColumnType> types; // todo: remove
  uint32_t row_num;
  uint32_t key_num;

  uint32_t rowNum() const { return row_num; }
  std::string toString();
  std::string toString(uint32_t print_row_num, bool raw = false);

  std::string rowToString(uint32_t row_idx);

  Table(std::vector<ColumnType> _types, uint32_t _row_num);

  Table();

  /**
   * @brief Get null value of a type.
   * @tparam T type
   * @return null value
   */
  template<class T>
  static T __device__ __host__
  null_value() {
    if (std::is_same<T, long>::value)return LONG_MAX;
    else if (std::is_same<T, double>::value) return NAN;
    else if (std::is_same<T, int>::value) return INT_MAX;
    else return 0;
  }

  /**
   * @brief Figure out whether a value is null
   * @tparam T type
   * @param value value
   * @return whether a value is null.
   */

  template<class T>
  static bool __device__ __host__
  is_null(T value) {
    if (std::is_same<T, long>::value) return value == LONG_MAX;
    else if (std::is_same<T, double>::value) return std::isnan(value);
    else if (std::is_same<T, int>::value) return value == INT_MAX;
    else return false;
  }
};
#endif