#ifndef PARSER_SRC_INCLUDE_UTIL_FILTERPREDICATE_HPP
#define PARSER_SRC_INCLUDE_UTIL_FILTERPREDICATE_HPP

#include <cstdint>
#include <iostream>
#include <string>
#include <thrust/device_vector.h>
#include <DataFlow/Table.hpp>
#include <Profile/Profiler.hpp>
#include <Profile/FilterProfiler.hpp>

#include "DataFlow/DataFlow.hpp"

/*
    Filter Mode
*/
enum PredicateMode {
  //  =
  FILTER_EQ = 0,

  //  <=
  FILTER_LE = 1,

  //  >=
  FILTER_GE = 2,

  //  <
  FILTER_LT = 3,

  //  >
  FILTER_GT = 4,

  // TODO: does hive have range and equal_range?
  //  > && <
  FILTER_RANGE = 5,

  //>= && <=
  FILTER_EQ_RANGE = 6,

  //  !=
  FILTER_NOT_EQ = 7,

  // TODO: does hive have range and equal not range?
  //  < || >
  FILTER_NOT_RANGE = 8,

  //  <= || >=
  FILTER_NOT_EQ_RANGE = 9,

  //  not null
  FILTER_NOT_NULL = 10,

  // and
  FILTER_AND = 11,

  // or
  FILTER_OR = 12
};

class FilterPredicate {
 public:
  std::string predicateLiteral;
  PredicateMode mode;

  int predColNum = 1; // how many cols will the predicate works on.

  uint32_t filterCol[2];  // which column to process (index of table)
  // Filter can also be done by the way of: col2 > col7
  // Give it two positions to save two columns.
  uint8_t paramNum;    // 1 or 2
  int32_t cardinality;

  //  TODO: Params could be int or double. Replace this with template
  //  implementation.
  std::string stringFilterParams[2];
  int64_t longFilterParams[2];
  int32_t intFilterParams[2];
  double doubleFilterParams[2];
  uint8_t dataType;  // 0: long; 1: double, 2: int, 3: string, 4: none (for and / or)
  FilterPredicate *childrenPredicate[2];  // for "and" and "or";

  int row_num;
  thrust::device_vector<int> result_bitmap;
  thrust::host_vector<int> host_result_bitmap;

  /**
   * @param predicateLiteral A literal of this predicate to parse.
   */
  FilterPredicate();

  std::string toString();

  std::string toString(int level);

  void process(Table *table, FilterProfiler &profiler);

  void cpu_process(Table *table);

  template<typename T>
  void gpu_filter(T *filter_col, T *val, FilterProfiler &profiler);

  template<typename T, typename F>
  void cpu_filter_two(T *filter_col_1, F *filter_col_2);

  template<typename T>
  void cpu_filter(T *filter_col, T *val);

  void cpu_string_filter(char *filter_col, int *index_col, std::string *pred);

  template<typename T, typename F>
  void gpu_filter_two(T *filter_col_1, F *filter_col_2, Profiler &profiler);

  void gpu_string_filter(char *filter_col, int *index_col, std::string *pred, Profiler &profiler);

  void gpu_string_filter_not_null(int *index_col, Profiler &profiler);
  void cpu_string_filter_not_null(int *index_col);
};

template<typename T, typename F>
struct filter_binary_op {

  __host__ __device__
  virtual inline int process_op(const T &x, const F &y) = 0;

  __host__ __device__
  int operator()(const T &x, const F &y) {
    return process_op(x, y);
  }
};

template<typename T, typename F>
struct filter_binary_eq : filter_binary_op<T, F> {

  __host__ __device__
  inline int process_op(const T &x, const F &y) {
    return !Table::is_null(x) && !Table::is_null(y) && x == y;
  }

};

template<typename T, typename F>
struct filter_binary_le : filter_binary_op<T, F> {

  __host__ __device__
  inline int process_op(const T &x, const F &y) {
    return !Table::is_null(x) && !Table::is_null(y) && x <= y;
  }

};

template<typename T, typename F>
struct filter_binary_ge : filter_binary_op<T, F> {

  __host__ __device__
  inline int process_op(const T &x, const F &y) {
    return !Table::is_null(x) && !Table::is_null(y) && x >= y;
  }

};

template<typename T, typename F>
struct filter_binary_lt : filter_binary_op<T, F> {

  __host__ __device__
  inline int process_op(const T &x, const F &y) {
    return !Table::is_null(x) && !Table::is_null(y) && x < y;
  }

};

template<typename T, typename F>
struct filter_binary_gt : filter_binary_op<T, F> {

  __host__ __device__
  inline int process_op(const T &x, const F &y) {
    return !Table::is_null(x) && !Table::is_null(y) && x > y;
  }

};

template<typename T, typename F>
struct filter_binary_ne : filter_binary_op<T, F> {

  __host__ __device__
  inline int process_op(const T &x, const F &y) {
    return !Table::is_null(x) && !Table::is_null(y) && x != y;
  }

};

// FILTER_AND == 11
template<typename T, typename F>
struct filter_binary_and : filter_binary_op<T, F> {
  __host__ __device__
  inline int process_op(const T &x, const F &y) {
    return !Table::is_null(x) && !Table::is_null(y) && x && y;
  }

};

// FILTER_AND == 12
template<typename T, typename F>
struct filter_binary_or : filter_binary_op<T, F> {

  __host__ __device__
  inline int process_op(const T &x, const F &y) {
    return !Table::is_null(x) && !Table::is_null(y) && (x || y);
  }

};

template<typename T>
struct filter_unary_op {

  __host__ __device__
  virtual inline int process_op(const T &x) = 0;

  __host__ __device__
  bool operator()(const T &x) {
    return process_op(x);
  }
};

template<typename T>
struct filter_unary_eq : filter_unary_op<T> {

  T val;
  filter_unary_eq(T _val) : val(_val) {}

  __host__ __device__
  inline int process_op(const T &x) {
    return !Table::is_null(x) && x == val;
  }
};

template<typename T>
struct filter_unary_le : filter_unary_op<T> {

  T val;
  filter_unary_le(T _val) : val(_val) {}

  __host__ __device__
  inline int process_op(const T &x) {
    return !Table::is_null(x) && x <= val;
  }
};

template<typename T>
struct filter_unary_ge : filter_unary_op<T> {

  T val;
  filter_unary_ge(T _val) : val(_val) {}

  __host__ __device__
  inline int process_op(const T &x) {
    return !Table::is_null(x) && x >= val;
  }
};

template<typename T>
struct filter_unary_lt : filter_unary_op<T> {

  T val;
  filter_unary_lt(T _val) : val(_val) {}

  __host__ __device__
  inline int process_op(const T &x) {
    return !Table::is_null(x) && x < val;
  }
};

template<typename T>
struct filter_unary_gt : filter_unary_op<T> {

  T val;
  filter_unary_gt(T _val) : val(_val) {}

  __host__ __device__
  inline int process_op(const T &x) {
    return !Table::is_null(x) && x > val;
  }
};

template<typename T>
struct filter_range : filter_unary_op<T> {

  T lower;
  T upper;
  filter_range(T _lower, T _upper) : lower(_lower), upper(_upper) {}

  __host__ __device__
  inline int process_op(const T &x) {
    return !Table::is_null(x) && x > lower && x < upper;
  }
};

template<typename T>
struct filter_eq_range : filter_unary_op<T> {

  T lower;
  T upper;
  filter_eq_range(T _lower, T _upper) : lower(_lower), upper(_upper) {}

  __host__ __device__
  inline int process_op(const T &x) {
    return !Table::is_null(x) && x >= lower && x <= upper;
  }
};

template<typename T>
struct filter_unary_ne : filter_unary_op<T> {

  T val;
  filter_unary_ne(T _val) : val(_val) {}

  __host__ __device__
  inline int process_op(const T &x) {
    return !Table::is_null(x) && x != val;
  }
};

template<typename T>
struct filter_not_range : filter_unary_op<T> {

  T lower;
  T upper;
  filter_not_range(T _lower, T _upper) : lower(_lower), upper(_upper) {}

  __host__ __device__
  inline int process_op(const T &x) {
    return !Table::is_null(x) && (x < lower || x > upper);
  }
};

template<typename T>
struct filter_not_eq_range : filter_unary_op<T> {

  T lower;
  T upper;
  filter_not_eq_range(T _lower, T _upper) : lower(_lower), upper(_upper) {}

  __host__ __device__
  inline int process_op(const T &x) {
    return !Table::is_null(x) && (x <= lower || x >= upper);
  }
};

template<typename T>
struct filter_not_null : filter_unary_op<T> {

  filter_not_null() {}

  // TODO: we does not define the "null" for each type now.
  __host__ __device__
  inline int process_op(const T &x) {
    return !Table::is_null(x);
  }
};

template<typename T>
struct filter_unary_and : filter_unary_op<T> {
  filter_unary_op<T> child_ops[2];

  filter_unary_and() {
  }

  __host__ __device__
  inline int process_op(const T &x) {
    return !Table::is_null(x) && (child_ops[0].process_op(x) && child_ops[1].process_op(x));
  }
};

template<typename T>
struct filter_unary_or : filter_unary_op<T> {
  filter_unary_op<T> child_ops[2];

  filter_unary_or() {
  }

  __host__ __device__
  inline int process_op(const T &x) {
    return !Table::is_null(x) && (child_ops[0].process_op(x) || child_ops[1].process_op(x));
  }
};



struct filter_str_substr_eq {

    char *data;
    int *idx;
    char *val;
    int size;

    filter_str_substr_eq(char *_data, int *_index, char *_val, int _size) :
            data(_data), idx(_index), val(_val), size(_size) {}

    __host__ __device__
    int operator()(const int &offset) {
        int s_offset = idx[2 * offset];
        if (s_offset < 0) return 0;

        int e_offset = idx[2 * offset + 1];
        if (e_offset - s_offset < size) {
            return 0;
        }
        for (int i = 0; i < size; i++) {
            if (data[s_offset + i] != val[i]) return 0;
        }
        return 1;
    }
};


// FILTER_EQ = 0;
struct filter_str_unary_eq {

  char *data;
  int *idx;
  char *val;
  int size;

  filter_str_unary_eq(char *_data, int *_index, char *_val, int _size) :
      data(_data), idx(_index), val(_val), size(_size) {}

  __host__ __device__
  int operator()(const int &offset) {
    int s_offset = idx[2 * offset];
    if (s_offset < 0) return 0;

    int e_offset = idx[2 * offset + 1];
    if (e_offset - s_offset != size) {
      return 0;
    }
    for (int i = 0; i < size; i++) {
      if (data[s_offset + i] != val[i]) return 0;
    }
    return 1;
  }
};

// FILTER_LE = 1;
struct filter_str_unary_le {

  char *data;
  int *idx;
  char *val;
  int size;

  filter_str_unary_le(char *_data, int *_index, char *_val, int _size) :
      data(_data), idx(_index), val(_val), size(_size) {}

  __host__ __device__
  int operator()(const int &offset) {
    int s_offset = idx[2 * offset];
    if (s_offset < 0) return 0;

    int e_offset = idx[2 * offset + 1];
    int len = e_offset - s_offset;
    int min_len = len < size ? len : size;

    for (int i = 0; i < min_len; i++) {
      if (data[s_offset + i] != val[i]) {
        return data[s_offset + i] < val[i];
      }
    }
    return len <= size;
  }
};

// FILTER_GE = 2;
struct filter_str_unary_ge {

  char *data;
  int *idx;
  char *val;
  int size;

  filter_str_unary_ge(char *_data, int *_index, char *_val, int _size) :
      data(_data), idx(_index), val(_val), size(_size) {}

  __host__ __device__
  int operator()(const int &offset) {
    int s_offset = idx[2 * offset];
    if (s_offset < 0) return 0;

    int e_offset = idx[2 * offset + 1];
    int len = e_offset - s_offset;
    int min_len = len < size ? len : size;

    for (int i = 0; i < min_len; i++) {
      if (data[s_offset + i] != val[i]) {
        return data[s_offset + i] > val[i];
      }
    }
    return len >= size;
  }
};

// FILTER_LT = 3;
struct filter_str_unary_lt {

  char *data;
  int *idx;
  char *val;
  int size;

  filter_str_unary_lt(char *_data, int *_index, char *_val, int _size) :
      data(_data), idx(_index), val(_val), size(_size) {}

  __host__ __device__
  int operator()(const int &offset) {
    int s_offset = idx[2 * offset];
    if (s_offset < 0) return 0;

    int e_offset = idx[2 * offset + 1];
    int len = e_offset - s_offset;
    int min_len = len < size ? len : size;

    for (int i = 0; i < min_len; i++) {
      if (data[s_offset + i] != val[i]) {
        return data[s_offset + i] < val[i];
      }
    }
    return len < size;
  }
};

// FILTER_GT = 3;
struct filter_str_unary_gt {

  char *data;
  int *idx;
  char *val;
  int size;

  filter_str_unary_gt(char *_data, int *_index, char *_val, int _size) :
      data(_data), idx(_index), val(_val), size(_size) {}

  __host__ __device__
  int operator()(const int &offset) {
    int s_offset = idx[2 * offset];
    if (s_offset < 0) return 0;

    int e_offset = idx[2 * offset + 1];
    int len = e_offset - s_offset;
    int min_len = len < size ? len : size;

    for (int i = 0; i < min_len; i++) {
      if (data[s_offset + i] != val[i]) {
        return data[s_offset + i] > val[i];
      }
    }
    return len > size;
  }
};

// FILTER_EQ_RANGE = 6;
struct filter_str_unary_eq_range {

  char *data;
  int *idx;
  char *val_1;
  int size_1;
  char *val_2;
  int size_2;

  filter_str_unary_eq_range(char *_data, int *_index, char *_val_1, int _size_1, char *_val_2, int _size_2) :
      data(_data), idx(_index), val_1(_val_1), size_1(_size_1), val_2(_val_2), size_2(_size_2) {}

  __host__ __device__
  int operator()(const int &offset) {
    int s_offset = idx[2 * offset];
    if (s_offset < 0) return 0;

    int e_offset = idx[2 * offset + 1];
    int len = e_offset - s_offset;

    int ge_true = -1;
    for (int i = 0; i < len && i < size_1; i++) {
      if (data[s_offset + i] != val_1[i]) {
        ge_true = data[s_offset + i] > val_1[i];
        break;
      }
    }
    ge_true = ge_true == 1 || (ge_true == -1 && len >= size_1);

    int le_true = -1;
    for (int i = 0; i < len && i < size_2; i++) {
      if (data[s_offset + i] != val_2[i]) {
        le_true = data[s_offset + i] < val_2[i];
        break;
      }
    }
    le_true = le_true == 1 || (le_true == -1 && len <= size_2);
    return ge_true && le_true;
  }
};

// FILTER_NOT_RANGE = 8;
struct filter_str_unary_not_range {

  char *data;
  int *idx;
  char *val_1;
  int size_1;
  char *val_2;
  int size_2;

  filter_str_unary_not_range(char *_data, int *_index, char *_val_1, int _size_1, char *_val_2, int _size_2) :
      data(_data), idx(_index), val_1(_val_1), size_1(_size_1), val_2(_val_2), size_2(_size_2) {}

  __host__ __device__
  int operator()(const int &offset) {
    int s_offset = idx[2 * offset];
    if (s_offset < 0) return 0;

    int e_offset = idx[2 * offset + 1];
    int len = e_offset - s_offset;

    int ge_true = 1;
    int min_len_1 = len < size_1 ? len : size_1;
    for (int i = 0; i < min_len_1; i++) {
      if (data[s_offset + i] != val_1[i]) {
        ge_true = data[s_offset + i] > val_1[i];
        break;
      }
    }
    ge_true = ge_true || (len >= size_1);

    int le_true = 1;
    int min_len_2 = len < size_2 ? len : size_2;
    for (int i = 0; i < min_len_2; i++) {
      if (data[s_offset + i] != val_2[i]) {
        le_true = data[s_offset + i] < val_2[i];
        break;
      }
    }
    le_true = le_true || (len <= size_2);
    return !(ge_true && le_true);
  }
};

// FILTER_NOT_EQ = 7;
struct filter_str_unary_ne {

  char *data;
  int *idx;
  char *val;
  int size;

  filter_str_unary_ne(char *_data, int *_index, char *_val, int _size) :
      data(_data), idx(_index), val(_val), size(_size) {}

  __host__ __device__
  int operator()(const int &offset) {
    int s_offset = idx[2 * offset];
    if (s_offset < 0) return 0;

    int e_offset = idx[2 * offset + 1];
    if (e_offset - s_offset != size) {
      return 1;
    }
    for (int i = 0; i < size; i++) {
      if (data[s_offset + i] != val[i]) return 1;
    }
    return e_offset != s_offset;
  }
};


// FILTER_NOT_EQ = 7;
struct filter_str_not_null {

  int32_t *idx;

  filter_str_not_null(int32_t *_idx) : idx(_idx){}

  __host__ __device__
  int operator()(const int &offset) {
    return idx[2 * offset] != -1 && idx[2 * offset + 1] != -1;
  }
};

#endif
