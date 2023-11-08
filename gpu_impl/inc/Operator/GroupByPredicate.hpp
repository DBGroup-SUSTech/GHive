#ifndef GPU_IMPL_CCODE_INCLUDE_OPERATORS_GROUPBYDESC_HPP
#define GPU_IMPL_CCODE_INCLUDE_OPERATORS_GROUPBYDESC_HPP

#include <cassert>
#include <cstdint>
#include <vector>
#include "DataFlow/DataFlow.hpp"
#include "DataFlow/Table.hpp"
#include "DataFlow/Column.hpp"
#include "Profile/Profiler.hpp"
#include <thrust/reduce.h>
#include <thrust/execution_policy.h>
#include <thrust/device_vector.h>
#include "Operator/AggregationDesc.hpp"
#include "Profile/SortGroupByProfiler.hpp"

enum class SortOrder : char {
  ASC = '+',
  DESC = '-'
};

struct sort_comparator {

  const void **data_ptr;
  const int *types;
  int data_col_num;
  const SortOrder *orders; // default is +

  sort_comparator(const void **data_ptr, const int *types, int data_col_num) :
      data_ptr(data_ptr), types(types), data_col_num(data_col_num), orders(nullptr) {}

  sort_comparator(const void **data_ptr, const int *types, int data_col_num, const SortOrder *orders) :
      data_ptr(data_ptr), types(types), data_col_num(data_col_num), orders(orders) {}

  __host__ __device__ bool operator()(const int i, const int j) const {

    for (int k = 0, l = 0; k < data_col_num; k++, l++) {
      auto order = orders ? SortOrder(orders[k]) : SortOrder::ASC;
      bool asc = (SortOrder::ASC == order);

      if (types[k] == 0) { // Long
        long *longCol = (long *) data_ptr[l];
        if (longCol[i] < longCol[j]) {
          return asc;
        } else if (longCol[i] > longCol[j]) {
          return !asc;
        }
      } else if (types[k] == 1) { // Double
        double *doubleCol = (double *) data_ptr[l];

        auto is_null_i = Table::is_null(doubleCol[i]);
        auto is_null_j = Table::is_null(doubleCol[j]);
        if (is_null_i && is_null_j) {
          // continue
        } else if (is_null_i || is_null_j) {
          return is_null_j ? asc : !asc;
        } else if (doubleCol[i] < doubleCol[j]) {
          return asc;
        } else if (doubleCol[i] > doubleCol[j]) {
          return !asc;
        }
      } else if (types[k] == 2) { // Int
        int *intCol = (int *) data_ptr[l];
        if (intCol[i] < intCol[j]) {
          return asc;
        } else if (intCol[i] > intCol[j]) {
          return !asc;
        }
      } else if (types[k] == 3) { // String
        char *strCol = (char *) data_ptr[l];
        l++;
        int *strIdxCol = (int *) data_ptr[l];

        auto start_i = strIdxCol[2 * i];
        auto start_j = strIdxCol[2 * j];
        if (start_i < 0 && start_j < 0) {
          // continue
        } else if (start_i < 0 || start_j < 0) {
          return start_j < 0 ? asc : !asc;
        } else {

          int length_1 = strIdxCol[2 * i + 1] - strIdxCol[2 * i];
          int length_2 = strIdxCol[2 * j + 1] - strIdxCol[2 * j];
          int length_min = length_1 < length_2 ? length_1 : length_2;
          for (int p = 0; p < length_min; p++) {
            if (*(strCol + strIdxCol[2 * i] + p) < *(strCol + strIdxCol[2 * j] + p)) {
              return asc;
            } else if (*(strCol + strIdxCol[2 * i] + p) > *(strCol + strIdxCol[2 * j] + p)) {
              return !asc;
            }
          }
          if (length_1 != length_2) {
            if (length_1 < length_2) return asc;
            else return !asc;
          }
        }
      }
    }
    return false;
  }
};


struct reducer_predicator {

  const void **data_ptr;
  const int *types;
  int data_col_num;

  reducer_predicator(const void **data_ptr, const int *types, int data_col_num)
      : data_ptr(data_ptr), types(types), data_col_num(data_col_num) {}

  __host__ __device__ bool operator()(const int i, const int j) const {

    for (int k = 0, l = 0; k < data_col_num; k++, l++) {
      if (types[k] == 0) { // Long
        long *longCol = (long *) data_ptr[l];
        if (longCol[i] != longCol[j]) {
          return false;
        }
      } else if (types[k] == 1) { // Double
        double *doubleCol = (double *) data_ptr[l];
        if (doubleCol[i] != doubleCol[j] &&
            !(Table::is_null(doubleCol[i]) && Table::is_null(doubleCol[j]))) {
          return false;
        }
      } else if (types[k] == 2) { // Int
        int *intCol = (int *) data_ptr[l];
        if (intCol[i] != intCol[j]) {
          return false;
        }
      } else if (types[k] == 3) { // String
        char *strCol = (char *) data_ptr[l++];
        int *strIdxCol = (int *) data_ptr[l];

        auto start_i = strIdxCol[2 * i];
        auto start_j = strIdxCol[2 * j];
        if (start_i < 0 && start_j < 0) {
          return true;
        }

        int length = strIdxCol[2 * i + 1] - strIdxCol[2 * i];
        if (length != strIdxCol[2 * j + 1] - strIdxCol[2 * j]) {
          return false;
        }
        for (int k = 0; k < length; k++) {
          if (*(strCol + strIdxCol[2 * i] + k) != *(strCol + strIdxCol[2 * j] + k)) {
            return false;
          }
        }
      }
    }
    return true;
  }
};

class GroupByPredicate {
 public:
  std::vector<uint32_t> keys;

 public:
  std::vector<AggregationDesc> aggregation_descs;

  GroupByPredicate();

  GroupByPredicate(std::vector<uint32_t> keys,
                   std::vector<AggregationDesc> aggregation_descs) {
    this->keys = keys;
    this->aggregation_descs = aggregation_descs;
  }

  Table *gpu_execute(Table *input_tbl, SortGroupByProfiler &profiler);

  Table *cpu_execute(Table *input_tbl, SortGroupByProfiler &profiler);

  template<typename T>
  Column *aggregation_sum(thrust::device_vector<uint32_t> &d_result_idx,
                          thrust::host_vector<uint32_t> &result_key_idx,
                          Column *column, uint32_t row_num,
                          reducer_predicator &rp, SortGroupByProfiler &profiler);

  template<typename T>
  Column *aggregation_max(thrust::device_vector<uint32_t> &d_result_idx,
                          thrust::host_vector<uint32_t> &result_key_idx,
                          Column *column, uint32_t row_num,
                          reducer_predicator &rp, SortGroupByProfiler &profiler);

  template<typename T>
  Column *aggregation_min(thrust::device_vector<uint32_t> &d_result_idx,
                          thrust::host_vector<uint32_t> &result_key_idx,
                          Column *column, uint32_t row_num,
                          reducer_predicator &rp, SortGroupByProfiler &profiler);

  template<typename T>
  Column *aggregation_avg(thrust::device_vector<uint32_t> &d_result_idx,
                          thrust::host_vector<uint32_t> &result_key_idx,
                          Column *column, uint32_t row_num, reducer_predicator &rp, SortGroupByProfiler &profiler);

    Column *aggregation_cnt(thrust::device_vector<uint32_t> &d_result_idx,
                          thrust::host_vector<uint32_t> &result_key_idx,
                          uint32_t row_num,
                          reducer_predicator &rp, SortGroupByProfiler &profiler);

  template<typename T>
  Column *aggregation_sum_cpu(thrust::host_vector<uint32_t> &d_result_idx,
                              thrust::host_vector<uint32_t> &result_key_idx,
                              Column *column,
                              uint32_t row_num,
                              reducer_predicator &rp);

  Column *no_aggregation(thrust::device_vector<uint32_t> &d_result_idx,
                         thrust::host_vector<uint32_t> &result_key_idx,
                         reducer_predicator &rp,
                         SortGroupByProfiler &profiler);
};

template <typename T>
struct agg_plus {
  __host__ __device__
  T operator()(const T &x, const T &y) {
    T result = 0;
    result += Table::is_null(x) ? 0 : x;
    result += Table::is_null(y) ? 0 : y;
    return result;
  }
};

template <typename T>
struct agg_maximum {
  __host__ __device__
  T operator()(const T &x, const T &y) {
    if (Table::is_null(x)) {
      return y;
    } else if (Table::is_null(y)) {
      return x;
    } else {
      return x > y ? x : y;
    }
  }
};

template <typename T>
struct agg_minimum {
  __host__ __device__
  T operator()(const T &x, const T &y) {
    if (Table::is_null(x)) {
      return y;
    } else if (Table::is_null(y)) {
      return x;
    } else {
      return x < y ? x : y;
    }
  }
};

template <typename T>
struct agg_cnt {
  __host__ __device__
  int operator()(const T &x, const T &y) {
    int result = 0;
    result += Table::is_null(x) ? 0 : 1;
    result += Table::is_null(y) ? 0 : 1;
    return result;
  }
};



#endif  // GPU_IMPL_CCODE_INCLUDE_OPERATORS_GROUPBYDESC_HPP
