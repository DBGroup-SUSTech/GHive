#ifndef GPUIMPL_PTFPREDICATE_HPP
#define GPUIMPL_PTFPREDICATE_HPP

#include <thrust/device_vector.h>
#include "AggregationDesc.hpp"
#include "Operator.hpp"
#include "GroupByPredicate.hpp"
#include "DataFlow/Table.hpp"

enum windowFrame{
  ROWS_PRECEDING_FOLLOWING=0,
  ROWS_PRECEDING_CURRENT=1
};
class WindowFunction {
public:
  std::string alias;
  windowFrame window_frame;
  AggregationType type;
  uint arguments;
};

class PTFPredicate{
public:
    std::vector<uint32_t> order_by_col;
    std::vector<SortOrder> asc;
    std::vector<bool> null_first;
    std::vector<uint32_t> partition_by_col;
    std::vector<WindowFunction> window_functions;

    Table* execute(Table* input_tbl,Profiler &profiler);
    std::string toString(){
      return "";
    }
  template <typename T>
  Column *aggregation_rank(thrust::device_vector<uint32_t> &d_result_idx,
                          uint32_t row_num, reducer_predicator &rp,windowFrame window_frame);
  template <typename T>
  Column *aggregation_avg(thrust::device_vector<uint32_t> &d_result_idx,
                          Column *column, uint32_t row_num, reducer_predicator &rp,windowFrame window_frame);
  template <typename T>
  Column *aggregation_max(thrust::device_vector<uint32_t> &d_result_idx,
                          Column *column, uint32_t row_num, reducer_predicator &rp,windowFrame window_frame);
  template <typename T>
  Column *aggregation_sum(thrust::device_vector<uint32_t> &d_result_idx,
                          Column *column, uint32_t row_num, reducer_predicator &,windowFrame window_frame);

};

template<typename T = void>
struct newPlus
{
  typedef T first_argument_type;
  typedef T second_argument_type;
  typedef T result_type;

  __thrust_exec_check_disable__
  __host__ __device__
  constexpr T operator()(const T &lhs, const T &rhs) const
  {
    if (Table::is_null(lhs)&& Table::is_null(rhs))
      return 0;
    if (Table::is_null(lhs))
      return rhs;
    else if (Table::is_null(rhs))
      return lhs;
    return lhs + rhs;
  }
};

template<typename T = void>
struct newMaximum
{
  typedef T first_argument_type;
  typedef T second_argument_type;
  typedef T result_type;
  __thrust_exec_check_disable__
  __host__ __device__
  constexpr T operator()(const T &lhs, const T &rhs) const
  {
    if (Table::is_null(lhs)&& Table::is_null(rhs)){
      if (std::is_same<T, long>::value)
        return LONG_MIN;
      else if (std::is_same<T, double>::value)
        return 0.0/0.0;
      else if (std::is_same<T, int>::value)
        return INT_MIN;
    }
    if (Table::is_null(lhs)){
      return rhs;
    }
    if (Table::is_null(rhs)){
      return lhs;
    }
    return lhs < rhs ? rhs : lhs;
  }
}; // end maximum





template<typename T = void>
struct countPlus
{
  typedef T first_argument_type;
  typedef T second_argument_type;
  typedef T result_type;

  __thrust_exec_check_disable__
  __host__ __device__
  constexpr T operator()(const T &lhs, const T &rhs) const
  {
    if (Table::is_null(lhs)&&Table::is_null(rhs))
      return 0;
    else if (Table::is_null(lhs))
      return 1;
    else if (Table::is_null(rhs))
      return 1;
    return 2;
  }
};
#endif //GPUIMPL_PTFPREDICATE_HPP
