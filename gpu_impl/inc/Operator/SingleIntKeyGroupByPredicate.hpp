#ifndef GPU_IMPL_SINGLEKEY_HPP
#define GPU_IMPL_SINGLEKEY_HPP

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




struct single_int_key_sort_comparator {
    static int abs(int a) {
        return a < 0? -a: a;
    }

  const int *data_ptr;

  explicit single_int_key_sort_comparator(const int *data_ptr):
      data_ptr(data_ptr) {};

  __host__ __device__
  bool operator()(const int i, const int j) const {
      return abs(data_ptr[i]) < abs(data_ptr[j]);
  }

};


struct single_int_key_comparator {
    static int abs(int a) {
        return a < 0? -a: a;
    }
  const int *data_ptr;

  explicit single_int_key_comparator(const int *data_ptr):
      data_ptr(data_ptr) {
  }

  __host__ __device__ bool operator()(const int i, const int j) const {
    return abs(data_ptr[i]) == abs(data_ptr[j]);
  }
};

class SingleIntKeyGroupByPredicate {
 public:
  std::vector<uint32_t> keys;

 public:
  std::vector<AggregationDesc> aggregation_descs;

  SingleIntKeyGroupByPredicate();

  SingleIntKeyGroupByPredicate(std::vector<uint32_t> keys,
                   std::vector<AggregationDesc> aggregation_descs) {
    this->keys = keys;
    this->aggregation_descs = aggregation_descs;
  }

  Table *gpu_execute(Table *input_tbl, SortGroupByProfiler &profiler);

//  Table *cpu_execute(Table *input_tbl, SortGroupByProfiler &profiler);

  Column *agg_sum(thrust::device_vector<uint32_t> &d_result_idx,
                          thrust::host_vector<uint32_t> &result_key_idx,
                          Column *column, uint32_t row_num,
                          single_int_key_comparator& comp, SortGroupByProfiler &profiler);


};



#endif  // GPU_IMPL_CCODE_INCLUDE_OPERATORS_GROUPBYDESC_HPP
