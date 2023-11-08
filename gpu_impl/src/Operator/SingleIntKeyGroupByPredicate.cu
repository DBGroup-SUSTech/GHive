#include <iostream>
#include <unordered_map>
#include <Profile/Profiler.hpp>
#include <DataFlow/Table.hpp>
#include <Util/Util.hpp>
#include <Profile/SortGroupByProfiler.hpp>
//#include "Util/Util.hpp"
#include "Operator/SingleIntKeyGroupByPredicate.hpp"
#include "Operator/GroupByAggregation.cuh"
#include "Operator/GroupByOperator.cuh"
#include "Operator/GroupByPredicate.hpp"
using namespace std;

SingleIntKeyGroupByPredicate::SingleIntKeyGroupByPredicate() {}

Table *SingleIntKeyGroupByPredicate::gpu_execute(Table *input_tbl, SortGroupByProfiler &profiler) {

  std::cout << "GHive-CPP [GroupByPredicate-execute]: gpu_execute starts executing" << std::endl;
  std::cout << "GHive-CPP [GroupByPredicate-execute]: keys: " << vector_to_string<uint32_t>(keys) << std::endl;
  uint32_t row_num = input_tbl->row_num;
  uint32_t key_num = keys.size();//the number of group by columns.
  assert(key_num == 1);
  assert(input_tbl->columns[keys[0]]->type == INT);
  uint32_t value_num = aggregation_descs.size();



  std::cout << "GHive-CPP [GroupByPredicate-execute]: Collect data and prepare sorting" << std::endl;
  profiler.start_gpu_alloc();
  profiler.start_pci_host2device();

  int32_t *d_int_ptr;
  cudaMalloc((void **) &d_int_ptr, row_num * sizeof(int32_t));
  cudaMemcpy(d_int_ptr, input_tbl->columns[keys[0]]->data_ptr, row_num * sizeof(int32_t), cudaMemcpyHostToDevice);

  struct single_int_key_sort_comparator sort_comparator(d_int_ptr);
  struct single_int_key_comparator comparator(d_int_ptr);

  thrust::device_vector<uint32_t> d_result_idx(row_num);
  thrust::sequence(d_result_idx.begin(), d_result_idx.end());


  profiler.end_gpu_alloc();
  profiler.end_pci_host2device();
  profiler.start_sort();
  profiler.start_gpu_exec();
  thrust::sort(d_result_idx.begin(), d_result_idx.end(), sort_comparator);

  thrust::host_vector<int> tmp = d_result_idx;
  for (int i: tmp) {
      std::cout << i << std::endl;
  }
  profiler.end_gpu_exec();
  profiler.end_sort();

  std::cout << "GHive-CPP [GroupByPredicate-execute]: Finish gpu sorting" << std::endl;

  thrust::host_vector<uint32_t> h_result_keys_idx;

  std::vector<Column *> result_columns;

  AggregationDesc desc = aggregation_descs[0];
  assert(desc.type == SUM);
  result_columns.push_back(agg_sum(d_result_idx,
    h_result_keys_idx,
    input_tbl->columns[desc.index],
    row_num,
    comparator,
    profiler));

  Table *result_tbl = new Table();
  result_tbl->columns = result_columns;
  result_tbl->row_num = h_result_keys_idx.size();

  profiler.end_data_recover();
  return result_tbl;
}


Column *SingleIntKeyGroupByPredicate::agg_sum(thrust::device_vector<uint32_t> &d_result_idx,
                                          thrust::host_vector<uint32_t> &result_key_idx,
                                          Column *column, uint32_t row_num,
                                          struct single_int_key_comparator &rp, SortGroupByProfiler &profiler) {
  std::cout << "GHive-CPP [GroupByPredicate-aggregation_sum]" << std::endl;
  int *p_data = (int *) column->data_ptr;
  thrust::device_vector<int> d_input_values(p_data, p_data + row_num);
  thrust::device_vector<int> d_result_keys_idx(row_num);
  thrust::device_vector<int> d_result_values(row_num);
  auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                         thrust::make_permutation_iterator(d_input_values.begin(),
                                                                           d_result_idx.begin()),
                                         d_result_keys_idx.begin(), d_result_values.begin(), rp,
                                         agg_plus<int>());
  result_key_idx = thrust::host_vector<int32_t>(d_result_keys_idx.begin(), new_end.first);
  uint32_t result_size = new_end.second - d_result_values.begin();
  int *p_result = new int[result_size];
  cudaMemcpy(p_result, thrust::raw_pointer_cast(d_result_values.data()),
             result_size * sizeof(int), cudaMemcpyDeviceToHost);
  return new Column(column->type, result_size, p_result);
}

