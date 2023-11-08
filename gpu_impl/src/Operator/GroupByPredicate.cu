#include <iostream>
#include <unordered_map>
#include <Profile/Profiler.hpp>
#include <DataFlow/Table.hpp>
#include <Util/Util.hpp>
#include <Profile/SortGroupByProfiler.hpp>
//#include "Util/Util.hpp"
#include "Operator/GroupByPredicate.hpp"
#include "Operator/GroupByAggregation.cuh"
#include "Operator/GroupByOperator.cuh"
using namespace std;

bool operator==(SortOrder order, char val) {
  return val == static_cast<char> (order);
}

GroupByPredicate::GroupByPredicate() {}

Table *GroupByPredicate::gpu_execute(Table *input_tbl, SortGroupByProfiler &profiler) {

  std::cout << "GHive-CPP [GroupByPredicate-execute]: gpu_execute starts executing" << std::endl;
  std::cout << "GHive-CPP [GroupByPredicate-execute]: keys: " << vector_to_string<uint32_t>(keys) << std::endl;
  uint32_t row_num = input_tbl->row_num;
  uint32_t key_num = keys.size();//the number of group by columns.
  uint32_t value_num = aggregation_descs.size();

  uint32_t key_vec_size = key_num;
  for (uint32_t key: keys) {
    if (input_tbl->columns[key]->type == STRING) {
      key_vec_size++;
    }
  }
  std::cout << "GHive-CPP [GroupByPredicate-execute]: Collect data and prepare sorting" << std::endl;
  profiler.start_gpu_alloc();
  profiler.start_pci_host2device();
  thrust::device_vector<const void *> d_keys_vec(key_vec_size);
  thrust::device_vector<int> d_keys_type(key_num);
  profiler.end_gpu_alloc();
  for (uint32_t i = 0, l = 0; i < keys.size(); i++, l++) {
    Column *column = input_tbl->columns[keys[i]];
    switch (column->type) {
      case LONG: {
        d_keys_type[i] = 0;
        long *d_long_ptr;
        cudaMalloc((void **) &d_long_ptr, row_num * sizeof(long));
        cudaMemcpy(d_long_ptr, column->data_ptr, row_num * sizeof(long), cudaMemcpyHostToDevice);
        d_keys_vec[l] = d_long_ptr;
        break;
      }
      case DOUBLE: {
        d_keys_type[i] = 1;
        double *d_double_ptr;
        cudaMalloc((void **) &d_double_ptr, row_num * sizeof(double));
        cudaMemcpy(d_double_ptr, column->data_ptr, row_num * sizeof(double), cudaMemcpyHostToDevice);
        d_keys_vec[l] = d_double_ptr;
        break;
      }
      case INT: {
        d_keys_type[i] = 2;
        int32_t *d_int_ptr;
        cudaMalloc((void **) &d_int_ptr, row_num * sizeof(int32_t));
        cudaMemcpy(d_int_ptr, column->data_ptr, row_num * sizeof(int32_t), cudaMemcpyHostToDevice);
        d_keys_vec[l] = d_int_ptr;
        break;
      }
      case STRING: {
        d_keys_type[i] = 3;
        char *d_str_ptr = nullptr;
        int32_t *d_str_idx_ptr = nullptr;
        int32_t size_char = 0;
        for (int x = 0; x < row_num; x++) {
          // std::cout << "x = " << x << std::endl;
          size_char = ((int32_t *) column->data_ptr_aux)[2 * x] > size_char ?
                      ((int32_t *) column->data_ptr_aux)[2 * x] : size_char;
          // std::cout << "x = " << x << std::endl;
          size_char = ((int32_t *) column->data_ptr_aux)[2 * x + 1] > size_char ?
                      ((int32_t *) column->data_ptr_aux)[2 * x + 1] : size_char;
        }
        cudaMalloc((void **) &d_str_ptr, size_char * sizeof(char));
        cudaMemcpy(d_str_ptr, column->data_ptr, size_char * sizeof(char), cudaMemcpyHostToDevice);
        cudaMalloc((void **) &d_str_idx_ptr, 2 * row_num * sizeof(int32_t));
        cudaMemcpy(d_str_idx_ptr, column->data_ptr_aux, 2 * row_num * sizeof(int32_t), cudaMemcpyHostToDevice);
        d_keys_vec[l++] = d_str_ptr;
        d_keys_vec[l] = d_str_idx_ptr;
        break;
      }
      case DEPEND: {
        break;
      }
    }
  }


  profiler.start_gpu_alloc();
  const void **d_keys_data_ptr = thrust::raw_pointer_cast(d_keys_vec.data());
  const int *d_keys_type_ptr = thrust::raw_pointer_cast(d_keys_type.data());
  struct reducer_predicator rp(d_keys_data_ptr, d_keys_type_ptr, key_num);

  thrust::device_vector<uint32_t> d_result_idx(row_num);
  thrust::sequence(d_result_idx.begin(), d_result_idx.end());
  profiler.end_gpu_alloc();
  profiler.end_pci_host2device();
  profiler.start_sort();
  profiler.start_gpu_exec();
  thrust::sort(d_result_idx.begin(), d_result_idx.end(), sort_comparator(d_keys_data_ptr, d_keys_type_ptr, key_num));
  profiler.end_gpu_exec();
  profiler.end_sort();

  std::cout << "GHive-CPP [GroupByPredicate-execute]: Finish gpu sorting" << std::endl;

  thrust::host_vector<uint32_t> h_result_keys_idx;

  std::vector<Column *> result_columns;

  if (aggregation_descs.size() == 0) {
    no_aggregation(d_result_idx, h_result_keys_idx, rp, profiler);
  }
  for (int i = 0; i < aggregation_descs.size(); i++) {
    AggregationDesc desc = aggregation_descs[i];
    std::cout << "GHive-CPP [GroupByPredicate-execute]: desc_index: " << desc.index << std::endl;
    Column *column = input_tbl->columns[desc.index];
    switch (desc.type) {
      case SUM: {
        switch (column->type) {
          case LONG: {
            result_columns.push_back(aggregation_sum<long>(d_result_idx,
                                                           h_result_keys_idx,
                                                           column,
                                                           row_num,
                                                           rp,
                                                           profiler));
            break;
          }
          case DOUBLE: {
            result_columns.push_back(aggregation_sum<double>(d_result_idx,
                                                             h_result_keys_idx,
                                                             column,
                                                             row_num,
                                                             rp,
                                                             profiler));
            break;
          }
          case INT: {
            result_columns.push_back(aggregation_sum<int32_t>(d_result_idx,
                                                              h_result_keys_idx,
                                                              column,
                                                              row_num,
                                                              rp,
                                                              profiler));
            break;
          }
          case STRING:
          case DEPEND:
            std::cout << "GHive-CPP-ERROR [GroupByPredicate-execute]: Unsupported type for SUM operation"
                      << column->type << std::endl;
            break;
        }
        break;
      }
      case MAX: {
        switch (column->type) {
          case LONG: {
            result_columns.push_back(aggregation_max<long>(d_result_idx,
                                                           h_result_keys_idx,
                                                           column,
                                                           row_num,
                                                           rp,
                                                           profiler));
            break;
          }
          case DOUBLE: {
            result_columns.push_back(aggregation_max<double>(d_result_idx,
                                                             h_result_keys_idx,
                                                             column,
                                                             row_num,
                                                             rp,
                                                             profiler));
            break;
          }
          case INT: {
            result_columns.push_back(aggregation_max<int32_t>(d_result_idx,
                                                              h_result_keys_idx,
                                                              column,
                                                              row_num,
                                                              rp,
                                                              profiler));
            break;
          }
          case STRING:
          case DEPEND:
            std::cout << "GHive-CPP-ERROR [GroupByPredicate-execute]: Unsupported type for MAX operation"
                      << column->type << std::endl;
            break;
        }
        break;
      }
      case MIN: {
        switch (column->type) {
          case LONG: {
            result_columns.push_back(aggregation_min<long>(d_result_idx,
                                                           h_result_keys_idx,
                                                           column,
                                                           row_num,
                                                           rp,
                                                           profiler));
            break;
          }
          case DOUBLE: {
            result_columns.push_back(aggregation_min<double>(d_result_idx,
                                                             h_result_keys_idx,
                                                             column,
                                                             row_num,
                                                             rp,
                                                             profiler));
            break;
          }
          case INT: {
            result_columns.push_back(aggregation_min<int32_t>(d_result_idx,
                                                              h_result_keys_idx,
                                                              column,
                                                              row_num,
                                                              rp,
                                                              profiler));
            break;
          }
          case STRING:
          case DEPEND:
            std::cout << "GHive-CPP-ERROR [GroupByPredicate-execute]: Unsupported type for MIN operation"
                      << column->type << std::endl;
            break;
        }
        break;
      }
      case AVG: {
        switch (column->type) {
          case LONG: {
            result_columns.push_back(aggregation_avg<long>(d_result_idx,
                                                           h_result_keys_idx,
                                                           column,
                                                           row_num,
                                                           rp,
                                                           profiler));
            break;
          }
          case DOUBLE: {
            result_columns.push_back(aggregation_avg<double>(d_result_idx,
                                                             h_result_keys_idx,
                                                             column,
                                                             row_num,
                                                             rp,
                                                             profiler));
            break;
          }
          case INT: {
            result_columns.push_back(aggregation_avg<int32_t>(d_result_idx,
                                                              h_result_keys_idx,
                                                              column,
                                                              row_num,
                                                              rp,
                                                              profiler));
            break;
          }
          case STRING:
          case DEPEND:
            std::cout << "GHive-CPP-ERROR [GroupByPredicate-execute]: Unsupported type for MIN operation"
                      << column->type << std::endl;
            break;
        }
        break;
      }
      case CNT: {
        result_columns.push_back(aggregation_cnt(d_result_idx, h_result_keys_idx, row_num, rp, profiler));
        break;
      }
      case RANK: {
        break;
      }
      case UNKNOWN: {
        break;
      }
    }
  }
  profiler.start_data_recover();
  for (int32_t i = keys.size() - 1; i >= 0; i--) {
    Column *key_column = input_tbl->columns[keys[i]];
    switch (key_column->type) {
      case LONG: {
        long *original_col = (long *) key_column->data_ptr;
        uint32_t new_size = h_result_keys_idx.size();
        long *new_col = new long[new_size];
        for (uint32_t j = 0; j < new_size; j++) {
          new_col[j] = original_col[h_result_keys_idx[j]];
        }
        Column *result_col = new Column(LONG, new_size, new_col);
        result_columns.insert(result_columns.begin(), result_col);
        break;
      }
      case DOUBLE: {
        double *original_col = (double *) key_column->data_ptr;
        uint32_t new_size = h_result_keys_idx.size();
        double *new_col = new double[new_size];
        for (uint32_t j = 0; j < new_size; j++) {
          new_col[j] = original_col[h_result_keys_idx[j]];
        }
        Column *result_col = new Column(DOUBLE, new_size, new_col);
        result_columns.insert(result_columns.begin(), result_col);
        break;
      }
      case INT: {
        int32_t *original_col = (int32_t *) key_column->data_ptr;
        uint32_t new_size = h_result_keys_idx.size();
        int32_t *new_col = new int32_t[new_size];
        for (uint32_t j = 0; j < new_size; j++) {
          new_col[j] = original_col[h_result_keys_idx[j]];
        }
        Column *result_col = new Column(INT, new_size, new_col);
        result_columns.insert(result_columns.begin(), result_col);
        break;
      }
      case STRING: {
        int32_t *str_idx_col = (int32_t *) key_column->data_ptr_aux;
        uint32_t new_size = h_result_keys_idx.size();
        uint32_t char_size = 0;
        for (uint32_t j = 0; j < new_size; j++) {
          uint32_t idx = h_result_keys_idx[j];
          if (str_idx_col[2 * idx] >= 0) {
            char_size += str_idx_col[2 * idx + 1] - str_idx_col[2 * idx];
          }
        }
        char *str_col_old = (char *) key_column->data_ptr;
        char *str_col_new = new char[char_size];
        int32_t *str_idx_col_new = new int32_t[new_size * 2];
        uint32_t char_idx = 0;
        for (uint32_t j = 0; j < new_size; j++) {
          uint32_t idx = h_result_keys_idx[j];
          if (str_idx_col[2 * idx] >= 0) {
            uint32_t str_len = str_idx_col[2 * idx + 1] - str_idx_col[2 * idx];
            memcpy(str_col_new + char_idx, str_col_old + str_idx_col[2 * idx], str_len * sizeof(char));
            str_idx_col_new[2 * j] = char_idx;
            char_idx += str_len;
            str_idx_col_new[2 * j + 1] = char_idx;
          }
        }
        Column *column = new Column(STRING, new_size, str_col_new, str_idx_col_new, char_size);
        result_columns.insert(result_columns.begin(), column);
      }
      case DEPEND:break;
    }
  }
  Table *result_tbl = new Table();
  result_tbl->columns = result_columns;
  if (key_num != 0) {
    result_tbl->row_num = h_result_keys_idx.size();
  } else {
    result_tbl->row_num = 1;
  }
  profiler.end_data_recover();
  return result_tbl;
}

Table *GroupByPredicate::cpu_execute(Table *input_tbl, SortGroupByProfiler &profiler) {
  profiler.start_sort();
  uint32_t row_num = input_tbl->row_num;
  uint32_t key_num = keys.size();//the number of group by columns.
  uint32_t value_num = aggregation_descs.size();

  uint32_t key_vec_size = key_num;
  for (uint32_t key: keys) {
    if (input_tbl->columns[key]->type == STRING) {
      key_vec_size++;
    }
  }

  thrust::host_vector<const void *> h_keys_vec(key_vec_size);
  thrust::host_vector<int> h_keys_type(key_num);
  for (uint32_t i = 0, l = 0; i < keys.size(); i++, l++) {
    Column *column = input_tbl->columns[keys[i]];
    switch (column->type) {
      case LONG: {
        h_keys_type[i] = 0;
        h_keys_vec[l] = column->data_ptr;
        break;
      }
      case DOUBLE: {
        h_keys_type[i] = 1;
        h_keys_vec[l] = column->data_ptr;
        break;
      }
      case INT: {
        h_keys_type[i] = 2;
        h_keys_vec[l] = column->data_ptr;
        break;
      }
      case STRING: {
        h_keys_type[i] = 3;
        h_keys_vec[l++] = column->data_ptr;
        h_keys_vec[l] = column->data_ptr_aux;
        break;
      }
      case DEPEND: {
        break;
      }
    }
  }

  const void **h_keys_data_ptr = h_keys_vec.data();
  const int *h_keys_type_ptr = h_keys_type.data();

  thrust::host_vector<uint32_t> result_sorted_idx(row_num);
  thrust::sequence(result_sorted_idx.begin(), result_sorted_idx.end());
  thrust::sort(result_sorted_idx.begin(), result_sorted_idx.end(),
               sort_comparator(h_keys_data_ptr, h_keys_type_ptr, key_num));
  std::cout << "GHive-CPP [GroupByPredicate-execute]: Finish cpu sorting" << std::endl;
  profiler.end_sort();
  profiler.start_agg();
  thrust::host_vector<uint32_t> h_result_keys_idx;

  struct reducer_predicator rp(h_keys_data_ptr, h_keys_type_ptr, key_num);
  std::vector<Column *> result_columns;

  for (int32_t i = 0; i < aggregation_descs.size(); i++) {
    AggregationDesc desc = aggregation_descs[i];
    std::cout << "GHive-CPP [GroupByPredicate-execute]: desc_index: " << desc.index << std::endl;
    Column *column = input_tbl->columns[desc.index];
    switch (desc.type) {
      case SUM: {
        switch (column->type) {
          case LONG: {
            result_columns.push_back(aggregation_sum_cpu<long>(result_sorted_idx,
                                                               h_result_keys_idx,
                                                               column,
                                                               row_num,
                                                               rp));
            break;
          }
          case DOUBLE: {
            result_columns.push_back(aggregation_sum_cpu<double>(result_sorted_idx,
                                                                 h_result_keys_idx,
                                                                 column,
                                                                 row_num,
                                                                 rp));
            break;
          }
          case INT: {
            result_columns.push_back(aggregation_sum_cpu<int32_t>(result_sorted_idx,
                                                                  h_result_keys_idx,
                                                                  column,
                                                                  row_num,
                                                                  rp));
            break;
          }
          case STRING:
          case DEPEND:
            std::cout << "GHive-CPP-ERROR [GroupByPredicate-execute]: Unsupported type for SUM operation"
                      << column->type << std::endl;
            break;
        }
        break;
      }
      case MAX: {
        break;
      }
      case MIN: {
        break;
      }
      case AVG: {
        break;
      }
      case CNT: {
        break;
      }
      case RANK: {
        break;
      }
      case UNKNOWN: {
        break;
      }
    }

  }
  profiler.end_agg();

  return nullptr;
}

Column *GroupByPredicate::no_aggregation(thrust::device_vector<uint32_t> &d_result_idx,
                                         thrust::host_vector<uint32_t> &result_key_idx,
                                         struct reducer_predicator &rp, SortGroupByProfiler &profiler) {

  std::cout << "GHive-CPP [GroupByPredicate-no_aggregation]" << std::endl;

  profiler.start_agg();
  profiler.start_gpu_exec();
  auto new_end = thrust::unique(d_result_idx.begin(), d_result_idx.end(), rp);
  profiler.end_gpu_exec();
  profiler.start_agg();

  profiler.start_pci_device2host();
  result_key_idx = thrust::host_vector<int32_t>(d_result_idx.begin(), new_end);
  profiler.end_pci_device2host();

  return nullptr;

}

template<typename T>
Column *GroupByPredicate::aggregation_sum(thrust::device_vector<uint32_t> &d_result_idx,
                                          thrust::host_vector<uint32_t> &result_key_idx,
                                          Column *column, uint32_t row_num,
                                          struct reducer_predicator &rp, SortGroupByProfiler &profiler) {
  std::cout << "GHive-CPP [GroupByPredicate-aggregation_sum]" << std::endl;
  T *p_data = (T *) column->data_ptr;
  profiler.start_pci_host2device();
  thrust::device_vector<T> d_input_values(p_data, p_data + row_num);
  profiler.end_pci_host2device();
  if (keys.size() != 0) {
    profiler.start_gpu_alloc();
    thrust::device_vector<int> d_result_keys_idx(row_num);
    thrust::device_vector<T> d_result_values(row_num);
    profiler.end_gpu_alloc();
    profiler.start_agg();
    profiler.start_gpu_exec();
    auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                         thrust::make_permutation_iterator(d_input_values.begin(),
                                                                           d_result_idx.begin()),
                                         d_result_keys_idx.begin(), d_result_values.begin(), rp,
                                         agg_plus<T>());
    profiler.end_gpu_exec();
    profiler.end_agg();
    profiler.start_data_recover();
    profiler.start_cpu_alloc();
    result_key_idx = thrust::host_vector<int32_t>(d_result_keys_idx.begin(), new_end.first);
    uint32_t result_size = new_end.second - d_result_values.begin();
    T *p_result = new T[result_size];
    profiler.end_cpu_alloc();
    profiler.end_data_recover();
    profiler.start_pci_device2host();
    cudaMemcpy(p_result, thrust::raw_pointer_cast(d_result_values.data()),
               result_size * sizeof(T), cudaMemcpyDeviceToHost);
    profiler.end_pci_device2host();
    return new Column(column->type, result_size, p_result);
  } else {
    T *p_result = new T[1];
    profiler.start_agg();
    profiler.start_gpu_exec();
    p_result[0] = thrust::reduce(d_input_values.begin(), d_input_values.end(), (T) 0.0, agg_plus<T>());
    profiler.end_gpu_exec();
    profiler.end_agg();
    return new Column(column->type, 1, p_result);
  }
}

template<typename T>
Column *GroupByPredicate::aggregation_max(thrust::device_vector<uint32_t> &d_result_idx,
                                          thrust::host_vector<uint32_t> &result_key_idx,
                                          Column *column,
                                          uint32_t row_num,
                                          struct reducer_predicator &rp,
                                          SortGroupByProfiler &profiler) {
  T *p_data = (T *) column->data_ptr;
  profiler.start_pci_host2device();
  thrust::device_vector<T> d_input_values(p_data, p_data + row_num);
  profiler.end_pci_host2device();
  if (keys.size() != 0) {
    profiler.start_gpu_alloc();
    thrust::device_vector<int> d_result_keys_idx(row_num);
    thrust::device_vector<T> d_result_values(row_num);
    profiler.end_gpu_alloc();
    profiler.start_agg();
    auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                         thrust::make_permutation_iterator(d_input_values.begin(),
                                                                           d_result_idx.begin()),
                                         d_result_keys_idx.begin(), d_result_values.begin(), rp,
                                         agg_maximum<T>());
    profiler.end_agg();
    profiler.start_cpu_alloc();
    result_key_idx = thrust::host_vector<int32_t>(d_result_keys_idx.begin(), new_end.first);
    uint32_t result_size = new_end.second - d_result_values.begin();
    T *p_result = new T[result_size];
    profiler.end_cpu_alloc();
    profiler.start_pci_device2host();
    cudaMemcpy(p_result, thrust::raw_pointer_cast(d_result_values.data()),
               result_size * sizeof(T), cudaMemcpyDeviceToHost);
    profiler.end_pci_device2host();
    return new Column(column->type, result_size, p_result);
  } else {
    T *p_result = new T[1];
    profiler.start_agg();
    p_result[0] = thrust::reduce(d_input_values.begin(), d_input_values.end(), 0, agg_maximum<T>());
    profiler.end_agg();
    return new Column(column->type, 1, p_result);
  }
}

template<typename T>
Column *GroupByPredicate::aggregation_min(thrust::device_vector<uint32_t> &d_result_idx,
                                          thrust::host_vector<uint32_t> &result_key_idx,
                                          Column *column, uint32_t row_num,
                                          struct reducer_predicator &rp, SortGroupByProfiler &profiler) {
  T *p_data = (T *) column->data_ptr;
  profiler.start_pci_host2device();
  thrust::device_vector<T> d_input_values(p_data, p_data + row_num);
  profiler.end_pci_host2device();
  if (keys.size() != 0) {
    profiler.start_gpu_alloc();
    thrust::device_vector<int> d_result_keys_idx(row_num);
    thrust::device_vector<T> d_result_values(row_num);
    profiler.end_gpu_alloc();
    profiler.start_agg();
    auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                         thrust::make_permutation_iterator(d_input_values.begin(),
                                                                           d_result_idx.begin()),
                                         d_result_keys_idx.begin(), d_result_values.begin(), rp,
                                         agg_minimum<T>());
    profiler.end_agg();
    profiler.start_cpu_alloc();
    result_key_idx = thrust::host_vector<int32_t>(d_result_keys_idx.begin(), new_end.first);
    uint32_t result_size = new_end.second - d_result_values.begin();
    T *p_result = new T[result_size];
    profiler.end_cpu_alloc();
    profiler.start_pci_device2host();
    cudaMemcpy(p_result, thrust::raw_pointer_cast(d_result_values.data()),
               result_size * sizeof(T), cudaMemcpyDeviceToHost);
    profiler.end_pci_device2host();
    return new Column(column->type, result_size, p_result);
  } else {
    T *p_result = new T[1];
    p_result[0] = thrust::reduce(d_input_values.begin(), d_input_values.end(), 0, agg_minimum<T>());
    return new Column(column->type, 1, p_result);
  }
}

template<typename T>
Column *GroupByPredicate::aggregation_avg(thrust::device_vector<uint32_t> &d_result_idx,
                                          thrust::host_vector<uint32_t> &result_key_idx,
                                          Column *column,
                                          uint32_t row_num,
                                          struct reducer_predicator &rp,
                                          SortGroupByProfiler &profiler) {
  T *p_data = (T *) column->data_ptr;
  profiler.start_pci_host2device();
  thrust::device_vector<T> d_input_values(p_data, p_data + row_num);
  profiler.end_pci_host2device();
  if (keys.size() != 0) {
    profiler.start_gpu_alloc();
    thrust::device_vector<int32_t> d_result_cnt(row_num);
    thrust::device_vector<int> d_result_keys_idx(row_num);
    profiler.end_gpu_alloc();
    profiler.start_agg();
    auto cnt_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                         thrust::make_permutation_iterator(d_input_values.begin(),
                                                                           d_result_idx.begin()),
                                         d_result_keys_idx.begin(), d_result_cnt.begin(),
                                         rp, agg_plus<int32_t>());
    profiler.end_agg();
    profiler.start_gpu_alloc();
    thrust::device_vector<double> d_avg_result(row_num);
    profiler.end_gpu_alloc();
    profiler.start_agg();
    auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                         thrust::make_permutation_iterator(d_input_values.begin(),
                                                                           d_result_idx.begin()),
                                         d_result_keys_idx.begin(), d_avg_result.begin(),
                                         rp, agg_plus<T>());
    thrust::transform(d_avg_result.begin(), new_end.second, d_result_cnt.begin(),
                      d_avg_result.begin(), divides<double>());
    profiler.end_agg();
    profiler.start_cpu_alloc();
    result_key_idx = thrust::host_vector<int32_t>(d_result_keys_idx.begin(), new_end.first);
    uint32_t result_size = cnt_end.second - d_result_cnt.begin();
    profiler.end_cpu_alloc();
    profiler.start_pci_device2host();
    double *p_result = new double[result_size];
    cudaMemcpy(p_result, thrust::raw_pointer_cast(d_avg_result.data()),
               result_size * sizeof(double), cudaMemcpyDeviceToHost);
    profiler.end_pci_device2host();
    return new Column(column->type, result_size, p_result);
  } else {
    T *p_result = new T[1];
    profiler.start_agg();
    T sum = thrust::reduce(d_input_values.begin(), d_input_values.end(), 0, agg_plus<T>());
    T cnt = thrust::reduce(d_input_values.begin(), d_input_values.end(), 0, agg_cnt<T>());
    profiler.end_agg();
    p_result[0] /= sum / cnt;
    return new Column(column->type, 1, p_result);
  }
}

Column *GroupByPredicate::aggregation_cnt(thrust::device_vector<uint32_t> &d_result_idx,
                                          thrust::host_vector<uint32_t> &result_key_idx,
                                          uint32_t row_num,
                                          struct reducer_predicator &rp,
                                          SortGroupByProfiler &profiler) {
  if (keys.size() != 0) {
    profiler.start_gpu_alloc();
    thrust::device_vector<int> d_result_keys_idx(row_num);
    thrust::device_vector<int32_t> d_result_values(row_num);
    profiler.end_gpu_alloc();
    profiler.start_agg();
    auto
        new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(), thrust::constant_iterator<int32_t>(1),
                                        d_result_keys_idx.begin(), d_result_values.begin(), rp,
                                        thrust::plus<int32_t>());
    profiler.end_agg();
    profiler.start_cpu_alloc();
    result_key_idx = thrust::host_vector<int32_t>(d_result_keys_idx.begin(), new_end.first);
    uint32_t result_size = new_end.second - d_result_values.begin();
    int32_t *p_result = new int32_t[result_size];
    profiler.end_cpu_alloc();
    profiler.start_pci_device2host();
    cudaMemcpy(p_result, thrust::raw_pointer_cast(d_result_values.data()),
               result_size * sizeof(int32_t), cudaMemcpyDeviceToHost);
    profiler.end_pci_device2host();
    return new Column(INT, result_size, p_result);
  } else {
    int32_t *p_result = new int32_t[1];
    p_result[0] = row_num;
    return new Column(INT, 1, p_result);
  }
}

template<typename T>
Column *GroupByPredicate::aggregation_sum_cpu(thrust::host_vector<uint32_t> &d_result_idx,
                                              thrust::host_vector<uint32_t> &result_key_idx,
                                              Column *column,
                                              uint32_t row_num,
                                              reducer_predicator &rp) {
  std::cout << "GHive-CPP [GroupByPredicate-aggregation_sum]" << std::endl;
  T *p_data = (T *) column->data_ptr;
  if (keys.size() != 0) {
    thrust::host_vector<int> h_result_keys_idx(row_num);
    T *result_values = new T[row_num];
    auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                         thrust::make_permutation_iterator(p_data,
                                                                           d_result_idx.begin()),
                                         h_result_keys_idx.begin(), result_values, rp,
                                         agg_plus<T>());
    result_key_idx = thrust::host_vector<int32_t>(h_result_keys_idx.begin(), new_end.first);
    uint32_t result_size = new_end.second - result_values;

    return new Column(column->type, result_size, result_values);
  } else {
    T *p_result = new T[1];
    p_result[0] = thrust::reduce(p_data, p_data + row_num, 0, agg_plus<T>());
    return new Column(column->type, 1, p_result);
  }

}


