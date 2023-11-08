#include <Operator/PTFPredicate.hpp>
#include <map>
#include <thrust/device_vector.h>
#include <regex>
#include <Operator/GroupByPredicate.hpp>
/*
DataFlow *PTFPredicate::execute(DataFlow *data_flow, Profiler &profiler){
  /*
  std::vector<uint32_t> sequence = data_flow->get_sequence();
  uint32_t row_num = data_flow->get_row_num();
  uint32_t partition_by_num = partition_by_col.size();
  uint32_t order_by_num =order_by_col.size();
  uint32_t window_function_num =window_functions.size();

  //string need one more col
  uint32_t partition_vec_size = partition_by_num ;
  for (unsigned int i : partition_by_col) {
    if (sequence[i] >= data_flow->longColNum + data_flow->doubleColNum + data_flow->intColNum) {
      partition_vec_size++;
    }
  }
  uint32_t order_by_vec_size = order_by_num ;
  for (unsigned int i : order_by_col) {
    if (sequence[i] >= data_flow->longColNum + data_flow->doubleColNum + data_flow->intColNum) {
      order_by_vec_size++;
    }
  }


  thrust::device_vector<const void *> d_key_vec(partition_vec_size+order_by_num);
  thrust::device_vector<const void *> d_partition_vec(partition_by_num);
  thrust::device_vector<int> d_keys_type(partition_vec_size+order_by_vec_size);

  //partition col
  int i=0;int l=0;
  cout<<"partition_by_num: "<<partition_by_num<<endl;
  for (; i < partition_by_num; i++, l++) {
    uint32_t ith_col = sequence[partition_by_col[i]];
    if (ith_col < data_flow->longColNum) {
      d_keys_type[i] = 0 ;
      std::cout << "partition long columns: " << std::endl;
      for (int x = 0; x < row_num; x ++) {
        std::cout << data_flow->longCols[ith_col][x] << " ";
        if (x > 1000) break;
      }
      cout<<endl;
      long *d_long_ptr;
      cudaMalloc((void **) &d_long_ptr, row_num * sizeof(long));
      cudaMemcpy(d_long_ptr, data_flow->longCols[ith_col], row_num * sizeof(long), cudaMemcpyHostToDevice);
      d_key_vec[l] = d_long_ptr;
      d_partition_vec[l]=d_long_ptr;
    } else if (ith_col < data_flow->longColNum + data_flow->doubleColNum) {
      d_keys_type[i] = 1;
      uint32_t idx = ith_col - data_flow->longColNum;
      double *d_double_ptr;
      cudaMalloc((void **) &d_double_ptr, row_num * sizeof(double));
      cudaMemcpy(d_double_ptr, data_flow->doubleCols[idx], row_num * sizeof(double), cudaMemcpyHostToDevice);
      d_key_vec[l] = d_double_ptr;
      d_partition_vec[l]=d_double_ptr;
    } else if (ith_col < data_flow->longColNum + data_flow->doubleColNum + data_flow->intColNum) {
      d_keys_type[i] = 2;
      uint32_t idx = ith_col - data_flow->longColNum - data_flow->doubleColNum;
      int *d_int_ptr;
      cudaMalloc((void **) &d_int_ptr, row_num * sizeof(int));
      cudaMemcpy(d_int_ptr, data_flow->intCols[idx], row_num * sizeof(int), cudaMemcpyHostToDevice);
      d_key_vec[l] = d_int_ptr;
      d_partition_vec[l]=d_int_ptr;
    } else { // String
      d_keys_type[i] = 3;
      uint32_t idx = ith_col - data_flow->longColNum - data_flow->doubleColNum - data_flow->intColNum;
      char *d_str_ptr;
      int *d_str_idx_ptr;
      uint32_t size_char = 0;
      for (int x = 0; x < row_num; x++) {
        size_char = data_flow->strIdxCols[idx][2 * x + 1] > size_char ?
                    data_flow->strIdxCols[idx][2 * x + 1] : size_char;
      }
      cudaMalloc((void **) &d_str_ptr, (size_char + 1) * sizeof(char));
      cudaMemcpy(d_str_ptr, data_flow->strCols[idx], (size_char + 1) * sizeof(char), cudaMemcpyHostToDevice);

      cudaMalloc((void **) &d_str_idx_ptr, 2 * row_num * sizeof(int));
      cudaMemcpy(d_str_idx_ptr, data_flow->strIdxCols[idx], 2 * row_num * sizeof(int), cudaMemcpyHostToDevice);
      d_key_vec[l++] = d_str_ptr;
      d_key_vec[l] = d_str_idx_ptr;
      d_partition_vec[l]=d_str_ptr;
      d_partition_vec[l]=d_str_idx_ptr;
    }
  }
  //order_by col
  for (; i <partition_by_num+ order_by_num; i++, l++) {
    uint32_t ith_col = sequence[order_by_col[i]];
    if (ith_col < data_flow->longColNum) {
      d_keys_type[i] = 0 ;
      for (int x = 0; x < row_num; x ++) {
        std::cout << data_flow->longCols[ith_col][x] << " ";
        if (x > 1000) break;
      }
      long *d_long_ptr;
      cudaMalloc((void **) &d_long_ptr, row_num * sizeof(long));
      cudaMemcpy(d_long_ptr, data_flow->longCols[ith_col], row_num * sizeof(long), cudaMemcpyHostToDevice);
      d_key_vec[l] = d_long_ptr;
    } else if (ith_col < data_flow->longColNum + data_flow->doubleColNum) {
      d_keys_type[i] = 1;
      uint32_t idx = ith_col - data_flow->longColNum;
      double *d_double_ptr;
      cudaMalloc((void **) &d_double_ptr, row_num * sizeof(double));
      cudaMemcpy(d_double_ptr, data_flow->doubleCols[idx], row_num * sizeof(double), cudaMemcpyHostToDevice);
      d_key_vec[l] = d_double_ptr;
    } else if (ith_col < data_flow->longColNum + data_flow->doubleColNum + data_flow->intColNum) {
      d_keys_type[i] = 2;
      uint32_t idx = ith_col - data_flow->longColNum - data_flow->doubleColNum;
      int *d_int_ptr;
      cudaMalloc((void **) &d_int_ptr, row_num * sizeof(int));
      cudaMemcpy(d_int_ptr, data_flow->intCols[idx], row_num * sizeof(int), cudaMemcpyHostToDevice);
      d_key_vec[l] = d_int_ptr;
    } else { // String
      d_keys_type[i] = 3;
      uint32_t idx = ith_col - data_flow->longColNum - data_flow->doubleColNum - data_flow->intColNum;
      char *d_str_ptr;
      int *d_str_idx_ptr;
      uint32_t size_char = 0;
      for (int x = 0; x < row_num; x++) {

        size_char = data_flow->strIdxCols[idx][2 * x + 1] > size_char ?
                    data_flow->strIdxCols[idx][2 * x + 1] : size_char;
      }
      cudaMalloc((void **) &d_str_ptr, (size_char + 1) * sizeof(char));
      cudaMemcpy(d_str_ptr, data_flow->strCols[idx], (size_char + 1) * sizeof(char), cudaMemcpyHostToDevice);

      cudaMalloc((void **) &d_str_idx_ptr, 2 * row_num * sizeof(int));
      cudaMemcpy(d_str_idx_ptr, data_flow->strIdxCols[idx], 2 * row_num * sizeof(int), cudaMemcpyHostToDevice);
      d_key_vec[l++] = d_str_ptr;
      d_key_vec[l] = d_str_idx_ptr;
    }
  }


  const void **d_keys_data_ptr = thrust::raw_pointer_cast(d_key_vec.data());
  const int *d_keys_type_ptr = thrust::raw_pointer_cast(d_keys_type.data());
  thrust::device_vector<int> d_result_idx(row_num);
  thrust::host_vector<int>h_result_idx(row_num);
  thrust::sequence(d_result_idx.begin(), d_result_idx.end());
  //TODO::CONSIDER ASC DEC!!!
  thrust::sort(d_result_idx.begin(), d_result_idx.end(), sort_comparator(d_keys_data_ptr, d_keys_type_ptr, order_by_num+partition_by_num));

  cudaMemcpy(h_result_idx.data(), thrust::raw_pointer_cast(d_result_idx.data()),
             row_num * sizeof(int), cudaMemcpyDeviceToHost);


  const void **d_partition_data_ptr = thrust::raw_pointer_cast(d_partition_vec.data());
  thrust::device_vector<int> d_result_keys_idx(row_num);
  thrust::host_vector<int> h_result_keys_idx;
  thrust::host_vector<void *> h_result_values(window_function_num);
  thrust::host_vector<uint32_t * >h_group_number(window_function_num);
  int values_type[window_function_num];
  thrust::host_vector<void *> window_value(window_function_num);

  uint32_t result_long_col_num = data_flow->longColNum;
  uint32_t result_double_col_num = data_flow->doubleColNum;
  uint32_t result_int_col_num = data_flow->intColNum;
  uint32_t result_str_col_num = data_flow->stringColNum;
  
  cout<<window_functions.size()<<endl;
  for (int j = 0; j < window_functions.size(); j++) {
    WindowFunction each_window = window_functions[j];
    uint32_t function_index = sequence[each_window.arguments];
    long result_size=0;
    switch (each_window.type) {
      case SUM: {
        thrust::device_vector<uint32_t> d_result_nums(row_num);
        //reduce for number
        auto cnt_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                                 thrust::constant_iterator<uint32_t>(1),
                                                 d_result_keys_idx.begin(), d_result_nums.begin(),
                                                 reducer_predicator(d_partition_data_ptr, d_keys_type_ptr,
                                                                    partition_by_num),
                                                 thrust::plus<uint32_t>());
        result_size = cnt_end.second - d_result_nums.begin();
        uint32_t *p_group_num = new uint32_t[result_size];
        cudaMemcpy(p_group_num, thrust::raw_pointer_cast(d_result_nums.data()),
                   result_size * sizeof(uint32_t), cudaMemcpyDeviceToHost);
        h_group_number[j] = p_group_num;

        if (function_index < data_flow->longColNum) {
          values_type[j] = 0;
          result_long_col_num++;
          thrust::device_vector<long> d_input_values(data_flow->longCols[function_index],
                                                     data_flow->longCols[function_index] + row_num);
          if (each_window.windowFrame == ROWS_PRECEDING_FOLLOWING) {
            //reduce for value
            thrust::device_vector<long> d_result_values(row_num);
            auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                                       thrust::make_permutation_iterator(d_input_values.begin(),
                                                                                         d_result_idx.begin()),
                                                       d_result_keys_idx.begin(), d_result_values.begin(),
                                                       reducer_predicator(d_partition_data_ptr, d_keys_type_ptr,
                                                                          partition_by_num),
                                                       thrust::plus<long>());
            //h_result_keys_idx = thrust::host_vector<int>(d_result_keys_idx.begin(), new_end.first);
            result_size = new_end.second - d_result_values.begin();
            long *p_result = new long[result_size];
            cudaMemcpy(p_result, thrust::raw_pointer_cast(d_result_values.data()),
                       result_size * sizeof(long), cudaMemcpyDeviceToHost);
            h_result_values[j] = p_result;

            long *result_value = static_cast<long *>(h_result_values[j]);
            long *each_value = new long[row_num];
            uint32_t *each_num = h_group_number[j];
            for (int k = 0, m = 0, idx = 1; k < row_num & m < result_size; k++) {
              each_value[k] = result_value[m];
              idx++;
              if (idx > each_num[m]) {
                idx = 1;
                m++;
              }
            }
            window_value[j]=each_value;
          } else {
            long *each_value = new long[row_num];
            uint32_t *each_num = h_group_number[j];
            uint32_t idx = 0;
            for (int k = 0; k < result_size; k++) {
              thrust::inclusive_scan(thrust::make_permutation_iterator(d_input_values.begin(),
                                                                       d_result_idx.begin()) + idx,
                                     thrust::make_permutation_iterator(d_input_values.begin(),
                                                                       d_result_idx.begin()) + idx + each_num[k],
                                     each_value + idx, thrust::plus<long>());
              idx = each_num[k];
            }
            window_value[j]=each_value;
          }
        } 
        else if (function_index < data_flow->longColNum + data_flow->doubleColNum) {
          values_type[j] = 1;
          result_double_col_num++;
          uint32_t index = function_index - data_flow->longColNum;
          thrust::device_vector<double> d_input_values(data_flow->doubleCols[index],
                                                       data_flow->doubleCols[index] + row_num);

          if (each_window.windowFrame == ROWS_PRECEDING_FOLLOWING) {
            thrust::device_vector<double> d_result_values(row_num);
            //reduce for value
            auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                                       thrust::make_permutation_iterator(d_input_values.begin(),
                                                                                         d_result_idx.begin()),
                                                       d_result_keys_idx.begin(), d_result_values.begin(),
                                                       reducer_predicator(d_partition_data_ptr, d_keys_type_ptr,
                                                                          partition_by_num),
                                                       thrust::plus<double>());
            //h_result_keys_idx = thrust::host_vector<int>(d_result_keys_idx.begin(), new_end.first);
            result_size = new_end.second - d_result_values.begin();
            double *p_result = new double[result_size];
            cudaMemcpy(p_result, thrust::raw_pointer_cast(d_result_values.data()),
                       result_size * sizeof(double), cudaMemcpyDeviceToHost);
            h_result_values[j] = p_result;

            double *result_value = static_cast<double *>(h_result_values[j]);
            double *each_value = new double[row_num];
            uint32_t *each_num = h_group_number[j];
            for (int k = 0, m = 0, idx = 1; k < row_num & m < result_size; k++) {
              each_value[k] = result_value[m];
              idx++;
              if (idx > each_num[m]) {
                idx = 1;
                m++;
              }
            }
            window_value[j]=each_value;
          } else {
            double *each_value = new double[row_num];
            uint32_t *each_num = h_group_number[j];
            uint32_t idx = 0;
            for (int k = 0; k < result_size; k++) {
              thrust::inclusive_scan(thrust::make_permutation_iterator(d_input_values.begin(),
                                                                       d_result_idx.begin()) + idx,
                                     thrust::make_permutation_iterator(d_input_values.begin(),
                                                                       d_result_idx.begin()) + idx + each_num[k],
                                     each_value + idx, thrust::plus<double>());
              idx = each_num[k];
            }
            window_value[j]=each_value;
          }
        } 
        else if (function_index < data_flow->longColNum + data_flow->doubleColNum + data_flow->intColNum) {
          values_type[j] = 2;
          result_int_col_num++;
          uint32_t index = function_index - data_flow->longColNum - data_flow->doubleColNum;
          thrust::device_vector<int> d_input_values(data_flow->intCols[index], data_flow->intCols[index] + row_num);

          if (each_window.windowFrame == ROWS_PRECEDING_FOLLOWING) {
            thrust::device_vector<int> d_result_values(row_num);
            //reduce for value
            auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                                       thrust::make_permutation_iterator(d_input_values.begin(),
                                                                                         d_result_idx.begin()),
                                                       d_result_keys_idx.begin(), d_result_values.begin(),
                                                       reducer_predicator(d_partition_data_ptr, d_keys_type_ptr,
                                                                          partition_by_num),
                                                       thrust::plus<int>());
            //h_result_keys_idx = thrust::host_vector<int>(d_result_keys_idx.begin(), new_end.first);
            result_size = new_end.second - d_result_values.begin();
            int32_t *p_result = new int32_t[result_size];
            cudaMemcpy(p_result, thrust::raw_pointer_cast(d_result_values.data()),
                       result_size * sizeof(int32_t), cudaMemcpyDeviceToHost);
            h_result_values[j] = p_result;

            int *result_value = static_cast<int *>(h_result_values[j]);
            int *each_value = new int[row_num];
            uint32_t *each_num = h_group_number[j];
            for (int k = 0, m = 0, idx = 1; k < row_num & m < result_size; k++) {
              each_value[k] = result_value[m];
              idx++;
              if (idx > each_num[m]) {
                idx = 1;
                m++;
              }
            }
            cout << "debug_point_0" << endl;
            for (int z = 0; z < row_num; z++) {
              cout << each_value[z] << endl;
            }
            window_value[j] = each_value;

          } else {
            int *each_value = new int[row_num];
            uint32_t *each_num = h_group_number[j];
            uint32_t idx = 0;
            for (int k = 0; k < result_size; k++) {
              thrust::inclusive_scan(thrust::make_permutation_iterator(d_input_values.begin(),
                                                                       d_result_idx.begin()) + idx,
                                     thrust::make_permutation_iterator(d_input_values.begin(),
                                                                       d_result_idx.begin()) + idx + each_num[k],
                                     each_value + idx, thrust::plus<int32_t>());
              idx = each_num[k];
            }
            window_value[j] = each_value;
          }
        } 
        else {
          result_str_col_num++;
        }
        break;
      }
      case MAX: {
        thrust::device_vector<uint32_t> d_result_nums(row_num);
        //reduce for number
        auto cnt_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                                 thrust::constant_iterator<uint32_t>(1),
                                                 d_result_keys_idx.begin(), d_result_nums.begin(),
                                                 reducer_predicator(d_partition_data_ptr, d_keys_type_ptr,
                                                                    partition_by_num),
                                                 thrust::plus<uint32_t>());
        result_size = cnt_end.second - d_result_nums.begin();
        uint32_t *p_group_num = new uint32_t[result_size];
        cudaMemcpy(p_group_num, thrust::raw_pointer_cast(d_result_nums.data()),
                   result_size * sizeof(uint32_t), cudaMemcpyDeviceToHost);
        h_group_number[j] = p_group_num;

        if (function_index < data_flow->longColNum) {
          values_type[j] = 0;
          result_long_col_num++;
          thrust::device_vector<long> d_input_values(data_flow->longCols[function_index],
                                                     data_flow->longCols[function_index] + row_num);
          if (each_window.windowFrame == ROWS_PRECEDING_FOLLOWING) {
            //reduce for value
            thrust::device_vector<long> d_result_values(row_num);
            auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                                       thrust::make_permutation_iterator(d_input_values.begin(),
                                                                                         d_result_idx.begin()),
                                                       d_result_keys_idx.begin(), d_result_values.begin(),
                                                       reducer_predicator(d_partition_data_ptr, d_keys_type_ptr,
                                                                          partition_by_num),
                                                       thrust::maximum<long>());
            //h_result_keys_idx = thrust::host_vector<int>(d_result_keys_idx.begin(), new_end.first);
            result_size = new_end.second - d_result_values.begin();
            long *p_result = new long[result_size];
            cudaMemcpy(p_result, thrust::raw_pointer_cast(d_result_values.data()),
                       result_size * sizeof(long), cudaMemcpyDeviceToHost);
            h_result_values[j] = p_result;

            long *result_value = static_cast<long *>(h_result_values[j]);
            long *each_value = new long[row_num];
            uint32_t *each_num = h_group_number[j];
            for (int k = 0, m = 0, idx = 1; k < row_num & m < result_size; k++) {
              each_value[k] = result_value[m];
              idx++;
              if (idx > each_num[m]) {
                idx = 1;
                m++;
              }
            }
            window_value[j]=each_value;
          } else {
            long *each_value = new long[row_num];
            uint32_t *each_num = h_group_number[j];
            uint32_t idx = 0;
            for (int k = 0; k < result_size; k++) {
              thrust::inclusive_scan(thrust::make_permutation_iterator(d_input_values.begin(),
                                                                       d_result_idx.begin()) + idx,
                                     thrust::make_permutation_iterator(d_input_values.begin(),
                                                                       d_result_idx.begin()) + idx + each_num[k],
                                     each_value + idx, thrust::maximum<long>());
              idx = each_num[k];
            }
            window_value[j]=each_value;
          }
        }
        else if (function_index < data_flow->longColNum + data_flow->doubleColNum) {
          values_type[j] = 1;
          result_double_col_num++;
          uint32_t index = function_index - data_flow->longColNum;
          thrust::device_vector<double> d_input_values(data_flow->doubleCols[index],
                                                       data_flow->doubleCols[index] + row_num);

          if (each_window.windowFrame == ROWS_PRECEDING_FOLLOWING) {
            thrust::device_vector<double> d_result_values(row_num);
            //reduce for value
            auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                                       thrust::make_permutation_iterator(d_input_values.begin(),
                                                                                         d_result_idx.begin()),
                                                       d_result_keys_idx.begin(), d_result_values.begin(),
                                                       reducer_predicator(d_partition_data_ptr, d_keys_type_ptr,
                                                                          partition_by_num),
                                                       thrust::plus<double>());
            //h_result_keys_idx = thrust::host_vector<int>(d_result_keys_idx.begin(), new_end.first);
            result_size = new_end.second - d_result_values.begin();
            double *p_result = new double[result_size];
            cudaMemcpy(p_result, thrust::raw_pointer_cast(d_result_values.data()),
                       result_size * sizeof(double), cudaMemcpyDeviceToHost);
            h_result_values[j] = p_result;

            double *result_value = static_cast<double *>(h_result_values[j]);
            double *each_value = new double[row_num];
            uint32_t *each_num = h_group_number[j];
            for (int k = 0, m = 0, idx = 1; k < row_num & m < result_size; k++) {
              each_value[k] = result_value[m];
              idx++;
              if (idx > each_num[m]) {
                idx = 1;
                m++;
              }
            }
            window_value[j]=each_value;
          } else {
            double *each_value = new double[row_num];
            uint32_t *each_num = h_group_number[j];
            uint32_t idx = 0;
            for (int k = 0; k < result_size; k++) {
              thrust::inclusive_scan(thrust::make_permutation_iterator(d_input_values.begin(),
                                                                       d_result_idx.begin()) + idx,
                                     thrust::make_permutation_iterator(d_input_values.begin(),
                                                                       d_result_idx.begin()) + idx + each_num[k],
                                     each_value + idx, thrust::plus<double>());
              idx = each_num[k];
            }
            window_value[j]=each_value;
          }
        }
        else if (function_index < data_flow->longColNum + data_flow->doubleColNum + data_flow->intColNum) {
          values_type[j] = 2;
          result_int_col_num++;
          uint32_t index = function_index - data_flow->longColNum - data_flow->doubleColNum;
          thrust::device_vector<int> d_input_values(data_flow->intCols[index], data_flow->intCols[index] + row_num);

          if (each_window.windowFrame == ROWS_PRECEDING_FOLLOWING) {
            thrust::device_vector<int> d_result_values(row_num);
            //reduce for value
            auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                                       thrust::make_permutation_iterator(d_input_values.begin(),
                                                                                         d_result_idx.begin()),
                                                       d_result_keys_idx.begin(), d_result_values.begin(),
                                                       reducer_predicator(d_partition_data_ptr, d_keys_type_ptr,
                                                                          partition_by_num),
                                                       thrust::plus<int32_t>());
            h_result_keys_idx = thrust::host_vector<int>(d_result_keys_idx.begin(), new_end.first);
            result_size = new_end.second - d_result_values.begin();
            int32_t *p_result = new int32_t[result_size];
            cudaMemcpy(p_result, thrust::raw_pointer_cast(d_result_values.data()),
                       result_size * sizeof(int32_t), cudaMemcpyDeviceToHost);
            h_result_values[j] = p_result;

            int *result_value = static_cast<int *>(h_result_values[j]);
            int *each_value = new int[row_num];
            uint32_t *each_num = h_group_number[j];
            for (int k = 0, m = 0, idx = 1; k < row_num & m < result_size; k++) {
              each_value[k] = result_value[m];
              idx++;
              if (idx > each_num[m]) {
                idx = 1;
                m++;
              }
            }
            cout << "debug_point_0" << endl;
            for (int z = 0; z < row_num; z++) {
              cout << each_value[z] << endl;
            }
            window_value[j] = each_value;

          } else {
            int *each_value = new int[row_num];
            uint32_t *each_num = h_group_number[j];
            uint32_t idx = 0;
            for (int k = 0; k < result_size; k++) {
              thrust::inclusive_scan(thrust::make_permutation_iterator(d_input_values.begin(),
                                                                       d_result_idx.begin()) + idx,
                                     thrust::make_permutation_iterator(d_input_values.begin(),
                                                                       d_result_idx.begin()) + idx + each_num[k],
                                     each_value + idx, thrust::plus<int32_t>());
              idx = each_num[k];
            }
            window_value[j] = each_value;
          }
        } 
        else {
          result_str_col_num++;
        }
        break;
      }
      case AVG: {
        values_type[j] = 1; // The result type of AVG is double.
        result_double_col_num++;
        thrust::device_vector<int> d_result_cnt(row_num);
        auto cnt_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                             thrust::constant_iterator<int>(1),
                                             d_result_keys_idx.begin(), d_result_cnt.begin(),
                                             reducer_predicator(d_partition_data_ptr, d_keys_type_ptr,
                                                                 partition_by_num),
                                             thrust::plus<int>());
        result_size = cnt_end.second - d_result_cnt.begin();
        uint32_t *p_group_num = new uint32_t[result_size];
        cudaMemcpy(p_group_num, thrust::raw_pointer_cast(d_result_cnt.data()),
                   result_size * sizeof(uint32_t), cudaMemcpyDeviceToHost);
        h_group_number[j] = p_group_num;
        thrust::device_vector<double> d_avg_result(row_num);

        if (function_index < data_flow->longColNum) {
          thrust::device_vector<long> d_input_values(data_flow->longCols[function_index],
                                                     data_flow->longCols[function_index] + row_num);
          auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                               thrust::make_permutation_iterator(d_input_values.begin(),
                                                                                 d_result_idx.begin()),
                                               d_result_keys_idx.begin(), d_avg_result.begin(),
                                               reducer_predicator(d_partition_data_ptr, d_keys_type_ptr,
                                                                   partition_by_num),
                                               thrust::plus<long>());
          h_result_keys_idx = thrust::host_vector<int>(d_result_keys_idx.begin(), new_end.first);

          thrust::transform(d_avg_result.begin(), new_end.second, d_result_cnt.begin(),
                            d_avg_result.begin(), divides<double>());
          result_size = cnt_end.second - d_result_cnt.begin();
          double *p_result = new double[result_size];
          cudaMemcpy(p_result, thrust::raw_pointer_cast(d_avg_result.data()),
                     result_size * sizeof(double), cudaMemcpyDeviceToHost);
          h_result_values[j] = p_result;
          
          long * result_value = static_cast<long *>(h_result_values[j]);
          long * each_value = new long [row_num];
          uint32_t * each_num = h_group_number[j];
          for (int k=0,m=0,idx=1;k<row_num&l<result_size;k++){
            each_value[k]=result_value[m];
            idx++;
            if (idx>each_num[m]){
              idx=1;
              m++;
            }
          }
          window_value[j]=each_value;
        }
        else if (function_index < data_flow->longColNum + data_flow->doubleColNum) {
          uint32_t index = function_index - data_flow->longColNum;
          thrust::device_vector<double> d_input_values(data_flow->doubleCols[index],
                                                       data_flow->doubleCols[index] + row_num);
          auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                               thrust::make_permutation_iterator(d_input_values.begin(),
                                                                                 d_result_idx.begin()),
                                               d_result_keys_idx.begin(), d_avg_result.begin(),
                                               reducer_predicator(d_partition_data_ptr, d_keys_type_ptr,
                                                                  order_by_num + partition_by_num),
                                               thrust::plus<double>());
          h_result_keys_idx = thrust::host_vector<int>(d_result_keys_idx.begin(), new_end.first);

          thrust::transform(d_avg_result.begin(), new_end.second, d_result_cnt.begin(),
                            d_avg_result.begin(), divides<double>());

          result_size = cnt_end.second - d_result_cnt.begin();
          double *p_result = new double[result_size];
          cudaMemcpy(p_result, thrust::raw_pointer_cast(d_avg_result.data()),
                     result_size * sizeof(double), cudaMemcpyDeviceToHost);
          h_result_values[j] = p_result;

          double * result_value = static_cast<double *>(h_result_values[j]);
          double * each_value = new double [row_num];
          uint32_t * each_num = h_group_number[j];
          for (int k=0,m=0,idx=1;k<row_num&l<result_size;k++){
            each_value[k]=result_value[m];
            idx++;
            if (idx>each_num[m]){
              idx=1;
              m++;
            }
          }
          window_value[j]=each_value;
        }
        else if (function_index < data_flow->longColNum + data_flow->doubleColNum + data_flow->intColNum) {
          uint32_t index = function_index - data_flow->longColNum - data_flow->doubleColNum;
          thrust::device_vector<int> d_input_values(data_flow->intCols[index], data_flow->intCols[index] + row_num);
          auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                               thrust::make_permutation_iterator(d_input_values.begin(),
                                                                                 d_result_idx.begin()),
                                               d_result_keys_idx.begin(), d_avg_result.begin(),
                                               reducer_predicator(d_partition_data_ptr, d_keys_type_ptr,
                                                                  order_by_num + partition_by_num),
                                               thrust::plus<int>());
          h_result_keys_idx = thrust::host_vector<int>(d_result_keys_idx.begin(), new_end.first);

          thrust::transform(d_avg_result.begin(), new_end.second, d_result_cnt.begin(),
                            d_avg_result.begin(), divides<double>());

          result_size = cnt_end.second - d_result_cnt.begin();
          double *p_result = new double[result_size];
          cudaMemcpy(p_result, thrust::raw_pointer_cast(d_avg_result.data()),
                     result_size * sizeof(double), cudaMemcpyDeviceToHost);
          h_result_values[j] = p_result;

          double * result_value = static_cast<double *>(h_result_values[j]);
          double * each_value = new double [row_num];
          uint32_t * each_num = h_group_number[j];
          for (int k=0,m=0,idx=1;k<row_num&l<result_size;k++){
            each_value[k]=result_value[m];
            idx++;
            if (idx>each_num[m]){
              idx=1;
              m++;
            }
          }
          window_value[j]=each_value;
        }
        else {
        }
        break;
      }
      case RANK:{
        //always int
        values_type[j] = 2;
        result_int_col_num++;
        thrust::device_vector<uint32_t> d_result_nums(row_num);
        auto cnt_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                             thrust::constant_iterator<uint32_t>(1),
                                             d_result_keys_idx.begin(), d_result_nums.begin(),
                                             reducer_predicator(d_partition_data_ptr, d_keys_type_ptr,
                                                                order_by_num + partition_by_num),
                                             thrust::plus<uint32_t>());
        result_size = cnt_end.second - d_result_nums.begin();
        uint32_t *p_group_num = new uint32_t[result_size];
        cudaMemcpy(p_group_num, thrust::raw_pointer_cast(d_result_nums.data()),
                   result_size * sizeof(uint32_t), cudaMemcpyDeviceToHost);
        h_group_number[j] = p_group_num;
        int32_t  *each_value = new int32_t [row_num];
        uint32_t *each_num = h_group_number[j];
        uint32_t idx = 0;
        for (int k = 0; k < result_size; k++) {
          thrust::inclusive_scan(thrust::constant_iterator<uint32_t>(1),
                                 thrust::constant_iterator<uint32_t>(1) + each_num[k],
                                 each_value + idx, thrust::plus<uint32_t>());
          idx += each_num[k];
        }
        break;
      }
      case UNKNOWN:
        break;
    }

  }


  std::vector<uint32_t> seq;
  DataFlow *result_df = new DataFlow(result_long_col_num, result_double_col_num, result_int_col_num,
                                     result_str_col_num, row_num);
  result_df->initCols();


  
  std::cout << "result_long_col_num: " << result_long_col_num << std::endl;
  std::cout << "result_double_col_num: " << result_double_col_num << std::endl;
  std::cout << "result_int_col_num: " << result_int_col_num << std::endl;
  std::cout << "result_str_col_num: " << result_str_col_num << std::endl;

  uint32_t tmp_long_idx = 0;
  uint32_t tmp_double_idx = 0;
  uint32_t tmp_int_idx = 0;
  uint32_t tmp_string_idx= 0;
  for (int n = 0; n < data_flow->longColNum + data_flow->doubleColNum + data_flow->intColNum + data_flow->stringColNum; n++) {
    if (n < data_flow->longColNum){
      seq.push_back(tmp_long_idx++);
      std::cout << "FILE: " << __FILE__ << ", LINE: " << __LINE__ << std::endl;
      for(int p=0;p<row_num;p++){
        result_df->longCols[n][p]=data_flow->longCols[n][h_result_idx[p]];
      }
    }else if(n < data_flow->longColNum + data_flow->doubleColNum){
      seq.push_back(result_long_col_num+tmp_double_idx++);
      std::cout << "FILE: " << __FILE__ << ", LINE: " << __LINE__ << std::endl;
      for (int p=0;p<row_num;p++){
        result_df->doubleCols[n-data_flow->longColNum][p]=data_flow->doubleCols[n-data_flow->longColNum][h_result_idx[p]];
      }
    }else if(n < data_flow->longColNum + data_flow->doubleColNum + data_flow->intColNum) {
      seq.push_back(result_long_col_num+result_double_col_num+tmp_int_idx++);
      std::cout << "FILE: " << __FILE__ << ", LINE: " << __LINE__ << std::endl;
      for (int p = 0; p < row_num; p++) {
        result_df->intCols[n - data_flow->longColNum - data_flow->doubleColNum][p] =
            data_flow->intCols[n - data_flow->longColNum - data_flow->doubleColNum][h_result_idx[p]];
      }
    }else{
      seq.push_back(result_long_col_num+result_double_col_num+result_int_col_num+tmp_string_idx++);
      int32_t position=0;
      for (int p = 0; p < row_num; p++) {
        int length=data_flow->strIdxCols[n- data_flow->longColNum - data_flow->doubleColNum-data_flow->intColNum][2 * h_result_idx[p] + 1] - data_flow->strIdxCols[n- data_flow->longColNum - data_flow->doubleColNum-data_flow->intColNum][2 * h_result_idx[p]];
        string content=std::string(data_flow->strCols[n- data_flow->longColNum - data_flow->doubleColNum-data_flow->intColNum] + data_flow->strIdxCols[n- data_flow->longColNum - data_flow->doubleColNum-data_flow->intColNum][2 * h_result_idx[p]],
                                   length);
        cout<<content<<endl;
        result_df->strIdxCols[n - data_flow->longColNum - data_flow->doubleColNum - data_flow->intColNum][p*2]=position;
        result_df->strIdxCols[n - data_flow->longColNum - data_flow->doubleColNum - data_flow->intColNum][p*2+1]=position+length;
        for (int q=0;q<length;q++){
          result_df->strCols[n - data_flow->longColNum - data_flow->doubleColNum - data_flow->intColNum][position+q]=
              content[q];
        }
        position+=length;
      }
    }
  }

  for (int s = 0; s < window_function_num; s++) {
    if (values_type[s] == 0) {
      uint32_t value_idx = tmp_long_idx ++;
      seq.push_back(value_idx);
      result_df->longCols[value_idx] = (long *)window_value[s];
    } else if (values_type[s] == 1) {
      uint32_t value_idx = tmp_double_idx ++;
      seq.push_back(result_long_col_num + value_idx);
      result_df->doubleCols[value_idx] = (double *)window_value[s];
    } else if (values_type[s] == 2) {
      uint32_t value_idx = tmp_int_idx ++;
      seq.push_back(result_long_col_num + result_double_col_num + value_idx);
      result_df->intCols[value_idx] = (int32_t *)window_value[s];
      for(int x=0;x<row_num;x++){
        cout<<((int32_t*)window_value[s])[x];
      }
    } else {

    }
  }
  result_df->set_sequence(seq);
  result_df->keyNum =data_flow->keyNum;
  return result_df;
}
*/
Table *PTFPredicate::execute(Table *input_tbl, Profiler &profiler) {

  uint32_t row_num = input_tbl->row_num;
  uint32_t partition_by_num = partition_by_col.size();
  uint32_t order_by_num =order_by_col.size();
  uint32_t window_function_num =window_functions.size();

  //string need one more col
  uint32_t partition_vec_size = partition_by_num ;
  for (unsigned int i : partition_by_col) {
    if (input_tbl->columns[i]->type==STRING) {
      partition_vec_size++;
    }
  }
  uint32_t order_by_vec_size = order_by_num ;
  for (unsigned int i : order_by_col) {
    if (input_tbl->columns[i]->type==STRING) {
      order_by_vec_size++;
    }
  }


  thrust::device_vector<const void *> d_key_vec(partition_vec_size+order_by_num);
  thrust::device_vector<const void *> d_partition_vec(partition_by_num);
  thrust::device_vector<int> d_keys_type(partition_vec_size+order_by_vec_size);

  //partition col
  int i=0;int l=0;
  std::vector<SortOrder> sort_order(partition_by_num+order_by_num);
  cout<<"partition_by_num: "<<partition_by_num<<endl;
  for (; i < partition_by_num; i++, l++) {
    sort_order.push_back(SortOrder::ASC);
    if (input_tbl->columns[i]->type==LONG) {
      d_keys_type[i] = 0 ;
      std::cout << "partition long columns: " << std::endl;
      long *d_long_ptr;
      cudaMalloc((void **) &d_long_ptr, row_num * sizeof(long));
      cudaMemcpy(d_long_ptr, input_tbl->columns[i]->data_ptr, row_num * sizeof(long), cudaMemcpyHostToDevice);
      d_key_vec[l] = d_long_ptr;
      d_partition_vec[l]=d_long_ptr;
    } else if (input_tbl->columns[i]->type==DOUBLE) {
      d_keys_type[i] = 1;
      double *d_double_ptr;
      cudaMalloc((void **) &d_double_ptr, row_num * sizeof(double));
      cudaMemcpy(d_double_ptr, input_tbl->columns[i]->data_ptr, row_num * sizeof(double), cudaMemcpyHostToDevice);
      d_key_vec[l] = d_double_ptr;
      d_partition_vec[l]=d_double_ptr;
    } else if (input_tbl->columns[i]->type==INT) {
      d_keys_type[i] = 2;
      int *d_int_ptr;
      cudaMalloc((void **) &d_int_ptr, row_num * sizeof(int));
      cudaMemcpy(d_int_ptr, input_tbl->columns[i]->data_ptr, row_num * sizeof(int), cudaMemcpyHostToDevice);
      d_key_vec[l] = d_int_ptr;
      d_partition_vec[l]=d_int_ptr;
    } else { // String
      d_keys_type[i] = 3;
      char *d_str_ptr;
      int *d_str_idx_ptr;
      uint32_t size_char = 0;
      for (int x = 0; x < row_num; x++) {
        size_char = ((int32_t *)(input_tbl->columns[i])->data_ptr_aux)[2 * x + 1] > size_char ?
                    ((int32_t *)(input_tbl->columns[i])->data_ptr_aux)[2 * x + 1] : size_char;
      }
      cudaMalloc((void **) &d_str_ptr, size_char * sizeof(char));
      cudaMemcpy(d_str_ptr, input_tbl->columns[i]->data_ptr, size_char * sizeof(char), cudaMemcpyHostToDevice);
      cudaMalloc((void **) &d_str_idx_ptr, 2 * row_num * sizeof(int));
      cudaMemcpy(d_str_idx_ptr, input_tbl->columns[i]->data_ptr, 2 * row_num * sizeof(int), cudaMemcpyHostToDevice);
      d_key_vec[l++] = d_str_ptr;
      d_key_vec[l] = d_str_idx_ptr;
      d_partition_vec[l]=d_str_ptr;
      d_partition_vec[l]=d_str_idx_ptr;
    }
  }
  //order_by col
  for (; i <partition_by_num+ order_by_num; i++, l++) {
    sort_order.push_back(asc[i]);
    if (input_tbl->columns[i]->type==LONG) {
      d_keys_type[i] = 0 ;
      long *d_long_ptr;
      cudaMalloc((void **) &d_long_ptr, row_num * sizeof(long));
      cudaMemcpy(d_long_ptr, input_tbl->columns[i]->data_ptr, row_num * sizeof(long), cudaMemcpyHostToDevice);
      d_key_vec[l] = d_long_ptr;
    } else if (input_tbl->columns[i]->type==DOUBLE) {
      d_keys_type[i] = 1;
      double *d_double_ptr;
      cudaMalloc((void **) &d_double_ptr, row_num * sizeof(double));
      cudaMemcpy(d_double_ptr, input_tbl->columns[i]->data_ptr, row_num * sizeof(double), cudaMemcpyHostToDevice);
      d_key_vec[l] = d_double_ptr;
    } else if (input_tbl->columns[i]->type==INT) {
      d_keys_type[i] = 2;
      int *d_int_ptr;
      cudaMalloc((void **) &d_int_ptr, row_num * sizeof(int));
      cudaMemcpy(d_int_ptr, input_tbl->columns[i]->data_ptr, row_num * sizeof(int), cudaMemcpyHostToDevice);
      d_key_vec[l] = d_int_ptr;
    } else { // String
      d_keys_type[i] = 3;
      char *d_str_ptr;
      int *d_str_idx_ptr;
      uint32_t size_char = 0;
      for (int x = 0; x < row_num; x++) {
        size_char = ((int32_t *) (input_tbl->columns[i])->data_ptr_aux)[2 * x + 1] > size_char ?
                    ((int32_t *) (input_tbl->columns[i])->data_ptr_aux)[2 * x + 1] : size_char;
      }
      cudaMalloc((void **) &d_str_ptr, size_char * sizeof(char ));
      cudaMemcpy(d_str_ptr, input_tbl->columns[i]->data_ptr, size_char * sizeof(char), cudaMemcpyHostToDevice);
      cudaMalloc((void **) &d_str_idx_ptr, 2 * row_num * sizeof(int));
      cudaMemcpy(d_str_idx_ptr, input_tbl->columns[i]->data_ptr, 2 * row_num * sizeof(int), cudaMemcpyHostToDevice);

      d_key_vec[l++] = d_str_ptr;
      d_key_vec[l] = d_str_idx_ptr;
    }
  }


  const void **d_keys_data_ptr = thrust::raw_pointer_cast(d_key_vec.data());
  const int *d_keys_type_ptr = thrust::raw_pointer_cast(d_keys_type.data());
  thrust::device_vector<uint> d_result_idx(row_num);
  thrust::host_vector<int>h_result_idx(row_num);
  thrust::sequence(d_result_idx.begin(), d_result_idx.end());
  //TODO::CONSIDER ASC DEC!!! solved!
  //TODO::null first or not?
  thrust::sort(d_result_idx.begin(), d_result_idx.end(), sort_comparator(d_keys_data_ptr, d_keys_type_ptr, order_by_num+partition_by_num,sort_order.data()));

  cudaMemcpy(h_result_idx.data(), thrust::raw_pointer_cast(d_result_idx.data()),
             row_num * sizeof(int), cudaMemcpyDeviceToHost);


  const void **d_partition_data_ptr = thrust::raw_pointer_cast(d_partition_vec.data());
  thrust::device_vector<int> d_result_keys_idx(row_num);
  thrust::host_vector<int> h_result_keys_idx;
  thrust::host_vector<void *> h_result_values(window_function_num);
  thrust::host_vector<uint32_t * >h_group_number(window_function_num);
  struct reducer_predicator rp(d_partition_data_ptr, d_keys_type_ptr, partition_by_num);

  std::vector<Column *> result_columns;

  for (int j = 0; j < window_functions.size(); j++) {

    WindowFunction each_window = window_functions[j];
    Column *column = input_tbl->columns[each_window.arguments];

    switch (each_window.type) {
      case SUM: {
        switch (column->type) {
          case LONG: {
            result_columns[i] = aggregation_sum<long>(d_result_idx, input_tbl->columns[each_window.arguments], row_num,
                                                      rp, each_window.window_frame);
            break;
          }
          case DOUBLE: {
            result_columns[i] = aggregation_sum<double>(d_result_idx, input_tbl->columns[each_window.arguments],
                                                        row_num, rp, each_window.window_frame);
            break;
          }
          case INT: {
            result_columns[i] = aggregation_sum<int>(d_result_idx, input_tbl->columns[each_window.arguments], row_num,
                                                     rp, each_window.window_frame);
            break;
          }
        }
        break;
      }

      case MAX: {
        switch (column->type) {
          case LONG: {
            result_columns[i] = aggregation_max<long>(d_result_idx, input_tbl->columns[each_window.arguments], row_num,
                                                      rp, each_window.window_frame);
            break;
          }
          case DOUBLE: {
            result_columns[i] = aggregation_max<double>(d_result_idx, input_tbl->columns[each_window.arguments],
                                                        row_num, rp, each_window.window_frame);
            break;
          }
          case INT: {
            result_columns[i] = aggregation_max<int>(d_result_idx, input_tbl->columns[each_window.arguments], row_num,
                                                     rp, each_window.window_frame);
            break;
          }
        }
        break;
      }
      case AVG: {
        switch (column->type) {
          case LONG: {
            result_columns[i] = aggregation_avg<long>(d_result_idx, input_tbl->columns[each_window.arguments], row_num,
                                                      rp, each_window.window_frame);
            break;
          }
          case DOUBLE: {
            result_columns[i] = aggregation_avg<double>(d_result_idx, input_tbl->columns[each_window.arguments],
                                                        row_num, rp, each_window.window_frame);
            break;
          }
          case INT: {
            result_columns[i] = aggregation_avg<int>(d_result_idx, input_tbl->columns[each_window.arguments], row_num,
                                                     rp, each_window.window_frame);
            break;
          }
        }
        break;
      }
      //todo:RANK depends on sort
      case RANK: {
        switch (column->type) {
          case LONG: {
            result_columns[i] = aggregation_rank<long>(d_result_idx, row_num,
                                                      rp, each_window.window_frame);
            break;
          }
          case DOUBLE: {
            result_columns[i] = aggregation_rank<double>(d_result_idx,
                                                        row_num, rp, each_window.window_frame);
            break;
          }
          case INT: {
            result_columns[i] = aggregation_rank<int>(d_result_idx, row_num,
                                                     rp, each_window.window_frame);
            break;
          }
        }
        break;
      }
    }
  }

  Table* out_tbl= new Table();
  out_tbl->row_num=input_tbl->row_num;
  for (auto & column : input_tbl->columns) {
    out_tbl->columns.push_back(column);
  }

  for (int s = 0; s < window_function_num; s++) {
    out_tbl->columns.push_back(result_columns[s]);
  }
  return out_tbl;

}


template<typename T>
Column *PTFPredicate::aggregation_sum(thrust::device_vector<uint32_t> &d_result_idx,
                                          Column *column, uint32_t row_num, struct reducer_predicator &rp,windowFrame window_frame) {
  thrust::device_vector<int> d_result_keys_idx(row_num);
  thrust::device_vector<uint32_t> d_result_nums(row_num);
  //reduce for number
  auto cnt_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                       thrust::constant_iterator<uint32_t>(1),
                                       d_result_keys_idx.begin(), d_result_nums.begin(),
                                       rp,
                                       thrust::plus<uint32_t>());
  uint result_size = cnt_end.second - d_result_nums.begin();
  uint32_t *p_group_num = new uint32_t[result_size];
  cudaMemcpy(p_group_num, thrust::raw_pointer_cast(d_result_nums.data()),
             result_size * sizeof(uint32_t), cudaMemcpyDeviceToHost);
  thrust::device_vector<T> d_input_values((T *) column->data_ptr,
                                             (T *)column->data_ptr + row_num);
  if (window_frame == ROWS_PRECEDING_FOLLOWING) {
    //reduce for value
    thrust::device_vector<T> d_result_values(row_num);
    auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                         thrust::make_permutation_iterator(d_input_values.begin(),
                                                                           d_result_idx.begin()),
                                         d_result_keys_idx.begin(), d_result_values.begin(),
                                         rp,
                                         newPlus<T>());
    result_size = new_end.second - d_result_values.begin();
    T *p_result = new T[result_size];
    cudaMemcpy(p_result, thrust::raw_pointer_cast(d_result_values.data()),
               result_size * sizeof(T), cudaMemcpyDeviceToHost);
    T *result_value = static_cast<T *>(p_result);
    T *each_value = new T[row_num];
    uint32_t *each_num = p_group_num;
    for (int k = 0, m = 0, idx = 1; k < row_num & m < result_size; k++) {
      each_value[k] = result_value[m];
      idx++;
      if (idx > each_num[m]) {
        idx = 1;
        m++;
      }
    }
    return new Column(column->type, row_num, each_value);
  } else {
    T *each_value = new T[row_num];
    uint32_t *each_num = p_group_num;
    uint32_t idx = 0;
    for (int k = 0; k < result_size; k++) {
      thrust::inclusive_scan(thrust::make_permutation_iterator(d_input_values.begin(),
                                                               d_result_idx.begin()) + idx,
                             thrust::make_permutation_iterator(d_input_values.begin(),
                                                               d_result_idx.begin()) + idx + each_num[k],
                             each_value + idx, newPlus<T>());
      idx = each_num[k];
    }
    return new Column(column->type, row_num, each_value);
  }
}


template<typename T>
Column *PTFPredicate::aggregation_max(thrust::device_vector<uint32_t> &d_result_idx,
                                          Column *column, uint32_t row_num, struct reducer_predicator &rp,windowFrame window_frame) {

  thrust::device_vector<int> d_result_keys_idx(row_num);
  thrust::device_vector<uint32_t> d_result_nums(row_num);
  //reduce for number
  auto cnt_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                       thrust::constant_iterator<uint32_t>(1),
                                       d_result_keys_idx.begin(), d_result_nums.begin(),
                                       rp,
                                       thrust::plus<uint32_t>());
  uint result_size = cnt_end.second - d_result_nums.begin();
  uint32_t *p_group_num = new uint32_t[result_size];
  cudaMemcpy(p_group_num, thrust::raw_pointer_cast(d_result_nums.data()),
             result_size * sizeof(uint32_t), cudaMemcpyDeviceToHost);
  thrust::device_vector<T> d_input_values((T *) column->data_ptr,
                                          (T *)column->data_ptr + row_num);
  if (window_frame == ROWS_PRECEDING_FOLLOWING) {
    //reduce for value
    thrust::device_vector<T> d_result_values(row_num);
    auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                         thrust::make_permutation_iterator(d_input_values.begin(),
                                                                           d_result_idx.begin()),
                                         d_result_keys_idx.begin(), d_result_values.begin(),
                                         rp,
                                         newMaximum<T>());
    result_size = new_end.second - d_result_values.begin();
    T *p_result = new T[result_size];
    cudaMemcpy(p_result, thrust::raw_pointer_cast(d_result_values.data()),
               result_size * sizeof(T), cudaMemcpyDeviceToHost);
    T *result_value = static_cast<T *>(p_result);
    T *each_value = new T[row_num];
    uint32_t *each_num = p_group_num;
    for (int k = 0, m = 0, idx = 1; k < row_num & m < result_size; k++) {
      each_value[k] = result_value[m];
      idx++;
      if (idx > each_num[m]) {
        idx = 1;
        m++;
      }
    }
    return new Column(column->type, row_num, each_value);
  } else {
    T *each_value = new T[row_num];
    uint32_t *each_num = p_group_num;
    uint32_t idx = 0;
    for (int k = 0; k < result_size; k++) {
      thrust::inclusive_scan(thrust::make_permutation_iterator(d_input_values.begin(),
                                                               d_result_idx.begin()) + idx,
                             thrust::make_permutation_iterator(d_input_values.begin(),
                                                               d_result_idx.begin()) + idx + each_num[k],
                             each_value + idx, newMaximum<T>());
      idx = each_num[k];
    }
    return new Column(column->type, row_num, each_value);
  }

}

template<typename T>
Column *PTFPredicate::aggregation_avg(thrust::device_vector<uint32_t> &d_result_idx,
                                          Column *column, uint32_t row_num, struct reducer_predicator &rp,windowFrame window_frame) {
  thrust::device_vector<int> d_result_keys_idx(row_num);
  thrust::device_vector<int> d_result_cnt(row_num);
  thrust::device_vector<T> d_input_values((T*)(column)->data_ptr,
                                          (T*)(column)->data_ptr + row_num);
  auto cnt_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                       thrust::make_permutation_iterator(d_input_values.begin(),
                                                                         d_result_idx.begin()),
                                       d_result_keys_idx.begin(), d_result_cnt.begin(),
                                       rp,countPlus<int>());
  uint32_t result_size = cnt_end.second - d_result_cnt.begin();
  uint32_t *p_group_num = new uint32_t[result_size];
  cudaMemcpy(p_group_num, thrust::raw_pointer_cast(d_result_cnt.data()),
             result_size * sizeof(uint32_t), cudaMemcpyDeviceToHost);

  thrust::device_vector<double> d_avg_result(row_num);
    auto new_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                         thrust::make_permutation_iterator(d_input_values.begin(),
                                                                           d_result_idx.begin()),
                                         d_result_keys_idx.begin(), d_avg_result.begin(),
                                         rp,
                                         newPlus<T>());

    thrust::transform(d_avg_result.begin(), new_end.second, d_result_cnt.begin(),
                      d_avg_result.begin(), divides<double>());
    result_size = cnt_end.second - d_result_cnt.begin();
    double *p_result = new double[result_size];
    cudaMemcpy(p_result, thrust::raw_pointer_cast(d_avg_result.data()),
               result_size * sizeof(double), cudaMemcpyDeviceToHost);

    double * result_value = static_cast<double *>(p_result);
    double * each_value = new double [row_num];
    uint32_t * each_num = p_group_num;
    for (int k=0,m=0,idx=1;k<row_num&m<result_size;k++){
      each_value[k]=result_value[m];
      idx++;
      if (idx>each_num[m]){
        idx=1;
        m++;
      }
    }
  return new Column(DOUBLE,row_num,each_value);
  }

template <typename T>
Column *PTFPredicate::aggregation_rank(thrust::device_vector<uint32_t> &d_result_idx,
                                          uint32_t row_num, struct reducer_predicator &rp,windowFrame window_frame) {
  thrust::device_vector<uint32_t> d_result_nums(row_num);
  thrust::device_vector<int> d_result_keys_idx(row_num);
  auto cnt_end = thrust::reduce_by_key(d_result_idx.begin(), d_result_idx.end(),
                                       thrust::constant_iterator<uint32_t>(1),
                                       d_result_keys_idx.begin(), d_result_nums.begin(),
                                       rp,
                                       thrust::plus<uint32_t>());
  uint32_t result_size = cnt_end.second - d_result_nums.begin();
  uint32_t *p_group_num = new uint32_t[result_size];
  cudaMemcpy(p_group_num, thrust::raw_pointer_cast(d_result_nums.data()),
             result_size * sizeof(uint32_t), cudaMemcpyDeviceToHost);
  int32_t  *each_value = new int32_t [row_num];
  uint32_t *each_num = p_group_num;
  uint32_t idx = 0;
  for (int k = 0; k < result_size; k++) {
    thrust::inclusive_scan(thrust::constant_iterator<uint32_t>(1),
                           thrust::constant_iterator<uint32_t>(1) + each_num[k],
                           each_value + idx, thrust::plus<uint32_t>());
    idx += each_num[k];
  }
  return new Column(INT,row_num,each_value);
}

