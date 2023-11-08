#include <list>
#include <thrust/device_vector.h>

#include "Operator/SortMergeJoinPredicate.hpp"
#include "Operator/GroupByPredicate.hpp"
#include "Util/Util.hpp"

Table *SortMergeJoinPredicate::process(const std::vector<Table *> &tables) {
  profiler.start_op();


  std::cout << "GHive-CPP [SortMergeJoinPredicate-process]: process starts with " << tables.size() << " tables." << std::endl;
  // 1. Sort each data flow, get sorted index.
  // 1.1 extract the key columns and copy them to GPU.
  uint32_t **sorted_index = new uint32_t *[join_cols.size()];

  for (uint32_t i = 0; i < join_cols.size(); i++) {

    profiler.start_pci_host2device();
    std::vector<uint32_t> each_tbl_cols = join_cols[i]; // keys cols for table i
    std::cout << "table [" << i << "]: " << tables[i]->toString() << std::endl;
    std::cout << "GHive-CPP [SortMergeJoinPredicate-process]: join_cols[i] "
              << vector_to_string<uint32_t>(each_tbl_cols) << std::endl;
    Table *tbl = tables[i];
    uint32_t key_space_size = each_tbl_cols.size();
    for (uint32_t j = 0; j < each_tbl_cols.size(); j++) {
      if (tbl->columns[each_tbl_cols[j]]->type == STRING) {
        key_space_size ++;
      }
    }
//    profiler.start_gpu_alloc();
    thrust::device_vector<const void *> d_join_keys(key_space_size);
    thrust::device_vector<int> d_keys_type(each_tbl_cols.size());
//    profiler.end_gpu_alloc();
    for (uint32_t j = 0, l = 0; j < each_tbl_cols.size(); j++, l++) {
      // uint32_t ith_col = each_tbl_cols[j];
      const auto &col = tbl->columns[each_tbl_cols[j]];
      switch (col->type) {
        case LONG: {
          d_keys_type[j] = 0;
          long *d_long_ptr;
//          profiler.start_gpu_alloc();
          cudaMalloc((void **) &d_long_ptr, col->row_num * sizeof(long));
//          profiler.end_gpu_alloc();
//          profiler.start_pci_host2device();
          cudaMemcpy(d_long_ptr, col->data_ptr, col->row_num * sizeof(long), cudaMemcpyHostToDevice);
//          profiler.end_pci_host2device();
          d_join_keys[l] = d_long_ptr;
          break;
        }
        case DOUBLE: {
          d_keys_type[j] = 1;
          double *d_double_ptr;
//          profiler.start_gpu_alloc();
          cudaMalloc((void **) &d_double_ptr, col->row_num * sizeof(double));
//          profiler.end_gpu_alloc();
//          profiler.start_pci_host2device();
          cudaMemcpy(d_double_ptr, col->data_ptr, col->row_num * sizeof(double), cudaMemcpyHostToDevice);
//          profiler.end_pci_host2device();
          d_join_keys[l] = d_double_ptr;
          break;
        }
        case INT: {
          d_keys_type[j] = 2;
          int32_t *d_int_ptr;
//          profiler.start_gpu_alloc();
          cudaMalloc((void **) &d_int_ptr, col->row_num * sizeof(int));
//          profiler.end_gpu_alloc();
//          profiler.start_pci_host2device();
          cudaMemcpy(d_int_ptr, col->data_ptr, tbl->row_num * sizeof(int), cudaMemcpyHostToDevice);
//          profiler.end_pci_host2device();
          d_join_keys[l] = d_int_ptr;
          break;
        }
        case STRING: {
          d_keys_type[j] = 3;
          char *d_str_ptr;
          int32_t *d_str_idx_ptr;
          uint32_t size_char = 0;
          for (uint32_t x = 0; x < col->row_num; x++) {
            size_char = max(static_cast<int32_t *>(col->data_ptr_aux)[2 * x + 1], size_char);
          }
//          profiler.start_gpu_alloc();
          cudaMalloc((void **) &d_str_ptr, (size_char + 1) * sizeof(char));
          cudaMalloc((void **) &d_str_idx_ptr, 2 * col->row_num * sizeof(int32_t));
//          profiler.end_gpu_alloc();
//          profiler.start_pci_host2device();
          cudaMemcpy(d_str_ptr, col->data_ptr, (size_char + 1) * sizeof(char), cudaMemcpyHostToDevice);
          cudaMemcpy(d_str_idx_ptr, col->data_ptr_aux, 2 * col->row_num * sizeof(int32_t), cudaMemcpyHostToDevice);
//          profiler.end_pci_host2device();
          d_join_keys[l++] = d_str_ptr;
          d_join_keys[l++] = d_str_idx_ptr;
          break;
        }
        case DEPEND: break;
        default:break;
      }
    }
//    profiler.start_gpu_alloc();
    const void **d_keys_data_ptr = thrust::raw_pointer_cast(d_join_keys.data());
    const int *d_keys_type_ptr = thrust::raw_pointer_cast(d_keys_type.data());

    thrust::device_vector<int> d_result_idx(tbl->row_num);
    thrust::sequence(d_result_idx.begin(), d_result_idx.end());
//    profiler.start_gpu_alloc();
    profiler.end_pci_host2device();
    // 1.2 invoking thrust::sort_by_key to acquire the sorted index.
    profiler.start_sort();
    profiler.start_gpu_exec();
    thrust::sort(d_result_idx.begin(),
                 d_result_idx.end(),
                 sort_comparator(d_keys_data_ptr, d_keys_type_ptr, each_tbl_cols.size()));
    profiler.end_gpu_exec();
    profiler.end_sort();
//    for (uint32_t x = 0; x < d_result_idx.size(); x++) {
//      std::cout << d_result_idx[x] << " ";
//    }
//    std::cout << std::endl;
    std::cout << "GHive-CPP [SortMergeJoinPredicate-process]: finish sort." << std::endl;
    // 1.3 copy back the result to CPU.
    profiler.start_cpu_alloc();
    uint32_t *result_index = new uint32_t[tbl->row_num];
    profiler.end_cpu_alloc();
    profiler.start_pci_device2host();
    cudaMemcpy(result_index,
               thrust::raw_pointer_cast(d_result_idx.data()),
               tbl->row_num * sizeof(int32_t),
               cudaMemcpyDeviceToHost);
    profiler.end_pci_device2host();
    sorted_index[i] = result_index;
  }

  std::cout << "GHive-CPP: [BaseJoinPredicate-sm_join]: Sorting ends." << std::endl;

  // 2. Merge the data according to the sorted indexes.

  // 2.1 Calculate the size of each offset pair. Semi Join only maintain 1 column.
  profiler.start_merge();
  uint32_t pair_counter = 0;
  std::vector<int32_t> corresponding_index(join_types.size() + 1);
  for (uint32_t i = 0; i < join_types.size(); i++) {
    uint32_t idx_second = join_types[i].second.second;
    if (i == 0) {
      result_to_tbl.push_back(i);
      corresponding_index[i] = pair_counter++;
    }
    if (join_types[i].first == LEFT_SEMI_JOIN) {
      corresponding_index[idx_second] = -1;
    } else {
      result_to_tbl.push_back(idx_second);
      corresponding_index[idx_second] = pair_counter;
      pair_counter++;
    }
  }
  std::cout << "GHive-CPP: [BaseJoinPredicate-sm_join]: corresponding index: " <<
            vector_to_string<int32_t>(corresponding_index) << std::endl;
  // 2.2 according to the join types, recording the result to tmp vector
  //     push back all the offsets to the result vector (result_offsets) finally.
  //     For Outer Join, -1 represents null.
  // TODO: join_col_size = 0;
  uint32_t join_col_size = join_cols[0].size(); // key的个数

  std::list<std::vector<int32_t>> result_pairs;
  for (uint32_t i = 0; i < join_types.size(); i++) {
    auto join_type = join_types[i];
    uint32_t tbl_idx_first = join_type.second.first;
    uint32_t tbl_idx_second = join_type.second.second;
    uint32_t result_idx_first = corresponding_index[tbl_idx_first];
    uint32_t result_idx_second = corresponding_index[tbl_idx_second];
    Table *tbl_first = tables[tbl_idx_first];
    Table *tbl_second = tables[tbl_idx_second];
    std::cout << "GHive-CPP: [BaseJoinPredicate-sm_join]: tbl_idx_first: "
              << tbl_idx_first << std::endl;
    std::cout << "GHive-CPP: [BaseJoinPredicate-sm_join]: tbl_idx_second: "
              << tbl_idx_second << std::endl;
    uint32_t ptr_alloc_size = join_col_size;
    uint32_t *sorted_idx_1 = sorted_index[tbl_idx_first];
    uint32_t *sorted_idx_2 = sorted_index[tbl_idx_second];
    for (auto idx: join_cols[tbl_idx_first]) {
      const auto &col = tbl_first->columns[idx];
      if (col->type == ColumnType::STRING) {
        ptr_alloc_size++;
      }
    }
    const void **data_ptr_1 = new const void *[ptr_alloc_size];
    const void **data_ptr_2 = new const void *[ptr_alloc_size];
    int *types = new int[join_col_size];
    for (uint32_t j = 0, l = 0; j < join_cols[tbl_idx_first].size(); j++, l++) {
      uint32_t idx = join_cols[tbl_idx_first][j];
      const auto &col = tbl_first->columns[idx];
      // std::cout << "df_first_data: ";
      // for (uint32_t s = 0; s < 4; s++) {
      //   std::cout << tbl_first->longCols[idx][s] << " ";
      // }
      switch (col->type) {
        case LONG: {
          types[j] = 0;
          data_ptr_1[l] = col->data_ptr;
          break;
        }
        case DOUBLE: {
          types[j] = 1;
          data_ptr_1[l] = col->data_ptr;
          break;
        }
        case INT: {
          types[j] = 2;
          data_ptr_1[l] = col->data_ptr;
          break;
        }
        case STRING: {
          types[j] = 3;
          data_ptr_1[l++] = col->data_ptr;
          data_ptr_1[l] = col->data_ptr_aux;
          break;
        }
        case DEPEND:break;
        default:break;
      }
    }

    for (uint32_t j = 0, l = 0; j < join_cols[tbl_idx_second].size(); j++, l++) {
      uint32_t idx = join_cols[tbl_idx_second][j];
      const auto &col = tbl_second->columns[idx];
      // std::cout << "df_second_data: ";
      // for (uint32_t s = 0; s < 4; s++) {
      //   std::cout << tbl_second->longCols[idx][s] << " ";
      // }
      switch (col->type) {
        case LONG: {
          data_ptr_2[l] = col->data_ptr;
          break;
        }
        case DOUBLE: {
          data_ptr_2[l] = col->data_ptr;
          break;
        }
        case INT: {
          data_ptr_2[l] = col->data_ptr;
          break;
        }
        case STRING: {
          data_ptr_2[l++] = col->data_ptr;
          data_ptr_2[l] = col->data_ptr_aux;
          break;
        }
        case DEPEND:break;
        default:break;
      }
    }
    struct two_key_comparator comparator(data_ptr_1, data_ptr_2, types, join_col_size);
    if (i == 0) {
      std::vector<int32_t> result_pair(pair_counter, 0x8fffffff);
      uint32_t iter_first = 0;
      uint32_t iter_second = 0;
      switch (join_type.first) {
        case INNER_JOIN: {
          while (iter_first < tbl_first->row_num) {
            while (comparator.compare(sorted_idx_1[iter_first], sorted_idx_2[iter_second]) < 0) {
              iter_second++;
              if (iter_second >= tbl_second->row_num) {
                goto finish_inner_join;
              }
            }
            uint32_t tmp_iter_second = iter_second;
            while (comparator.compare(sorted_idx_1[iter_first], sorted_idx_2[tmp_iter_second]) == 0) {
              result_pair[result_idx_first] = sorted_idx_1[iter_first];
              result_pair[result_idx_second] = sorted_idx_2[tmp_iter_second];
              result_pairs.push_back(result_pair);
              tmp_iter_second++;
              if (tmp_iter_second >= tbl_second->row_num) {
                break;
              }
            }
            iter_first++;
          }
          finish_inner_join:
          break;
        }
        case LEFT_OUTER_JOIN: {
          while (iter_first < tbl_first->row_num) {
            while (comparator.compare(sorted_idx_1[iter_first], sorted_idx_2[iter_second]) < 0) {
              iter_second++;
              if (iter_second >= tbl_second->row_num) {
                while (iter_first < tbl_first->row_num) {
                  result_pair[result_idx_first] = sorted_idx_1[iter_first];
                  result_pair[result_idx_second] = -1;
                  result_pairs.push_back(result_pair);
                  iter_first++;
                }
                goto finish_left_outer_join;
              }
            }
            if (comparator.compare(sorted_idx_1[iter_first], sorted_idx_2[iter_second]) > 0) {
              result_pair[result_idx_first] = sorted_idx_1[iter_first];
              result_pair[result_idx_second] = -1;
              result_pairs.push_back(result_pair);
            }

            uint32_t tmp_iter_second = iter_second;
            while (comparator.compare(sorted_idx_1[iter_first], sorted_idx_2[tmp_iter_second]) == 0) {
              result_pair[result_idx_first] = sorted_idx_1[iter_first];
              result_pair[result_idx_second] = sorted_idx_2[tmp_iter_second];
              result_pairs.push_back(result_pair);
              tmp_iter_second++;
              if (tmp_iter_second >= tbl_second->row_num) {
                break;
              }
            }
            iter_first++;
          }
          finish_left_outer_join:
          break;
        }
        case RIGHT_OUTER_JOIN: {
          break;
        }
        case FULL_OUTER_JOIN: {
          break;
        }
        case LEFT_SEMI_JOIN: {
          while (iter_first < tbl_first->row_num) {
            while (comparator.compare(sorted_idx_1[iter_first], sorted_idx_2[iter_second]) < 0) {
              iter_second++;
              if (iter_second >= tbl_second->row_num) {
                goto finish_left_semi_join;
              }
            }
            if (comparator.compare(sorted_idx_1[iter_first], sorted_idx_2[iter_second]) == 0) {
              result_pair[result_idx_first] = sorted_idx_1[iter_first];
              result_pairs.push_back(result_pair);
            }
            iter_first++;
          }
          finish_left_semi_join:
          break;
        }
        case CROSS_JOIN: {
          break;
        }
      }
    } else {

      switch (join_type.first) {
        case INNER_JOIN: {
          auto first_vec_iter = result_pairs.begin();
          uint32_t iter_second = 0;
          while (first_vec_iter != result_pairs.end()) {
            uint32_t first_idx = (*first_vec_iter)[result_idx_first];
            while (comparator.compare(first_idx, sorted_idx_2[iter_second]) < 0) {
              iter_second++;
              if (iter_second >= tbl_second->row_num) {
                goto finish_inner_join_2;
              }
            }
            if (comparator.compare(first_idx, sorted_idx_2[iter_second]) > 0) {
              result_pairs.erase(first_vec_iter++);
              continue;
            }
            uint32_t tmp_iter_second = iter_second;
            if (comparator.compare(first_idx, sorted_idx_2[tmp_iter_second]) == 0) {
              (*first_vec_iter)[result_idx_second] = sorted_idx_2[tmp_iter_second];
              tmp_iter_second++;
            }
            while (tmp_iter_second < tbl_second->row_num
                && comparator.compare(first_idx, sorted_idx_2[tmp_iter_second]) == 0) {
              result_pairs.insert(first_vec_iter, *first_vec_iter);
              (*first_vec_iter)[result_idx_second] = sorted_idx_2[tmp_iter_second];
              tmp_iter_second++;
            }
          }
          finish_inner_join_2:
          break;
        }
        case LEFT_OUTER_JOIN: {
          auto first_vec_iter = result_pairs.begin();
          uint32_t iter_second = 0;
          while (first_vec_iter != result_pairs.end()) {
            uint32_t first_idx = (*first_vec_iter)[result_idx_first];
            while (comparator.compare(first_idx, sorted_idx_2[iter_second]) < 0) {
              iter_second++;
              if (iter_second >= tbl_second->row_num) {
                while (first_vec_iter != result_pairs.end()) {
                  (*first_vec_iter)[result_idx_second] = -1;
                  first_vec_iter++;
                }
                goto finish_left_outer_join_2;
              }
            }
            if (comparator.compare(first_idx, sorted_idx_2[iter_second]) > 0) {
              (*first_vec_iter)[result_idx_second] = -1;
              first_vec_iter++;
              continue;
            }
            uint32_t tmp_iter_second = iter_second;
            if (comparator.compare(first_idx, sorted_idx_2[tmp_iter_second]) == 0) {
              (*first_vec_iter)[result_idx_second] = sorted_idx_2[tmp_iter_second];
              tmp_iter_second++;
            }
            while (tmp_iter_second < tbl_second->row_num
                && comparator.compare(first_idx, sorted_idx_2[tmp_iter_second]) == 0) {
              result_pairs.insert(first_vec_iter, *first_vec_iter);
//              first_vec_iter++;
              (*first_vec_iter)[result_idx_second] = sorted_idx_2[tmp_iter_second];
              tmp_iter_second++;
            }
            first_vec_iter++;
          }
          finish_left_outer_join_2:
          break;
        }
        case RIGHT_OUTER_JOIN: {

          break;
        }
        case FULL_OUTER_JOIN: {

          break;
        }
        case LEFT_SEMI_JOIN: {
          auto first_vec_iter = result_pairs.begin();
          uint32_t iter_second = 0;
          while (first_vec_iter != result_pairs.end()) {
            uint32_t first_idx = (*first_vec_iter)[result_idx_first];
            while (comparator.compare(first_idx, sorted_idx_2[iter_second]) < 0) {
              iter_second++;
              if (iter_second >= tbl_second->row_num) {
                goto finish_left_semi_join_2;
              }
            }
            if (comparator.compare(first_idx, sorted_idx_2[iter_second]) > 0) {
              result_pairs.erase(first_vec_iter++);
              continue;
            }
            if (comparator.compare(first_idx, sorted_idx_2[iter_second]) == 0) {
              first_vec_iter ++;
            }
          }
          finish_left_semi_join_2:
          break;
        }
        case CROSS_JOIN: {

          break;
        }
      }
    }
  }
  cardinality = result_pairs.size();
  std::cout << "GHive-CPP: [BaseJoinPredicate-sm_join]: cardinality: " << cardinality << std::endl;
  for (std::vector<int32_t> vec: result_pairs) {
    result_offsets.insert(result_offsets.end(), vec.begin(), vec.end());
  }
  profiler.end_merge();
  // 3. Generate the result dataflow according to the maintain_cols.
  profiler.start_data_recover();
  Table *result_tbl = generate_result(tables);
  profiler.end_data_recover();
  profiler.end_op();
  return result_tbl;
}

SortMergeJoinPredicate::SortMergeJoinPredicate() { }
