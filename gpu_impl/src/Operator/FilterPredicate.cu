#include <thrust/iterator/counting_iterator.h>
#include <thrust/iterator/reverse_iterator.h>
#include <thrust/partition.h>
#include <thrust/device_vector.h>
#include <algorithm>
#include <Profile/FilterProfiler.hpp>
#include "Operator/FilterOperator.hpp"
#include "Operator/FilterPredicate.hpp"

FilterPredicate::FilterPredicate() {}

void FilterPredicate::gpu_string_filter(char *filter_col, int *index_col, std::string *val, Profiler &profiler) {
  std::cout << "GHive-CPP [FilterPredicate-process]: processing string starts." << std::endl;
  int size_char = 0;
  for (int x = 0; x < row_num; x++) {
    size_char = ((int32_t *) index_col)[2 * x] > size_char ?
                ((int32_t *) index_col)[2 * x] : size_char;
    size_char = ((int32_t *) index_col)[2 * x + 1] > size_char ?
                ((int32_t *) index_col)[2 * x + 1] : size_char;
  }
  thrust::device_vector<char> d_filter_col(filter_col, filter_col + size_char);
  thrust::device_vector<int> d_index_col(index_col, index_col + 2 * row_num);
  thrust::device_vector<char> d_val_0_vec(val[0].begin(), val[0].end());

  thrust::counting_iterator<int> iter(0);

  int size_0 = val[0].size();
  char *d_data = thrust::raw_pointer_cast(d_filter_col.data());
  int *d_index = thrust::raw_pointer_cast(d_index_col.data());
  char *d_val_0 = thrust::raw_pointer_cast(d_val_0_vec.data());

  switch (mode) {
    case FILTER_EQ: {
      thrust::transform(iter, iter + row_num, result_bitmap.begin(),
                        filter_str_unary_eq(d_data, d_index, d_val_0, size_0));
      break;
    }
    case FILTER_LE: {
      thrust::transform(iter, iter + row_num, result_bitmap.begin(),
                        filter_str_unary_le(d_data, d_index, d_val_0, size_0));
      break;
    }
    case FILTER_GE: {
      thrust::transform(iter, iter + row_num, result_bitmap.begin(),
                        filter_str_unary_ge(d_data, d_index, d_val_0, size_0));
      break;
    }
    case FILTER_LT: {
      thrust::transform(iter, iter + row_num, result_bitmap.begin(),
                        filter_str_unary_lt(d_data, d_index, d_val_0, size_0));

      break;
    }
    case FILTER_GT: {
      thrust::transform(iter, iter + row_num, result_bitmap.begin(),
                        filter_str_unary_gt(d_data, d_index, d_val_0, size_0));
      break;
    }
    case FILTER_EQ_RANGE: {
      thrust::device_vector<char> d_val_1_vec(val[1].begin(), val[1].end());
      char *d_val_1 = thrust::raw_pointer_cast(d_val_1_vec.data());
      int size_1 = val[1].size();
      thrust::transform(iter, iter + row_num, result_bitmap.begin(),
                        filter_str_unary_eq_range(d_data, d_index, d_val_0, size_0, d_val_1, size_1));
      break;
    }
    case FILTER_NOT_EQ: {
      thrust::transform(iter, iter + row_num, result_bitmap.begin(),
                        filter_str_unary_ne(d_data, d_index, d_val_0, size_0));
      break;
    }
    case FILTER_NOT_RANGE: {
      thrust::device_vector<char> d_val_1_vec(val[1].begin(), val[1].end());
      char *d_val_1 = thrust::raw_pointer_cast(d_val_1_vec.data());
      int size_1 = val[1].size();
      thrust::transform(iter, iter + row_num, result_bitmap.begin(),
                        filter_str_unary_not_range(d_data, d_index, d_val_0, size_0, d_val_1, size_1));
      break;
    }
    case FILTER_RANGE:
    case FILTER_NOT_EQ_RANGE:
    case FILTER_NOT_NULL:
    case FILTER_AND:
    case FILTER_OR:break;
  }
}

void FilterPredicate::gpu_string_filter_not_null(int *index_col, Profiler &profiler) {
  std::cout << "GHive-CPP [FilterPredicate-process]: processing string starts." << std::endl;
  profiler.start_pci_host2device();
  thrust::device_vector<int> d_index_col(index_col, index_col + 2 * row_num);
  profiler.end_pci_host2device();
  int *d_index = thrust::raw_pointer_cast(d_index_col.data());
  thrust::counting_iterator<int> iter(0);
  thrust::transform(iter, iter + row_num, result_bitmap.begin(),
                    filter_str_not_null(d_index));

}

void FilterPredicate::cpu_string_filter_not_null(int *index_col) {
  thrust::counting_iterator<int> iter(0);
  thrust::transform(iter, iter + row_num, host_result_bitmap.begin(),
                    filter_str_not_null(index_col));

}

void FilterPredicate::cpu_string_filter(char *filter_col, int *index_col, std::string *val) {
  std::cout << "GHive-CPP [FilterPredicate-process]: processing string starts." << std::endl;

  thrust::counting_iterator<int> iter(0);
  char *v_0 = const_cast<char *>(val[0].data());
  uint32_t v_size = val[0].size();

  switch (mode) {
    case FILTER_EQ: {
      thrust::transform(iter, iter + row_num, host_result_bitmap.begin(),
                        filter_str_unary_eq(filter_col, index_col, v_0, v_size));
      break;
    }
    case FILTER_LE: {
      thrust::transform(iter, iter + row_num, host_result_bitmap.begin(),
                        filter_str_unary_le(filter_col, index_col, v_0, v_size));
      break;
    }
    case FILTER_GE: {
      thrust::transform(iter, iter + row_num, host_result_bitmap.begin(),
                        filter_str_unary_ge(filter_col, index_col, v_0, v_size));
      break;
    }
    case FILTER_LT: {
      thrust::transform(iter, iter + row_num, host_result_bitmap.begin(),
                        filter_str_unary_lt(filter_col, index_col, v_0, v_size));

      break;
    }
    case FILTER_GT: {
      thrust::transform(iter, iter + row_num, host_result_bitmap.begin(),
                        filter_str_unary_gt(filter_col, index_col, v_0, v_size));
      break;
    }
    case FILTER_EQ_RANGE: {
      char *v_1 = const_cast<char *>(val[1].data());
      uint32_t v_1_size = val[1].size();

      thrust::transform(iter, iter + row_num, host_result_bitmap.begin(),
                        filter_str_unary_eq_range(filter_col, index_col, v_0, v_size, v_1, v_1_size));
      break;
    }
    case FILTER_NOT_EQ: {
      thrust::transform(iter, iter + row_num, host_result_bitmap.begin(),
                        filter_str_unary_ne(filter_col, index_col, v_0, v_size));
      break;
    }
    case FILTER_NOT_RANGE: {
      char *v_1 = const_cast<char *>(val[1].data());
      uint32_t v_1_size = val[1].size();
      thrust::transform(iter, iter + row_num, host_result_bitmap.begin(),
                        filter_str_unary_not_range(filter_col, index_col, v_0, v_size, v_1, v_1_size));
      break;
    }
    case FILTER_RANGE:
    case FILTER_NOT_EQ_RANGE:
    case FILTER_NOT_NULL:
    case FILTER_AND:
    case FILTER_OR:break;
  }
}

template<typename T>
void FilterPredicate::gpu_filter(T *filter_col, T *val, FilterProfiler &profiler) {
  profiler.start_pci_host2device();
  std::cout << "row number: " << row_num << std::endl;
  thrust::device_vector<T> d_filter_col(filter_col, filter_col + row_num);
  profiler.end_pci_host2device();
  profiler.start_gpu_exec();
  profiler.start_transform();
  switch (mode) {
    case FILTER_EQ: // val[0] is never null
      thrust::transform(d_filter_col.begin(), d_filter_col.end(), result_bitmap.begin(), filter_unary_eq<T>(val[0]));
      break;
    case FILTER_LE:
      thrust::transform(d_filter_col.begin(),
                        d_filter_col.end(),
                        result_bitmap.begin(),
                        filter_unary_le<T>(val[0]));
      break;
    case FILTER_GE: // val[0] is never null
      thrust::transform(d_filter_col.begin(), d_filter_col.end(), result_bitmap.begin(), filter_unary_ge<T>(val[0]));
      break;
    case FILTER_LT:
      thrust::transform(d_filter_col.begin(),
                        d_filter_col.end(),
                        result_bitmap.begin(),
                        filter_unary_lt<T>(val[0]));
      break;
    case FILTER_GT:
      thrust::transform(d_filter_col.begin(),
                        d_filter_col.end(),
                        result_bitmap.begin(),
                        filter_unary_gt<T>(val[0]));
      break;
    case FILTER_NOT_EQ:
      thrust::transform(d_filter_col.begin(),
                        d_filter_col.end(),
                        result_bitmap.begin(),
                        filter_unary_ne<T>(val[0]));
      break;
    case FILTER_NOT_NULL:
      thrust::transform(d_filter_col.begin(),
                        d_filter_col.end(),
                        result_bitmap.begin(),
                        filter_not_null<T>());
      break;
    case FILTER_AND:
    case FILTER_OR:break;
    case FILTER_EQ_RANGE:
      thrust::transform(d_filter_col.begin(),
                        d_filter_col.end(),
                        result_bitmap.begin(),
                        filter_eq_range<T>(val[0], val[1]));
      break;
    case FILTER_NOT_RANGE:
      thrust::transform(d_filter_col.begin(),
                        d_filter_col.end(),
                        result_bitmap.begin(),
                        filter_not_range<T>(val[0], val[1]));
      break;
    case FILTER_NOT_EQ_RANGE:
      thrust::transform(d_filter_col.begin(),
                        d_filter_col.end(),
                        result_bitmap.begin(),
                        filter_not_eq_range<T>(val[0], val[1]));
      break;
    case FILTER_RANGE:
      thrust::transform(d_filter_col.begin(),
                        d_filter_col.end(),
                        result_bitmap.begin(),
                        filter_range<T>(val[0], val[1]));
      break;
  }
  profiler.end_gpu_exec();
  profiler.end_transform();
}

template<typename T>
void FilterPredicate::cpu_filter(T *filter_col, T *val) {
  host_result_bitmap = thrust::host_vector<T>(row_num);
  switch (mode) {
    case FILTER_EQ:
      thrust::transform(filter_col,
                        filter_col + row_num,
                        host_result_bitmap.begin(),
                        filter_unary_eq<T>(val[0]));
      break;
    case FILTER_LE:
      thrust::transform(filter_col,
                        filter_col + row_num,
                        host_result_bitmap.begin(),
                        filter_unary_le<T>(val[0]));
      break;
    case FILTER_GE:
      thrust::transform(filter_col,
                        filter_col + row_num,
                        host_result_bitmap.begin(),
                        filter_unary_ge<T>(val[0]));
      break;
    case FILTER_LT:
      thrust::transform(filter_col,
                        filter_col + row_num,
                        host_result_bitmap.begin(),
                        filter_unary_lt<T>(val[0]));
      break;
    case FILTER_GT:
      thrust::transform(filter_col,
                        filter_col + row_num,
                        host_result_bitmap.begin(),
                        filter_unary_gt<T>(val[0]));
      break;
    case FILTER_NOT_EQ:
      thrust::transform(filter_col,
                        filter_col + row_num,
                        host_result_bitmap.begin(),
                        filter_unary_ne<T>(val[0]));
      break;
    case FILTER_NOT_NULL:
      thrust::transform(filter_col,
                        filter_col + row_num,
                        host_result_bitmap.begin(),
                        filter_not_null<T>());
      break;
    case FILTER_AND:
    case FILTER_OR:break;
    case FILTER_EQ_RANGE:
      thrust::transform(filter_col,
                        filter_col + row_num,
                        host_result_bitmap.begin(),
                        filter_eq_range<T>(val[0], val[1]));
      break;
    case FILTER_NOT_RANGE:
      thrust::transform(filter_col,
                        filter_col + row_num,
                        host_result_bitmap.begin(),
                        filter_not_range<T>(val[0], val[1]));
      break;
    case FILTER_NOT_EQ_RANGE:
      thrust::transform(filter_col,
                        filter_col + row_num,
                        host_result_bitmap.begin(),
                        filter_not_eq_range<T>(val[0], val[1]));
      break;
    case FILTER_RANGE:
      thrust::transform(filter_col,
                        filter_col + row_num,
                        host_result_bitmap.begin(),
                        filter_range<T>(val[0], val[1]));
      break;
  }
}

template<typename T, typename F>
void FilterPredicate::cpu_filter_two(T *filter_col_1, F *filter_col_2) {
  host_result_bitmap = thrust::host_vector<T>(row_num);
  switch (mode) {
    case FILTER_EQ:
      thrust::transform(filter_col_1,
                        filter_col_1,
                        filter_col_2,
                        host_result_bitmap.begin(),
                        filter_binary_eq<T, F>());
      break;
    case FILTER_LE:
      thrust::transform(filter_col_1,
                        filter_col_1 + row_num,
                        filter_col_2,
                        host_result_bitmap.begin(),
                        filter_binary_le<T, F>());
      break;
    case FILTER_GE:
      thrust::transform(filter_col_1,
                        filter_col_1 + row_num,
                        filter_col_2,
                        host_result_bitmap.begin(),
                        filter_binary_ge<T, F>());
      break;
    case FILTER_LT:
      thrust::transform(filter_col_1,
                        filter_col_1 + row_num,
                        filter_col_2,
                        host_result_bitmap.begin(),
                        filter_binary_lt<T, F>());
      break;
    case FILTER_GT:
      thrust::transform(filter_col_1,
                        filter_col_1 + row_num,
                        filter_col_2,
                        host_result_bitmap.begin(),
                        filter_binary_gt<T, F>());
      break;
    case FILTER_NOT_EQ:
      thrust::transform(filter_col_1,
                        filter_col_1 + row_num,
                        filter_col_2,
                        host_result_bitmap.begin(),
                        filter_binary_ne<T, F>());
      break;
    case FILTER_NOT_NULL:
    case FILTER_AND:
    case FILTER_OR:
    case FILTER_EQ_RANGE:
    case FILTER_NOT_RANGE:
    case FILTER_NOT_EQ_RANGE:
    case FILTER_RANGE:break;
  }
//  result_bitmap = host_result_bitmap;
}

template<typename T, typename F>
void FilterPredicate::gpu_filter_two(T *filter_col_1, F *filter_col_2, Profiler &profiler) {
  thrust::device_vector<T> d_filter_col_1(filter_col_1, filter_col_1 + row_num);
  thrust::device_vector<T> d_filter_col_2(filter_col_2, filter_col_2 + row_num);
  switch (mode) {
    case FILTER_EQ:
      thrust::transform(d_filter_col_1.begin(),
                        d_filter_col_1.end(),
                        d_filter_col_2.begin(),
                        result_bitmap.begin(),
                        filter_binary_eq<T, F>());
      break;
    case FILTER_LE:
      thrust::transform(d_filter_col_1.begin(),
                        d_filter_col_1.end(),
                        d_filter_col_2.begin(),
                        result_bitmap.begin(),
                        filter_binary_le<T, F>());
      break;
    case FILTER_GE:
      thrust::transform(d_filter_col_1.begin(),
                        d_filter_col_1.end(),
                        d_filter_col_2.begin(),
                        result_bitmap.begin(),
                        filter_binary_ge<T, F>());
      break;
    case FILTER_LT:
      thrust::transform(d_filter_col_1.begin(),
                        d_filter_col_1.end(),
                        d_filter_col_2.begin(),
                        result_bitmap.begin(),
                        filter_binary_lt<T, F>());
      break;
    case FILTER_GT:
      thrust::transform(d_filter_col_1.begin(),
                        d_filter_col_1.end(),
                        d_filter_col_2.begin(),
                        result_bitmap.begin(),
                        filter_binary_gt<T, F>());
      break;
    case FILTER_NOT_EQ:
      thrust::transform(d_filter_col_1.begin(),
                        d_filter_col_1.end(),
                        d_filter_col_2.begin(),
                        result_bitmap.begin(),
                        filter_binary_ne<T, F>());
      break;
    case FILTER_NOT_NULL:
    case FILTER_AND:
    case FILTER_OR:
    case FILTER_EQ_RANGE:
    case FILTER_NOT_RANGE:
    case FILTER_NOT_EQ_RANGE:
    case FILTER_RANGE:break;
  }
}

void FilterPredicate::process(Table *table, FilterProfiler &profiler) {
  std::cout << "GHive-CPP [FilterPredicate-process]: processing starts." << std::endl;
  std::cout << this->toString(0) << std::endl;
  row_num = table->row_num;
  profiler.start_gpu_alloc();
  result_bitmap = thrust::device_vector<int>(row_num);
  profiler.end_gpu_alloc();
//  int *bitmap;
//  profiler.start_pci_host2device();
//  cudaMalloc((void **)&bitmap, row_num * sizeof(int));
//  profiler.end_pci_host2device();

  if (predColNum == 0) { // and / or
    if (mode == FILTER_AND) {
      childrenPredicate[0]->process(table, profiler);
      childrenPredicate[1]->process(table, profiler);
      std::cout << "childrenPredicate[0] with result_bitmap.size() = "
                << childrenPredicate[0]->result_bitmap.size() << ":" << std::endl;
      auto len0 = std::min<::size_t>(100, childrenPredicate[0]->result_bitmap.size());
      // auto len0 = childrenPredicate[0]->result_bitmap.size();
      for (int i = 0; i < len0; i++) {
        std::cout << childrenPredicate[0]->result_bitmap[i] << " ";
      }
      std::cout << std::endl;

      std::cout << "childrenPredicate[1] with result_bitmap.size() = "
                << childrenPredicate[1]->result_bitmap.size() << ":" << std::endl;
      auto len1 = std::min<::size_t>(100, childrenPredicate[1]->result_bitmap.size());
      // auto len1 = childrenPredicate[1]->result_bitmap.size();
      std::cout << "childrenPredicate[1]: " << std::endl;
      for (int i = 0; i < len1; i++) {
        std::cout << childrenPredicate[1]->result_bitmap[i] << " ";
      }
      std::cout << std::endl;

      thrust::transform(childrenPredicate[0]->result_bitmap.begin(), childrenPredicate[0]->result_bitmap.end(),
                        childrenPredicate[1]->result_bitmap.begin(), result_bitmap.begin(),
                        filter_binary_and<int, int>());
    } else if (mode == FILTER_OR) {
      childrenPredicate[0]->process(table, profiler);
      childrenPredicate[1]->process(table, profiler);
      thrust::transform(childrenPredicate[0]->result_bitmap.begin(), childrenPredicate[0]->result_bitmap.end(),
                        childrenPredicate[1]->result_bitmap.begin(), result_bitmap.begin(),
                        filter_binary_or<int, int>());
    }

  } else if (predColNum == 1) {
    Column *filter_column = table->columns[filterCol[0]];
    if (dataType == 0) {
      gpu_filter<long>((long *) filter_column->data_ptr, longFilterParams, profiler);
    } else if ((dataType == 2 && filter_column->type == LONG)) {
      longFilterParams[0] = intFilterParams[0];
      longFilterParams[1] = intFilterParams[1];
      gpu_filter<long>((long *) filter_column->data_ptr, longFilterParams, profiler);
    } else if (dataType == 1) {
      gpu_filter<double>((double *) filter_column->data_ptr, doubleFilterParams, profiler);
    } else if (dataType == 2) {
      gpu_filter<int32_t>((int32_t *) filter_column->data_ptr, intFilterParams, profiler);
    } else if (dataType == 3) {
      // Only consider the "=" predicate now.
      gpu_string_filter((char *) filter_column->data_ptr,
                        (int32_t *) filter_column->data_ptr_aux,
                        stringFilterParams,
                        profiler);
    } else if (dataType == 4 && mode == FILTER_NOT_NULL) { // not null
      switch (table->columns[filterCol[0]]->type) {
        case LONG: {
          gpu_filter<long>((long *) filter_column->data_ptr, longFilterParams, profiler);
          break;
        }
        case DOUBLE: {
          gpu_filter<double>((double *) filter_column->data_ptr, doubleFilterParams, profiler);
          break;
        }
        case INT: {
          gpu_filter<int32_t>((int32_t *) filter_column->data_ptr, intFilterParams, profiler);
          break;
        }
        case STRING: {
          gpu_string_filter_not_null((int32_t *) filter_column->data_ptr_aux, profiler);
          break;
        }
        case DEPEND: break;
      }
//      result_bitmap = thrust::device_vector<int>(row_num, (uint32_t)1);
    }
  } else if (predColNum == 2) {
    Column *filter_column_1 = table->columns[filterCol[0]];
    Column *filter_column_2 = table->columns[filterCol[1]];
    if (filter_column_1->type == LONG) {
      if (filter_column_2->type == LONG) {
        gpu_filter_two<long, long>((long *) filter_column_1->data_ptr, (long *) filter_column_2->data_ptr, profiler);
      } else if (filter_column_2->type == DOUBLE) {
        gpu_filter_two<long, double>((long *) filter_column_1->data_ptr,
                                     (double *) filter_column_2->data_ptr,
                                     profiler);
      } else if (filter_column_2->type == INT) {
        gpu_filter_two<int32_t, int32_t>((int32_t *) filter_column_1->data_ptr,
                                         (int32_t *) filter_column_2->data_ptr,
                                         profiler);
      }
    } else if (filter_column_1->type == DOUBLE) {
      if (filter_column_2->type == LONG) {
        gpu_filter_two<double, long>((double *) filter_column_1->data_ptr,
                                     (long *) filter_column_2->data_ptr,
                                     profiler);
      } else if (filter_column_2->type == DOUBLE) {
        gpu_filter_two<double, double>((double *) filter_column_1->data_ptr,
                                       (double *) filter_column_2->data_ptr,
                                       profiler);
      } else if (filter_column_2->type == INT) {
        gpu_filter_two<double, int32_t>((double *) filter_column_1->data_ptr,
                                        (int32_t *) filter_column_2->data_ptr,
                                        profiler);
      }
    } else if (filter_column_1->type == INT) {
      if (filter_column_2->type == LONG) {
        gpu_filter_two<int32_t, long>((int32_t *) filter_column_1->data_ptr,
                                      (long *) filter_column_2->data_ptr,
                                      profiler);
      } else if (filter_column_2->type == DOUBLE) {
        gpu_filter_two<int32_t, double>((int32_t *) filter_column_1->data_ptr,
                                        (double *) filter_column_2->data_ptr,
                                        profiler);
      } else if (filter_column_2->type == INT) {
        gpu_filter_two<int32_t, int32_t>((int32_t *) filter_column_1->data_ptr,
                                         (int32_t *) filter_column_2->data_ptr,
                                         profiler);
      }
    } else if (filter_column_1->type == STRING) {
      assert(filter_column_2->type == STRING);
      //TODO: Does not consider string col v.s. string col currently.
      std::cout << "GHive-CPP-ERROR [FilterPredicate-process]: Does not consider string col v.s. string col currently"
                << std::endl;
    }
  }
  std::cout << "GHive-CPP [FilterPredicate-process]: processing ends." << std::endl;
}

void FilterPredicate::cpu_process(Table *table) {
  std::cout << "GHive-CPP [FilterPredicate-process]: processing starts." << std::endl;
  std::cout << this->toString(0) << std::endl;
  row_num = table->row_num;

  if (predColNum == 0) { // and / or
    if (mode == FILTER_AND) {
      childrenPredicate[0]->cpu_process(table);
      childrenPredicate[1]->cpu_process(table);
      thrust::transform(childrenPredicate[0]->result_bitmap.begin(), childrenPredicate[0]->result_bitmap.end(),
                        childrenPredicate[1]->result_bitmap.begin(), result_bitmap.begin(),
                        filter_binary_and<int, int>());
    } else if (mode == FILTER_OR) {
      childrenPredicate[0]->cpu_process(table);
      childrenPredicate[1]->cpu_process(table);
      thrust::transform(childrenPredicate[0]->result_bitmap.begin(), childrenPredicate[0]->result_bitmap.end(),
                        childrenPredicate[1]->result_bitmap.begin(), result_bitmap.begin(),
                        filter_binary_or<int, int>());
    }

  } else if (predColNum == 1) {
    Column *filter_column = table->columns[filterCol[0]];
    if (dataType == 0) {
      cpu_filter<long>((long *) filter_column->data_ptr, longFilterParams);
    } else if ((dataType == 2 && filter_column->type == LONG)) {
      longFilterParams[0] = intFilterParams[0];
      longFilterParams[1] = intFilterParams[1];
      cpu_filter<long>((long *) filter_column->data_ptr, longFilterParams);
    } else if (dataType == 1) {
      cpu_filter<double>((double *) filter_column->data_ptr, doubleFilterParams);
    } else if (dataType == 2) {
      cpu_filter<int32_t>((int32_t *) filter_column->data_ptr, intFilterParams);
    } else if (dataType == 3) {
      // Only consider the "=" predicate now.
      cpu_string_filter((char *) filter_column->data_ptr, (int32_t *) filter_column->data_ptr_aux, stringFilterParams);
    } else if (dataType == 4) { // not null
      switch (table->columns[filterCol[0]]->type) {
        case LONG: {
          cpu_filter<long>((long *) filter_column->data_ptr, longFilterParams);
          break;
        }
        case DOUBLE: {
          cpu_filter<double>((double *) filter_column->data_ptr, doubleFilterParams);
          break;
        }
        case INT: {
          cpu_filter<int32_t>((int32_t *) filter_column->data_ptr, intFilterParams);
          break;
        }
        case STRING: {
          cpu_string_filter_not_null((int32_t *) filter_column->data_ptr_aux);
          break;
        }
        case DEPEND: break;
      }
    }
  } else if (predColNum == 2) {
    Column *filter_column_1 = table->columns[filterCol[0]];
    Column *filter_column_2 = table->columns[filterCol[1]];
    if (filter_column_1->type == LONG) {
      if (filter_column_2->type == LONG) {
        cpu_filter_two<long, long>((long *) filter_column_1->data_ptr, (long *) filter_column_2->data_ptr);
      } else if (filter_column_2->type == DOUBLE) {
        cpu_filter_two<long, double>((long *) filter_column_1->data_ptr, (double *) filter_column_2->data_ptr);
      } else if (filter_column_2->type == INT) {
        cpu_filter_two<int32_t, int32_t>((int32_t *) filter_column_1->data_ptr, (int32_t *) filter_column_2->data_ptr);
      }
    } else if (filter_column_1->type == DOUBLE) {
      if (filter_column_2->type == LONG) {
        cpu_filter_two<double, long>((double *) filter_column_1->data_ptr, (long *) filter_column_2->data_ptr);
      } else if (filter_column_2->type == DOUBLE) {
        cpu_filter_two<double, double>((double *) filter_column_1->data_ptr, (double *) filter_column_2->data_ptr);
      } else if (filter_column_2->type == INT) {
        cpu_filter_two<double, int32_t>((double *) filter_column_1->data_ptr, (int32_t *) filter_column_2->data_ptr);
      }
    } else if (filter_column_1->type == INT) {
      if (filter_column_2->type == LONG) {
        cpu_filter_two<int32_t, long>((int32_t *) filter_column_1->data_ptr, (long *) filter_column_2->data_ptr);
      } else if (filter_column_2->type == DOUBLE) {
        cpu_filter_two<int32_t, double>((int32_t *) filter_column_1->data_ptr, (double *) filter_column_2->data_ptr);
      } else if (filter_column_2->type == INT) {
        cpu_filter_two<int32_t, int32_t>((int32_t *) filter_column_1->data_ptr, (int32_t *) filter_column_2->data_ptr);
      }
    } else if (filter_column_1->type == STRING) {
      assert(filter_column_2->type == STRING);
      //TODO: Does not consider string col v.s. string col currently.
      std::cout << "GHive-CPP-ERROR [FilterPredicate-process]: Does not consider string col v.s. string col currently"
                << std::endl;
    }
  }
  std::cout << "GHive-CPP [FilterPredicate-process]: processing ends." << std::endl;
}

std::string FilterPredicate::toString() {
  return this->predicateLiteral;
}

std::string FilterPredicate::toString(int level) {
  std::string str;
  for (int i = 0; i < level; i++) {
    str += "  ";
  }
  str += "predicate: " + std::to_string(static_cast<int>(mode)) + "; ";
  if (mode == FILTER_AND || mode == FILTER_OR) {
    str += "filterCol: x;";
  } else {
    str += "filterCol: " + std::to_string(filterCol[0]) + ";";
  }
  str += "paramNum: " + std::to_string(paramNum) + ";";
  str += "params: ";
  if (paramNum == 0) {
    str += "x";
  } else if (dataType == 0) {
    for (int i = 0; i < paramNum; i++) {
      str += std::to_string(longFilterParams[i]) + " ";
    }
  } else if (dataType == 1) {
    for (int i = 0; i < paramNum; i++) {
      str += std::to_string(doubleFilterParams[i]) + " ";
    }
  } else if (dataType == 2) {
    for (int i = 0; i < paramNum; i++) {
      str += std::to_string(intFilterParams[i]) + " ";
    }
  } else if (dataType == 3) {
    for (int i = 0; i < paramNum; i++) {
      str += std::string(stringFilterParams[i]) + " ";
    }
  }
  str += ";\n";
  if (mode == FILTER_AND || mode == FILTER_OR) {
    if (childrenPredicate[0] != nullptr && childrenPredicate[1] != nullptr) {
      str += childrenPredicate[0]->toString(level + 1);
      str += childrenPredicate[1]->toString(level + 1);
    }
  }
  return str;
}
