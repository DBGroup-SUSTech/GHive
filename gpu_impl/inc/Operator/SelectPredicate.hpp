#ifndef SELECT_PREDICATE_HPP
#define SELECT_PREDICATE_HPP

#include <cstdint>
#include <iostream>
#include <string>
#include "DataFlow/Column.hpp"
#include "DataFlow/Table.hpp"
#include <functional>
#include <utility>
#include <thrust/device_vector.h>

#include "DataFlow/DataFlow.hpp"

class SelectPredicate {
 public:
  // TODO: column type use template
  ColumnType colType;
  Column *column;

  virtual void process(Table *table) = 0;

  void setColType(std::string col_type);
};


struct string_convert_int_functor {

    struct gpu_operator {
        char *e = "";
        char *data;
        int *idx;
        gpu_operator(void *data, void *idx): data((char *)data), idx((int *)idx) {}
        __host__ __device__
        int operator()(const int &offset) {
            int s_offset = idx[2 * offset];
            if (s_offset < 0) return -1;
            int e_offset = idx[2 * offset + 1];
            if (e_offset < 0 || e_offset < s_offset) return -1;
            int num = 0;
            bool neg_flag = false;
            for (int i = s_offset; i < e_offset; i ++){
                int pos = data[i] - '0';
                if (pos == '-') {
                    neg_flag = true;
                }
                else if (pos > 9 || pos < 0) {
                    printf("nan error");
                    asm("trap;");
                }
                num = num * 10 + (data[i] - '0');
            }
            return neg_flag? - num: num;
        }
    };

    Column* operator()(Column *col) {
        assert(col->type = STRING);

        std::cout << "debug point 0" << std::endl;
        char *dDataPtr;
        int *dIdxPtr;
        cudaMalloc((void **) &dDataPtr, sizeof(char) * col->char_size);
        cudaMalloc((void **) &dIdxPtr, sizeof(int) * col->row_num * 2);
        thrust::device_vector<int> res(col->row_num);
        std::cout << "debug point 1" << std::endl;
        CubDebugExit(cudaMemcpy(dDataPtr, col->data_ptr, sizeof(char) * col->char_size, cudaMemcpyHostToDevice));
        CubDebugExit(cudaMemcpy(dIdxPtr, col->data_ptr_aux, sizeof(int) * col->row_num * 2, cudaMemcpyHostToDevice));

        std::cout << "debug point 2" << std::endl;
        thrust::counting_iterator<int> iter(0);
        thrust::transform(iter, iter + col->row_num, res.begin(),
                          gpu_operator(dDataPtr, dIdxPtr));
        std::cout << "debug point 2" << std::endl;
//        thrust::host_vector<int> h_data = res;
        int *h_data = new int[col->row_num];
        cudaMemcpy(h_data, res.data().get(), sizeof(int) * col->row_num, cudaMemcpyDeviceToHost);
        return new Column(INT, col->row_num, h_data);
    }
};



struct substr_functor {

    int start, end;

    substr_functor(int start, int end): start(start), end(end) {}

    struct gpu_operator {
        char *data;
        int *idx;
        int start, end;
        gpu_operator(void *data, void *idx, int start, int end):
            data((char *)data), idx((int *)idx), start(start), end(end) {}
        __host__ __device__
        int operator()(const int &offset) {
            int s_offset = idx[2 * offset];
            int e_offset = idx[2 * offset + 1];
            idx [2 * offset] = s_offset + start;
            idx [2 * offset + 1] = s_offset + end;
            return 0;
        }
    };

    Column* operator()(Column *col) {
        assert(col->type = STRING);

        std::cout << "debug point 0" << std::endl;
        char *dDataPtr;
        int *dIdxPtr;
        cudaMalloc((void **) &dDataPtr, sizeof(char) * col->char_size);
        cudaMalloc((void **) &dIdxPtr, sizeof(int) * col->row_num * 2);
        thrust::device_vector<int> res(col->row_num);
        std::cout << "debug point 1" << std::endl;
        CubDebugExit(cudaMemcpy(dDataPtr, col->data_ptr, sizeof(char) * col->char_size, cudaMemcpyHostToDevice));
        CubDebugExit(cudaMemcpy(dIdxPtr, col->data_ptr_aux, sizeof(int) * col->row_num * 2, cudaMemcpyHostToDevice));

        std::cout << "debug point 2" << std::endl;
        thrust::counting_iterator<int> iter(0);
        thrust::transform(iter, iter + col->row_num, res.begin(),
                          gpu_operator(dDataPtr, dIdxPtr, start, end));
        std::cout << "debug point 2" << std::endl;
//        thrust::host_vector<int> h_data = res;
        CubDebugExit(cudaMemcpy(col->data_ptr, dDataPtr, sizeof(char) * col->char_size, cudaMemcpyDeviceToHost));
        CubDebugExit(cudaMemcpy(col->data_ptr_aux, dIdxPtr, sizeof(int) * col->row_num * 2, cudaMemcpyDeviceToHost));

        return col;
    }
};




class FunctorSelectPredicate: public SelectPredicate {
public:
    std::function<Column *(Column *)> fn;
    int offset;
    FunctorSelectPredicate(std::function<Column *(Column *)> fn, int offset): fn(std::move(fn)), offset(offset) {}
    void process(Table *table) {
        std::cout << "Functor Select Predicate begins" << std::endl;
        column = fn(table->columns[offset]);
    }
};

class ConstantSelectPredicate : public SelectPredicate {
 public:
  int64_t longConstant;
  double doubleConstant;
  int32_t intConstant;
  std::string stringConstant;
  ConstantSelectPredicate();

  void process(Table *table);

};

enum SelectCalculationType { ADDITION, SUBTRACTION, MULTIPLICATION, DIVISION };

class ColumnSwitchSelectPredicate : public SelectPredicate {
 public:
  int offset;

  ColumnSwitchSelectPredicate(int offset) { this->offset = offset; }

  void process(Table *table);
};

class ConcatSelectPredicate : public SelectPredicate {
 public:
  std::vector<SelectPredicate *> predicates;

  ConcatSelectPredicate() {
  }

  void process(Table *table);
};

class CalculationSelectPredicate : public SelectPredicate {
 public:
  SelectPredicate *predicate_1;
  SelectPredicate *predicate_2;
  SelectCalculationType calculate_type;

  CalculationSelectPredicate(SelectPredicate *predicate_1,
                             SelectPredicate *predicate_2,
                             SelectCalculationType calculation_type);

  void process(Table *table);
};

template<typename T, typename F, typename R>
struct select_plus {
  __host__ __device__
  R operator()(const T &x, const F &y) {
    if (Table::is_null(x) || Table::is_null(y))
      return Table::null_value<T>();
    else
      return x + y;
  }
};

template<typename T, typename F, typename R>
struct select_minus {
  __host__ __device__
  R operator()(const T &x, const F &y) {
    if (Table::is_null(x) || Table::is_null(y))
      return Table::null_value<T>();
    else
      return x - y;
  }
};

template<typename T, typename F, typename R>
struct select_multiply {
  __host__ __device__
  R operator()(const T &x, const F &y) {
    if (Table::is_null(x) || Table::is_null(y))
      return Table::null_value<T>();
    else
      return x * y;
  }
};

template<typename T, typename F, typename R>
struct select_divide {
  __host__ __device__
  R operator()(const T &x, const F &y) {
    if (Table::is_null(x) || Table::is_null(y))
      return Table::null_value<T>();
    else
      return x / y;
  }
};

class condition {
 public:
  uint32_t colIdx;
  std::string type;

};
class CaseSelectPredicate : public SelectPredicate {
 public:
  std::vector<condition> conditions;
  std::string yes_value;
  std::string no_value;

  CaseSelectPredicate(std::vector<condition> conditions, std::string yes_value,
                      std::string no_value, ColumnType colType);

};

#endif
