#ifndef GPUIMPL_SRC_OPERATOR_SORTMERGEJOINPREDICATE_CUH_
#define GPUIMPL_SRC_OPERATOR_SORTMERGEJOINPREDICATE_CUH_

#include <Profile/SortMergeJoinProfiler.hpp>
#include "Operator/BaseJoinPredicate.hpp"

class SortMergeJoinPredicate : public BaseJoinPredicate {
 public:
  struct two_key_comparator {
    const void **data_ptr_1;
    const void **data_ptr_2;
    const int *types; // types[i] is the same for two input data.
    uint32_t data_col_num;
    two_key_comparator(const void **_data_ptr_1, const void **_data_ptr_2, const int *_types, uint32_t _data_col_num) :
        data_ptr_1(_data_ptr_1), data_ptr_2(_data_ptr_2), types(_types), data_col_num(_data_col_num) {}

    // NULL is biggest
    int compare(const int i, const int j) {
      for (int k = 0, l = 0; k < data_col_num; k++, l++) {
        if (types[k] == 0) { // Long
          long *longCol_1 = (long *) data_ptr_1[l];
          long *longCol_2 = (long *) data_ptr_2[l];
          if (longCol_1[i] < longCol_2[j]) {
            return 1;
          } else if (longCol_1[i] > longCol_2[j]) {
            return -1;
          } else if (Table::is_null(longCol_1[i]) && Table::is_null(longCol_2[j])) {
            return -1;
          }
        } else if (types[k] == 1) { // Double
          double *doubleCol_1 = (double *) data_ptr_1[l];
          double *doubleCol_2 = (double *) data_ptr_2[l];
          bool is_null_1 = Table::is_null(doubleCol_1[i]);
          bool is_null_2 = Table::is_null(doubleCol_2[j]);
          if (is_null_1 && is_null_2) {
            return -1;
          } else if (is_null_1 || is_null_2) {
            return is_null_2 ? 1 : -1;
          } else if (doubleCol_1[i] < doubleCol_2[j]) {
            return 1;
          } else if (doubleCol_1[i] > doubleCol_2[j]) {
            return -1;
          }
        } else if (types[k] == 2) { // Int
          int *intCol_1 = (int *) data_ptr_1[l];
          int *intCol_2 = (int *) data_ptr_2[l];
          if (intCol_1[i] < intCol_2[j]) {
            return 1;
          } else if (intCol_1[i] > intCol_2[j]) {
            return -1;
          } else if (Table::is_null(intCol_1[i]) && Table::is_null(intCol_2[j])) {
            return -1;
          }
        } else if (types[k] == 3) { // String
          char *strCol_1 = (char *) data_ptr_1[l];
          char *strCol_2 = (char *) data_ptr_2[l];
          l++;
          int *strIdxCol_1 = (int *) data_ptr_1[l];
          int *strIdxCol_2 = (int *) data_ptr_2[l];

          auto start_1 = strIdxCol_1[2 * i];
          auto start_2 = strIdxCol_2[2 * j];
          if (start_1 < 0 && start_2 < 0) {
            return -1;
          } else if (start_1 < 0 || start_2 < 0) {
            return start_2 < 0 ? 1 : -1;
          }

          int length_1 = strIdxCol_1[2 * i + 1] - strIdxCol_1[2 * i];
          int length_2 = strIdxCol_2[2 * j + 1] - strIdxCol_2[2 * j];
          int length_min = length_1 < length_2 ? length_1 : length_2;
          for (int p = 0; p < length_min; p++) {
            if (*(strCol_1 + strIdxCol_1[2 * i] + p) < *(strCol_2 + strIdxCol_2[2 * j] + p)) {
              return 1;
            } else if (*(strCol_1 + strIdxCol_1[2 * i] + p) > *(strCol_2 + strIdxCol_2[2 * j] + p)) {
              return -1;
            }
          }
          if (length_1 != length_2) return length_1 < length_2 ? 1 : -1;
        }
      }
      return 0;
    }
  };
  SortMergeJoinProfiler profiler;

  SortMergeJoinPredicate();

  Table *process(const std::vector<Table *> &tables) override;

};

#endif //GPUIMPL_SRC_OPERATOR_SORTMERGEJOINPREDICATE_CUH_
