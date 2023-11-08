#ifndef DATAFLOW_HPP
#define DATAFLOW_HPP
#include <cassert>
#include <cstdint>
#include <string>
#include <vector>

class DataFlow {
 private:
 public:
  DataFlow();

  int64_t** longCols;
  double** doubleCols;
  int32_t** intCols;
  char*** stringCols;

  char** strCols;
  int32_t** strIdxCols;

  uint32_t longColNum;
  uint32_t doubleColNum;
  uint32_t intColNum;
  uint32_t stringColNum;

  uint32_t rowNum;
  uint32_t keyNum;
  std::vector<uint32_t> sequence;


  uint32_t get_row_num() const;

  uint32_t get_long_col_num() const;

  uint32_t get_double_col_num() const;


  uint32_t get_int_col_num() const;


  uint32_t get_string_col_num() const;


  /**
   * Constructor of a DataFlow obj. Set params but do not allocate space for
   * data.
   * @param longColNum Number of long cols.
   * @param doubleColNum Number of double cols.
   * @param intColNum Number of int cols.
   * @param stringColNum Number of string cols.

   * @param rowNum Number of rows.
   * @param sequence Sequence of long cols and double cols and int cols and string cols.
   * if the number < longColNum, it's long cols,if it's between longColNum and doubleColNum it's double.
   * then the same for int and string cols.
   *
   * @param colNames Name of columns.
   */
  DataFlow(uint32_t longColNum, uint32_t doubleColNum, uint32_t intColNum , uint32_t stringColNum , uint32_t rowNum,
           std::vector<uint32_t> sequence, std::vector<std::string> colNames);

  DataFlow(uint32_t longColNum, uint32_t doubleColNum, uint32_t intColNum,uint32_t stringColNum, uint32_t rowNum);

  /**
   * Allocate space for col pointers, but not cols.
   */
  void initColPointers();

  /**
   * Allocate space for col pointers and cols.
   * TODO: Check for memory leak.
   */
  void initCols();

  std::string toString();

  /**
   * Output first strRowNum rows as string.
   * @param strRowNum Num of first rows.
   * @return A string of rows.
   */
  // std::string rowsToString(uint32_t strRowNum);
  std::string rowsToString(uint32_t strRowNum);

  /**
   * Output sequence as string.
   * @return
   */
  std::string sequenceToString();

  std::vector<uint32_t> get_sequence() const;

  void set_sequence(std::vector<uint32_t> sequence);

  int64_t** get_long_cols() const;

  void set_long_cols(int64_t** long_cols);

  double** get_double_cols() const;

  void set_double_cols(double** double_cols);

  int32_t **get_int_cols() const;

  void set_int_cols(int32_t **intCols);

  char ***get_string_cols() const;

  void set_string_cols(char ***stringCols);

    /*
     * Covert old-version sequence (0 2 4 1 3 5) to new-version sequence (0 3 1 4
     * 2 5).
     * @param old_sequence Reference to old-version sequence.
     * @param long_col_num Number of long columns.
     * @param double_col_num Number of double columns.
     * @return New-version sequence.
     */
  //   static DataFlow::Sequence sequence_old_to_new(
  //       DataFlow::Sequence& old_sequence, uint32_t long_col_num,
  //       uint32_t double_col_num);

//  static std::vector<uint32_t> sequence_old_to_new(
//      std::vector<uint32_t>& old_sequence, uint32_t long_col_num,
//      uint32_t double_col_num);
  /*
   * Convert new-version sequence (0 3 1 4 2 5) to old-version sequence (0 2 4 1
   * 3 5).
   * @param new_sequence Reference to new-version sequence.
   * @param long_col_num Number of long columns.
   * @param double_col_num Number of double columns.
   * @return Old-version sequence.
   */
  //   static DataFlow::Sequence sequence_new_to_old(
  //       DataFlow::Sequence& old_sequence, uint32_t long_col_num,
  //       uint32_t double_col_num);

//  static std::vector<uint32_t> sequence_new_to_old(
//      std::vector<uint32_t>& old_sequence, uint32_t long_col_num,
//      uint32_t double_col_num);
  /**
   * TODO: Memory leak.
   */
  ~DataFlow();

  void cleanData(int *result_offset);
};

#endif
