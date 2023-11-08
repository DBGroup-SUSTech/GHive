#include <iostream>
#include <gtest/gtest.h>
#include <Operator/FilterPredicate.hpp>
#include <Operator/GroupByPredicate.hpp>
#include "DataFlow/DataFlow.hpp"
#include <thrust/partition.h>
#include <thrust/device_vector.h>
#include <stack>
#include <Operator/FilterOperator.hpp>
#include <Profile/FilterProfiler.hpp>

using namespace  std;
class FilterPredicateTest : public ::testing::Test {

};

TEST_F(FilterPredicateTest, parsePredicateTest) {

  FilterOperator filterOp("FIL", "(lo_discount BETWEEN 1.0D AND 3.0D "
                                 "and (lo_quantity < 25.0D) and lo_orderdate is not null)");
  filterOp.output_cols = {"lo_orderdate", "lo_quantity", "lo_extendedprice", "lo_discount"};
  FilterPredicate *pred = filterOp.generatePredicate();
  std::string result_1 = "predicate: 11; filterCol: x;paramNum: 0;params: x;\n"
                         "  predicate: 11; filterCol: x;paramNum: 0;params: x;\n"
                         "    predicate: 10; filterCol: 0;paramNum: 0;params: x;\n"
                         "    predicate: 3; filterCol: 1;paramNum: 1;params: 25.000000 ;\n"
                         "  predicate: 6; filterCol: 3;paramNum: 2;params: 1.000000 3.000000 ;\n";
  ASSERT_EQ(pred->toString(0), result_1);


  FilterOperator filterOp2("FIL", "d_datekey is not null");
  filterOp2.output_cols = {"d_datekey","d_year"};
  FilterPredicate *pred2 = filterOp2.generatePredicate();
  std::string result_2 = "predicate: 10; filterCol: 0;paramNum: 0;params: x;\n";
  ASSERT_EQ(pred2->toString(0), result_2);
}




TEST_F(FilterPredicateTest, simpleOneColTest) {

  Table *tbl = new Table();

  long *longCol = new long[10];
  double *doubleCol = new double[10];
  int *intCol = new int[10];
  longCol[0] = 1;   doubleCol[0] = 2;   intCol[0] = 2;
  longCol[1] = 2;   doubleCol[1] = 3.5; intCol[1] = 3;
  longCol[2] = 2;   doubleCol[2] = 3;   intCol[2] = 2;
  longCol[3] = 1;   doubleCol[3] = 2;   intCol[3] = 2;
  longCol[4] = 5;   doubleCol[4] = 2;   intCol[4] = 2;
  longCol[5] = 2;   doubleCol[5] = 3.5; intCol[5] = 2;
  longCol[6] = 2;   doubleCol[6] = 3;   intCol[6] = 4;
  longCol[7] = 2;   doubleCol[7] = 3.5; intCol[7] = 3;
  longCol[8] = 2;   doubleCol[8] = 3;   intCol[8] = 2;
  longCol[9] = 2;   doubleCol[9] = 3;   intCol[9] = 2;

  char *strCol = new char[30];
  int strIdxCol[20] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5,
                       6, 6, 7, 7, 8, 8, 9, 9, 10};

  stpcpy(strCol, "xxxyxyyyyx");

  tbl->columns.push_back(new Column(LONG, 10, longCol));
  tbl->columns.push_back(new Column(DOUBLE, 10, doubleCol));
  tbl->columns.push_back(new Column(INT, 10, intCol));
  tbl->row_num = 10;
  FilterPredicate *predicate = new FilterPredicate();
  predicate->paramNum = 1;
  predicate->mode = FILTER_LE;
  predicate->filterCol[0] = 1;
  predicate->doubleFilterParams[0] = 3;
  predicate->dataType = 1;

  FilterProfiler profiler;
  std::cout << tbl->toString() << std::endl;
  predicate->process(tbl, profiler);
  FilterOperator filter_operator;
  filter_operator.h_result = predicate->result_bitmap;
  Table *res = filter_operator.clean_data(tbl);

  std::string result_1 = "row number: 7\n"
                         "column types: 0; 1; 2\n"
                         "row[0]: [1, 2.000000, 2]\n"
                         "row[1]: [2, 3.000000, 2]\n"
                         "row[2]: [1, 2.000000, 2]\n"
                         "row[3]: [5, 2.000000, 2]\n"
                         "row[4]: [2, 3.000000, 4]\n"
                         "row[5]: [2, 3.000000, 2]\n"
                         "row[6]: [2, 3.000000, 2]\n";
//  ASSERT_EQ(result_1, res->toString());

}




TEST_F(FilterPredicateTest, stringFilterTest) {


  Table *tbl = new Table();
  tbl->row_num = 20;

  double *doubleCol = new double[20];
  doubleCol[0] = 30000.0;
  doubleCol[1] = 80000.0;
  doubleCol[2] = 75000.0;
  doubleCol[3] = 27000.0;
  doubleCol[4] = 14660.0;
  doubleCol[5] = 23333.0;
  doubleCol[6] = 77000.0;
  doubleCol[7] = 38438.0;
  doubleCol[8] = 10395.0;
  doubleCol[9] = 22422.0;
  doubleCol[10] = 73300.0;
  doubleCol[11] = 27999.0;
  doubleCol[12] = 69897.0;
  doubleCol[13] = 12941.0;
  doubleCol[14] = 20719.0;
  doubleCol[15] = 75246.5;
  doubleCol[16] = 13932.4;
  doubleCol[17] = 83299.1;
  doubleCol[18] = 49278.3;
  doubleCol[19] = 46527.6;


  int *intCol = new int[20];
  intCol[0] = 1998;
  intCol[1] = 1998;
  intCol[2] = 1998;
  intCol[3] = 1998;
  intCol[4] = 1998;
  intCol[5] = 1998;
  intCol[6] = 1998;
  intCol[7] = 1998;
  intCol[8] = 1998;
  intCol[9] = 1998;
  intCol[10] = 1998;
  intCol[11] = 1998;
  intCol[12] = 1998;
  intCol[13] = 1997;
  intCol[14] = 1997;
  intCol[15] = 1997;
  intCol[16] = 1997;
  intCol[17] = 1997;
  intCol[18] = 1997;
  intCol[19] = 1997;


  std::vector<std::string> all_str{
      "MFGR#1221", "MFGR#1223", "MFGR#1238", "MFGR#1221", "MFGR#1238",
      "MFGR#122", "MFGR#1232", "MFGR#1228", "MFGR#123", "MFGR#1224",
      "MFGR#1248", "MFGR#12223", "MFGR#1237", "MFGR#1222", "MFGR#1230",
      "MFGR#1221", "MFGR#1234", "MFGR#1220", "MFGR#1239", "MFGR#12234",
  };

  std::vector<char> all_chars;
  int *strIdxCol = new int[40];
  int size_char = 0;
  for (int i = 0; i < all_str.size(); i++) {
    strIdxCol[2 * i] = size_char;
    size_char += all_str[i].size();
    for (int j = 0; j < all_str[i].size(); j++) {
      all_chars.push_back(all_str[i][j]);
    }
    strIdxCol[2 * i + 1] = size_char;
  }

  all_chars.push_back('\0');
  char *strCol = all_chars.data();

  tbl->columns.push_back(new Column(INT, 20, intCol));
  tbl->columns.push_back(new Column(STRING, 20, strCol, strIdxCol, 200));
  tbl->columns.push_back(new Column(DOUBLE, 20, doubleCol));

  FilterOperator filter_operator;
  FilterPredicate *str_predicate = new FilterPredicate();
  str_predicate->paramNum = 2;
  str_predicate->mode = FILTER_EQ_RANGE;
  str_predicate->filterCol[0] = 1;
  str_predicate->stringFilterParams[0] = "MFGR#1221";
  str_predicate->stringFilterParams[1] = "MFGR#1223";
  str_predicate->dataType = 3;
  FilterProfiler profiler;
  profiler.start_op();
  str_predicate->process(tbl, profiler);
//  filter_operator.h_result = str_predicate->result_bitmap;
//  filter_operator.clean_data(tbl);
  filter_operator.clean_data(str_predicate->result_bitmap, tbl, profiler);
  profiler.end_op();
  std::cout << profiler.toString() << std::endl;
  std::cout << tbl->toString() << std::endl;

//  std::string result = "row number: 6\n"
//                       "column types: 2; 3; 1\n"
//                       "row[0]: [1998, MFGR#1221, 30000.000000]\n"
//                       "row[1]: [1998, MFGR#1223, 80000.000000]\n"
//                       "row[2]: [1998, MFGR#1221, 27000.000000]\n"
//                       "row[3]: [1998, MFGR#12223, 27999.000000]\n"
//                       "row[4]: [1997, MFGR#1222, 12941.000000]\n"
//                       "row[5]: [1997, MFGR#1221, 75246.500000]\n";
//  ASSERT_EQ(result, tbl->toString());

}


TEST_F(FilterPredicateTest, gpuBenchmark) {

  const uint32_t ROW_CNT = 1342177280;
  Table *tbl = new Table();
  tbl->row_num = ROW_CNT;
  int32_t *key = new int32_t[ROW_CNT];
  double *value = new double[ROW_CNT];

  for (uint32_t x = 0; x < ROW_CNT; x++) {
    key[x] = x % 10;
    value[x] = x * 1.2;
  }
  Column *col_0 = new Column(INT, ROW_CNT, key);
  Column *col_1 = new Column(DOUBLE, ROW_CNT, value);
  tbl->columns = {col_0, col_1};
  FilterPredicate filter_predicate;
  filter_predicate.predColNum = 1;
  filter_predicate.filterCol[0] = 0;
  filter_predicate.paramNum = 1;
  filter_predicate.intFilterParams[0] = 5;
  filter_predicate.dataType = 2;
  filter_predicate.mode = FILTER_EQ;

  FilterOperator filter_operator;

  FilterProfiler profiler;
  profiler.start_op();
  filter_predicate.process(tbl, profiler);
//  profiler.start_pci_device2host();
//  filter_operator.h_result = filter_predicate.result_bitmap;
//  profiler.end_pci_device2host();
  filter_operator.clean_data(filter_predicate.result_bitmap, tbl, profiler);
  profiler.end_op();
  std::cout << tbl->toString() << std::endl;
  std::cout << profiler.toString() << std::endl;
}

TEST_F(FilterPredicateTest, gpuStringBenchmark) {

}

TEST_F(FilterPredicateTest, cpuBenchmark) {
  const uint32_t ROW_CNT = 536870912;
  Table *tbl = new Table();
  tbl->row_num = ROW_CNT;
  int32_t *key = new int32_t[ROW_CNT];
  double *value = new double[ROW_CNT];

  for (uint32_t x = 0; x < ROW_CNT; x++) {
    key[x] = x % 10;
    value[x] = x * 1.2;
  }
  Column *col_0 = new Column(INT, ROW_CNT, key);
  Column *col_1 = new Column(DOUBLE, ROW_CNT, value);
  tbl->columns = {col_0, col_1};
  FilterPredicate filter_predicate;
  filter_predicate.predColNum = 1;
  filter_predicate.filterCol[0] = 0;
  filter_predicate.paramNum = 1;
  filter_predicate.intFilterParams[0] = 5;
  filter_predicate.dataType = 2;
  filter_predicate.mode = FILTER_EQ;

  FilterOperator filter_operator;

  FilterProfiler profiler;
  profiler.start_op();
  filter_predicate.cpu_process(tbl);
  profiler.start_data_recover();
  filter_operator.clean_data(tbl, filter_predicate.host_result_bitmap);
  profiler.end_data_recover();
  profiler.end_op();
  std::cout << profiler.toString() << std::endl;
}

TEST_F(FilterPredicateTest, cpuStringBenchmark) {

}
