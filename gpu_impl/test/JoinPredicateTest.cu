#include <iostream>
#include <gtest/gtest.h>
#include <Operator/JoinPredicate.hpp>
#include <Operator/BaseJoinPredicate.hpp>
#include "Operator/SortMergeJoinPredicate.hpp"
#include "DataFlow/DataFlow.hpp"
#include "Util/Util.hpp"

class JoinPredicateTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}

  virtual void SetUp() {}

  virtual void TearDown() {}
};

/*
 * Overall example
 * condition map:
    Inner Join 0 to 1
    Inner Join 0 to 2
  keys:
    0 _col1 (type: bigint)
    1 _col0 (type: bigint)
    2 _col0 (type: bigint)

  Idx    Tbl0           Tbl1          Tbl2
  0       2  2.0  ab     4   11.5 NULL 8   NULL  klm
  1       4  3.0  cd     2   3.0  a    2   2.0   ab
  2       5  4.1  e      11  11.1 b    11  11.1  b
  3       6  10.5 fgh    8   NULL klm  4   11.5  NULL
  4       4  11.5 NULL   8   NULL klm
  5       8  NULL klm    100 6.6  c
  6                      4   7.0  d

 **/

TEST_F(JoinPredicateTest, GPU_HASHJOIN_TABLE) {

  Table tb0;

  long dataCol0_0[]{1, 2, 3, 4, 5, 6};
  long dataCol0_1[]{2, 4, 5, 6, 4, 8};                  // key
  double dataCol0_2[]{2.0, 3.0, 4.1, 10.5, 11.5, Table::null_value<double>()}; // key
  char dataCol0_3[] = {"ab"
                       "cd"
                       "e"
                       "fgh"
                       "ij"
                       "klm"};
  int32_t dataAuxCol0_3[]{0, 2,
                          2, 4,
                          4, 5,
                          5, 8,
                          -1, -1,
                          10, 13};

  Column col0_0(LONG, 6, dataCol0_0);
  Column col0_1(LONG, 6, dataCol0_1);
  Column col0_2(DOUBLE, 6, dataCol0_2);
  Column col0_3(STRING, 6, dataCol0_3, dataAuxCol0_3, 13);

  tb0.row_num = 6;
  tb0.columns = {&col0_0, &col0_1, &col0_2, &col0_3};

  Table tb1;

  long dataCol1_0[]{4, 2, 11, 8, 8, 100, 4};                  // key
  double dataCol1_1[]{11.5, 3.0, 11.1, Table::null_value<double>(), Table::null_value<double>(), 6.6, 7.0}; // key
  char dataCol1_2[] = {"ij"
                       "a"
                       "b"
                       "klm"
                       "klm"
                       "c"
                       "d"};
  int32_t dataAuxCol1_2[]{-1, -1,
                          2, 3,
                          3, 4,
                          4, 7,
                          7, 10,
                          10, 11,
                          11, 12
  }; // key

  Column col1_0(LONG, 7, dataCol1_0);
  Column col1_1(DOUBLE, 7, dataCol1_1);
  Column col1_2(STRING, 7, dataCol1_2, dataAuxCol1_2, 12);

  tb1.row_num = 7;
  tb1.columns = {&col1_0, &col1_1, &col1_2};

  Table tb2;

  long dataCol2_0[]{8, 2, 11, 4};             // key
  double dataCol2_1[]{Table::null_value<double>(), 2.0, 11.1, 11.5}; // key
  char dataCol2_2[]{"klm"
                    "ab"
                    "b"
                    "ij"};
  int32_t dataAuxCol2_2[]{0, 3,
                          3, 5,
                          5, 6,
                          -1, -1};

  Column col2_0(LONG, 4, dataCol2_0);
  Column col2_1(DOUBLE, 4, dataCol2_1);
  Column col2_2(STRING, 4, dataCol2_2, dataAuxCol2_2, 8);

  tb2.row_num = 4;
  tb2.columns = {&col2_0, &col2_1, &col2_2};

  std::cout << "Table 0: " << std::endl << tb0.toString() << std::endl
            << "Table 1: " << std::endl << tb1.toString() << std::endl
            << "Table 2: " << std::endl << tb2.toString() << std::endl;

  BaseJoinPredicate *predicate = new HashJoinPredicate;
  predicate->join_cols = {{1, 2, 3}, {0, 1, 2}, {0, 1, 2}};
  predicate->join_types = {{LEFT_OUTER_JOIN, {0, 1}},
                           {LEFT_SEMI_JOIN, {0, 2}}};

  auto tbJoin = predicate->process({&tb0, &tb1, &tb2});

  std::vector<int32_t> expect_result_bitmap = {0, -1, 4, 0, 5, 3, 5, 4};
  // std::cout << tbJoin->toString() << std::endl;

  // ASSERT_EQ(expect_result_bitmap, predicate->result_offsets);

}

/*
 * Overall example
 * condition map:
    Left Semi Join 0 to 1
    Left Outer Join 0 to 2
    Left Outer Join 0 to 3
  keys:
    0 _col0 (type: bigint)
    1 _col0 (type: bigint)
    2 _col0 (type: bigint)
    3 _col0 (type: bigint)

  Idx    Tbl1        Tbl2        Tbl3        Tbl4
  0       4           1           1           5
  1       1           4           1           2
  2       3           4           3           4
  3       4           3           3           3
**/

TEST_F(JoinPredicateTest, SEMI_OUTER_TEST_TABLE) {
  std::vector<std::vector<uint32_t>> join_cols = {{0}, {0}, {0}, {0}};
  std::vector<std::pair<BaseJoinType, std::pair<uint32_t, uint32_t>>>
      join_types = {{LEFT_SEMI_JOIN, {0, 1}},
                    {LEFT_OUTER_JOIN, {0, 2}},
                    {LEFT_OUTER_JOIN, {0, 3}}};

  BaseJoinPredicate *predicate = new SortMergeJoinPredicate();
  predicate->join_cols = join_cols;
  predicate->join_types = join_types;
  predicate->maintain_cols = {{0}, {0}, {0}, {0}};

  Table tbl_1, tbl_2, tbl_3, tbl_4;
  tbl_1.row_num = tbl_2.row_num = tbl_3.row_num = tbl_4.row_num = 4;

  long long_col_1[4]{4, 1, 3, 4};
  long long_col_2[4]{1, 4, 4, 3};
  long long_col_3[4]{1, 1, 3, 3};
  long long_col_4[4]{5, 2, 4, 3};

  Column col_1(LONG, 4, long_col_1);
  Column col_2(LONG, 4, long_col_2);
  Column col_3(LONG, 4, long_col_3);
  Column col_4(LONG, 4, long_col_4);

  tbl_1.columns = {&col_1};
  tbl_2.columns = {&col_2};
  tbl_3.columns = {&col_3};
  tbl_4.columns = {&col_4};

  tbl_1.row_num = 4;
  tbl_2.row_num = 4;
  tbl_3.row_num = 4;
  tbl_4.row_num = 4;
  auto tbl_join = predicate->process({&tbl_1, &tbl_2, &tbl_3, &tbl_4});
  std::cout << "result offsets" << vector_to_string<int32_t>(predicate->result_offsets) << std::endl;
  std::cout << tbl_join->toString() << std::endl;
  std::vector<int32_t> expect_result_bitmap = {1, 0, -1, 1, 1, -1, 2, 2, 3, 2, 3, 3, 0, -1, 2, 3, -1, 2};
  ASSERT_EQ(vector_to_string<int32_t>(predicate->result_offsets), vector_to_string<int32_t>(expect_result_bitmap));

}

TEST_F(JoinPredicateTest, InnerJoinTest) {
  std::vector<std::vector<uint32_t>> join_cols = {{0}, {0}};
  std::vector<std::pair<BaseJoinType, std::pair<uint32_t, uint32_t>>>
      join_types = {{INNER_JOIN, {0, 1}}};

  SortMergeJoinPredicate *predicate = new SortMergeJoinPredicate();
  predicate->join_cols = join_cols;
  predicate->join_types = join_types;

  Table tbl_1, tbl_2;
  tbl_1.row_num = tbl_2.row_num = 4;

  long long_col_1[4]{19980101, 19980202, 19980202, 19980101};
  long long_col_2[4]{19980202, 19980202, 19980202, 19980202};

  Column col_1(LONG, 4, long_col_1);
  Column col_2(LONG, 4, long_col_2);

  tbl_1.columns = {&col_1};
  tbl_2.columns = {&col_2};

  tbl_1.row_num = 4;
  tbl_2.row_num = 4;

  std::vector<Table *> tables;
  tables.push_back(&tbl_1);
  tables.push_back(&tbl_2);

  predicate->maintain_cols = {{0}, {0}};
  std::cout << predicate->process(tables)->toString() << std::endl;
  std::cout << "result offsets" << vector_to_string<int32_t>(predicate->result_offsets) << std::endl;

}

TEST_F(JoinPredicateTest, SMInnerJoinBenchmark) {
  const uint32_t TABLE_1_ROW_NUM = 67108864;
  const uint32_t TABLE_2_ROW_NUM = 67108864;

  Table tbl_1, tbl_2;
  tbl_1.row_num = TABLE_1_ROW_NUM;
  tbl_2.row_num = TABLE_2_ROW_NUM;

  std::vector<std::vector<uint32_t>> join_cols = {{0}, {0}};
  std::vector<std::pair<BaseJoinType, std::pair<uint32_t, uint32_t>>>
      join_types = {{INNER_JOIN, {0, 1}}};
  SortMergeJoinPredicate *predicate = new SortMergeJoinPredicate();
  predicate->join_cols = join_cols;
  predicate->join_types = join_types;
  predicate->maintain_cols = {{0}, {0}};

  long *long_col_1 = new long[TABLE_1_ROW_NUM];
  long *long_col_2 = new long[TABLE_2_ROW_NUM];
  Column col_1(LONG, TABLE_1_ROW_NUM, long_col_1);
  Column col_2(LONG, TABLE_2_ROW_NUM, long_col_2);
  for (uint32_t i = 0; i < TABLE_1_ROW_NUM; i++) {
    long_col_1[i] = i * 2 + 5;
  }
  for (uint32_t i = 0; i < TABLE_2_ROW_NUM; i++) {
    long_col_2[i] = i * 20;
  }
  tbl_1.columns.push_back(&col_1);
  tbl_2.columns.push_back(&col_2);

  std::vector<Table *> tables;
  tables.push_back(&tbl_1);
  tables.push_back(&tbl_2);

  Table *res = predicate->process(tables);
  std::cout << predicate->profiler.toString() << std::endl;

}
