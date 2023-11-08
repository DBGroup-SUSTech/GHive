#ifndef GPUIMPL_BASEJOINPREDICATE_HPP
#define GPUIMPL_BASEJOINPREDICATE_HPP

#include <cstdint>
#include <vector>
#include <map>
#include "DataFlow/DataFlow.hpp"
#include "DataFlow/Table.hpp"
#include "Profile/Profiler.hpp"

enum BaseJoinType {

  INNER_JOIN = 0,

  LEFT_OUTER_JOIN = 1,

  RIGHT_OUTER_JOIN = 2,

  FULL_OUTER_JOIN = 3,

  LEFT_SEMI_JOIN = 4,

  CROSS_JOIN = 5,

};

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

 * */


class BaseJoinPredicate {
 public:
  BaseJoinPredicate() = default;;

  /*
   * join_cols: the index used to join for each table
   * example: join_cols = {{0}, {0}, {0}, {0}}
   *  join_cols[0]: {0}
   *  join_cols[1]: {0}
   *  join_cols[2]: {0}
   *  join_cols[3]: {0}
   * */
  std::vector<std::vector<uint32_t>> join_cols;

  std::vector<std::vector<uint32_t>> maintain_cols;

  /*
   * join_types:
   *  <LSJ, <0, 1>>
   *  <LOJ, <0, 2>>
   *  <LOJ, <0, 3>>
   * */
  std::vector<std::pair<BaseJoinType, std::pair<uint32_t, uint32_t>>> join_types;

  /*
   * 1, 0, -1, 1, 1, -1, 2, 2, 1, 2, 3, 1, 0, -1, 2, 3, -1, 2
   * -1 represents null for outer join.
   * semi join: idx for the second table not contained.
   * */
  std::vector<int32_t> result_offsets;

  uint32_t cardinality{};

  std::vector<uint32_t> result_to_tbl;

  virtual Table *process(const std::vector<Table *> &tables) = 0;

  Table *generate_result(std::vector<Table *> tables);
};

#endif //GPUIMPL_BASEJOINPREDICATE_HPP
