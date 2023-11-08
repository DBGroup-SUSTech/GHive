#ifndef GPU_IMPL_CCODE_INCLUDE_OPERATORS_JOIN_PREDICATE_HPP
#define GPU_IMPL_CCODE_INCLUDE_OPERATORS_JOIN_PREDICATE_HPP

#include <cstdint>
#include <vector>
#include <map>
#include "DataFlow/DataFlow.hpp"
#include "Profile/Profiler.hpp"
enum JoinCondition {
  //  Equal Join
  EQ = 0,

  //  Unknown Condition
  UNKNOWN_CONDITION = -1,
};

/**
 * Currently, only Inner Join (Equal) is implemented.
 */
enum JoinType {
  //  Inner Join
  IJ = 0,

  //  Left Outer Join
  LOJ = 1,

  //  Right Outer Join
  ROJ = 2,

  //  Full Outer Join
  FOJ = 3,

  //  Left Semi Join
  LSJ = 4,

  //  Cross Join
  CJ = 5,

  //  UNKNOWN
  UNKNOWN_TYPE = -1,
};

class JoinPredicate {
 private:
  JoinCondition join_condition;
  JoinType join_type;
  uint32_t left_join_col;
  uint32_t right_join_col;

 public:

  std::map<JoinType, std::pair<uint32_t, uint32_t>> join_types;
  std::vector<std::vector<uint32_t>> join_cols;

  JoinPredicate();

  JoinPredicate(JoinCondition join_condition, JoinType join_type,
                uint32_t left_join_col, uint32_t right_join_col);

  DataFlow* execute(DataFlow* left_ptr, DataFlow *right_ptr, Profiler& profiler);

};

#endif  // GPU_IMPL_CCODE_INCLUDE_OPERATORS_JOIN_PREDICATE_HPP
