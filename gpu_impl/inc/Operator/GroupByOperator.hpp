#ifndef GPU_IMPL_CCODE_INCLUDE_OPERATORS_GROUPBY_HPP
#define GPU_IMPL_CCODE_INCLUDE_OPERATORS_GROUPBY_HPP

#include <string>
#include <vector>

#include "Operator/GroupByPredicate.hpp"
#include "Operator/Operator.hpp"

class GroupByOperator : public Operator {
 private:
  GroupByPredicate predicate;

 public:
  std::vector<std::string> aggs;
  std::vector<std::string> keys;
  SortGroupByProfiler profiler;

  GroupByOperator(std::string name, std::vector<std::string> cols,
                  std::vector<std::string> aggs, std::vector<std::string> keys);

  std::string toString() override;

  std::string toString(int level) override;

  void parseExtended() override;

  void execute() override;
};

#endif
