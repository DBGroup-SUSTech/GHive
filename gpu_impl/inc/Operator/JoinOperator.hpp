#ifndef JOIN_HPP
#define JOIN_HPP
#include <map>

#include "JoinPredicate.hpp"
#include "Operator.hpp"
#include "SortMergeJoinPredicate.hpp"

class JoinOperator : public Operator {

 public:
  //static std::map<std::string, std::map<int, std::vector<int>>> retain_list_map;

  static std::map<std::string, std::map<int, std::vector<std::string>>> maintain_cols_map;

  static std::map<std::string, std::map<int, std::vector<std::string>>>
  create_maintain_cols_map() {
    map<std::string, std::map<int, std::vector<std::string>>> m;
    return m;
  }

  std::string conds;

  std::vector<Operator *> join_ops;

  SortMergeJoinPredicate predicate;

  std::string toString() override;

  std::string toString(int level) override;

  void parseExtended() override;

  void execute() override;

  JoinOperator(string operator_name, string conds, vector<string> outputs);
};

#endif
