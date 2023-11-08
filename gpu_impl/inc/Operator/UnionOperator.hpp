#ifndef UNION_OPERATOR_HPP
#define UNION_OPERATOR_HPP

#include "Operator.hpp"

/**
 * @brief Union operator in Hive.
 * Check https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Union for behavior. 
 * Duplicate rows are removed.
 */
class UnionOperator : public Operator {
 private:

 public:
  std::string toString() override;

  std::string toString(int level) override;

  void execute() override;
};

#endif