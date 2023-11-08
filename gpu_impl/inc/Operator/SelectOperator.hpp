#ifndef SELECT_HPP
#define SELECT_HPP
#include <regex>

#include "Operator/Operator.hpp"
#include "Operator/SelectPredicate.hpp"
#include "Util/Util.hpp"

class SelectOperator : public Operator {
 public:
  std::vector<SelectPredicate *> predicates;

  SelectOperator(std::string op_name, vector<string> cols);

  std::string toString() override;

  std::string toString(int level);

  SelectPredicate *generatePredicate(std::string expression,
                                     std::string expression_type = "");
                                     


  void parseExtended() override;

  void execute() override;
};

#endif
