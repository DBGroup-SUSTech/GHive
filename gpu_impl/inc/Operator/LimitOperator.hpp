#ifndef LIMIT_OPERATOR_HPP
#define LIMIT_OPERATOR_HPP

#include "Operator.hpp"

class LimitOperator : public Operator {
 private:
  int limit_num;

 public:
  LimitOperator(std::string operator_name, int limit_num) {
    this->operator_name = operator_name;
    this->limit_num = limit_num;
  }

  std::string toString() override;

  std::string toString(int level) override;

  void execute() override;
};


#endif
