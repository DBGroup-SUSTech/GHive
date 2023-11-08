#include <Operator/LimitOperator.hpp>

void LimitOperator::execute() {
  for (auto child: this->children) {
    child->execute();
  }
  std::cout << "GHive-CPP [LimitOperator-process]: " << this->operator_name << " starts to execute, with input" <<
            std::endl << children[0]->op_result->toString() << std::endl;

  this->op_result = children[0]->op_result;
  this->op_result->row_num = min(limit_num, children[0]->op_result->row_num);

  std::cout << "GHive-CPP [LimitOperator-process]: " << this->operator_name << " ends to execute, with input" <<
            std::endl << children[0]->op_result->toString() << std::endl;
}

std::string LimitOperator::toString() {
  std::string ret = "limit_num: " + std::to_string(limit_num);
  return Operator::toString() + " " + ret;
}

std::string LimitOperator::toString(int level) {
  std::string ret;
  for (int i = 0; i < level; i++) {
    ret += " ";
  }
  ret += this->toString() + "\n";
  return ret;
}
