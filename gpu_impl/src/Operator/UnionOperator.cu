#include "Operator/UnionOperator.hpp"

void UnionOperator::execute() {
  std::cout << "GHive-CPP [UnionOperator-execute] inside operator_name: "
            << operator_name << std::endl;
  for (auto child : this->children) {
    child->execute();
  }
  std::cout << "GHive-CPP [UnionOperator-execute]: " << operator_name
            << " starts to execute" << std::endl;
  std::cout << "GHive-CPP [UnionOperator-execute]: " << operator_name
            << " input 1: " << children[0]->op_result->toString() << std::endl;
  std::cout << "GHive-CPP [UnionOperator-execute]: " << operator_name
            << " input 2: " << children[1]->op_result->toString() << std::endl;


  std::cout << "GHive-CPP [UnionOperator-execute]: " << operator_name
            << " ends to execute with output: " << op_result->toString() << std::endl;
}


std::string UnionOperator::toString() {
  return Operator::toString();
}

std::string UnionOperator::toString(int level) {
  std::string ret = "";
  for (uint32_t i = 0; i < level; i++) {
    ret += " ";
  }
  ret += this->toString() + "\n";
  return ret;
}
