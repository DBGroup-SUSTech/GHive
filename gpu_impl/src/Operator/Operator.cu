#include <sstream>
#include "Operator/Operator.hpp"
#include "Operator/SinkOperator.hpp"

std::string Operator::toString() { return operator_name; }

std::string Operator::toString(int level) {
  std::string ret = "";
  for (int i = 0; i < level; i++) {
    ret += "  ";
  }
  ret += this->toString() + "\n";
  for (auto op : children) {
    ret += op->toString(level + 1);
  }
  return ret;
}

void Operator::execute_test() {
  std::cout << operator_name << std::endl;
  std::cout << extended_info << std::endl;
  for (auto child : this->children) {
    cout<<"child_execute: "<<child->operator_name<<endl;
    child->execute_test();
  }
}

void Operator::execute() {
  std::cout << "execute@Operators no implementation" << std::endl;
}

void Operator::parseExtended() { return; }

void Operator::profilerToString() {
//  if (typeid(*this) != typeid(SinkOperator) || !(((SinkOperator*)this)->is_input)) {
//    for (Operator *child: children) {
//      child->profilerToString();
//    }
//  }
}
