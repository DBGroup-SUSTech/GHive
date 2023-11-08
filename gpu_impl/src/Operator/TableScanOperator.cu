#include "Operator/TableScanOperator.hpp"
#include "Operator/SinkOperator.hpp"

TableScanOperator::TableScanOperator(std::string operator_name,
                                     std::vector<std::string> scan_cols) {
  this->operator_name = operator_name;
  this->output_cols = scan_cols;

}

std::string TableScanOperator::toString() {
  std::string output_cols_string = "scan columns: " + output_cols[0];
  for (uint32_t i = 1; i < output_cols.size(); i++) {
    output_cols_string += ",";
    output_cols_string += output_cols[i];
  }
  return "[" + operator_name + "]; " + output_cols_string;
}

std::string TableScanOperator::toString(int) { return this->toString(); }

void TableScanOperator::execute() {
  std::cout << "GHive-CPP [FilterOperator-process]: " << this->operator_name << " starts to execute" << std::endl;
  if (SinkOperator::table_map.find("TableScan") != SinkOperator::table_map.end()) {
    std::cout << "GHive-CPP: data for TableScan " << operator_name << " has already prepared.";
    op_result = SinkOperator::table_map["TableScan"];
  } else {
    std::cout << "GHive-CPP-ERROR: data for TableScan " << operator_name << " not found.";
  }
  std::cout << "GHive-CPP [FilterOperator-process]: " << this->operator_name << " ends to execute" << std::endl;
}
