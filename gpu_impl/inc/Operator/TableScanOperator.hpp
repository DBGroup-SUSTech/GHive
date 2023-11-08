#include <string>
#include <vector>

#include "Operator/Operator.hpp"

#ifndef TABLESCAN_HPP
#define TABLESCAN_HPP

class TableScanOperator : public Operator {
 public:
  TableScanOperator(std::string operator_name,
                    std::vector<std::string> scan_cols);

  std::string toString() override;

  std::string toString(int) override;

  void execute() override;
};

#endif