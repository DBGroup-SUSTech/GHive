#ifndef FILTER_HPP
#define FILTER_HPP

#include <cassert>
#include <cstdint>
#include <regex>
#include <stack>
#include <string>
#include <vector>

#include "Operator/FilterOperator.cuh"
#include "Operator/FilterPredicate.hpp"
#include "Operator/Operator.hpp"

using namespace std;

class FilterOperator : public Operator {
 private:
  //  TODO: Parse the literal of several predicates to a vector of predicates.
  void parsePredicates();

 public:
  std::string predicatesLiteral;
  FilterPredicate *filter_predicate;
  thrust::host_vector<int> h_result;
  FilterProfiler profiler;

  FilterOperator();

  FilterOperator(std::string op_name, std::string predicates);

  std::string toString() override;

  std::string toString(int level) override;

  void execute() override;

  FilterPredicate *generatePredicate();

  FilterPredicate *generateBasicPredicate(std::string pred_string);

  void parseExtended() override;

  void parsePredicate(FilterPredicate *filterPredicate, string match_col_name, int param_num,
                      string match_value_1, string match_value_2);

  Table *clean_data(Table *tbl);
  Table *clean_data(Table *tbl, thrust::host_vector<int32_t> &host_result);
  Table *clean_data(thrust::device_vector<int32_t> &bitmap, Table *tbl, FilterProfiler &profiler);
};

#endif
