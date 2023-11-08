#pragma once

#include "Profiler.hpp"

class SortGroupByProfiler: public Profiler {
 private:
  uint64_t sort_start;
  uint64_t sort_end;
  uint64_t sort_total;
  uint64_t agg_start;
  uint64_t agg_end;
  uint64_t agg_total;


 public:
  SortGroupByProfiler();

  void start_sort();

  void end_sort();

  void start_agg();

  void end_agg();

  std::string toString() override;

};
