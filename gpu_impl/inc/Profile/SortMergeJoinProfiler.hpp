#include "Profiler.hpp"

class SortMergeJoinProfiler: public Profiler {
public:
  uint64_t sort_start;
  uint64_t sort_end;
  uint64_t sort_total;
  uint64_t merge_start;
  uint64_t merge_end;
  uint64_t merge_total;


  SortMergeJoinProfiler();

  void start_sort();

  void end_sort();

  void start_merge();

  void end_merge();

  std::string toString();

};
