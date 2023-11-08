#include <sstream>
#include "Profile/SortGroupByProfiler.hpp"

SortGroupByProfiler::SortGroupByProfiler() {
  sort_total = 0;
  agg_total = 0;
}

void SortGroupByProfiler::start_sort() {
  sort_start = profiler_timestamp();
}

void SortGroupByProfiler::end_sort() {
  sort_end = profiler_timestamp();
  sort_total += sort_end - sort_start;
  sort_start = 0;
  sort_end = 0;
}

void SortGroupByProfiler::start_agg() {
  agg_start = profiler_timestamp();
}

void SortGroupByProfiler::end_agg() {
  agg_end = profiler_timestamp();
  agg_total += agg_end - agg_start;
  agg_start = 0;
  agg_end = 0;
}

std::string SortGroupByProfiler::toString() {
  std::stringstream ss;
  ss << Profiler::toString();
  ss << "gpu execute time: " << sort_total + agg_total << std::endl;
  ss << "    sort total time: " << sort_total << std::endl;
  ss << "    aggregation total time: " << agg_total << std::endl;
  return ss.str();
}
