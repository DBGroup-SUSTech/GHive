#include <sstream>
#include "Profile/SortMergeJoinProfiler.hpp"

SortMergeJoinProfiler::SortMergeJoinProfiler() {
  sort_start = 0;
  sort_end = 0;
  sort_total = 0;
  merge_start = 0;
  merge_end = 0;
  merge_total = 0;
}

void SortMergeJoinProfiler::start_sort() {
  sort_start = profiler_timestamp();
}

void SortMergeJoinProfiler::end_sort() {
  sort_end = profiler_timestamp();
  sort_total += sort_end - sort_start;
  sort_start = 0;
  sort_end = 0;
}

void SortMergeJoinProfiler::start_merge() {
  merge_start = profiler_timestamp();
}


void SortMergeJoinProfiler::end_merge() {
  merge_end = profiler_timestamp();
  merge_total += merge_end - merge_start;
  merge_start = 0;
  merge_end = 0;
}


std::string SortMergeJoinProfiler::toString() {
  std::stringstream ss;
  ss << Profiler::toString();
  ss << "sort total time: " << sort_total << std::endl;
  ss << "merge total time: " << merge_total << std::endl;
  return ss.str();
}