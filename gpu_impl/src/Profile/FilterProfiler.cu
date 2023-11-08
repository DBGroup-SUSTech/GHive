#include <sstream>
#include "Profile/FilterProfiler.hpp"

FilterProfiler::FilterProfiler(): Profiler() {
  transform_total = 0;
  copy_if_total = 0;
}



void FilterProfiler::start_transform() {
  transform_start = profiler_timestamp();
}

void FilterProfiler::end_transform() {
  transform_end = profiler_timestamp();
  transform_total += transform_end - transform_start;
  transform_start = 0;
  transform_end = 0;
}

void FilterProfiler::start_copy_if() {
  copy_if_start = profiler_timestamp();
}

void FilterProfiler::end_copy_if() {
  copy_if_end = profiler_timestamp();
  copy_if_total += copy_if_end - copy_if_start;
  copy_if_start = 0;
  copy_if_end = 0;
}

std::string FilterProfiler::toString() {
  std::stringstream ss;
  ss << "total time: " << op_end - op_start << std::endl;
  ss << "total pcie time: " << total_pcie_h2d + total_pcie_d2h << std::endl;
  ss << "    host to device: " << total_pcie_h2d << std::endl;
  ss << "    device to host: " << total_pcie_d2h << std::endl;
  ss << "gpu exec time: " << gpu_exec_total << std::endl;
  ss << "    transform time: " << transform_total << std::endl;
  ss << "    copy if time: " << copy_if_total << std::endl;
  ss << "total gpu memory alloc time: " << gpu_alloc_total << std::endl;
  ss << "total cpu memory alloc time: " << cpu_alloc_total << std::endl;
  ss << "data recover time: " << data_recover_total << std::endl;
  return ss.str();
}