#include "Profiler.hpp"

#ifndef GPUIMPL_PROFILE_FILTERPROFILER_HPP
#define GPUIMPL_PROFILE_FILTERPROFILER_HPP

class FilterProfiler: public Profiler {
 private:
  uint64_t transform_start;
  uint64_t transform_end;
  uint64_t transform_total;
  uint64_t copy_if_start;
  uint64_t copy_if_end;
  uint64_t copy_if_total;
 public:
  FilterProfiler();

  std::string toString();

  void start_transform();

  void end_transform();

  void start_copy_if();

  void end_copy_if();

};


#endif
