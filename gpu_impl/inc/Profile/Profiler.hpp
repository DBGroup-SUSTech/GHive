#ifndef GPUIMPL_PROFILER_HPP
#define GPUIMPL_PROFILER_HPP
#include <cstdint>
#include <chrono>
#include <cstdint>
#include <iostream>

/*
 * Return the nano timestamp.
 * */
inline int64_t profiler_timestamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
    ).count();
//  return std::chrono::high_resolution_clock::now().time_since_epoch().count();
}

inline long profiler_millis_timestamp() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()
  ).count();
}

class Profiler {
 public:
  uint64_t op_start;
  uint64_t op_end;

  uint64_t total_pcie_h2d;
  uint64_t total_pcie_d2h;

  uint64_t pci1_start;
  uint64_t pci1_end;
  uint64_t pci2_start;
  uint64_t pci2_end;

  uint64_t gpu_alloc_start;
  uint64_t gpu_alloc_end;
  uint64_t gpu_alloc_total;

  uint64_t gpu_exec_start;
  uint64_t gpu_exec_end;
  uint64_t gpu_exec_total;

  uint64_t cpu_alloc_start;
  uint64_t cpu_alloc_end;
  uint64_t cpu_alloc_total;

  uint64_t data_recover_start;
  uint64_t data_recover_end;
  uint64_t data_recover_total;


  Profiler();

  void start_pci_host2device();

  void end_pci_host2device();

  void start_pci_device2host();

  void end_pci_device2host();

  void start_gpu_alloc();

  void end_gpu_alloc();

  void start_gpu_exec();

  void end_gpu_exec();

  void start_cpu_alloc();

  void end_cpu_alloc();

  void start_data_recover();

  void end_data_recover();

  void start_op();

  void end_op();

  virtual std::string toString();

};

#endif //GPUIMPL_PROFILER_HPP
