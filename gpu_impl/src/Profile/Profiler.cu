#include <Profile/Profiler.hpp>
#include <sstream>

Profiler::Profiler() {
  total_pcie_h2d = 0;
  total_pcie_d2h = 0;
  data_recover_total = 0;
  gpu_alloc_total = 0;
  cpu_alloc_total = 0;
  gpu_exec_total = 0;
};

void Profiler::start_pci_host2device() {
  pci1_start = profiler_timestamp();
}

void Profiler::end_pci_host2device() {
  pci1_end = profiler_timestamp();
  total_pcie_h2d += pci1_end - pci1_start;
  pci1_start = 0;
  pci1_end = 0;
}

void Profiler::start_pci_device2host() {
  pci2_start = profiler_timestamp();
}

void Profiler::end_pci_device2host() {
  pci2_end = profiler_timestamp();
  total_pcie_d2h += pci2_end - pci2_start;
  pci2_start = 0;
  pci2_end = 0;
}

void Profiler::start_gpu_alloc() {
  gpu_alloc_start = profiler_timestamp();
}

void Profiler::end_gpu_alloc() {
  gpu_alloc_end = profiler_timestamp();
  gpu_alloc_total += gpu_alloc_end - gpu_alloc_start;
  gpu_alloc_start = 0;
  gpu_alloc_end = 0;
}

void Profiler::start_cpu_alloc() {
  cpu_alloc_start = profiler_timestamp();
}

void Profiler::end_cpu_alloc() {
  cpu_alloc_end = profiler_timestamp();
  cpu_alloc_total += cpu_alloc_end - cpu_alloc_start;
  cpu_alloc_start = 0;
  cpu_alloc_end = 0;
}

void Profiler::start_op() {
  op_start = profiler_timestamp();
};

void Profiler::end_op() {
  op_end = profiler_timestamp();
}


void Profiler::start_data_recover() {
  data_recover_start = profiler_timestamp();
}

void Profiler::end_data_recover() {
  data_recover_end = profiler_timestamp();
  data_recover_total += data_recover_end - data_recover_start;
  data_recover_start = 0;
  data_recover_end = 0;
}

std::string Profiler::toString() {
  std::stringstream ss;
  ss << "total time: " << op_end - op_start << std::endl;
  ss << "total pcie time: " << total_pcie_h2d + total_pcie_d2h << std::endl;
  ss << "    host to device: " << total_pcie_h2d << std::endl;
  ss << "    device to host: " << total_pcie_d2h << std::endl;
  ss << "gpu exec time: " << gpu_exec_total << std::endl;
  ss << "total gpu memory alloc time: " << gpu_alloc_total << std::endl;
  ss << "total cpu memory alloc time: " << cpu_alloc_total << std::endl;
  ss << "data recover time: " << data_recover_total << std::endl;
  return ss.str();
}

void Profiler::start_gpu_exec() {
  gpu_exec_start = profiler_timestamp();
}

void Profiler::end_gpu_exec() {
  gpu_exec_end = profiler_timestamp();
  gpu_exec_total += gpu_exec_end - gpu_exec_start;
  gpu_exec_start = 0;
  gpu_exec_end = 0;
}

