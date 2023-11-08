#include <gtest/gtest.h>
#include <thrust/sort.h>
#include <thrust/device_vector.h>
#include <chrono>
#include <Util/Util.hpp>

class OperatorBenchmarkTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}

  virtual void SetUp() {}

  virtual void TearDown() {}
};

struct Distribution {
  struct SAME;
  struct ASC;
};

template<typename DISTRIBUTION, typename T, int N_COLUMN>
thrust::host_vector<T>
generate_data(unsigned long n) {
  return {};
}

template<>
thrust::host_vector<int>
generate_data<Distribution::SAME, int, 1>(unsigned long n) {
  return thrust::host_vector<int>(n, 1);
}

template<>
thrust::host_vector<int>
generate_data<Distribution::ASC, int, 1>(unsigned long n) {
  thrust::host_vector<int> ret(n);
  thrust::sequence(ret.begin(), ret.end());
  return ret;
}

template<>
thrust::host_vector<int>
generate_data<Distribution::ASC, int, 2>(unsigned long n) {
  thrust::host_vector<int> ret(2 * n);
  thrust::sequence(ret.begin(), ret.begin() + n);
  thrust::sequence(ret.begin() + n, ret.end());
  return ret;
}

template<typename T>
class RAIIProfiler {
 private:
  decltype(std::chrono::high_resolution_clock::now()) _start;
  uint64_t &_total_time;
 public:
  explicit RAIIProfiler(uint64_t &total_time) :
      _start(std::chrono::high_resolution_clock::now()),
      _total_time(total_time) {}
  ~RAIIProfiler() {
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<T>(end - _start);
    _total_time = duration.count();
  }
};

struct Comp {
  struct Greater;
};

template<int N_COLUMN, typename COMP, typename T>
struct SortComparator {
  __host__ __device__ bool operator()(const T i, const T j) {
    return false;
  }
};

template<typename T>
struct SortComparator<2, Comp::Greater, T> {
  T *_value;
  SortComparator() = delete;
  explicit SortComparator(T *value) : _value(value) {}
  __host__ __device__ bool operator()(const T i, const T j) const {
    return _value[i] > _value[j];
  }
};

struct Transform {
  struct Abs;
};

template<int N_COLUMN, typename TRANSFORM, typename T>
struct UnaryTransform {};

template<typename T>
struct UnaryTransform<2, Transform::Abs, T> {
  T *_value;
  UnaryTransform() = delete;
  explicit UnaryTransform(T *value) : _value(value) {}
  __host__ __device__ T operator()(const T &x) const {
    return x < T(0) ? -x : x;
  }
};

struct Predicate {
  struct Equal;
};

template<int N_COLUMN, typename PREDICATE, typename T>
struct UnaryPredicate {};

template<typename T>
struct UnaryPredicate<2, Predicate::Equal, T> {
  const T _value;
  UnaryPredicate() = delete;
  explicit UnaryPredicate(const T value) : _value(value) {}
  __host__ __device__ bool operator()(const T x) const {
    return x == _value;
  }
};

TEST_F(OperatorBenchmarkTest, thrust_sort_test) {
  // 2^n: 2 ^ 0 - 2^28
  unsigned long generate_n = 1;
  std::vector<uint64_t> cost(28, 0);
  for (int times = 0; times < 28; ++times, generate_n *= 2) {

    uint64_t mus;

    // std::cout << generate_n << std::endl;
    auto h_key = generate_data<Distribution::ASC, int, 1>(generate_n);
    auto h_value = generate_data<Distribution::ASC, int, 1>(generate_n);

    auto d_key = thrust::device_vector<int>(h_key);
    auto d_value = thrust::device_vector<int>(h_value);

    auto d_raw_val = thrust::raw_pointer_cast(d_value.data());
    //
    // for (auto i: h_key) std::cout << i << ",";
    // std::cout << std::endl;

    {
      RAIIProfiler<std::chrono::microseconds> profiler(mus);
      thrust::sort(d_key.begin(), d_key.end(), SortComparator<2, Comp::Greater, int>(d_raw_val));
    }

    // for (auto i: d_key) std::cout << i << ",";
    // std::cout << std::endl;

    cost.at(times) = mus;
  }

  // todo : format output
  std::cout << vector_to_string(cost) << std::endl;

}

TEST_F(OperatorBenchmarkTest, thrust_reduce_test) {
  unsigned long generate_n = 1;
  std::vector<uint64_t> cost(28, 0);
  for (int times = 0; times < 28; ++times, generate_n *= 2) {

    uint64_t mus;

    // std::cout << generate_n << std::endl;
    auto h_input_key = generate_data<Distribution::SAME, int, 1>(generate_n);
    auto h_input_value = generate_data<Distribution::SAME, int, 1>(generate_n);

    auto d_input_key = thrust::device_vector<int>(h_input_key);
    auto d_input_value = thrust::device_vector<int>(h_input_value);

    auto d_output_key = thrust::device_vector<int>(h_input_key);
    auto d_output_value = thrust::device_vector<int>(h_input_value);

    //
    // for (auto i: h_input_key) std::cout << i << ",";
    // std::cout << std::endl;

    {
      RAIIProfiler<std::chrono::microseconds> profiler(mus);
      thrust::reduce_by_key(d_input_key.begin(), d_input_key.end(), d_input_value.begin(),
                            d_output_key.begin(), d_output_value.begin(),
                            thrust::equal_to<int>(), thrust::plus<int>());
    }

    // for (auto i: d_input_key) std::cout << i << ",";
    // std::cout << std::endl;
    // std::cout << " result = " << d_output_value[0] << std::endl;
    cost.at(times) = mus;
  }

  // todo : format output
  std::cout << vector_to_string(cost) << std::endl;
}

TEST_F(OperatorBenchmarkTest, thrust_transform_reduce_test) {
  // 2^n: 2 ^ 0 - 2^28
  unsigned long generate_n = 1;
  std::vector<uint64_t> cost(28, 0);
  for (int times = 0; times < 28; ++times, generate_n *= 2) {

    uint64_t mus;

    // std::cout << generate_n << std::endl;
    auto h_key = generate_data<Distribution::SAME, int, 1>(generate_n);
    auto h_value = generate_data<Distribution::SAME, int, 1>(generate_n);

    auto d_key = thrust::device_vector<int>(h_key);
    auto d_value = thrust::device_vector<int>(h_value);

    auto d_raw_value = thrust::raw_pointer_cast(d_value.data());

    //
    // for (auto i: h_key) std::cout << i << ",";
    // std::cout << std::endl;
    int result = 0;
    {
      RAIIProfiler<std::chrono::microseconds> profiler(mus);
      result = thrust::transform_reduce(d_key.begin(),
                                        d_key.end(),
                                        UnaryTransform<2, Transform::Abs, int>(d_raw_value),
                                        0,
                                        thrust::plus<int>());
    }

    // for (auto i: d_key) std::cout << i << ",";
    // std::cout << result << std::endl;
    cost.at(times) = mus;
  }

  // todo : format output
  std::cout << vector_to_string(cost) << std::endl;
}

TEST_F(OperatorBenchmarkTest, thrust_copy_if_test) {
  unsigned long generate_n = 1;
  std::vector<uint64_t> cost(28, 0);
  for (int times = 0; times < 28; ++times, generate_n *= 2) {

    uint64_t mus;

    // std::cout << generate_n << std::endl;
    auto h_key = generate_data<Distribution::ASC, int, 1>(generate_n);
    auto h_value = generate_data<Distribution::SAME, int, 1>(generate_n);

    auto d_key = thrust::device_vector<int>(h_key);
    auto d_value = thrust::device_vector<int>(h_value);

    auto d_output_key = thrust::device_vector<int>(h_key);

    //
    // for (auto i: h_key) std::cout << i << ",";
    // std::cout << std::endl;
    auto iter = d_output_key.begin();
    {
      RAIIProfiler<std::chrono::microseconds> profiler(mus);
      iter = thrust::copy_if(d_key.begin(), d_key.end(), d_value.begin(),
                             d_output_key.begin(), UnaryPredicate<2, Predicate::Equal, int>(1));
    }
    // std::cout << (iter == d_output_key.end()) << std::endl;
    // for (auto i: d_key) std::cout << i << ",";
    cost.at(times) = mus;
  }

  // todo : format output
  std::cout << vector_to_string(cost) << std::endl;
}

TEST_F(OperatorBenchmarkTest, thrust_unique_test) {
  unsigned long generate_n = 1;
  std::vector<uint64_t> cost(28, 0);
  for (int times = 0; times < 28; ++times, generate_n *= 2) {

    }

  // todo : format output
  std::cout << vector_to_string(cost) << std::endl;
}

TEST_F(OperatorBenchmarkTest, hashjoin_build_test) {}

TEST_F(OperatorBenchmarkTest, hashjoin_probe_test) {}
