#include <gtest/gtest.h>
#include <regex>
#include <Profile/Profiler.hpp>
#include <thrust/device_vector.h>

using namespace  std;
class GeneralTest : public ::testing::Test {

};

TEST_F(GeneralTest, shouldSuccess) {

  std::cout << "GeneralTest-shouldSuccess" << std::endl;

  std::string conds("SEL_51._col1=RS_49._col0(Inner)");
  std::regex split_re("[\\.=]");
  std::vector<std::string> v(std::sregex_token_iterator(conds.begin(), conds.end(), split_re, -1),
                             std::sregex_token_iterator());



  std::vector<std::string> children_names;
  for (std::string cond_split: v) {
    std::cout << cond_split  << std::endl;
    if (std::regex_match(cond_split, std::regex("[A-Z]+_[0-9]+"))) {
//      if (find(children_names.begin(), children_names.end(), cond_split) != children_names.end()) {
//        break;
//      }
      children_names.push_back(cond_split);
    }
  }
  for (std::string s: children_names) {
    std::cout << s << ";";
  }
}

TEST_F(GeneralTest, performanceTest) {
  uint32_t ROW_NUM = 300000000;
  Profiler profiler;

  profiler.start_op();
  int32_t *d_data;
//  cudaMalloc((void **)&d_data, ROW_NUM * sizeof(int32_t));
  thrust::host_vector<int32_t> vec(ROW_NUM);
  profiler.end_op();
  std::cout << profiler.toString() << std::endl;



}