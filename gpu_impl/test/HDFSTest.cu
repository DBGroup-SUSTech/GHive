#include <iostream>
#include <gtest/gtest.h>
#include <Operator/FilterPredicate.hpp>
#include <Operator/GroupByPredicate.hpp>
#include "DataFlow/DataFlow.hpp"
#include "Hdfs/hdfs.h"

using namespace  std;
class HDFSTest : public ::testing::Test {

};
void getRawExecPlanFromHDFS(std::string &execPlan,
                            const std::string &file_path) {
  hdfsBuilder *bld = hdfsNewBuilder();
  //hdfsBuilderSetNameNode(bld, "default");
  hdfsBuilderSetNameNode(bld, "hdfs://dbg20:9000");
  hdfsFS fs = hdfsBuilderConnect(bld);
  hdfsFile readFile = hdfsOpenFile(fs, file_path.c_str(), O_RDONLY, 0, 0, 0);
  if (!readFile) {
    std::cout << "Failed to open hdfs file" << file_path << std::endl;
  }
  int MAX_BUF_LEN = 1024;
  char buffer[MAX_BUF_LEN + 1];
  while (true) {
    memset(buffer, 0, sizeof(buffer));
    int rc = hdfsRead(fs, readFile, buffer, MAX_BUF_LEN);
    if (rc <= 0) {
      break;
    } else {
      buffer[rc] = '\0';
    }
    execPlan.append(buffer);
  }
  hdfsCloseFile(fs, readFile);
}

TEST_F(HDFSTest, shouldSuccess) {
  std::string hdfs_file_path = "/tmp/plan/explain.txt";
  std::string hdfs_extended_plan_file_path = "/tmp/plan/explain_extended.txt";

  std::string raw_exec_plan;
  std::string raw_extended_exec_plan;

  getRawExecPlanFromHDFS(raw_exec_plan, hdfs_file_path);
  getRawExecPlanFromHDFS(raw_extended_exec_plan, hdfs_extended_plan_file_path);

  std::cout << raw_exec_plan << std::endl;
  std::cout << raw_extended_exec_plan << std::endl;

}