#ifndef PARSER_PARSER_HPP
#define PARSER_PARSER_HPP
#include <fstream>
#include <iostream>
#include <map>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

// #include "Operator/FilterOperator.hpp"
// #include "Operator/GroupByOperator.hpp"
// #include "Operator/JoinOperator.hpp"
// // #include "Operator/HashJoinOperator.hpp"
// #include "Operator/Operator.hpp"
// #include "Operator/SelectOperator.hpp"
// #include "Operator/TableScanOperator.hpp"
// #include "Operator/SinkOperator.hpp"
// #include "Util/Util.hpp"
#include "Operator/Operator.hpp"
#include "DataFlow/DataFlow.hpp"



void getRawExecPlanFromHDFS(std::string &execPlan,
                            const std::string &file_path);

/**
 * Get all content of an exec plan.
 * @param execPlan The reference of output exec plan.
 * @param file_path The file path.
 */
void getRawExecPlanFromFile(std::string& execPlan,
                            const std::string& file_path);
/*
void getRawExecPlan(std::string& execPlan, const std::string& hdfs_path) {
  std::unique_ptr<orc::InputStream> filePtr = orc::readHdfsFile(hdfs_path);
  char* textBuffer = new char[filePtr->getLength() + 1];
  filePtr->read(textBuffer, filePtr->getLength(), 0);
  execPlan = std::string(textBuffer);
  delete[] textBuffer;
}
*/

/**
 * Given a vertex name, extract the exec plan only related to it.
 * @param result The exec plan related to a vertex.
 * @param execPlan The complete exec plan.
 * @param vertexName The name of vertex.
 */
void getExecPlanBlock(std::string& result, std::string& execPlan,
                      std::string& vertexName);

void getExtendedExecPlanBlock(std::string& result, std::string& execPlan,
                              std::string& vertexName);

void getExtendedOperatorInfo(std::vector<std::string>& operators_info,
                             std::string& vertex_plan);

/**
 * Get an operator tree from an exec plan.
 * @param container_plan The exec plan related to a vertex.
 * @return The operator tree.
 */
Operator* getOperatorTree(std::string& container_plan);

void parse_test(std::string vertex_name);

// TODO: currently only support left-deep tree
// This is: no join(join(xxx), join(xxx))
// our operator tree is mirrored. i.e. the left-most child is actually
// right-most
void linkOperatorTreeAndExtendedPlan(std::vector<std::string>& vector_info,
                                     Operator* op, int ith);

Table *execute_plan(std::string vertex_name);

#endif
