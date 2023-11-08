#include "DataFlow/DataFlow.hpp"
#include <iostream>

uint32_t DataFlow::get_row_num() const { return this->rowNum; }


uint32_t DataFlow::get_long_col_num() const  { return this->longColNum; }


uint32_t DataFlow::get_double_col_num() const { return this->doubleColNum; }


uint32_t DataFlow::get_int_col_num() const {
  return intColNum;
}


uint32_t DataFlow::get_string_col_num() const {
  return this->stringColNum;
}




DataFlow::DataFlow(uint32_t longColNum, uint32_t doubleColNum,uint32_t intColNum, uint32_t stringColNum, uint32_t rowNum,
                   std::vector<uint32_t> sequence,
                   std::vector<std::string> colNames) {
  this->longColNum = longColNum;
  this->doubleColNum = doubleColNum;
  this->intColNum=intColNum;
  this->stringColNum=stringColNum;
  this->rowNum = rowNum;
  this->sequence = sequence;
}

DataFlow::DataFlow(uint32_t longColNum, uint32_t doubleColNum,uint32_t intColNum,uint32_t stringColNum,
                   uint32_t rowNum) {
  this->longColNum = longColNum;
  this->doubleColNum = doubleColNum;
  this->intColNum=intColNum;
  this->stringColNum=stringColNum;
  this->rowNum = rowNum;
}

void DataFlow::initColPointers() {
  this->longCols =
      this->longColNum != 0 ? new int64_t *[this->longColNum] : nullptr;
  this->doubleCols =
      this->doubleColNum != 0 ? new double *[this->doubleColNum] : nullptr;
  this->intCols =
      this->intColNum != 0 ? new int32_t *[this->intColNum] : nullptr;
  this->stringCols =
      this->stringColNum != 0 ? new char **[this->stringColNum] : nullptr;
  this->strCols =
      this->stringColNum != 0 ? new char *[this->stringColNum] : nullptr;
  this->strIdxCols =
      this->stringColNum != 0 ? new int32_t *[this->stringColNum] : nullptr;

}

void DataFlow::initCols() {
  assert(this->rowNum > 0);
  this->initColPointers();
  for (uint32_t i = 0; i < this->longColNum; ++i) {
    longCols[i] = new int64_t[this->rowNum];
  }
  for (uint32_t i = 0; i < this->doubleColNum; ++i) {
    doubleCols[i] = new double[this->rowNum];
  }
  for (uint32_t i = 0; i < this->intColNum; ++i) {
    intCols[i] = new int32_t [this->rowNum];
  }
  for (uint32_t i = 0; i < this->stringColNum; ++i) {
    stringCols[i] = new char* [this->rowNum];
    strIdxCols[i] = new int[this->rowNum * 2];
  }

}


std::string DataFlow::toString() {
  std::string rowNumLiteral = "rowNumLiteral: " + std::to_string(this->rowNum);
  std::string longColNumLiteral =
      "longColNumLiteral: " + std::to_string(this->longColNum);
  std::string doubleColNumLiteral =
      "doubleColNumLiteral: " + std::to_string(this->doubleColNum);
  std::string intColNumLiteral =
      "intColNumLiteral: " + std::to_string(this->intColNum);
  std::string stringColNumLiteral =
      "stringColNumLiteral: " + std::to_string(this->stringColNum);
  std::string sequenceLiteral = sequenceToString();
  std::string res = rowNumLiteral + "\n" + longColNumLiteral + "\n" +
                    doubleColNumLiteral + "\n" +intColNumLiteral+"\n"+stringColNumLiteral+"\n"
                    +sequenceLiteral + "\n";
  res += rowsToString(20);
  return res;
}

std::string DataFlow::rowsToString(uint32_t strRowNum) {
  strRowNum = strRowNum < rowNum ? strRowNum : rowNum;
  std::string res;
  for (uint32_t i = 0; i < strRowNum; ++i) {
    res += "row[" + std::to_string(i) + "]: [";
    for (uint32_t j = 0; j < this->longColNum; ++j) {
      res += std::to_string(longCols[j][i]) + ", ";
    }
    for (uint32_t j = 0; j < this->doubleColNum; ++j) {
      res += std::to_string(doubleCols[j][i]) + ", ";
    }
    for (uint32_t j = 0; j < this->intColNum; ++j) {
      res += std::to_string(intCols[j][i]) + ", ";
    }
    for (uint32_t j = 0; j < this->stringColNum; ++j) {
      res += std::string(strCols[j] + strIdxCols[j][2 * i],
                         strIdxCols[j][2 * i + 1] - strIdxCols[j][2 * i]) + ", ";
    }

    res += "]\n";
  }
  return res;
}

std::string DataFlow::sequenceToString() {
  std::string sequenceLiteral = "sequenceLiteral: [";
  for (auto i : this->sequence) {
    sequenceLiteral += std::to_string(i) + ", ";
  }
  sequenceLiteral += "]";
  return sequenceLiteral;
}

void DataFlow::cleanData(int *result_offset) {
  int idx = 0;
//  int str_idx[stringColNum];
//  memset(str_idx, 0, stringColNum * sizeof(int32_t));

  for (int i = 0; i < rowNum; i++) {
    if (result_offset[i]) {
      for (int j = 0; j < longColNum; j++) {
        longCols[j][idx] = longCols[j][i];
      }
      for (int j = 0; j < doubleColNum; j++) {
        doubleCols[j][idx] = doubleCols[j][i];
      }
      for (int j = 0; j < intColNum; j++) {
        intCols[j][idx] = intCols[j][i];
      }
      for (int j = 0; j < stringColNum; j++) {
        strIdxCols[j][2 * idx] = strIdxCols[j][2 * i];
        strIdxCols[j][2 * idx + 1] = strIdxCols[j][2 * i + 1];
      }
      idx ++;
    }
  }
  rowNum = idx;
}

std::vector<uint32_t> DataFlow::get_sequence() const { return this->sequence; }

void DataFlow::set_sequence(std::vector<uint32_t> sequence) {
  this->sequence = sequence;
}

int64_t **DataFlow::get_long_cols() const { return this->longCols; }

void DataFlow::set_long_cols(int64_t **long_cols) {
  this->longCols = long_cols;
}

double **DataFlow::get_double_cols() const { return this->doubleCols; }

void DataFlow::set_double_cols(double **double_cols) {
  this->doubleCols = double_cols;
}

int32_t **DataFlow::get_int_cols() const {
  return intCols;
}

void DataFlow::set_int_cols(int32_t **intCols) {
  DataFlow::intCols = intCols;
}

char ***DataFlow::get_string_cols() const {
  return stringCols;
}

void DataFlow::set_string_cols(char ***stringCols) {
  DataFlow::stringCols = stringCols;
}

DataFlow::~DataFlow() {}

DataFlow::DataFlow() {
}

