#include "ghive_JNIInterface.h"
#include <iostream>
#include <sstream>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <map>
#include <Operator/JoinOperator.hpp>
#include <Operator/SinkOperator.hpp>
#include <Parser/Parser.hpp>

namespace pt = boost::property_tree;
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_hive_ql_exec_ghive_JNIInterface_GPUProcess
    (JNIEnv *env, jclass clz, jobject gpuInputJavaObject) {

  jclass gpuInputClass = env->GetObjectClass(gpuInputJavaObject);

  // Get Field of the maintainValuesJsonString
  jfieldID maintainValuesJsonStringId =
      env->GetFieldID(gpuInputClass, "maintainValuesJsonString", "Ljava/lang/String;");
  jstring
      maintainValuesJsonStringField = (jstring) (env->GetObjectField(gpuInputJavaObject, maintainValuesJsonStringId));

  jfieldID thisVertexNameId = env->GetFieldID(gpuInputClass, "thisVertexName", "Ljava/lang/String;");
  jstring thisVertexNameJString = (jstring) (env->GetObjectField(gpuInputJavaObject, thisVertexNameId));
  std::string thisVertexName = (std::string) env->GetStringUTFChars(thisVertexNameJString, nullptr);

  std::cout << "GHive-CPP: " << thisVertexName << " invokes the JNI" << std::endl;

  std::string maintainValuesJsonString =
      std::string(env->GetStringUTFChars(maintainValuesJsonStringField, nullptr));
  std::cout << "maintainValuesJsonStringField: " << maintainValuesJsonString << std::endl;

  // Start Parsing maintainValuesJsonString to maintain_cols_map.
  std::stringstream remain_col_ss(maintainValuesJsonString);

  pt::ptree json_root;
  pt::read_json(remain_col_ss, json_root);

  for (pt::ptree::iterator each_op = json_root.begin(); each_op != json_root.end(); ++each_op) {
    // each operator
    std::string op_name = each_op->first.data();
    std::map<int, std::vector<std::string>> tbls_col;
    for (pt::ptree::iterator tbl_col = each_op->second.begin(); tbl_col != each_op->second.end(); tbl_col++) {
      int tbl = stoi(tbl_col->first.data());
      std::vector<std::string> cols_name;
      for (pt::ptree::iterator col = tbl_col->second.begin(); col != tbl_col->second.end(); col++) {
        std::string col_name = col->second.get_value<std::string>();
        cols_name.push_back(col_name);
      }
      tbls_col[tbl] = cols_name;
    }
    JoinOperator::maintain_cols_map[op_name] = tbls_col;
  }
  // End Parsing maintainValuesJsonString to maintain_cols_map.


  jfieldID typesFieldId = env->GetFieldID(gpuInputClass, "types", "[[I");
  jobjectArray typesField = (jobjectArray) env->GetObjectField(gpuInputJavaObject, typesFieldId);

  // Get data fields.
  jfieldID longBufferArraysId = env->GetFieldID(gpuInputClass, "longCols", "[Ljava/nio/ByteBuffer;");
  jobjectArray longCols = (jobjectArray) env->GetObjectField(gpuInputJavaObject, longBufferArraysId);

  jfieldID doubleBufferArrayId = env->GetFieldID(gpuInputClass, "doubleCols", "[Ljava/nio/ByteBuffer;");
  jobjectArray doubleCols = (jobjectArray) env->GetObjectField(gpuInputJavaObject, doubleBufferArrayId);

  jfieldID intBufferArrayId = env->GetFieldID(gpuInputClass, "intCols", "[Ljava/nio/ByteBuffer;");
  jobjectArray intCols = (jobjectArray) env->GetObjectField(gpuInputJavaObject, intBufferArrayId);

  jfieldID stringBufferArrayId = env->GetFieldID(gpuInputClass, "stringCols", "[Ljava/nio/ByteBuffer;");
  jobjectArray stringCols = (jobjectArray) env->GetObjectField(gpuInputJavaObject, stringBufferArrayId);

  jfieldID stringIdxBufferArrayId = env->GetFieldID(gpuInputClass, "stringIdxCols", "[Ljava/nio/ByteBuffer;");
  jobjectArray stringIdxCols = (jobjectArray)
      env->GetObjectField(gpuInputJavaObject, stringIdxBufferArrayId);


  // Get column count and other meta fields.
  jfieldID vertexNameId = env->GetFieldID(gpuInputClass, "vertexName", "[Ljava/lang/String;");
  jobjectArray vertexNameField = (jobjectArray) env->GetObjectField(gpuInputJavaObject, vertexNameId);

  jfieldID keyCntId = env->GetFieldID(gpuInputClass, "keyCnt", "[I");
  jintArray keyCntArr = (jintArray) env->GetObjectField(gpuInputJavaObject, keyCntId);

  jfieldID rowCntId = env->GetFieldID(gpuInputClass, "rowCnt", "[I");
  jintArray rowCntArr = (jintArray) env->GetObjectField(gpuInputJavaObject, rowCntId);

  jint tableCnt = env->GetArrayLength(typesField);
  jint allLongColLength = env->GetArrayLength(longCols);
  jint allDoubleColLength = env->GetArrayLength(doubleCols);
  jint allIntColLength = env->GetArrayLength(intCols);
  jint allStringColLength = env->GetArrayLength(stringCols);

  std::cout << "GHive-CPP: all long col length: " << allLongColLength << std::endl;
  std::cout << "GHive-CPP: all double col length: " << allDoubleColLength << std::endl;
  std::cout << "GHive-CPP: all int col length: " << allIntColLength << std::endl;
  std::cout << "GHive-CPP: all string col length: " << allStringColLength << std::endl;

  jint *keyCntEachTable = env->GetIntArrayElements(keyCntArr, nullptr);
  jint *rowCntEachTable = env->GetIntArrayElements(rowCntArr, nullptr);

  jint longRetrieveIdx = 0;
  jint doubleRetrieveIdx = 0;
  jint intRetrieveIdx = 0;
  jint stringRetrieveIdx = 0;

  // If some table is missing, break and return nullptr back.
  bool insufficientInput = false;

  for (int i = 0; i < tableCnt; i++) {
    std::cout << "GHive-CPP: Read " << i << "-th table at C++ Side: " << std::endl;
    jstring vName = (jstring) env->GetObjectArrayElement(vertexNameField, i);

    if (vName == nullptr) {
      insufficientInput = true;
      break;
    }
    std::string vertexName =
        (std::string) env->GetStringUTFChars(vName, nullptr);
    std::cout << "GHive-CPP: Input Vertex Name: " << vertexName << std::endl;

    jintArray eachTypeArr =
        (jintArray) env->GetObjectArrayElement(typesField, i);
    jint eachTypeSize = env->GetArrayLength(eachTypeArr);
    jint *eachType = env->GetIntArrayElements(eachTypeArr, nullptr);

    uint32_t keyNum = keyCntEachTable[i];
    uint32_t rowNum = rowCntEachTable[i];

    std::cout << "GHive-CPP: Input keyNum: " << keyNum << std::endl;
    std::cout << "GHive-CPP: Input row_num: " << rowNum << std::endl;

    std::vector<ColumnType> table_types;
    std::cout << "GHive-CPP: Input eachType: ";
    for (jint j = 0; j < eachTypeSize; j++) {
      std::cout << eachType[j] << " ";
      if (eachType[j] == 0) {
        table_types.push_back(LONG);
      } else if (eachType[j] == 1) {
        table_types.push_back(DOUBLE);
      } else if (eachType[j] == 2) {
        table_types.push_back(INT);
      } else if (eachType[j] == 3) {
        table_types.push_back(STRING);
      }
    }
    Table *tbl = new Table();
    tbl->row_num = rowNum;
    tbl->key_num = keyNum;
    std::cout << "GHive-CPP: create the input table successfully" << std::endl;

    for (ColumnType type: table_types) {
      switch (type) {
        case LONG: {
          jobject eachLongCol = (jobject) env->GetObjectArrayElement(longCols, longRetrieveIdx++);
          Column *longColumn = new Column(LONG, rowNum);
          longColumn->set_data_ptr(env->GetDirectBufferAddress(eachLongCol));
          tbl->columns.push_back(longColumn);
          break;
        }
        case DOUBLE: {
          jobject eachDoubleCol = (jobject) env->GetObjectArrayElement(doubleCols, doubleRetrieveIdx++);
          Column *doubleColumn = new Column(DOUBLE, rowNum);
          doubleColumn->set_data_ptr(env->GetDirectBufferAddress(eachDoubleCol));
          tbl->columns.push_back(doubleColumn);
          break;
        }
        case INT: {
          jobject eachIntCol = (jobject) env->GetObjectArrayElement(intCols, intRetrieveIdx++);
          Column *intColumn = new Column(INT, rowNum);
          intColumn->set_data_ptr(env->GetDirectBufferAddress(eachIntCol));
          tbl->columns.push_back(intColumn);
          break;
        }
        case STRING: {
          jobject eachStringColBuffer = (jobject) env->GetObjectArrayElement(stringCols, stringRetrieveIdx);
          jobject eachStringIdxColBuffer = (jobject) env->GetObjectArrayElement(stringIdxCols, stringRetrieveIdx);
          stringRetrieveIdx++;
          auto *data_ptr = env->GetDirectBufferAddress(eachStringColBuffer);
          auto *data_ptr_aux = env->GetDirectBufferAddress(eachStringIdxColBuffer);
          int size_char = 0;
          for (int x = 0; x < rowNum; x++) {
            size_char = ((int32_t *) data_ptr_aux)[2 * x] > size_char ?
                        ((int32_t *) data_ptr_aux)[2 * x] : size_char;
            size_char = ((int32_t *) data_ptr_aux)[2 * x + 1] > size_char ?
                        ((int32_t *) data_ptr_aux)[2 * x + 1] : size_char;
          }
          auto *stringColumn = new Column(STRING, rowNum, data_ptr, data_ptr_aux, size_char);

          tbl->columns.push_back(stringColumn);
          break;
        }
        case DEPEND: {

          break;
        }
      }

    }

    if (thisVertexName == vertexName) {
      SinkOperator::table_map["TableScan"] = tbl;
    } else {
      SinkOperator::table_map[vertexName] = tbl;
    }
    std::cout << "GHive-CPP: Input Data: " << tbl->toString() << std::endl;
  }

  Table *result_tbl = nullptr;
  std::cout << "GHive-CPP: Input Finished insufficientInput=" << insufficientInput << std::endl;
  if (!insufficientInput) {
    result_tbl = execute_plan(thisVertexName);
  }
  if (result_tbl != nullptr) {
    auto result = generate_tbl_result(env, result_tbl);
    return result;
  } else {
    return nullptr;
  }
}

jobject generate_tbl_result(JNIEnv *env, Table *result_tbl) {

  uint32_t long_col_num = 0;
  uint32_t double_col_num = 0;
  uint32_t int_col_num = 0;
  uint32_t str_col_num = 0;
  for (Column *column: result_tbl->columns) {
    switch (column->type) {
      case LONG: {
        long_col_num++;
        break;
      }
      case DOUBLE: {
        double_col_num++;
        break;
      }
      case INT: {
        int_col_num++;
        break;
      }
      case STRING: {
        str_col_num++;
        break;
      }
      case DEPEND:break;
    }
  }
  std::cout << "GHive-CPP [generate_tbl_result]: long_col_num = " << long_col_num << std::endl;
  std::cout << "GHive-CPP [generate_tbl_result]: double_col_num = " << double_col_num << std::endl;
  std::cout << "GHive-CPP [generate_tbl_result]: int_col_num = " << int_col_num << std::endl;
  std::cout << "GHive-CPP [generate_tbl_result]: string_col_num = " << str_col_num << std::endl;
  // Find the Class of Array.
  jclass longArrayClass = env->FindClass("[J");
  std::cout << "GHive-CPP [generate_tbl_result]: find long columns class" << std::endl;
  jobjectArray long_cols_res =
      env->NewObjectArray(long_col_num, longArrayClass, NULL);
  std::cout << "GHive-CPP [generate_tbl_result]: generate long columns" << std::endl;
  jclass doubleArrayClass = env->FindClass("[D");
  std::cout << "GHive-CPP [generate_tbl_result]: find double columns class" << std::endl;
  jobjectArray double_cols_res =
      env->NewObjectArray(double_col_num, doubleArrayClass, NULL);
  std::cout << "GHive-CPP [generate_tbl_result]: generate double columns" << std::endl;
  jclass intArrayClass = env->FindClass("[I");
  jobjectArray int_cols_res =
      env->NewObjectArray(int_col_num, intArrayClass, NULL);
  jclass stringArrayClass = env->FindClass("[Ljava/lang/String;");
  jobjectArray string_cols_res =
      env->NewObjectArray(str_col_num, stringArrayClass, NULL);

  uint32_t seq_num = long_col_num + double_col_num + int_col_num + str_col_num;
  std::cout << "GHive-CPP [generate_tbl_result]: seq_num: " << seq_num << std::endl;
  jintArray seq = env->NewIntArray(result_tbl->columns.size());
  int32_t *seq_arr = new int32_t[result_tbl->columns.size()];

  uint32_t long_col_idx = 0;
  uint32_t double_col_idx = 0;
  uint32_t int_col_idx = 0;
  uint32_t str_col_idx = 0;
  uint32_t idx = 0;

  for (Column *column: result_tbl->columns) {
    switch (column->type) {
      case LONG: {
        seq_arr[idx++] = long_col_idx;
        jlongArray each_long_col = env->NewLongArray(result_tbl->row_num);
        env->SetLongArrayRegion(each_long_col, 0, result_tbl->row_num, (long *) column->data_ptr);
        env->SetObjectArrayElement(long_cols_res, long_col_idx++, each_long_col);
        break;
      }
      case DOUBLE: {
        seq_arr[idx++] = long_col_num + double_col_idx;
        jdoubleArray each_double_col = env->NewDoubleArray(result_tbl->row_num);
        env->SetDoubleArrayRegion(each_double_col, 0, result_tbl->row_num, (double *) column->data_ptr);
        env->SetObjectArrayElement(double_cols_res, double_col_idx++, each_double_col);
        break;
      }
      case INT: {
        seq_arr[idx++] = long_col_num + double_col_num + int_col_idx;
        jintArray each_int_col = env->NewIntArray(result_tbl->row_num);
        env->SetIntArrayRegion(each_int_col, 0, result_tbl->row_num, (int32_t *) column->data_ptr);
        env->SetObjectArrayElement(int_cols_res, int_col_idx++, each_int_col);
        break;
      }
      case STRING: {
        seq_arr[idx++] = long_col_num + double_col_num + int_col_num + str_col_idx;
        jclass stringClass = env->FindClass("Ljava/lang/String;");
        jobjectArray each_str_col = env->NewObjectArray(result_tbl->row_num, stringClass, nullptr);
        char *str_col = (char *) column->data_ptr;
        int32_t *str_idx_col = (int32_t *) column->data_ptr_aux;
        for (uint32_t j = 0; j < result_tbl->row_num; j++) {
          env->SetObjectArrayElement(each_str_col, j, env->NewStringUTF(
              std::string(str_col + str_idx_col[2 * j], str_idx_col[2 * j + 1] - str_idx_col[2 * j]).c_str()));
        }
        env->SetObjectArrayElement(string_cols_res, str_col_idx++, each_str_col);
        break;
      }
      case DEPEND: {
        break;
      }
    }
  }
  for (uint32_t i = 0; i < result_tbl->columns.size(); i++) {
    std::cout << "seq[" << i << "] = " << seq_arr[i];
  }
  env->SetIntArrayRegion(seq, 0, result_tbl->columns.size(), seq_arr);

  printf("generate result: FILE: %s, LINE: %d\n", __FILE__, __LINE__);
  jclass resultcls = env->FindClass("Lorg/apache/hadoop/hive/ql/exec/ghive/GPUResult;");
  if (resultcls == NULL) {
    std::cout << "resultcls is NULL" << std::endl;
  }
  jmethodID constructMID =
      env->GetMethodID(resultcls, "<init>", "([[J[[D[[I[[Ljava/lang/String;I[I)V");
  printf("generate result: FILE: %s, LINE: %d\n", __FILE__, __LINE__);
  jobject resultObj = env->NewObject(resultcls, constructMID, long_cols_res,
                                     double_cols_res, int_cols_res, string_cols_res, result_tbl->row_num, seq);
  printf("generate result: FILE: %s, LINE: %d\n", __FILE__, __LINE__);
  return resultObj;

}
