#include <regex>
#include <cfloat>
#include <DataFlow/Table.hpp>
#include <thrust/transform.h>
#include "Operator/SelectPredicate.hpp"

//1-finished
void SelectPredicate::setColType(std::string col_type) {
  if (col_type == "bigint") {
    colType = LONG;
  } else if (col_type == "double") {
    colType = DOUBLE;
  } else if (col_type == "int") {
    colType = INT;
  } else if (col_type == "string") {
    colType = STRING;
  } else {
    colType = DEPEND;
  }
}

void ConstantSelectPredicate::process(Table *table) {
  std::cout << "before constant select. type: " << colType;
  uint32_t row_num = table->row_num;
  switch (colType) {
    case LONG: {
      long *longCol = new long[row_num];
      for (uint32_t i = 0; i < row_num; i++) {
        longCol[i] = longConstant;
      }
      column = new Column(LONG, row_num, longCol);
      break;
    }
    case DOUBLE: {
      double *doubleCol = new double[row_num];
      for (uint32_t i = 0; i < row_num; i++) {
        doubleCol[i] = doubleConstant;
      }
      column = new Column(DOUBLE, row_num, doubleCol);
      break;
    }
    case INT: {
      int32_t *intCol = new int32_t[row_num];
      for (uint32_t i = 0; i < row_num; i++) {
        intCol[i] = intConstant;
      }
      column = new Column(INT, row_num, intCol);
      break;
    }
    case STRING: {
      char *str_col = new char[stringConstant.size()];
      int32_t *str_idx_col = new int32_t[2 * row_num];
      memcpy(str_col, stringConstant.c_str(), stringConstant.size() * sizeof(char));
      for (uint32_t i = 0; i < row_num; i++) {
        str_idx_col[2 * i] = 0;
        str_idx_col[2 * i + 1] = stringConstant.size();
      }
      column = new Column(STRING, row_num, str_col, str_idx_col, stringConstant.size());
      break;
    }
    case DEPEND: {
      break;
    }
  }
}

ConstantSelectPredicate::ConstantSelectPredicate() {}

void ColumnSwitchSelectPredicate::process(Table *table) {
//todo :1-finished1
  std::cout << "GHive-CPP [SelectPredicate-process]: select starts processing. offset: " << offset << std::endl;
  column = table->columns[offset];
  std::cout << "GHive-CPP [SelectPredicate-process]: end column switch. offset: " << offset << std::endl;
}

CalculationSelectPredicate::CalculationSelectPredicate(
    SelectPredicate *predicate_1, SelectPredicate *predicate_2,
    SelectCalculationType calculation_type) {
  this->predicate_1 = predicate_1;
  this->predicate_2 = predicate_2;
  this->calculate_type = calculation_type;
}

void CalculationSelectPredicate::process(Table *table) {
  std::cout << "GHive-CPP [CalculationSelectPredicate-process]: calculation select starts processing" << std::endl;
  std::cout << "predicate_1: " << predicate_1 << std::endl;
  std::cout << "predicate_2: " << predicate_2 << std::endl;
  predicate_1->process(table);
  predicate_2->process(table);
  switch (calculate_type) {
    case ADDITION: {
      if (predicate_1->column->type == LONG && predicate_2->column->type == LONG) {

        long *long_col_1 = (long *) predicate_1->column->data_ptr;
        long *long_col_2 = (long *) predicate_2->column->data_ptr;
        long *long_col = new long[table->row_num];
        thrust::transform(long_col_1,
                          long_col_1 + table->row_num,
                          long_col_2,
                          long_col,
                          select_plus<long, long, long>());
        column = new Column(LONG, table->row_num, long_col);
      } else if (predicate_1->column->type == INT && predicate_2->column->type == INT) {
        int32_t *int_col_1 = (int32_t *) predicate_1->column->data_ptr;
        int32_t *int_col_2 = (int32_t *) predicate_2->column->data_ptr;
        int32_t *int_col = new int32_t[table->row_num];
        thrust::transform(int_col_1,
                          int_col_1 + table->row_num,
                          int_col_2,
                          int_col,
                          select_plus<int32_t, int32_t, int32_t>());
        column = new Column(INT, table->row_num, int_col);
      } else if (predicate_1->column->type == DOUBLE && predicate_2->column->type == DOUBLE) {
        double *double_col_1 = (double *) predicate_1->column->data_ptr;
        double *double_col_2 = (double *) predicate_2->column->data_ptr;
        double *double_col = new double[table->row_num];
        thrust::transform(double_col_1,
                          double_col_1 + table->row_num,
                          double_col_2,
                          double_col,
                          select_plus<double, double, double>());
        column = new Column(DOUBLE, table->row_num, double_col);
      } else {
        std::cout << "GHive-CPP-ERROR [CalculationSelectPredicate]: Calculation only supports same type" << std::endl;
      }
      break;
    }
    case SUBTRACTION: {
      if (predicate_1->column->type == LONG && predicate_2->column->type == LONG) {

        long *long_col_1 = (long *) predicate_1->column->data_ptr;
        long *long_col_2 = (long *) predicate_2->column->data_ptr;
        long *long_col = new long[table->row_num];
        thrust::transform(long_col_1,
                          long_col_1 + table->row_num,
                          long_col_2,
                          long_col,
                          select_minus<long, long, long>());
        column = new Column(LONG, table->row_num, long_col);
      } else if (predicate_1->column->type == INT && predicate_2->column->type == INT) {
        int32_t *int_col_1 = (int32_t *) predicate_1->column->data_ptr;
        int32_t *int_col_2 = (int32_t *) predicate_2->column->data_ptr;
        int32_t *int_col = new int32_t[table->row_num];
        thrust::transform(int_col_1,
                          int_col_1 + table->row_num,
                          int_col_2,
                          int_col,
                          select_minus<int32_t, int32_t, int32_t>());
        column = new Column(INT, table->row_num, int_col);
      } else if (predicate_1->column->type == DOUBLE && predicate_2->column->type == DOUBLE) {
        std::cout << "predicate 1 and predicate 2 are null" << std::endl;
        double *double_col_1 = (double *) predicate_1->column->data_ptr;
        double *double_col_2 = (double *) predicate_2->column->data_ptr;
        double *double_col = new double[table->row_num];
        thrust::transform(double_col_1,
                          double_col_1 + table->row_num,
                          double_col_2,
                          double_col,
                          select_minus<double, double, double>());
        column = new Column(DOUBLE, table->row_num, double_col);
      } else {
        std::cout << "GHive-CPP-ERROR [CalculationSelectPredicate]: Calculation only supports same type" << std::endl;
      }
      break;
    }
    case MULTIPLICATION: {
      if (predicate_1->column->type == LONG && predicate_2->column->type == LONG) {

        long *long_col_1 = (long *) predicate_1->column->data_ptr;
        long *long_col_2 = (long *) predicate_2->column->data_ptr;
        long *long_col = new long[table->row_num];
        thrust::transform(long_col_1,
                          long_col_1 + table->row_num,
                          long_col_2,
                          long_col,
                          select_multiply<long, long, long>());
        column = new Column(LONG, table->row_num, long_col);
      } else if (predicate_1->column->type == INT && predicate_2->column->type == INT) {
        int32_t *int_col_1 = (int32_t *) predicate_1->column->data_ptr;
        int32_t *int_col_2 = (int32_t *) predicate_2->column->data_ptr;
        int32_t *int_col = new int32_t[table->row_num];
        thrust::transform(int_col_1,
                          int_col_1 + table->row_num,
                          int_col_2,
                          int_col,
                          select_multiply<int32_t, int32_t, int32_t>());
        column = new Column(INT, table->row_num, int_col);
      } else if (predicate_1->column->type == DOUBLE && predicate_2->column->type == DOUBLE) {
        double *double_col_1 = (double *) predicate_1->column->data_ptr;
        double *double_col_2 = (double *) predicate_2->column->data_ptr;
        double *double_col = new double[table->row_num];
        thrust::transform(double_col_1,
                          double_col_1 + table->row_num,
                          double_col_2,
                          double_col,
                          select_multiply<double, double, double>());
        column = new Column(DOUBLE, table->row_num, double_col);
      } else {
        std::cout << "GHive-CPP-ERROR [CalculationSelectPredicate]: Calculation only supports same type" << std::endl;
      }
      break;
    }
    case DIVISION: {
      // todo: division between different types
      // todo: division result is double?
      // long long
      // int int
      // double double
      // long int
      // double int

      if (predicate_1->column->type == LONG && predicate_2->column->type == LONG) {
        auto *long_col_1 = static_cast<long *> (predicate_1->column->data_ptr);
        auto *long_col_2 = static_cast<long *> (predicate_2->column->data_ptr);
        auto *double_col = new double[table->row_num];
        thrust::transform(long_col_1,
                          long_col_1 + table->row_num,
                          long_col_2,
                          double_col,
                          select_divide<long, long, double>());
        column = new Column(DOUBLE, table->row_num, double_col);
      } else if (predicate_1->column->type == INT && predicate_2->column->type == INT) {
        auto *int_col_1 = static_cast<int32_t *> (predicate_1->column->data_ptr);
        auto *int_col_2 = static_cast<int32_t *> (predicate_2->column->data_ptr);
        auto *double_col = new double[table->row_num];
        thrust::transform(int_col_1,
                          int_col_1 + table->row_num,
                          int_col_2,
                          double_col,
                          select_divide<int32_t, int32_t, double>());
        column = new Column(DOUBLE, table->row_num, double_col);
      } else if (predicate_1->column->type == DOUBLE && predicate_2->column->type == DOUBLE) {
        auto *double_col_1 = static_cast<double *> (predicate_1->column->data_ptr);
        auto *double_col_2 = static_cast<double *> (predicate_2->column->data_ptr);
        auto *double_col = new double[table->row_num];
        thrust::transform(double_col_1,
                          double_col_1 + table->row_num,
                          double_col_2,
                          double_col,
                          select_divide<double, double, double>());
        column = new Column(DOUBLE, table->row_num, double_col);
      } else if (predicate_1->column->type == LONG && predicate_2->column->type == INT) {
        auto *long_col1 = static_cast<long *> (predicate_1->column->data_ptr);
        auto *int_col_2 = static_cast<int32_t *> (predicate_2->column->data_ptr);
        auto *double_col = new double[table->row_num];
        thrust::transform(long_col1,
                          long_col1 + table->row_num,
                          int_col_2,
                          double_col,
                          select_divide<long, int32_t, double>());
        column = new Column(DOUBLE, table->row_num, double_col);
      } else if (predicate_1->column->type == DOUBLE && predicate_2->column->type == INT) {
        auto *int_col_1 = static_cast<double *> (predicate_1->column->data_ptr);
        auto *double_col_2 = static_cast<int32_t *> (predicate_2->column->data_ptr);
        auto *double_col = new double[table->row_num];
        thrust::transform(int_col_1,
                          int_col_1 + table->row_num,
                          double_col_2,
                          double_col,
                          select_divide<double, int32_t, double>());
        column = new Column(DOUBLE, table->row_num, double_col);
      } else {
        std::cout << "GHive-CPP-ERROR [CalculationSelectPredicate]: Calculation only supports same type: "
                  << " predicate_1.type == " << predicate_1->column->type
                  << " predicate_2.type == " << predicate_2->column->type << std::endl;
      }
      break;
    }
  }


//  if (colType == LONG) {
//    long *long_col = new long[table->row_num];
//    if (predicate_1->colType != LONG || predicate_2->colType != LONG) {
//      std::cout << "GHive-CPP-ERROR [CalculationSelectPredicate]: only long and long can produce long." << std::endl;
//      return;
//    }
//    long *long_col_1 = (long *) predicate_1->column->data_ptr;
//    long *long_col_2 = (long *) predicate_2->column->data_ptr;
//
//    switch (calculate_type) {
//      case ADDITION:
//        thrust::transform(long_col_1,
//                          long_col_1 + table->row_num,
//                          long_col_2,
//                          long_col,
//                          select_plus<long, long, long>());
//        break;
//      case SUBTRACTION:
//        thrust::transform(long_col_1,
//                          long_col_1 + table->row_num,
//                          long_col_2,
//                          long_col,
//                          select_minus<long, long, long>());
//        break;
//      case MULTIPLICATION:
//        thrust::transform(long_col_1,
//                          long_col_1 + table->row_num,
//                          long_col_2,
//                          long_col,
//                          select_multiply<long, long, long>());
//        break;
//      case DIVISION:
//        thrust::transform(long_col_1,
//                          long_col_1 + table->row_num,
//                          long_col_2,
//                          long_col,
//                          select_divide<long, long, long>());
//        break;
//    }
//    column = new Column(LONG, table->row_num, long_col);
//  }
//  if (colType == DOUBLE) {
//    double *double_col = new double[table->row_num];
//    if (predicate_1->colType != DOUBLE || predicate_2->colType != DOUBLE) {
//      std::cout << "GHive-CPP-ERROR [CalculationSelectPredicate]: only double and double can produce double."
//                << std::endl;
//      return;
//    }
//
//    double *double_col_1 = (double *) predicate_1->column->data_ptr;
//    double *double_col_2 = (double *) predicate_2->column->data_ptr;
//
//    switch (calculate_type) {
//      case ADDITION:
//        thrust::transform(double_col_1,
//                          double_col_1 + table->row_num,
//                          double_col_2,
//                          double_col,
//                          select_plus<double, double, double>());
//        break;
//      case SUBTRACTION:
//        thrust::transform(double_col_1,
//                          double_col_1 + table->row_num,
//                          double_col_2,
//                          double_col,
//                          select_minus<double, double, double>());
//        break;
//      case MULTIPLICATION:
//        thrust::transform(double_col_1,
//                          double_col_1 + table->row_num,
//                          double_col_2,
//                          double_col,
//                          select_multiply<double, double, double>());
//        break;
//      case DIVISION:
//        thrust::transform(double_col_1,
//                          double_col_1 + table->row_num,
//                          double_col_2,
//                          double_col,
//                          select_divide<double, double, double>());
//        break;
//    }
//    column = new Column(DOUBLE, table->row_num, double_col);
//  }
//  if (colType == INT) {
//    int32_t *int_col = new int[table->row_num];
//    if (predicate_1->colType != INT || predicate_2->colType != INT) {
//      std::cout << "GHive-CPP-ERROR [CalculationSelectPredicate]: only int and int can produce double." << std::endl;
//      return;
//    }
//
//    int32_t *int_col_1 = (int32_t *) predicate_1->column->data_ptr;
//    int32_t *int_col_2 = (int32_t *) predicate_2->column->data_ptr;
//
//    switch (calculate_type) {
//      case ADDITION:
//        thrust::transform(int_col_1,
//                          int_col_1 + table->row_num,
//                          int_col_2,
//                          int_col,
//                          select_plus<int32_t, int32_t, int32_t>());
//        break;
//      case SUBTRACTION:
//        thrust::transform(int_col_1,
//                          int_col_1 + table->row_num,
//                          int_col_2,
//                          int_col,
//                          select_minus<int32_t, int32_t, int32_t>());
//        break;
//      case MULTIPLICATION:
//        thrust::transform(int_col_1,
//                          int_col_1 + table->row_num,
//                          int_col_2,
//                          int_col,
//                          select_multiply<int32_t, int32_t, int32_t>());
//        break;
//      case DIVISION:
//        thrust::transform(int_col_1,
//                          int_col_1 + table->row_num,
//                          int_col_2,
//                          int_col,
//                          select_divide<int32_t, int32_t, int32_t>());
//        break;
//    }
//    column = new Column(DOUBLE, table->row_num, int_col);
//  }
  if (colType == STRING) {
    if (predicate_1->colType != STRING || predicate_2->colType != STRING) {
      std::cout << "GHive-CPP-ERROR [CalculationSelectPredicate]: only string and string can produce double."
                << std::endl;
      return;
    }
    std::cout << "GHive-CPP-ERROR [CalculationSelectPredicate-process]: "
                 "unsupported string calculation." << std::endl;
  }
  std::cout << "GHive-CPP [CalculationSelectPredicate-process]: "
               "End processing, with calculate_type: " << calculate_type << std::endl;
}

void ConcatSelectPredicate::process(Table *table) {
  std::cout << "GHive-CPP [ConcatSelectPredicate-process]: Concat select starts processing" << std::endl;
  for (SelectPredicate *each_predicate: predicates) {
    each_predicate->process(table);
  }
  // TODO: please fill the method.
  std::cout << "GHive-CPP [ConcatSelectPredicate-process]: " "End processing" << std::endl;
}

CaseSelectPredicate::CaseSelectPredicate(std::vector<condition> conditions, std::string yes_value,
                                         std::string no_value, ColumnType colType) :
    conditions(conditions), yes_value(yes_value), no_value(no_value) {
  this->colType = colType;
}