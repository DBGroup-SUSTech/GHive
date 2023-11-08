#include <iostream>
#include <gtest/gtest.h>
#include <Operator/FilterPredicate.hpp>
#include <Operator/GroupByPredicate.hpp>

#include "Operator/SelectPredicate.hpp"
#include "Operator/FilterOperator.hpp"


using namespace  std;
class SelectPredicateTest : public ::testing::Test {

};

TEST_F(SelectPredicateTest, stringConversionTest) {

    Table *tbl = new Table();
    tbl->row_num = 20;
    std::vector<std::string> all_str{
            "1221", "1223", "1238", "1221", "1238",
            "122", "1232", "1228", "123", "1224",
            "1248", "12223", "1237", "1222", "1230",
            "1221", "1234", "1220", "1239", "12234",
    };

    std::vector<char> all_chars;
    int *strIdxCol = new int[40];
    int size_char = 0;
    for (int i = 0; i < all_str.size(); i++) {
        strIdxCol[2 * i] = size_char;
        size_char += all_str[i].size();
        for (int j = 0; j < all_str[i].size(); j++) {
            all_chars.push_back(all_str[i][j]);
        }
        strIdxCol[2 * i + 1] = size_char;
    }
    all_chars.push_back('\0');
    char *strCol = all_chars.data();

    std::cout << std::string(strCol).size() << std::endl;
    tbl->columns.push_back(new Column(STRING, 20, strCol, strIdxCol, 80));
    FunctorSelectPredicate predicate(string_convert_int_functor(), 0);
    predicate.process(tbl);

    tbl->columns.push_back(predicate.column);
    std::cout << tbl->toString() << std::endl;

}



TEST_F(SelectPredicateTest, substringTest) {

    Table *tbl = new Table();
    tbl->row_num = 20;
    std::vector<std::string> all_str{
            "1221", "1223", "1238", "1221", "1238",
            "122", "1232", "1228", "123", "1224",
            "1248", "12223", "1237", "1222", "1230",
            "1221", "1234", "1220", "1239", "12234",
    };

    std::vector<char> all_chars;
    int *strIdxCol = new int[40];
    int size_char = 0;
    for (int i = 0; i < all_str.size(); i++) {
        strIdxCol[2 * i] = size_char;
        size_char += all_str[i].size();
        for (int j = 0; j < all_str[i].size(); j++) {
            all_chars.push_back(all_str[i][j]);
        }
        strIdxCol[2 * i + 1] = size_char;
    }
    all_chars.push_back('\0');
    char *strCol = all_chars.data();

    std::cout << std::string(strCol).size() << std::endl;
    tbl->columns.push_back(new Column(STRING, 20, strCol, strIdxCol, 80));
    substr_functor f(0, 3);
    FunctorSelectPredicate predicate(f, 0);
    predicate.process(tbl);

//    tbl->columns.push_back(predicate.column);

    FilterOperator filter_operator;
    FilterPredicate str_predicate;
    str_predicate.paramNum = 1;
    str_predicate.mode = FILTER_EQ;
    str_predicate.filterCol[0] = 0;
    str_predicate.stringFilterParams[0] = "122";
    str_predicate.dataType = 3;
    FilterProfiler profiler;
    profiler.start_op();
    str_predicate.process(tbl, profiler);
//  filter_operator.h_result = str_predicate->result_bitmap;
//  filter_operator.clean_data(tbl);
    filter_operator.clean_data(str_predicate.result_bitmap, tbl, profiler);
    profiler.end_op();

    std::cout << tbl->toString() << std::endl;
}
