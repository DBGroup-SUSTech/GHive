#include <gtest/gtest.h>
#include "Operator/Base64.h"
#include "Operator/RSA.h"
#include "DataFlow/Table.hpp"
#include "Operator/SelectPredicate.hpp"
#include <chrono>
#include <fstream>
#include "Operator/udf/RSADecrypt.h"
#include "Util/Util.hpp"



using namespace  std;
class RSATest : public ::testing::Test {



};



TEST_F(RSATest, RSATest) {
    char exp[128] = {107, -82, -91, -54, 88, 72, -25, 12, -34, 65, -54, -59, 124, 6, -88, -19, 51, -84, 105, -73, 79, 55, -40, -68, 115, 86, 72, -43, 46, 107, -115, 91, 41, -25, -90, 25, -8, -18, -66, -50, -16, -1, 106, 91, 60, 90, 14, -14, -40, 75, 95, -122, -22, -126, -8, -18, -51, 8, -50, -16, -23, 103, 71, -20, 83, 66, 60, 63, -32, 81, -77, 18, 2, -14, -28, 53, -66, 4, 31, -125, -44, 115, 49, 51, -86, 6, 54, 80, 18, 114, 78, 78, -40, 123, -63, -18, -86, -24, 2, 84, -6, -116, 24, -3, -111, 100, 43, -70, 55, 13, 45, 94, 36, -123, -83, -16, -39, -35, 96, -97, 91, 39, 80, 10, 56, 8, -102, 57};
    char mod[128] = {-112, 10, -96, 10, 91, 74, 30, -58, 90, 77, -36, 28, 42, -24, -42, 53, 108, -61, -88, 50, 10, 108, -55, 118, 46, -81, 101, -27, -50, -56, 122, 2, -79, 58, -19, -43, -31, 98, 109, -4, -62, 104, 117, 25, 59, 10, 79, -115, 85, -9, 8, 97, -13, 82, -64, -80, -18, -28, 16, -6, -18, -12, -27, -8, -27, 33, -80, 120, 7, 79, -32, 113, -62, -121, 127, -128, -7, -85, -59, -33, 31, 123, 66, 4, 26, 51, 84, -103, -123, 18, -47, 67, 85, -86, 5, -120, -110, -128, -82, 120, -117, 82, -40, -56, -122, -25, -14, 97, 68, -62, 94, 69, -120, 37, -60, -102, 103, 30, 83, 39, 10, -62, 32, 76, -39, 51, -58, 61};

    std::string Exponent = "75617024498692365584385751462342930957698313562765988763410623235194287559492097847431217744113616978356293714871925947116894300288386889742408415109822044974996134092237936326927814553796840211041819295015584421019306153830736962213019023732670655511549016137442333404992390016420132441347998671388482771513";
    std::string Modulus = "101149384303604554910884781679135581217591427031126150254881046964218639502519361893846179476996655017155905237203768028660650319590154133017271916990905179280478495022740066538101553337912020856443802362559881457536975295984320051395381431923531695001924526297612284788206607224582504897188631446446010517053";
//    std::string cipherText = "iqyntVuXixaY3QkN6eYVVC+Tf4eeDtJAX9eonxpjXdKNIeeGNITmRdB/+NyyJDK8qO8hJpEiC2OoxDS64aUgcJJH1Pv3vmHE0YqT9a8AyLJ3sxGV4ivahUN8pGvdhSTQazfRzVpaGqcUTS92GgWjvWb/kHzQskwBC5FLkkeDj/8=";
    std::string cipherText = "fbUwlfNfDic7V+Wh6DCsfctVsFVaSh+FuShiCOOJC+kYz/eLjNVHQBYAFHv9LykRzrW6ayQTSHKjPjy8VTSKbQg0VAxIUR3XxIQD0uu2wd80+Xx7X2xZD9NIEB2HVRBEmKzSWtLIKU/6k/TYFPhQcS8h90puRvGmZru91rXmFGo=";

    char cipherBuffer[128] = {0};
    macaron::Base64::Decode(cipherText, cipherBuffer);

    char tmp_exp[128], tmp_mod[128], tmp_ciph[128];
    std::cout << "char exp[128] = {";
    for (int i = 0; i < 128; i ++) {
        std::cout << (int) exp[127 - i];
        std::cout << (i == 127 ? "}": ",");
    }
    std::cout << std::endl;
    std::cout << "char mod[128] = {";
    for (int i = 0; i < 128; i ++) {
        std::cout << (int) mod[127 - i];
        std::cout << (i == 127 ? "}": ",");
    }


    for (int i = 0; i < 128; i ++) {
        tmp_exp[i] = exp[127 - i];
        tmp_mod[i] = mod[127 - i];
        tmp_ciph[i] = cipherBuffer[127 - i];
    }
    for (int i = 0; i < 128; i ++) {
        exp[i] = tmp_exp[i];
        mod[i] = tmp_mod[i];
        cipherBuffer[i] = tmp_ciph[i];
    }


    for (int i = 0; i < 128 ; i ++) {
        std::cout << (int)cipherBuffer[i];
    }
    char res[128] = {0};


    auto start = std::chrono::system_clock::now();
    rsa1024(reinterpret_cast<unsigned long *>((long *) res), reinterpret_cast<unsigned long *>(cipherBuffer),
            reinterpret_cast<unsigned long *>(exp), reinterpret_cast<unsigned long *>(mod));

    auto end = std::chrono::system_clock::now();
    std::cout << std::endl;
    std::cout << "result: ";
    for (int i = 0; i <128; i ++) {
        std::cout << (int)res[i];
    }

    std::cout << "time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() <<std::endl;

//    std::cout << std::endl;
//        int a=1;
//        char b =*(char*)(&a);
//        std::cout << "b: " << b;

}




TEST_F(RSATest, simpleDescrpytTest) {

    Table *tbl = new Table();
    tbl->row_num = 1;

    std::string cipherText = "fbUwlfNfDic7V+Wh6DCsfctVsFVaSh+FuShiCOOJC+kYz/eLjNVHQBYAFHv9LykRzrW6ayQTSHKjPjy8VTSKbQg0VAxIUR3XxIQD0uu2wd80+Xx7X2xZD9NIEB2HVRBEmKzSWtLIKU/6k/TYFPhQcS8h90puRvGmZru91rXmFGo=";

    char cipherBuffer[128] = {0};
    macaron::Base64::Decode(cipherText, cipherBuffer);

    char *strCol = new char[128];
    int *strIdxCol = new int[2];

    for (int i = 0; i < 128; i++) {
        strCol[i] = cipherBuffer[127 - i];
    }
    strIdxCol[0] = 0;
    strIdxCol[1] = 127;


    tbl->columns.push_back(new Column(STRING, 1, strCol, strIdxCol, 128));
    std::cout << tbl->toString(1, true) << std::endl;

    FunctorSelectPredicate predicate(rsa_decrypt_functor(), 0);
    auto start = std::chrono::system_clock::now();
    predicate.process(tbl);
    auto end = std::chrono::system_clock::now();
    std::cout << "time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() <<std::endl;
    tbl->columns.push_back(predicate.column);

    std::cout << tbl->toString(10) << std::endl;

//    tbl->columns.push_back(predicate.column);
//    std::cout << tbl->toString() << std::endl;

}



TEST_F(RSATest, MultipleDescrpytTest) {

    std::ifstream input( "number.txt" );
    std::vector<std::string> lines;
    for( std::string line; getline( input, line ); )
    {
        std::vector<std::string> each_line_elems;
        split(line, each_line_elems, ",");
        lines.push_back(each_line_elems[2]);
    }


    int row_num = 1000000;
    int char_size = row_num * 128;

    char *strCol = new char[char_size];
    int *strIdxCol = new int[row_num * 2];

    for (int i = 0; i < row_num; i++) {
        if (i % 100000 == 0) {
            std::cout << "number: " << i << std::endl;
        }
        strIdxCol[2 * i] = i * 128;
        strIdxCol[2 * i + 1] = i * 128 + 128;

        char cipherBuffer[128] = {0};
        macaron::Base64::Decode(lines[i], cipherBuffer);
        for (int j = 0; j < 128; j++) {
            strCol[128 * i + j] = cipherBuffer[127 - j];
        }
    }


    Table *tbl = new Table();
    tbl->row_num = row_num;

    tbl->columns.push_back(new Column(STRING, row_num, strCol, strIdxCol, char_size));
//    std::cout << tbl->toString(10, true) << std::endl;

    FunctorSelectPredicate predicate(rsa_decrypt_functor(), 0);
    auto start = std::chrono::system_clock::now();
    predicate.process(tbl);
    auto end = std::chrono::system_clock::now();
    std::cout << "time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() <<std::endl;
    tbl->columns.push_back(predicate.column);

//    std::cout << tbl->toString(10) << std::endl;

//    tbl->columns.push_back(predicate.column);

}
