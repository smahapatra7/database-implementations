#include "gtest/gtest.h"
#include <iostream>
#include <stdlib.h>
#include "Statistics.h"
#include "ParseTree.h"
#include <math.h>
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

char *fName = "Statistics.txt";

TEST (Write_Test, t1) {

    //std::cout << "TEST 1 --> Testing Write!" << std::endl;
    Statistics stat;
    double resWrite;
    char *rNames[] = {"customer","orders"};
    stat.AddRel(rNames[0],150000);
    stat.AddAtt(rNames[0], "c_custkey",150000);
    stat.AddRel(rNames[1],2000000);
    stat.AddAtt(rNames[1], "o_custkey", 2000000);
    char *parseCNF = "(c_custkey=o_custkey)";
    yy_scan_string(parseCNF);
    yyparse();
    resWrite = stat.Estimate(final, rNames, 2);
    stat.Apply(final, rNames, 2);
    stat.Write(fName);
    ASSERT_EQ(true,resWrite>0);

}

TEST (Read_Test, t2) {

    //std::cout << "TEST 2 --> Testing Read!" << std::endl;
    char *rNames[] = { "part",  "partsupp"};
    Statistics stat,statRead;
    stat.AddRel(rNames[0],150000);
    stat.AddAtt(rNames[0], "p_partkey",150000);
    stat.AddAtt(rNames[0], "p_size",40);
    stat.AddRel(rNames[1],900000);
    stat.AddAtt(rNames[1], "ps_partkey",100000);
    char *parseCNF = "(p_partkey=ps_partkey) AND (p_size =10)";
    yy_scan_string(parseCNF);
    yyparse();
    stat.Estimate(final, rNames,2);
    stat.Apply(final, rNames,2);
    stat.Write(fName);
    statRead.Read(fName);
    parseCNF = "(p_size>2)";
    yy_scan_string(parseCNF);
    yyparse();
    ASSERT_NEAR(135000000000,statRead.Estimate(final, rNames, 2),0.5);

}


int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}