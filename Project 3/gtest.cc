#include<iostream>
#include<gtest/gtest.h>
#include "Record.h"
#include "File.h"
#include "Heap.h"
#include "Sort.h"
#include "GenericDBFile.h"
#include "DBFile.h"
#include <cstring>
#include "BigQ.h"
#include "RelOp.h"



extern "C" {
	int yyparse(void);   // defined in y.tab.c
	int yyfuncparse(void);   // defined in yyfunc.tab.c
	void init_lexical_parser(char *); // defined in lex.yy.c (from Lexer.l)
	void close_lexical_parser(); // defined in lex.yy.c
	void init_lexical_parser_func(char *); // defined in lex.yyfunc.c (from Lexerfunc.l)
	void close_lexical_parser_func(); // defined in lex.yyfunc.c
}



//Test case to test SelectFile
TEST(TestSelectFile, SubTest1) {
	extern struct AndList *final;
	Record lit_ps;
	DBFile dbf_ps;
	CNF cnf_ps;
	SelectFile SF_ps;
	Pipe _ps(100);

	char *pred_ps = "(ps_supplycost < 1.03)";
	char filename[100] = "partsupp.bin";
	Schema testSchema("catalog", "partsupp");
	dbf_ps.Open(filename);
	init_lexical_parser(pred_ps);
	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF " << pred_ps << endl;
		exit(1);
	}
	cnf_ps.GrowFromParseTree(final, &testSchema, lit_ps); // constructs CNF predicate
	close_lexical_parser();

	SF_ps.Use_n_Pages(4);
	SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);
	SF_ps.WaitUntilDone();
	Record rec;
	int cnt = 0;
	while (_ps.Remove(&rec)) {
		cnt++;
	}
	dbf_ps.Close();
	if(cnt>0){ ASSERT_TRUE(true); }
	else{ ASSERT_FALSE(false); }

}

//Test Select Pipe
TEST(TestSelectPipe, SubTest2) {
	extern struct AndList *final;
	Record lit_ps;
	DBFile dbf_ps;
	CNF cnf_ps;
	SelectFile SF_ps;
	SelectPipe SP;
	Pipe _ps(100),_out(100);

	char *pred_ps = "(ps_supplycost < 1.03)";
	char filename[100] = "partsupp.bin";
	Schema testSchema("catalog", "partsupp");
	dbf_ps.Open(filename);
	init_lexical_parser(pred_ps);
	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF " << pred_ps << endl;
		exit(1);
	}
	cnf_ps.GrowFromParseTree(final, &testSchema, lit_ps); // constructs CNF predicate
	close_lexical_parser();

	SF_ps.Use_n_Pages(4);
	SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);
	SF_ps.WaitUntilDone();
	dbf_ps.Close();

	SP.Use_n_Pages(4);
	SP.Run(_ps,_out,cnf_ps,lit_ps);

	Record rec;
	int cnt = 0;
	while (_out.Remove(&rec)) {
		cnt++;
	}
	if (cnt > 0) { ASSERT_TRUE(true); }
	else { ASSERT_FALSE(false); }
}
//
// //Test Project
TEST(TestProject, SubTest3) {
	extern struct AndList *final;
	Record lit_ps;
	DBFile dbf_ps;
	CNF cnf_ps;
	SelectFile SF_p;
	Pipe _ps(100), _p(1000);

	char *pred_p = "(p_retailprice > 931.01) AND (p_retailprice < 931.3)";
	char filename[100] = "part.bin";
	Schema testSchema("catalog", "part");
	dbf_ps.Open(filename);
	init_lexical_parser(pred_p);
	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF " << pred_p << endl;
		exit(1);
	}
	cnf_ps.GrowFromParseTree(final, &testSchema, lit_ps); // constructs CNF predicate
	close_lexical_parser();
	SF_p.Use_n_Pages(8);

	Project P_p;
	Pipe _out(1000);
	int keepMe[] = { 0,1,7 };
	int numAttsIn = 9;
	int numAttsOut = 3;
	P_p.Use_n_Pages(8);

	SF_p.Run(dbf_ps, _p, cnf_ps, lit_ps);
	P_p.Run(_p, _out, keepMe, numAttsIn, numAttsOut);

	SF_p.WaitUntilDone();
	P_p.WaitUntilDone();

	Record rec;
	int cnt = 0;
	while (_out.Remove(&rec)) {
		cnt++;
	}
	if (cnt > 0) { ASSERT_TRUE(true); }
	else { ASSERT_FALSE(false); }
	dbf_ps.Close();
}
//
// //Test SUM

TEST(TestSUM, SubTest4) {
	extern struct AndList *final;
	Record lit_ps;
	DBFile dbf_ps;
	CNF cnf_ps;
	SelectFile SF_p;
	Pipe _ps(100), _p(1000);
	extern struct FuncOperator *finalfunc;

	char *pred_s = "(s_suppkey = s_suppkey)";
	char filename[100] = "supplier.bin";
	Schema testSchema("catalog", "supplier");
	dbf_ps.Open(filename);
	init_lexical_parser(pred_s);
	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF " << pred_s << endl;
		exit(1);
	}
	cnf_ps.GrowFromParseTree(final, &testSchema, lit_ps); // constructs CNF predicate
	close_lexical_parser();
	SF_p.Use_n_Pages(8);

	Sum T;
	Pipe _out(1000);
	Function func;
	char *str_sum = "(s_acctbal + (s_acctbal * 1.05))";
	init_lexical_parser_func(str_sum);
	if (yyfuncparse() != 0) {
		cout << " Error: can't parse your arithmetic expr. " << str_sum << endl;
		exit(1);
	}
	func.GrowFromParseTree(finalfunc, testSchema); // constructs CNF predicate
	close_lexical_parser_func();

	T.Use_n_Pages(1);

	SF_p.Run(dbf_ps, _ps, cnf_ps, lit_ps);
	T.Run(_ps, _out, func);

	SF_p.WaitUntilDone();
	T.WaitUntilDone();

	Record rec;
	int cnt = 0;
	while (_out.Remove(&rec)) {
		cnt++;
	}
	if (cnt > 0) { ASSERT_TRUE(true); }
	else { ASSERT_FALSE(false); }
	dbf_ps.Close();
 }


int main(int argc, char *argv[]) 
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

//
// //TestJoin
/*
TEST(TestJoin, SubTest5) {
	extern struct AndList *final;
	Record lit_ps,lit_s;
	DBFile dbf_ps,dbf_s;
	CNF cnf_ps, cnf_s;
	SelectFile SF_s,SF_ps;
	Pipe _s(100),_ps(100);

	char *pred_s = "(s_suppkey = s_suppkey)";
	char filename[100] = "supplier.bin";
	Schema testSchema("catalog", "supplier");
	dbf_s.Open(filename);
	init_lexical_parser(pred_s);
	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF " << pred_s << endl;
		exit(1);
	}
	cnf_s.GrowFromParseTree(final, &testSchema, lit_s); // constructs CNF predicate
	close_lexical_parser();
	SF_s.Run(dbf_s, _s, cnf_s, lit_s); // 10k recs qualified

	char *pred_ps = "(ps_suppkey = ps_suppkey)";
	char filename1[100] = "partsupp.bin";
	Schema testSchema1("catalog", "partsupp");
	dbf_ps.Open(filename1);
	init_lexical_parser(pred_ps);
	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF " << pred_ps << endl;
		exit(1);
	}
	cnf_ps.GrowFromParseTree(final, &testSchema1, lit_ps); // constructs CNF predicate
	close_lexical_parser();

	Join J;
	Pipe _s_ps(100000);
	CNF cnf_p_ps;
	Record lit_p_ps;
	init_lexical_parser("(s_suppkey = ps_suppkey)");
	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF " << "(s_suppkey = ps_suppkey)" << endl;
		exit(1);
	}
	cnf_p_ps.GrowFromParseTree(final, &testSchema, &testSchema1, lit_p_ps); // constructs CNF predicate
	close_lexical_parser();

	J.Use_n_Pages(8);
	SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);
	J.Run(_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);

	Record rec;
	int cnt = 0;
	while (_s_ps.Remove(&rec)) {
		cnt++;
	}
	dbf_ps.Close();
	if (cnt > 0) { ASSERT_TRUE(true); }
	else { ASSERT_FALSE(false); }
}

//Test duplicate removal and writeout
TEST(TestDuplicate, SubTest6) {

	try {
		extern struct AndList *final;
		Record lit_ps;
		DBFile dbf_ps;
		CNF cnf_ps;
		SelectFile SF_ps;
		Pipe _ps(100);

		char *pred_ps = "(ps_supplycost < 100.11)";
		//init_SF_ps(pred_ps, 100);
		char filename1[100] = "partsupp.bin";
		Schema testSchema1("catalog", "partsupp");
		dbf_ps.Open(filename1);
		init_lexical_parser(pred_ps);
		if (yyparse() != 0) {
			cout << " Error: can't parse your CNF " << pred_ps << endl;
			exit(1);
		}
		cnf_ps.GrowFromParseTree(final, &testSchema1, lit_ps); // constructs CNF predicate
		close_lexical_parser();

		Project P_ps;
		Pipe __ps(1000);
		int keepMe[] = { 1 };
		int numAttsIn = 5;
		int numAttsOut = 1;
		P_ps.Use_n_Pages(8);

		DuplicateRemoval D;
		Pipe ___ps(1000);
		Attribute IA = { "int", Int };
		Schema _ps_sch("_ps", 1, &IA);

		WriteOut W;
		char *fwpath = "gtest.ps.w.tmp";
		FILE *writefile = fopen(fwpath, "w");
		SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);
		D.Use_n_Pages(15);
		P_ps.Run(_ps, __ps, keepMe, numAttsIn, numAttsOut);
		D.Run(_ps, __ps, __ps_sch);
		W.Run(___ps, writefile, __ps_sch);

		SF_ps.WaitUntilDone();
		P_ps.WaitUntilDone();
		D.WaitUntilDone();
		W.WaitUntilDone();

		dbf_ps.Close();
	}
	catch(...) {
		ASSERT_FALSE(false);
	}
	ASSERT_TRUE(true);
}*/
