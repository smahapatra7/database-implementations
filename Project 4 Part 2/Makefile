CC = g++ -std=c++11 -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

gtest.out: Record.o gtest.o TreeNode.o Query.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o TreeFile.o SortedFile.o MetaStruct.o HeapFile.o Statistics.o
	$(CC) -o gtest.out Record.o gtest.o TreeNode.o Query.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o TreeFile.o SortedFile.o MetaStruct.o HeapFile.o Pipe.o BigQ.o Statistics.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o RelOp.o -ll -lpthread -lgtest

#a1-test.out: Record.o SortedFile.o Comparison.o TreeFile.o Pipe.o ComparisonEngine.o HeapFile.o Schema.o File.o DBFile.o y.tab.o lex.yy.o MetaStruct.o BigQ.o a1-test.o
#	$(CC) -D F_DEBUG -o a1-test.out BigQ.o Pipe.o TreeFile.o HeapFile.o SortedFile.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o MetaStruct.o a1-test.o -ll -lpthread
	
#a2-test.out: Record.o BigQ.o SortedFile.o Comparison.o TreeFile.o Pipe.o ComparisonEngine.o HeapFile.o Schema.o File.o DBFile.o y.tab.o lex.yy.o MetaStruct.o a2-test.o
#	$(CC) -D F_DEBUG -o a2-test.out		 BigQ.o Pipe.o TreeFile.o HeapFile.o SortedFile.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o MetaStruct.o a2-test.o -ll -lpthread

#test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o TreeFile.o SortedFile.o MetaStruct.o HeapFile.o test.o
#	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o TreeFile.o SortedFile.o MetaStruct.o HeapFile.o Pipe.o BigQ.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o RelOp.o test.o -ll -lpthread

#a4-1.out: Record.o Comparison.o Statistics.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o TreeFile.o SortedFile.o MetaStruct.o HeapFile.o test.o
#	$(CC) -o a4-1.out Record.o Statistics.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o TreeFile.o SortedFile.o MetaStruct.o HeapFile.o Pipe.o BigQ.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o RelOp.o test.o -ll -lpthread

a42.out: TreeNode.o Record.o Query.o Comparison.o Statistics.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o TreeFile.o SortedFile.o MetaStruct.o HeapFile.o a42.o
	$(CC) -o a42.out TreeNode.o Query.o Record.o Statistics.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o TreeFile.o SortedFile.o MetaStruct.o HeapFile.o Pipe.o BigQ.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o RelOp.o a42.o -ll

MetaStruct.o: MetaStruct.cc
	$(CC) -g -c MetaStruct.cc

a42.o: a42.cc
	$(CC) -g -c a42.cc

gtest.o: gtest.cc
	$(CC) -g -c gtest.cc

test.o: test.cc
	$(CC) -g -c test.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

a2-test.o: a2-test.cc
	$(CC) -g -c a2-test.cc
	
Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

HeapFile.o: HeapFile.cc
	$(CC) -g -c HeapFile.cc

SortedFile.o: SortedFile.cc
	$(CC) -g -c SortedFile.cc

TreeFile.o: TreeFile.cc
	$(CC) -g -c TreeFile.cc
	
RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

Query.o: Query.cc
	$(CC) -g -c Query.cc

TreeNode.o: TreeNode.cc
	$(CC) -g -c TreeNode.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	#sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f lex.yy.*
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yyfunc*
