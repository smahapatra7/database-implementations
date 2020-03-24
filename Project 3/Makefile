
CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Heap.o Sort.o BigQ.o DBFile.o Pipe.o RelOp.o     Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Heap.o Sort.o BigQ.o  DBFile.o Pipe.o RelOp.o  Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o -ll -lpthread -std=c++11

# main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o  main.o
# 	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o  main.o -ll -lpthread -std=c++11

# test1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test1.o
# 	$(CC) -o test1.out  Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test1.o -ll

gtest: Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Heap.o Sort.o BigQ.o DBFile.o Pipe.o RelOp.o     Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o gtest.o
	g++ gtest.cc -std=c++11 -stdlib=libc++  Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Heap.o Sort.o BigQ.o DBFile.o Pipe.o RelOp.o     Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o   -ll -lgtest -lpthread -o gtest

gtest.o: gtest.cc
	g++ gtest.cc -std=c++11 -stdlib=libc++  Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Heap.o Sort.o BigQ.o DBFile.o Pipe.o RelOp.o     Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o  -ll -lgtest -lpthread -o gtest

a1-test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Heap.o Sort.o BigQ.o DBFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a1-test.o
	$(CC) -o a1-test.out  Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Heap.o Sort.o BigQ.o DBFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a1-test.o -ll

a2-test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Heap.o Sort.o BigQ.o DBFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a2-test.o
	$(CC) -o a2-test.out  Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Heap.o Sort.o BigQ.o DBFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a2-test.o -ll

a2-2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Heap.o Sort.o BigQ.o DBFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a2-2test.o
	$(CC) -o a2-2test.out  Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Heap.o Sort.o BigQ.o DBFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a2-2test.o -ll


test.o: test.cc
	$(CC) -g -c test.cc

a2-2test.o: a2-2test.cc
	$(CC) -g -c a2-2test.cc

a2-test.o: a2-test.cc
	$(CC) -g -c a2-test.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

main.o: main.cc
	$(CC) -g -c main.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc



Heap.o: Heap.cc
	$(CC) -g -c Heap.cc

Sort.o: Sort.cc
	$(CC) -g -c Sort.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

#   : Join.cc
# 	$(CC) -g -c Join.cc

# GroupBy.o: GroupBy.cc
# 	$(CC) -g -c GroupBy.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

# y.tab.o: Parser.y
# 	yacc -d Parser.y
# 	gsed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
# 	g++ -c y.tab.c

# lex.yy.o: Lexer.l
# 	lex  Lexer.l
# 	gcc  -c lex.yy.c

y.tab.o: Parser.y
	yacc -d Parser.y
	#sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean:
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
