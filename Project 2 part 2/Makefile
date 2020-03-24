CC = g++ -O2 -Wno-deprecated -std=c++11

tag = -i
test_out_tag = -ll

ifdef linux
tag = -n
test_out_tag = -lfl
endif

gtest.o: gtest.cc
	$(CC) -g -c gtest.cc

gtest: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o y.tab.o lex.yy.o gtest.o
	$(CC) -o gtest Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o y.tab.o lex.yy.o gtest.o -l pthread -lgtest

test.out:Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o y.tab.o lex.yy.o test.o $(test_out_tag)

a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o $(test_out_tag)
	
test.o: test.cc
	$(CC) -g -c test.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc
	
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

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) -e "s/  _attribute_ ((_unused))$$/# ifndef __cplusplus\n  __attribute_ ((_unused_));\n# endif/" y.tab.c 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h