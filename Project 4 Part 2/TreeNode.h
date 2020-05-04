#ifndef OPTIMIZER_H_
#define OPTIMIZER_H_
#include "Function.h"
#include "Statistics.h"
#include "ComparisonEngine.h"
#include "Record.h"
#include <unordered_map>
#include "Pipe.h" 
#include "RelOp.h"
#include <iostream>
#include "DBFile.h"
#include <vector>
#include "ParseTree.h"

struct Node{
    Node *left=NULL,*right=NULL,*parent=NULL;
    CNF *cnf;
	Record *literal;
	Schema *outputSchema;
	Function *function;
    OrderMaker *order;
    int l_pipe,r_pipe,o_pipe,numAttsOutput;
    int *keepMe;
    DBFile *db;
    //Pipe *out_pipe;
    string dbfilePath;
    virtual void Execute();
    virtual void Print();
    Node():cnf(new CNF()),literal(new Record()){};
};

struct SelectFNode:public Node{
    void Execute() override;
    void Print() override;
};


struct SelectPNode:public Node{
    void Execute() override;
    void Print() override;
};

struct ProjectNode:public Node{
    void Execute() override;
    void Print() override;
}; 

struct JoinNode:public Node{
    void Execute() override;
    void Print() override;
};

struct SumNode:public Node{
    void Execute() override;
    void Print() override;
};

struct GroupByNode:public Node{
    void Execute() override;
    void Print() override;
};

struct DistinctNode:public Node{
    void Execute() override;
    void Print() override;
};

struct WriteOutNode:public Node{
    void Execute() override;
    void Print() override;
};

#endif