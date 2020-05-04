#ifndef QUERY_H
#define QUERY_H_
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
#include "TreeNode.h"
#include <map>

struct Node;

class Query
{

    Node *root;
    //As given in the project description
    struct FuncOperator * finalFunction; 
	struct TableList * tables;   
	struct AndList *cnfAndList;  
	struct NameList * groupAtts; 
	struct NameList * selectAtts; 

    int dist_fn;
	int dist_atts;   
    
    Statistics *s;
    int pipeSelect=0;
    std::unordered_map<int,Pipe *> pipe;//hashmap for pipes
    std::unordered_map<string, AndList *> Selectors(std::vector<AndList *> list);
    void JoinsAndSelects(std::vector<AndList*> &joins, std::vector<AndList*> &selects,std::vector<AndList*> &selAboveJoin); 
    Function *GenerateFunc(Schema *schema);
    OrderMaker *GenerateOM(Schema *schema);
    map<string, AndList*>OptimizeSelectAndApply(vector<AndList*> selects);
    vector<AndList*> OptimizeJoinOrder(vector<AndList*> joins);
    
    public:
    //Constructor
    Query(struct FuncOperator *finalFunction,
			struct TableList *tables,
			struct AndList * boolean,
			struct NameList * pGrpAtts,
	        struct NameList * pAttsToSelect,
	        int distinct_atts, int distinct_func, Statistics *s,string dir,string tpch,string catalog);
    void ExecuteQuery();
    void PrintQuery();

};

#endif