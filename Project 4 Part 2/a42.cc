
#include <iostream>
#include "ParseTree.h"
#include "Query.h"
#include "Statistics.h"
#include "test.h"
#include <string>
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
	int yyfuncparse(void);   // defined in yyfunc.tab.c
}

extern	FuncOperator *finalFunction;
extern struct TableList *tables; 
extern struct AndList *boolean;
extern struct   NameList   *groupingAtts;
extern struct   NameList   *attsToSelect; 
extern int distinctAtts;
extern int distinctFunc;

int main () {
	setup();
	Statistics * s=new Statistics();
	s->LoadAllStatistics();
	yyparse();
	Query *q=new Query(finalFunction,tables,boolean,groupingAtts,attsToSelect,distinctAtts,distinctFunc,s,std::string(dbfile_dir),string(tpch_dir),string(catalog_path));
	q->PrintQuery();
	cleanup();
}	
