#include <iostream>

#include "Errors.h"
#include "Interpreter.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryPlan.h"
#include "Ddl.h"

using std::cout;

extern "C" {
  int yyparse(void);     // defined in y.tab.c
}

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query
extern char* newtable;
extern char* oldtable;
extern char* newfile;
extern char* deoutput;
extern struct AttrList *newattrs; //Use this to build Attribute* array and record schema

void Interpreter::run() {
  char *fileName = "Statistics.txt";
  /*  Statistics s;

  Ddl d; 
  QueryPlan plan(&s); */
      Statistics s;
  Ddl d; 
  QueryPlan plan(&s);
  while(true) {
    cout << "SimpleDB> ";
    // if (yyparse() != 0) {
    //   cout << "Can't parse your CNF.\n";
    //   continue;
    // }
    yyparse();

    s.Read(fileName);

    if (newtable) {
      if (d.createTable()) cout << "Table created with the name " << newtable << std::endl;
      else cout << "Table " << newtable << " already exists." << std::endl;
    } else if (oldtable && newfile) {
      if (d.insertInto()) cout << "Values inserted into " << oldtable << std::endl;
      else cout << "Insert failed." << std::endl;
    } else if (oldtable && !newfile) {
      if (d.dropTable()) cout << oldtable <<"dropped" << std::endl;
      else cout << "Table " << oldtable << " does not exist." << std::endl;
    } else if (deoutput) {
      plan.setOutput(deoutput);
    } else if (attsToSelect || finalFunction) {
      plan.plan();
      plan.print();
      plan.execute();
    }
    clear();
  }
}

// TODO: free lists
void Interpreter::clear() {
  newattrs = NULL;
  finalFunction = NULL;
  tables = NULL;
  boolean = NULL;
  groupingAtts = NULL;
  attsToSelect = NULL;
  newtable = oldtable = newfile = deoutput = NULL;
  distinctAtts = distinctFunc = 0;
  FATALIF (!remove ("*.tmp"), "remove tmp files failed");
}
