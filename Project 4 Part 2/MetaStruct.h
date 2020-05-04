#ifndef METASTRUCT_H
#define METASTRUCT_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Schema.h"
#include<string>

typedef enum {heap, sorted, tree} fType;

class MetaStruct {

private:
	fType mode;
	int n_pages;
	OrderMaker *myOrder;
	int runlen;
    string mPath;
public:
    MetaStruct ();
	MetaStruct (const char *fpath, fType m, int n,int r,OrderMaker *o); 
	MetaStruct(const char *fpath);

	int Open ();
	int Close ();
    void incPage();
	int getPages();
	fType getType();
	OrderMaker * getOrderMaker();
	int getRunLength();
};
#endif