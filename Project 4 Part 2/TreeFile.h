#pragma once
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "MetaStruct.h"

class TreeFile{
    protected:
    File file;
	Page w_page;
	Page r_page;
	int cur_page;
    public:
    TreeFile(){
        cur_page=0;
    }
    friend class DBFile;
    virtual void Add (Record &addme){};
    int Create (const char *fpath, fType file_type, void *startup);
	virtual int Close ();
	void Load (Schema &myschema, const char *loadpath);
	void MoveFirst ();
	int GetNext (Record &fetchme);
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	virtual void setAttribute(OrderMaker *o,int run) {};
	File getFile();
	void check_write(){
    	if(!w_page.empty())
		{
			int pos = !file.GetLength()? 0 : file.GetLength()-1;
			file.AddPage(&w_page, pos);
			w_page.EmptyItOut();       
		}
	}
};