#ifndef DBFILE_H
#define DBFILE_H

#pragma once
#include <iostream>
#include "HeapFile.h"
#include "SortedFile.h"


// stub DBFile header..replace it with your own DBFile.h 

class DBFile {
	TreeFile * dbfile;
	bool openFile;
	MetaStruct metaData;
	//this function is used to clear out write page when going to read mode from write
public:
	DBFile (); 
	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();
	void Load (Schema &myschema, const char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	TreeFile * getDBfile();
};
#endif
