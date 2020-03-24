#ifndef DBFILE_H
#define DBFILE_H
#include <string>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {
    char file_path[20];
    Record* curr; //A pointer referencing the current record to be accessed.
    Page* rPage;//Pointers to the page which is currently read or written in.
    Page* wPage;
    File* f; //A pointer to the file being used for processing
    off_t pageIndex;//Offset to the page in the file or point in the page from where data is read to.
    off_t writeIndex;//Offset to the page in the file or point in the page from where data is written to.
    char* n;
    int writeIsDirty; //A flag that indicates switch between read and writes.
    int endOfFile; //A flag indicating the reaching of end of file.
public:
	DBFile ();
    ~DBFile();
	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    File *getFile();
    

};
#endif
