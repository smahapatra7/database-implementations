#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>

using namespace std;


DBFile::DBFile () {
    openFile =false;

}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    if(openFile)
    {
        #ifdef F_DEBUG
            std::cout<<"File already is opened when we are trying to create";
        #endif
        return 0;
    }
    openFile=true;
    switch(static_cast<fType>(f_type))
    {
        case 0:
                metaData=MetaStruct(f_path,heap,0,0,NULL);
                dbfile=new HeapFile();
                dbfile->file.Open(0,const_cast<char*>(f_path));
                break;
        case 1:
                typedef struct { OrderMaker* o; int l; } * pOrder;
                pOrder po=(pOrder) startup;
                metaData=MetaStruct(f_path,sorted,0,po->l,po->o);
                dbfile = new SortedFile();
                dbfile->file.Open(0,const_cast<char*>(f_path));
                break;
    }
    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
   
    if(!openFile)
    {
        #ifdef F_DEBUG
            std::cout<<"The file is being loaded without being either created or opened";
        #endif
        return ;
    }
    dbfile->Load(f_schema,loadpath);
    return ;
}

int DBFile::Open (const char *f_path) {
     if(openFile)
    {
        #ifdef F_DEBUG
            std::cout<<"The file is already opened and is being opened again without closing";
        #endif
        return 0;
    }
    metaData = MetaStruct(f_path);
	cout<<f_path<<endl;
    if(!metaData.Open())
    {
        std::cout<<"We had some error: E(1)\n";
        return 0;
    }
    openFile=true;
    switch(metaData.getType())
    {
        case heap:dbfile = new HeapFile();
            break;
        case sorted:dbfile = new SortedFile();
             dbfile->setAttribute(metaData.getOrderMaker(),metaData.getRunLength());
            break;
        case tree:
        default:NEED_TO_IMPLEMENT
        return 0;
    }
    dbfile->file.Open(1,const_cast<char *>(f_path));
    return 1;
}

void DBFile::MoveFirst () {
    if(!openFile || !dbfile)
    {
        #ifdef F_DEBUG
            std::cout<<"The file is not opened and read record is being moved to first";
        #endif
        return;
    }
    
    dbfile->MoveFirst();
}

int DBFile::Close () {
    if(!openFile || !dbfile)
    {
        #ifdef F_DEBUG
            std::cout<<"The file has we are trying to close has already been closed\n";
        #endif
        return 0;
    }
    openFile = false;
    dbfile->Close();
    metaData.Close();
    return 1;
}

void DBFile::Add (Record &rec) {
    if(!openFile || !dbfile)
    {
        #ifdef F_DEBUG
            std::cout<<"The file is already closed and we are trying to close it again";
        #endif
        return ;
    }
    dbfile->Add(rec);
}

int DBFile::GetNext (Record &fetchme)
{
    if(!openFile || !dbfile){
        #ifdef F_DEBUG
            std::cout<<"The file is already closed and we are trying to read it again";
        #endif
        return 0;
    }
    return dbfile->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    if(!openFile || !dbfile){
        #ifdef F_DEBUG
            std::cout<<"The file is already closed and we are trying to read it again";
        #endif
        return 0;
    }
    return dbfile->GetNext(fetchme,cnf,literal);
}

TreeFile * DBFile::getDBfile(){
    return dbfile;
}
