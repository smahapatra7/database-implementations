#include <fstream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
using std::string;
using std::ifstream;
using std::ofstream;
// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
    this->f = new File();
    this->rPage = new Page();
    this->wPage = new Page();
    this->curr = new Record();
}
DBFile::~DBFile(){
    delete f;
    delete rPage;
    delete wPage;
    delete curr;

}
int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    this->f->Open(0,(char*)f_path);
    pageIndex=1;
    writeIndex=1;
    writeIsDirty=0;
    endOfFile=1;
    
    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE* tableFile = fopen (loadpath,"r");
    Record temp;
    
    while(temp.SuckNextRecord(&f_schema,tableFile)!=0)
        this->Add(temp);
        
    fclose(tableFile);
}

int DBFile::Open (const char *f_path) {
    this->f->Open(1,(char*)f_path);
    pageIndex=1;
    endOfFile = 0;
    return 1;
}


void DBFile::MoveFirst () {
    this->f->GetPage(this->rPage,1);
    this->rPage->GetFirst(this->curr);
}

int DBFile::Close () {
    if(this->writeIsDirty==1){
        this->f->AddPage(wPage,writeIndex);
        writeIndex++;
    }
    endOfFile = 1;
    return this->f->Close();
    
}

void DBFile::Add (Record &rec) {
    this->writeIsDirty=1;

    Record write;
    write.Consume(&rec);
    
    if(wPage->Append(&write)==0)
    {
        this->f->AddPage(wPage,writeIndex);
        writeIndex++;
        this->wPage->EmptyItOut();
        wPage->Append(&write);
    }
    
}

int DBFile::GetNext (Record &fetchme) {
   if(endOfFile!=1){
       
       
       fetchme.Copy(this->curr);
   
       int result = this->rPage->GetFirst(this->curr);
   

       if(result==0){
       
           pageIndex++;
       
           if(pageIndex>=this->f->GetLength()-1){
               endOfFile = 1;
           }
       
           else{
               this->f->GetPage(this->rPage,pageIndex);
               this->rPage->GetFirst(this->curr);
           }
       
       
       }
   
   
   
  
       return 1;
   }
   return 0;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine compare;
    int result1 = 0;
    int result2 = 1;
    
    
    while(result1==0&&result2!=0){
        
        result2 = this->GetNext(fetchme);
        result1 = compare.Compare(&fetchme,&literal,&cnf);
    
    }
    
    if(result2==0)
        return 0;
        
    if(result1==1)
        return 1;

    
    return 0;
}
