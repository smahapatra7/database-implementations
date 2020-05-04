#include "TreeFile.h"

void TreeFile::MoveFirst(){
    check_write();
    r_page.EmptyItOut();
    file.GetPage(&r_page,cur_page=0);
}

int TreeFile::GetNext(Record &fetchme){
    check_write();
    while (!r_page.GetFirst(&fetchme)) {
        if(++cur_page >=file.GetLength()-1) 
            return 0;
        file.GetPage(&r_page, cur_page);
    }
    return 1;
}

int TreeFile::GetNext(Record &fetchme, CNF &cnf, Record &literal){
    ComparisonEngine comp;
    while(GetNext(fetchme))
    {
        if(comp.Compare(&fetchme, &literal, &cnf)) 
        {
            return 1;
        }
    }
    return 0;
}

void TreeFile::Load (Schema &myschema, const char *loadpath){
    FILE * fileLoad = fopen(loadpath,"r");
    if(!fileLoad)
    {
        #ifdef F_DEBUG
            std::cout<<"File could not be opened";
            exit(1);
        #endif
    }
    Record pull;
    while(pull.SuckNextRecord(&myschema,fileLoad))
    {
        Add(pull);
    }
}

File TreeFile::getFile(){
    return file;
}

int TreeFile::Close(){
    check_write();
    file.Close();
    return 1;
}
