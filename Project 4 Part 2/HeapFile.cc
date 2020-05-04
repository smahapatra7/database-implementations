#include "HeapFile.h"
#include <iostream>
using namespace std;

HeapFile::HeapFile(){
}

void HeapFile::Add (Record &rec){
    if(!w_page.Append(&rec))
    {
        int position = file.GetLength()==0? 0:file.GetLength()-1; 
        file.AddPage(&w_page,position);
        w_page.EmptyItOut();
        w_page.Append(&rec);
    }
    return ;
}

