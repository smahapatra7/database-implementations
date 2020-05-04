#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#pragma once
#include "TreeFile.h"
#include "Record.h"
#include "Pipe.h"
#include "BigQ.h"
#include "Schema.h"

class BigQ;

class SortedFile: public TreeFile{
    static const size_t  BUFFERSIZE=100;
    OrderMaker *myOrder;
    int runLength;
    pthread_t threadres;
    BigQ *bigQ;
        static void * helper(void *args){
        SortedFile * sb=(SortedFile *)args;
        Record rec;
        while(sb->out->Remove(&rec)){
            if(!sb->w_page.Append(&rec))
            {
                int pos = sb->file.GetLength()==0? 0:sb->file.GetLength()-1; 
                sb->file.AddPage(&sb->w_page,pos);
                sb->w_page.EmptyItOut();
                sb->w_page.Append(&rec);
            }
        }
    }
    public:
    Pipe *in,*out;
    SortedFile();
    int Close () override;
	int GetNext (Record &fetchme, CNF &cnf, Record &literal) override;
    void Add (Record &addme) override;
    void setAttribute(OrderMaker *o,int run) override;
    int BinarySearch(Record& fetchme, OrderMaker& queryorder, Record& literal, OrderMaker& cnforder, ComparisonEngine& cmp);
};

#endif