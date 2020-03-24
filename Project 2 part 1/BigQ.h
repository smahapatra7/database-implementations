#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "ComparisonEngine.h"
#include <vector>
#include <queue>

using namespace std;

class BigQ {
    int no_runs;
    pthread_t worker;
    int pageIndex;
    char *file_name;
    
    struct args_phase1_struct {
        
        Pipe *inp;  //input
        Pipe *outp; //output
        OrderMaker *sort_order;
        int *run_length;
    
    }args_phase1;

    typedef struct args_phase1_struct args_phase1_struct;

    static void* TPMMS_Phase1(void* arg);
    void* TPMMS_Phase2(void* arg);
    static void quicksort(vector<Record> &rb, int left, int right,OrderMaker &sortorder);
    static void sort_run(Page*,int,File&,int&,OrderMaker *);
    

public:

    BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    ~BigQ ();
};

typedef struct rwrap{
    
    Record rec;
    int run;

}rwrap;


class sort_func{

private:

    OrderMaker *sort_order;

public:

    sort_func(OrderMaker *order){
        this->sort_order = order;
    }

    sort_func(){};

    bool operator()(Record *one,Record *two) const{

        

        ComparisonEngine *comp;

        if(comp->Compare(one,two,this->sort_order)<0){
            return true;
        }

        else{
            return false;
        }
    }


    bool operator()(rwrap *one,rwrap *two) const{

        ComparisonEngine *comp;

        if(comp->Compare(&(one->rec),&(two->rec),this->sort_order)<0){
            
            return false;
        }

        else{
            
            return true;
        }
    }

    

};
#endif
