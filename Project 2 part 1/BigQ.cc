#include "BigQ.h"
#include <algorithm>

void BigQ::sort_run(Page *p, int num_recs,File& new_file,int& gp_index,OrderMaker *sort_order){
    Record **record_Buffer= new Record*[num_recs];
    int c   =  0;
    int p_i =  0;
    do{
        record_Buffer[c] = new Record();
        if((p+p_i)->GetFirst(record_Buffer[c])==0){
            (p+p_i)->EmptyItOut();
            p_i++;
            (p+p_i)->GetFirst(record_Buffer[c]);
            c++;
        }
        else{
            c++;
        }
    }while(c < num_recs);
    (p+p_i)->EmptyItOut();
    sort(record_Buffer,record_Buffer+(num_recs),sort_func(sort_order));
    c=0;
    Page *tp = new Page();
    int Dirtypage =0;
    while(c < num_recs){
        if(tp->Append(*(record_Buffer+c))==0){
            Dirtypage =0;
            new_file.AddPage(tp,(off_t)(gp_index++));
            tp->EmptyItOut();
            tp->Append(*(record_Buffer+c));
            c++;
        }
        else{
            Dirtypage =1;
            c++;
        }
    }        
    if(Dirtypage==1){
        new_file.AddPage(tp,(off_t)(gp_index++));
    }
    delete tp;
    for(int i=0;i<num_recs;i++){
        delete *(record_Buffer+i);
    }
    delete record_Buffer;
}
void* BigQ::TPMMS_Phase1(void* arg){
    args_phase1_struct *args;
    args = (args_phase1_struct *)arg;
    File new_file;
    char f_path[8] = "runfile";
    new_file.Open(0,f_path);
    Page *p = new Page[*(args->run_length)]();
    Record *temporary = new Record();
    int result    =  1;
    int num_recs  =  0;
    int p_index   =  0;
    int gp_index   =  1;
    int num_runs  =  1;
    while(result!=0){
            result = args->inp->Remove(temporary);
        if(result!=0){
            if((p+p_index)->Append(temporary)==1){
                num_recs++;
            }
            else if(++p_index == *(args->run_length)){
                sort_run(p,num_recs,new_file,gp_index,args->sort_order);
                num_runs++;
                p_index=0;
                num_recs=0;
                (p+p_index)->Append(temporary);
                num_recs++;
                temporary = new Record();
            }
            else{
                (p+p_index)->Append(temporary);
                num_recs++;
                temporary = new Record();
            }
        }

        else{
            sort_run(p,num_recs,new_file,gp_index,args->sort_order);
        }
    }
    delete temporary;
    new_file.Close();
    new_file.Open(1,f_path);
    priority_queue<rwrap* , vector<rwrap*> , sort_func> pQueue (sort_func(args->sort_order));
    Page *buf = new Page[num_runs];
    rwrap *temp = new rwrap;
    Record *t = new Record();
    for (int i=0;i<num_runs;i++){
        new_file.GetPage((buf+i),(off_t)(1+(*args->run_length)*i));
        (buf+i)->GetFirst(t);
        (temp->rec).Consume(t);
        temp->run=i;
        pQueue.push(temp);
        t = new Record();
        temp = new rwrap;
    }
    int flags  = num_runs;
    int next  = 0;
    int start = 0;
    int end   = 1;
    int fin[num_runs];
    int c_i[num_runs];
    int index[num_runs][2];
    for(int i=0;i<num_runs;i++){
        fin[i]=0;
        c_i[i]=0;
    }
    for(int i=0;i<num_runs-1;i++){
        index[i][start] = 1+((*(args->run_length))*i);
        index[i][end]   = (*(args->run_length))*(i+1);
    }
    index[num_runs-1][start] = 1+((*(args->run_length))*(num_runs-1));
    index[num_runs-1][end] = gp_index-1;
    while(flags!=0){
        rwrap *temp;
        temp = pQueue.top();
        pQueue.pop();
        next = temp->run;
        args->outp->Insert(&(temp->rec));
        rwrap *insert = new rwrap;
        Record *t = new Record();
        if(fin[next]==0)
        {
            if((buf+next)->GetFirst(t)==0){
            
                if( index[next][start]+ (++c_i[next]) > index[next][end]){
               
                    flags--;
                    fin[next]=1;
                }
                else{
                    new_file.GetPage(buf+next,(off_t)(index[next][start]+c_i[next]));
                    (buf+next)->GetFirst(t);
                    insert->rec = *t;
                    insert->run = next;
                    pQueue.push(insert);
                }

            }
            else{
                insert->rec = *t;
                insert->run = next;
                pQueue.push(insert);

            }
        }
    
    
    }
    while(!pQueue.empty()){
        args->outp->Insert(&((pQueue.top())->rec));
        pQueue.pop();
    }
    
}
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    
    this->no_runs = 0;
    args_phase1.inp = &in;
    args_phase1.outp = &out;
    args_phase1.sort_order = &sortorder;
    args_phase1.run_length = &runlen;
    pthread_create (&worker, NULL, &BigQ::TPMMS_Phase1 , (void *)&args_phase1);
    pthread_join(worker,NULL);
    out.ShutDown ();
}

BigQ::~BigQ () {
    
}
