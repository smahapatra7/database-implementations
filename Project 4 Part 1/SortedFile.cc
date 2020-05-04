#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <fstream>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

SortedFile::SortedFile () {
	inPipe = new Pipe(100);	
	outPipe = new Pipe(100);	
	bq = NULL;
	file = new File();
	isDirty=0;
	si = NULL;
	current = new Record();	
	readPageBuffer = new Page();		// store contents of current page from where program is reading
	tobeMerged = new Page();			// used for merging existing records with those from BigQ
	m = R;								// Mode set to read
	OrderMaker *queryOrder = NULL;		// queryOrder used in GetNext(3 parameters)
	bool queryChange = true;			// maintains if query is changed
}

void* SortedFile::instantiate_BigQ(void* arg){

	thread_arguments *args;
	args = (thread_arguments *) arg;
    args->b = new BigQ(*(args->in),*(args->out),*((args->s).myOrder),(args->s).runLength);

}

int SortedFile::Create (char *f_path, fType f_type, void *startup) {
	file->Open(0,f_path);
	fileName = (char *)malloc(sizeof(f_path)+1);
	strcpy(fileName,f_path);
	isDirty=0;
	si = (SortInfo *) startup;
	pageIndex=1;
	recordIndex = 0;
	endOfFile=1;
	return 1;
}

void SortedFile::Load (Schema &f_schema, char *loadpath) {

	if(m!=W){
		m = W;
		isDirty=1;
		if(bq==NULL)bq = new BigQ(*inPipe,*outPipe,*(si->myOrder),si->runLength);
	}
	FILE* tableFile = fopen (loadpath,"r");
	Record temp;
	while(temp.SuckNextRecord(&f_schema,tableFile)!=0)
		inPipe->Insert(&temp);
	fclose(tableFile);	
}

int SortedFile::Open (char *f_path) {
	isDirty=0;
	char *fName = new char[20];
	sprintf(fName, "%s.meta", f_path);
	fileName = (char *)malloc(sizeof(f_path)+1);
	strcpy(fileName,f_path);
	ifstream ifs(fName,ios::binary);
	ifs.seekg(sizeof(fileName)-1);
	if(si==NULL){
		si = new SortInfo;
		si->myOrder = new OrderMaker();
	}
	ifs.read((char*)si->myOrder, sizeof(*(si->myOrder)));
	ifs.read((char*)&(si->runLength), sizeof(si->runLength));
	ifs.close();
	m = R;
	file->Open(1, f_path);	// open the corresponding file
	pageIndex = 1;
	recordIndex = 0;
	endOfFile = 0;
}

void SortedFile::MoveFirst () {
	isDirty=0;	
	pageIndex = 1;
	recordIndex = 0;
	if(m==R){
		if(file->GetLength()!=0){
			file->GetPage(readPageBuffer,pageIndex);
			int result = readPageBuffer->GetFirst(current);
		}
		else{
		//	pageIndex = 1;
		}

	}
	else{
		m = R;
		MergeFromOutpipe();
		file->GetPage(readPageBuffer,pageIndex);
		readPageBuffer->GetFirst(current);
	}
	queryChange = true;
}

int SortedFile::Close () {
	if(m==W)	
		MergeFromOutpipe();
	file->Close();
	isDirty=0;
	endOfFile = 1;
	char fName[30];
	sprintf(fName,"%s.meta",fileName);
	ofstream out(fName);
    	out <<"sorted"<<endl;
    	out.close();
	ofstream ofs(fName,ios::binary|ios::app);
	ofs.write((char*)si->myOrder,sizeof(*(si->myOrder)));	
	ofs.write((char*)&(si->runLength),sizeof(si->runLength));
	ofs.close();
}

void SortedFile::Add (Record &rec) {
	inPipe->Insert(&rec);
	if(m!=W){
		isDirty=1;
		m = W;
		inPipe= new Pipe(100);
		outPipe= new Pipe(100);
		if(bq==NULL){
			thread_args.in = inPipe;
			thread_args.out = outPipe;
			thread_args.s.myOrder = si->myOrder;
			thread_args.s.runLength =  si->runLength;
			thread_args.b = bq;

			pthread_create(&bigQ_t, NULL, &SortedFile::instantiate_BigQ , (void *)&thread_args);
		}
	}
	queryChange = true;
}

int SortedFile::GetNext (Record &fetchme) {

	if(m!=R){
		isDirty=0;
		m = R;
		readPageBuffer->EmptyItOut();
		MergeFromOutpipe();
		MoveFirst();
	}

	if(endOfFile==1) return 0;
	fetchme.Copy(current);
	if(!readPageBuffer->GetFirst(current)) {
		if(pageIndex>=this->file->GetLength()-2){
				endOfFile = 1;
				return 0;	
		}
		else {
			pageIndex++;
			file->GetPage(readPageBuffer,pageIndex);
			readPageBuffer->GetFirst(current);
		}
	}

	return 1;
}

int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	if(m!=R){
		isDirty=0;
		m = R;
		readPageBuffer->EmptyItOut();
		MergeFromOutpipe();
		MoveFirst();
	}
	if(queryChange != true){
	
	}
	else{
		queryOrder = cnf.CreateQueryMaker(*(si->myOrder));
	}
	
	ComparisonEngine *comp;
		
	if(queryOrder==NULL) {
		while(GetNext(fetchme)){
			if(comp->Compare(&fetchme, &literal, &cnf)) {
                return 1;
			}
		}
		return 0;
	}
	else{
		Record *r1 = new Record();
        cout<<"matchpage called using "<< queryOrder<<endl;
		r1 = GetMatchPage(literal);
		if(r1==NULL) return 0;
		fetchme.Consume(r1);
		if(comp->Compare(&fetchme,&literal,&cnf)){
			 return 1;
		}
		while(GetNext(fetchme)) {
			if(comp->Compare(&fetchme, &literal, queryOrder)!=0) {
				return 0;
			} else {
				if(comp->Compare(&fetchme, &literal, &cnf)) {
					return 1;
				}
			}
		}
	
	}
	return 0;
}


OrderMaker *SortedFile::checkIfMatches(CNF &cnf, OrderMaker &sortOrder) {	

	OrderMaker *matchOrder= new OrderMaker;
	for(int i = 0; i<sortOrder.numAtts; i++) {
		bool match = false;
		for(int j = 0; j<cnf.numAnds; j++) {
			if(!match) {
				for(int k=0; k<cnf.orLens[j]; k++) {
					if(cnf.orList[j][k].op == Equals) {
						if(cnf.orList[j][k].operand1 == Literal) {
							if((sortOrder.whichAtts[i] == cnf.orList[j][k].whichAtt1)
									&& (sortOrder.whichTypes[i] == cnf.orList[j][k].attType)){
								
								matchOrder->whichAtts[matchOrder->numAtts] = sortOrder.whichAtts[i];
								matchOrder->whichTypes[matchOrder->numAtts++] = sortOrder.whichTypes[i];
								match = true;
								break;
							}
	
						} else if(cnf.orList[j][k].operand2 == Literal) {
						
							if((sortOrder.whichAtts[i] == cnf.orList[j][k].whichAtt2)
									&& (sortOrder.whichTypes[i] == cnf.orList[j][k].attType)){
						
								matchOrder->whichAtts[matchOrder->numAtts] = sortOrder.whichAtts[i];
								matchOrder->whichTypes[matchOrder->numAtts++] = sortOrder.whichTypes[i];
								match = true;
								break;
							}
						}
					}
				}
			}
		}
		if(!match) break;
	}
	if(matchOrder->numAtts == 0)
	{
		cout <<"No query OrderMaker can be constructed!"<<endl;
		delete matchOrder;
		return NULL;
	}
	return matchOrder;
}



Record* SortedFile::GetMatchPage(Record &literal) {
	if(queryChange) {
		int low = pageIndex;
		int high = file->GetLength()-2;
		int matchPage = bsearch(low, high, queryOrder, literal);
		if(matchPage == -1) {
			return NULL;
		}
		if(matchPage != pageIndex) {
			readPageBuffer->EmptyItOut();
			file->GetPage(readPageBuffer, matchPage);
			pageIndex = matchPage+1;
		}
		queryChange = false;
	}
	Record *returnRcd = new Record;
	ComparisonEngine cmp1;
	while(readPageBuffer->GetFirst(returnRcd)) {
		if(cmp1.Compare(returnRcd, &literal, queryOrder) == 0) {
			return returnRcd;
		}
	}
	if(pageIndex >= file->GetLength()-2) {
		return NULL;
	} else {
		pageIndex++;
		file->GetPage(readPageBuffer, pageIndex);
		while(readPageBuffer->GetFirst(returnRcd)) {
			if(cmp1.Compare(returnRcd, &literal, queryOrder) == 0) {
				return returnRcd;
			}
		}
	}
	return NULL;
		

}

int SortedFile::bsearch(int low, int high, OrderMaker *queryOM, Record &literal) {
	cout<<"serach OM "<<endl;
	queryOM->Print();
	cout<<endl<<"file om"<<endl;
	si->myOrder->Print();
	if(high < low) return -1;
	if(high == low) return low;
	ComparisonEngine *comp;
	Page *tmpPage = new Page;
	Record *tmpRcd = new Record;
	int mid = (int) (high+low)/2;
	file->GetPage(tmpPage, mid);
	int res;
	Schema nu("catalog","lineitem");
	tmpPage->GetFirst(tmpRcd);
	tmpRcd->Print(&nu);
    res = comp->Compare(tmpRcd,si->myOrder, &literal,queryOM );


	delete tmpPage;
	delete tmpRcd;

	
	if( res == -1) {
		if(low==mid)
			return mid;
		return bsearch(low, mid-1, queryOM, literal);
	}
	else if(res == 0) {
		return mid;//bsearch(low, mid-1, queryOM, literal);
	}
	else
		return bsearch(mid+1, high,queryOM, literal);
}


void SortedFile:: MergeFromOutpipe(){

	inPipe->ShutDown();
	ComparisonEngine *ce;
	if(!tobeMerged){ tobeMerged = new Page(); }
	pagePtrForMerge = 0; 
	Record *rFromFile = new Record();
	GetNew(rFromFile);						// loads the first record from existing records

	Record *rtemp = new Record();		
	Page *ptowrite = new Page();			// new page that would be added
	File *newFile = new File();				// new file after merging
	newFile->Open(0,"mergedFile");				

	bool nomore = false;
        int result =GetNew(rFromFile);
	int pageIndex = 1;


	Schema nu("catalog","lineitem");


	if(result==0)
		nomore =true;
	while(isDirty!=0&&!nomore){

		if(outPipe->Remove(rtemp)==1){		// got the record from out pipe

			while(ce->Compare(rFromFile,rtemp,si->myOrder)<0){ 		// merging this record with others

				if(ptowrite->Append(rFromFile)==0){


						newFile->AddPage(ptowrite,pageIndex++);
						ptowrite->EmptyItOut();
						ptowrite->Append(rFromFile);		// does this consume the record ?
						
				}

				if(!GetNew(rFromFile)){ nomore = true; break; }
			}


			if(ptowrite->Append(rtemp)!=1){
						newFile->AddPage(ptowrite,pageIndex++);
						ptowrite->EmptyItOut();
						ptowrite->Append(rtemp);		// does this consume the record ?
			}

		}
		else{
			do{
				
				if(ptowrite->Append(rFromFile)!=1){
					newFile->AddPage(ptowrite,pageIndex++);
					
					ptowrite->EmptyItOut();
					// append the current record ?
					ptowrite->Append(rFromFile);		// does this consume the record ?
					
				}
			}while(GetNew(rFromFile)!=0);
			break;
		}
	}

	outPipe->Remove(rtemp);//1 missing record

	

	if(nomore==true){									// file is empty
		do{
            if(ptowrite->Append(rtemp)!=1){				// copy record from pipe
						
						newFile->AddPage(ptowrite,pageIndex++);
						ptowrite->EmptyItOut();
						ptowrite->Append(rtemp);
						
	
			}
		}while(outPipe->Remove(rtemp)!=0);
	}

	newFile->AddPage(ptowrite,pageIndex);


	newFile->Close();
	file->Close();

	if(rename(fileName,"mergefile.tmp")!=0) {				// making merged file the new file
		cerr <<"rename file error!"<<endl;
		return;
	}
	
	remove("mergefile.tmp");

	if(rename("mergedFile",fileName)!=0) {				// making merged file the new file
		cerr <<"rename file error!"<<endl;
		return;
	}

	readPageBuffer->EmptyItOut();
	file->Open(1, this->fileName);

}


int SortedFile:: GetNew(Record *r1){

	while(!this->tobeMerged->GetFirst(r1)) {
		if(pagePtrForMerge >= file->GetLength()-1)
			return 0;
		else {
			file->GetPage(tobeMerged, pagePtrForMerge);
			pagePtrForMerge++;
		}
	}

	return 1;
}	


SortedFile::~SortedFile() {
	delete readPageBuffer;
	delete inPipe;
	delete outPipe;
}
