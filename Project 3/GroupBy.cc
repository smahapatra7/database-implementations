#include "RelOp.h"

GroupBy::GroupBy() {

}

GroupBy::~GroupBy() {

}

void GroupBy::Run(Pipe & inPipe, Pipe & outPipe, OrderMaker & groupAtts, Function & computeMe) {

	inputPipe = &inPipe;
	outputPipe = &outPipe;
	inputOrder = &groupAtts;
	inputFunc = &computeMe;

	pthread_create(&thread, NULL, helperThread, (void *)this);

}

void * GroupBy::helperThread(void * op) {

	GroupBy *order = static_cast <GroupBy*> (op);

	order->Group();

	pthread_exit(NULL);

}

void GroupBy::Group() {

	Type type;
	Record * prev = NULL;
	Record * readIn = new Record;

	// Store the overall computation results
	int intResult = 0;
	double doubleResult = 0.0;

	// Sort all records using BigQ
	int pipeSize = 100;
	Pipe sortedPipe(pipeSize);

	//cout << "Before bigQ in Groupby" << endl;

	BigQ sort(*inputPipe, sortedPipe, *inputOrder, runlen);
	//cout << "After bigQ in Groupby" << endl;
	ComparisonEngine cEngine;

	// Pull records from the pipe and check them against the CNF
	while (sortedPipe.Remove(readIn)) {
		//for first record:
		if (prev == NULL) {
			prev = new Record;
			prev -> Copy(readIn);
			type = IncrementSum(readIn, intResult, doubleResult);
			//cout << "After IS in in while IF in Groupby*******************" << endl;
		}
		//For all other records
		else {
			//if the records are of same group increment sum
			if (cEngine.Compare(readIn, prev, inputOrder) == 0) {
				//call sum and increment sum
				type = IncrementSum(readIn, intResult, doubleResult);
				//cout << "After IS in while in SECOND IF in Groupby*******************" << endl;
			}
			//else create new record and add to the output pipe.
			else {
				MergeAndOutputRecords(prev, type, intResult, doubleResult);
				prev -> Copy(readIn);
				intResult = 0;
				doubleResult = 0.0;
				type = IncrementSum(readIn, intResult, doubleResult);
			} 
		}
	}
	// For last group Create a new Record and put it into the pipe
	MergeAndOutputRecords(prev, type, intResult, doubleResult);
	//delete all temps 
	delete prev;
	delete readIn;
	outputPipe->ShutDown();
}

Type GroupBy::IncrementSum(Record * rec, int & isum, double & dsum) {
	//code similar to Sum to add up the attribute
	Type type;
	int iIncrement = 0;
	double dIncrement = 0.0;
	type = inputFunc-> Apply(*rec, iIncrement, dIncrement);
	isum += iIncrement;
	dsum += dIncrement;
	return type;
}

void GroupBy::MergeAndOutputRecords(Record * rec, Type type,
	int isum, double dsum) {
	Record * aggregate = new Record;
	Record * toPipe = new Record;
	// Code similar to sum to create sum record
	Attribute * atts = new Attribute[1];
	std::stringstream s;
	if (type == Int) {
		s << isum;
	}
	else {
		s << dsum;
	}
	s << "|";
	atts[0].name = "SUM";
	atts[0].myType = type;
	Schema sumSchema("dummy", 1, atts);
	aggregate -> ComposeRecord(&sumSchema, s.str().c_str());

	// create rest of the attribures and merge them to record
	int numAttsToKeep = 1 + inputOrder-> numAtts;
	int * attsToKeep = new int[numAttsToKeep];
	attsToKeep[0] = 0;
	for (int i = 1; i < numAttsToKeep; i++) {
		attsToKeep[i] = inputOrder-> whichAtts[i - 1];
	}
	toPipe -> MergeRecords(aggregate, rec, 1, inputOrder-> numAtts,
		attsToKeep, numAttsToKeep, 1);
	//cout << "after MR in CAIR in Groupby*******************" << endl;
	outputPipe-> Insert(toPipe);
	//cout << "after insert in CAIR in Groupby*******************" << endl;

	delete toPipe;
	delete aggregate;
	delete[] atts;
	delete[] attsToKeep;

}

void GroupBy::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void GroupBy::Use_n_Pages(int n) {
	runlen = n;
}