#include "TreeNode.h"

void SelectFNode::Print(){
    if(left)
        left->Print();
	cout <<"****** Select Operation : File *******"<<endl;
	cout <<endl;
	cout <<"Input DB File: "<<dbfilePath<<endl;
	cout <<"Output Pipe: "<<o_pipe<<endl;
	cout <<"Output Schema: " <<endl;
    outputSchema->Print();
	cout <<"Select CNF : " <<endl;
	cout <<"\t"; cnf->Print();
	cout <<"\n\n";
	if(right)
		right->Print();
}

void SelectPNode::Print(){
	if(left)
		left->Print();
	cout <<"****** Select Operation : Pipe ******"<<endl;
	cout <<endl;
	cout <<"Input Pipe: "<<l_pipe<<endl;
	cout <<"Output Pipe: "<<o_pipe<<endl;
	cout <<"Output Schema: " <<endl;
    outputSchema->Print();
	cout <<"Select CNF: " <<endl;
	cout <<"\t"; cnf->Print();
	cout <<"\n\n";
	if(right)
		right->Print();
}

void ProjectNode::Print(){
	if(left)
		left->Print();
	cout <<"****** Project Operation ******"<<endl;
	cout <<endl;
	cout <<"Input Pipe: "<<l_pipe<<endl;
	cout <<"Output Pipe: "<<o_pipe<<endl;
	cout <<"Output Schema: " <<endl;
    outputSchema->Print();
	cout <<"Attributes to keep: "<<endl;
	cout <<"\t";
	for(int i=0;i<numAttsOutput;i++) {
		cout <<keepMe[i] <<", ";
	}
	cout <<endl;
	cout <<"\n";
	if(right)
		right->Print();
}

void JoinNode::Print(){
	if(left)
		left->Print();
	cout <<"****** Join Operation ******"<<endl;
	cout <<endl;
	cout <<"Left Input Pipe: "<<l_pipe<<endl;
	cout <<"Right Input Pipe: "<<r_pipe<<endl;
	cout <<"Output Pipe: "<<o_pipe<<endl;
	cout <<"Output Schema: " <<endl;
    outputSchema->Print();
	cout <<"Select CNF:	" <<endl;
	cout <<"\t"; cnf->Print();
	cout <<"\n\n";
	if(right)
		right->Print();
}

void SumNode::Print(){
	if(left)
		left->Print();
	cout <<"****** Sum Operation ******"<<endl;
	cout <<endl;
	cout <<"Input Pipe:	"<<l_pipe<<endl;
	cout <<"Output Pipe: "<<o_pipe<<endl;
	cout <<"Output Schema: " <<endl;
    outputSchema->Print();
	cout <<"Sum Function: " <<endl;
		function->Print();
	cout <<endl;
	cout <<"\n";
	if(right)
		right->Print();
}

void GroupByNode::Print(){
	if(left)
		left->Print();
	cout <<"****** GroupBy Operation ******"<<endl;
	cout <<endl;
	cout <<"Input Pipe:	"<<l_pipe<<endl;
	cout <<"Output Pipe: "<<o_pipe<<endl;
	cout <<"Output Schema: " <<endl;
    outputSchema->Print();
	cout <<"Group By OrderMaker: " <<endl;
	order->Print();
	cout <<endl;
	cout <<"Group By Function: " <<endl;
	function->Print();
	cout <<endl;
	cout <<"\n";
	if(right)
		right->Print();
}

void DistinctNode::Print(){
	if(left)
		left->Print();
	cout <<"****** Duplicate Removal Operation ******"<<endl;
	cout <<endl;
	cout <<"Input Pipe: "<<l_pipe<<endl;
	cout <<"Output Pipe: "<<o_pipe<<endl;
	cout <<"Output Schema: " <<endl;
    outputSchema->Print();
	cout <<"\n";
	if(right)
		right->Print();
}

void WriteOutNode::Print(){
	if(left)
		left->Print();
	cout <<"****** Write Out ******"<<endl;
	cout <<endl;
	cout <<"Input Pipe: "<<l_pipe<<endl;
	cout <<"Output Schema: " <<endl;
    outputSchema->Print();
	cout <<"\n";
	if(right)
		right->Print();
}

void Node::Print(){
    if(left)
        left->Print();
    if(right)
        right->Print();
}


void Node::Execute(){
	NEED_TO_IMPLEMENT
}
void SelectFNode::Execute(){
	NEED_TO_IMPLEMENT
}

void SelectPNode::Execute(){
	NEED_TO_IMPLEMENT
}

void ProjectNode::Execute(){
	NEED_TO_IMPLEMENT
}

void JoinNode::Execute(){
	NEED_TO_IMPLEMENT
}

void SumNode::Execute(){
	NEED_TO_IMPLEMENT
}

void GroupByNode::Execute(){
	NEED_TO_IMPLEMENT
}

void DistinctNode::Execute(){
	NEED_TO_IMPLEMENT
}

void WriteOutNode::Execute(){
	NEED_TO_IMPLEMENT
}