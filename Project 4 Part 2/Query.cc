#include "Query.h"
using namespace std;

void Query::PrintQuery()
{
    if(root==nullptr)
    {
        std::cout<<"The Tree is null\n";
        return ;
    }
    root->Print();
}

void Query::ExecuteQuery()
{
	if(root==nullptr)
	{
		std::cout<<"The tree is null\n";
		return ;
	}
	root->Execute();
}




void Query::JoinsAndSelects(vector<AndList*> &joins, vector<AndList*> &selects,
			vector<AndList*> &selAboveJoin) 
{
	OrList *aOrList;
	AndList *aAndList = this->cnfAndList;
	while(aAndList) 
	{
		aOrList = aAndList->left;
		if(!aOrList) 
		{
			cerr <<"Error in cnf AndList"<<endl;
			return;
		}
		if(aOrList->left->code == EQUALS && aOrList->left->left->code == NAME && aOrList->left->right->code == NAME)
		{ 
			AndList *newAnd = new AndList();
			newAnd->left= aOrList;
			newAnd->rightAnd = NULL;
			joins.push_back(newAnd);
		} 
		else 
		{
			if(!aOrList->rightOr) 
			{
				AndList *newAnd = new AndList();
				newAnd->left= aOrList;
				newAnd->rightAnd = NULL;
				selects.push_back(newAnd);
			} 
			else 
			{ 
				vector<string> involvedTables;
				OrList *olp = aOrList;
				while(aOrList != NULL)
				{
					Operand *op = aOrList->left->left;
					if(op->code != NAME)
					{
						op = aOrList->left->right;
					}
					string rel;
					if(s->ParseRelation(string(op->value), rel) ==0) 
					{
						cerr <<"Error in parse relations"<<endl;
						return;
					}
					if(involvedTables.size() == 0)
					{
						involvedTables.push_back(rel);
					}
					else if(rel.compare(involvedTables[0]) != 0)
					{
							involvedTables.push_back(rel);
					}

					aOrList = aOrList->rightOr;
				}

				if(involvedTables.size() > 1)
				{
					AndList *newAnd = new AndList();
					newAnd->left= olp;
					newAnd->rightAnd = NULL;
					selAboveJoin.push_back(newAnd);
				}
				else
				{
					AndList *newAnd = new AndList();
					newAnd->left= olp;
					newAnd->rightAnd = NULL;
					selects.push_back(newAnd);
				}
			}
		}
		aAndList = aAndList->rightAnd;
	}
}

vector<AndList*> Query::OptimizeJoinOrder(vector<AndList*> joins) 
{
	if(joins.size() <=1 ) 
	{
		return joins;
	}
	vector<AndList*> orderedAndList;
	orderedAndList.reserve(joins.size());
	int size = joins.size();
	for(int i=0; i<size; i++) 
	{
		int joinNum = 2;
		double smallest = 0.0;
		string left_rel="", right_rel="";
		AndList *chooseAndList=NULL;
		int choosePos = -1;
		for(int j=0; j<joins.size(); j++ )
		{
			string rel1, rel2;
			AndList *andList = joins[j];
			Operand *l = andList->left->left->left;
			Operand *r = andList->left->left->right;
			this->s->ParseRelation(string(l->value), rel1);
			this->s->ParseRelation(string(r->value), rel2);
			char *estrels[] = {(char*)rel1.c_str(), (char*)rel2.c_str()};
			double cost = this->s->Estimate(andList,estrels, 2);
			if(choosePos == -1 || cost < smallest) 
			{
				smallest = cost;
				left_rel = rel1;
				right_rel = rel2;
				chooseAndList = andList;
				choosePos = j;
			}
		}
		orderedAndList.emplace_back(chooseAndList);
		char *aplyrels[] = {(char*) left_rel.c_str(), (char*)right_rel.c_str()};
		this->s->Apply(chooseAndList, aplyrels, 2);
		joins.erase(joins.begin()+choosePos);
	}
	return orderedAndList;
}




OrderMaker *Query::GenerateOM(Schema *schema) 
{
	OrderMaker *order = new OrderMaker();
	NameList *name = this->groupAtts;
	while(name) 
	{
		order->whichAtts[order->numAtts] = schema->Find(name->name);
		order->whichTypes[order->numAtts] = schema->FindType(name->name);
		order->numAtts++;
		name=name->next;
	}
	return order;
}

Function *Query::GenerateFunc(Schema *schema) 
{
	Function *func = new Function();
	func->GrowFromParseTree(this->finalFunction, *schema);
	return func;
}


std::unordered_map<string, AndList *> Query::Selectors(std::vector<AndList *> lists)
{
    unordered_map<string,AndList *> mp;
    for(auto list:lists){
        AndList *aAndList = list;
		Operand *op = aAndList->left->left->left;
		if(op->code != NAME)
			op = aAndList->left->left->right;
		string rel;
		s->ParseRelation(string(op->value), rel);
        auto mit=mp.begin();
        for(;mit!=mp.end();mit++){
            if(mit->first.compare(rel) == 0) 
            {
                AndList *lastAnd = mit->second;
                while(lastAnd->rightAnd!=NULL)
                {
                    lastAnd = lastAnd->rightAnd;
				}
                lastAnd->rightAnd = aAndList;
                break;
            }
        }
        if(mit == mp.end())
            mp[rel]=aAndList;
    }
    return mp;
}

Query:: Query(struct FuncOperator *finalFunction,
			struct TableList *tables,
			struct AndList * boolean,
			struct NameList * pGrpAtts,
	        struct NameList * pAttsToSelect,
	        int distinct_atts, int distinct_func, Statistics *s,std::string dir,string tpch,string catalog):root(new Node()),
            finalFunction(finalFunction),
            tables(tables),
            cnfAndList(boolean),
            groupAtts(pGrpAtts),
            selectAtts(pAttsToSelect),
            dist_atts(dist_atts),
            dist_fn(distinct_func),
            s(s)
            {
                 TableList *list=tables;
 				std::vector<AndList*> joins, selectors, selAboveJoin;
				JoinsAndSelects(joins,selectors,selAboveJoin);
                std::unordered_map<string,AndList *> select(std::move(Selectors(selectors)));
				while(list)
				{
					if(list->aliasAs)
					{
						s->CopyRel(list->tableName,list->aliasAs);
					}
					list=list->next;
                }
                root=new Node();
				vector<AndList *> orderJoin(std::move(OptimizeJoinOrder(joins)));
 				std::map<string,Node *> selectNode;
 				list=tables;
 				while(list)
 				{
 					Node *sel=new SelectFNode();
 					sel->dbfilePath=dir+string(list->tableName);
 					sel->o_pipe=pipeSelect++;
 					sel->outputSchema=new Schema(&catalog[0u],list->tableName);
 					string relName(list->tableName);
 					if(list->aliasAs)
 					{
 						sel->outputSchema->AdjustSchemaWithAlias(list->aliasAs);
 						relName=string(list->aliasAs);
 					}
 					AndList *andList=nullptr;
 					for(auto it:select)
 					{
 						if(!relName.compare(it.first))
 						{
 							andList=it.second;
 							break;
 						}
 					}
					sel->cnf=new CNF();
 					sel->cnf->GrowFromParseTree(andList,sel->outputSchema,*(sel->literal));
 					selectNode.emplace(relName,sel);
					list=list->next;
				}
				Node *join=nullptr;
				unordered_map<string,Node *> joinNode;
				for(auto it:orderJoin)
				{
					join=new JoinNode();
					AndList *inner=it;
					Operand *left=inner->left->left->left;
					Operand *right=inner->left->left->right;
					string leftRel=left->value,rightRel=right->value;
					s->ParseRelation(string(left->value), leftRel);
					s->ParseRelation(string(right->value),rightRel);
 					Node *leftUpMost =joinNode[leftRel];
 					Node *rightUpMost=joinNode[rightRel];
					if(!leftUpMost && !rightUpMost)
					{
						join->left=selectNode[leftRel];
						join->right=selectNode[rightRel];
						join->outputSchema=new Schema(join->left->outputSchema,join->right->outputSchema);//need to deal with it
					}
					else if(leftUpMost)
					{
						while(leftUpMost->parent )
							leftUpMost = leftUpMost->parent;
						join->left = leftUpMost; 
						leftUpMost->parent = join;
						join->right = selectNode[rightRel];
					}
					else if(rightUpMost) 
					{ 
						while(rightUpMost->parent)
							rightUpMost = rightUpMost->parent;
						join->left = rightUpMost;
						rightUpMost->parent = join;
						join->right = selectNode[leftRel];
					} 
					else 
					{ 
						while(leftUpMost->parent )
							leftUpMost = leftUpMost->parent;
						while(rightUpMost->parent)
							rightUpMost = rightUpMost->parent;
						join->left = leftUpMost;
						leftUpMost->parent = join;
						join->right  = rightUpMost;
						rightUpMost->parent = join;
					}
					joinNode[leftRel] = join;
					joinNode[rightRel] = join;
					join->l_pipe = join->left->o_pipe;
					join->r_pipe = join->right->o_pipe;
					join->outputSchema = new Schema(join->left->outputSchema, join->right->outputSchema);
					join->o_pipe = pipeSelect++;	
					join->cnf=new CNF();
					join->cnf->GrowFromParseTree(inner, join->left->outputSchema, join->right->outputSchema, *(join->literal));
	
				}

 				Node *selAbvJoin=nullptr;
				if(selAboveJoin.size() > 0 ) 
				{
					selAbvJoin = new SelectPNode();
					if(join==NULL) 
					{
						selAbvJoin->left = selectNode.begin()->second;
					} 
					else 
					{
						selAbvJoin->left = join;
					}
					selAbvJoin->l_pipe = selAbvJoin->left->o_pipe;
					selAbvJoin->o_pipe = pipeSelect++;
					selAbvJoin->outputSchema = selAbvJoin->left->outputSchema;
					AndList *andList = *(selAboveJoin.begin());
					for(auto it=selAboveJoin.begin(); it!= selAboveJoin.end(); it++) 
					{
						if(it!=selAboveJoin.begin()) 
						{
							andList->rightAnd = *it;
						}
					}
					selAbvJoin->cnf->GrowFromParseTree(andList, selAbvJoin->outputSchema, *(selAbvJoin->literal));
				}				
				Node *groupBy=nullptr;
				if(groupAtts)
				{
					groupBy=new GroupByNode();
					if(selAbvJoin) 
					{
						groupBy ->left = selAbvJoin;
					} 
					else if(join) 
					{
						groupBy->left = join;
					} 
					else 
					{
						groupBy->left = selectNode.begin()->second;
					}
					groupBy->l_pipe=groupBy->left->o_pipe;
					groupBy->o_pipe=pipeSelect++;
					groupBy->function=GenerateFunc(groupBy->left->outputSchema);
					groupBy->order=GenerateOM(groupBy->left->outputSchema);
					Attribute DA = {"double", Double};
					Attribute attr;
					attr.name = (char *)"sum";
					attr.myType = Double;
					NameList *attName = groupAtts;
					Schema *schema = new Schema ((char *)"dummy", 1, &attr);
					int numGroupAttr=0;
					while(attName)
					{
						numGroupAttr++;
						attName=attName->next;
					}
					if(!numGroupAttr)
						groupBy->outputSchema=schema;
					else
					{
						Attribute *attrs = new Attribute[numGroupAttr];
						int i = 0;
						attName = groupAtts;
						while(attName) 
						{
							attrs[i].name = &string(attName->name)[0u];
							attrs[i++].myType = groupBy->left->outputSchema->FindType(attName->name);
							attName = attName->next;
						}
						Schema *outSchema = new Schema((char *)"dummy", numGroupAttr, attrs);
						groupBy->outputSchema = new Schema(schema, outSchema);
					}
				}
				Node *sum=nullptr;
				if(!groupBy && finalFunction)
				{
					sum=new SumNode();
					if(selAbvJoin)
						sum->left=selAbvJoin;
					else if(join)
						sum->left=join;
					else
						sum->left=selectNode.begin()->second;
					sum->left->l_pipe=sum->left->o_pipe;
					sum->o_pipe=pipeSelect++;
					sum->function=GenerateFunc(sum->left->outputSchema);
					Attribute attr;
					attr.name="sum";
					attr.myType=Double;
					sum->outputSchema=new Schema((char *)"Dummy",1,&attr);
				}


				Node *project = new ProjectNode();
				int outputNum = 0;
				NameList *name = selectAtts;
				Attribute *outputAtts;
				int ithAttr = 0;
				while(name) 
				{  
					outputNum++;
					name = name->next;
				}
				if(groupBy)
				{
					project->left=groupBy;
					outputNum++;
					project->keepMe = new int[outputNum];
					project->keepMe[0] = groupBy->outputSchema->Find((char *)"sum");
					outputAtts = new Attribute[outputNum+1];
					outputAtts[0].name = (char *)"sum";
					outputAtts[0].myType = Double;
					ithAttr = 1;
				}
				else if(sum) 
				{ 
					project->left = sum;
					outputNum++;
					project->keepMe = new int[outputNum];
					project->keepMe[0] = sum->outputSchema->Find((char *) "sum");
					outputAtts = new Attribute[outputNum];
					outputAtts[0].name = (char*)"sum";
					outputAtts[0].myType = Double;
					ithAttr = 1;
				}
				else if(join) 
				{
					project->left = join;
					if(outputNum == 0) 
					{
						cerr <<"No attributes assigned to select!"<<endl;
						return ;
					}
					project->keepMe = new int[outputNum];
					outputAtts = new Attribute[outputNum];
				}
				else 
				{
					project->left = selectNode.begin()->second;
					if(outputNum == 0) 
					{
						cerr <<"No attributes assigned to select!"<<endl;
						return ;
					}
					project->keepMe = new int[outputNum];
					outputAtts = new Attribute[outputNum];
				}
				name = this->selectAtts;
				while(name) 
				{
					project->keepMe[ithAttr] = project->left->outputSchema->Find(name->name);
					outputAtts[ithAttr].name = name->name;
					outputAtts[ithAttr].myType = project->left->outputSchema->FindType(name->name);
					ithAttr++;
					name = name->next;
				}
				
				project->numAttsOutput = project->left->outputSchema->GetNumAtts();
				project->numAttsOutput = outputNum;
				project->l_pipe = project->left->o_pipe;
				project->o_pipe = pipeSelect++;
				project->outputSchema = new Schema((char*)"dummy", outputNum, outputAtts);
				root=project;


				Node *distinct = nullptr;
				if(dist_atts)
				{
					distinct=new DistinctNode();
					distinct->left = project;
					distinct->l_pipe = distinct->left->o_pipe;
					distinct->outputSchema = distinct->left->outputSchema;
					distinct->o_pipe = pipeSelect++;
					root = distinct;
				}
            }