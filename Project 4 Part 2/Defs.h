#ifndef DEFS_H
#define DEFS_H


#define MAX_ANDS 20
#define MAX_ORS 20

#define RUNLEN 5

#define PAGE_SIZE 131072

#define NEED_TO_IMPLEMENT std::cout<<"Need to implement"<<"\n";

enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};


unsigned int Random_Generate();


#endif
