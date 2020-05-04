#ifndef HEAPFILE_H
#define HEAPFILE_H

#pragma once
#include "TreeFile.h"
#include "Record.h"
#include "DBFile.h"
class HeapFile:public TreeFile{
public:
    HeapFile();
    void Add (Record &addme) override; 
};

#endif