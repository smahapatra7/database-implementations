//#include <std.h>
#include "gtest/gtest.h"
#include "DBFile.h"
#include "Schema.h"
#include "Record.h"
#include "TwoWayList.cc"
#include <iostream>
#include <pthread.h>
#include "BigQ.h"
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Pipe.h"
#include "test.h"


TEST( BinFileOpen, One )
{
    int return_value;
    DBFile dbf;
    return_value = dbf.Open ("runfile");
    EXPECT_TRUE(return_value == 1);
    dbf.Close ();
}

TEST( BinFileNotEmpty, Two)
{
    int NotEmpty;
    Record temp;
    int counter = 0;

    DBFile dbf;
    dbf.Open ("nation.bin");
    dbf.MoveFirst ();
    NotEmpty = dbf.GetNext (temp);
    EXPECT_TRUE(NotEmpty == 1);
}

TEST( WriteinFile, three )
{
    char *file_path = "gtestwrite/file";
    fType file_type = heap;
    DBFile dbf;
    int a = dbf.Create(file_path, file_type, NULL);
    Schema mySchema ("catalog", "orders");
    Record record;
    FILE *orderFile = fopen ("orders.tbl", "r");
    record.SuckNextRecord(&mySchema,orderFile);
    dbf.Add(record);
    dbf.Close();
    int file  = dbf.Open(file_path);
    EXPECT_TRUE(dbf.getFile()->GetLength() > 0);
    dbf.Close();
}

TEST(MoveFirst, four )
{
    char *file_path = "gtestwrite/file";
    fType file_type = heap;
    DBFile dbf;
    int return_value = dbf.Create(file_path, file_type, NULL);
    Schema mySchema ("catalog", "orders");
    Record record;
    FILE *orderFile = fopen ("orders.tbl", "r");
    record.SuckNextRecord(&mySchema,orderFile);
    dbf.Add(record);
    record.SuckNextRecord(&mySchema,orderFile);
    dbf.Add(record);
    dbf.Close();
    int file = dbf.Open(file_path);
    dbf.MoveFirst();
    dbf.Close();
    EXPECT_TRUE(true);
}

int main(int argc, char **argv) 
{
  ::testing::InitGoogleTest(&argc, argv); 
  return RUN_ALL_TESTS();
}
