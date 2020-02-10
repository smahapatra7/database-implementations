#include "DBFile.h"
#include "gtest/gtest.h"
#include "test.h"

const char *dbfile_dir = "bin/";
const char *tpch_dir ="tpch-dbgen/";
const char *catalog_path = "catalog";

relation *reltn;
DBFile dbf;

TEST (DBFileTest, Create) {
    
    EXPECT_EQ(1,dbf.Create(reltn->path(),heap,NULL));
    dbf.Close();
}

TEST(DBFileTest, Open) {

    dbf.Create(reltn->path(),heap,NULL);
    EXPECT_EQ(1,dbf.Open(reltn->path()));
    dbf.Close();
}

TEST(DBFileTest, Close) {

  dbf.Create(reltn->path(),heap,NULL);
  dbf.Open(reltn->path());
  EXPECT_EQ(1,dbf.Close());
}

int main(int argc, char **argv) {

    testing::InitGoogleTest(&argc, argv);

    setup (catalog_path, dbfile_dir, tpch_dir);

    int get_tbl = 0;
    relation *tbl_p[] = {n, r, c, p, ps, o, li};

    while (get_tbl <= 0 || get_tbl >= 8) {
      cout << "\n Select the table to test:"<<endl;
      cout << "1.Nation"<<endl;
      cout << "2.Region"<<endl;
      cout << "3.Customer"<<endl;
      cout << "4.Part"<<endl;
      cout << "5.Partsupp"<<endl;
      cout << "6.Orders"<<endl;
      cout << "7.Lineitem"<<endl;
      cin >> get_tbl;
    }

    get_tbl--;
    reltn = tbl_p [get_tbl];

    return RUN_ALL_TESTS();
}
