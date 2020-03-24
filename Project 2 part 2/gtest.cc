#include "DBFile.h"
//#include "SortedFile.h"
#include "BigQ.h"
#include <pthread.h>
#include "gtest/gtest.h"
#include "test.h"

TEST (SortedFile, SortCheck)
{
	OrderMaker o;

	int runlen = 0;
	while (runlen < 1) {
			cout << "\t\n specify runlength:\n\t ";
			cin >> runlen;
	}
	rel->get_sort_order (o);
	struct {OrderMaker *o; int l;} startup = {&o, runlen};

	DBFile dbfile;
	Record* Rec = new Record;

	EXPECT_EQ(1,dbfile.Create (rel->path(), sorted, &startup));
	dbfile.Close ();
	EXPECT_EQ(1,dbfile.Open (rel->path()));

	char path[100];
	sprintf(path, "%s.meta", rel->path());

	EXPECT_EQ(1,path != NULL);

}

int main (int argc, char *argv[]) {

		testing::InitGoogleTest(&argc, argv);

		setup ();

    relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};

    int findx = 0;
    while (findx < 1 || findx > 8) {
        cout << "\n select table: \n";
        cout << "\t 1. nation \n";
        cout << "\t 2. region \n";
        cout << "\t 3. customer \n";
        cout << "\t 4. part \n";
        cout << "\t 5. partsupp \n";
        cout << "\t 6. supplier \n";
        cout << "\t 7. orders \n";
        cout << "\t 8. lineitem \n \t ";
        cin >> findx;
    }
    rel = rel_ptr [findx - 1];

    cleanup ();
    cout << "\n\n";

		return RUN_ALL_TESTS();
}
