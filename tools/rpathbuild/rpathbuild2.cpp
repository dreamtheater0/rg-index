#include "rts/database/Database.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rpath/RPathTreeIndex.hpp"
#include "rpath/RPathSegment.hpp"
#include <cassert>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define QUOTEME_(x) #x
#define QUOTEME(x) QUOTEME_(x)
int main(int argc,char* argv[])
{
   char DIR[100];
   // Greeting
   cerr << "RPath Builder " << endl
        << "(c) 2011 Internet Database Laboratory. Web site: http://idb.snu.ac.kr" << endl;

   // Check the arguments
   if (argc<3 || !getenv("TH_MINCNT") || !getenv("TH_DIFF")) {
      cerr << "usage: " << argv[0] << " <database> <maxL> [outputDIR]" << endl;
      cerr << "ENV: TH_DIFF, TH_MINCNT" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      cerr << "unable to open database " << argv[1] << endl;
      return 1;
   }

   int maxL;
   maxL = atoi(argv[2]);

   // Output directory
   if (argv[3]) {
      sprintf(DIR, "%s", argv[3]);
   }
   else {
      // current time
      time_t t;
      struct tm *t2;
      time(&t);
      t2 = localtime(&t);
      sprintf(DIR, "rpath_%d_%d_%d_%d", t2->tm_mon + 1, t2->tm_mday, t2->tm_hour, t2->tm_min);
   }
   cout << DIR << endl;
   if (mkdir(DIR, S_IRWXU) != 0) {
      cerr << "unable to make foler:" << DIR << endl;
   }
   if (chdir(DIR) != 0) {
      cerr << "unable to move to foler:" << DIR << endl;
   }

   std::cout << argv[1] << " " << maxL << " TH_DIFF:" QUOTEME(TH_DIFF) " TH_MINBLK:" QUOTEME(TH_MINBLK) " REVERSE:" QUOTEME(REVERSE) " VARPREDICATE:" QUOTEME(VARP) << endl;
   std::cout << "Retrieving the predicate list..." << endl;

   char dir[256];
   strcpy(dir, ".");

   RPathTreeIndex temp_rpathTreeIdx(argv[1], dir, maxL, false);
   temp_rpathTreeIdx.rpathBuild(db, maxL);
}
