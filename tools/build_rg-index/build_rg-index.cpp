#include "rts/database/Database.hpp"
#include "rg-index/rg-index.hpp"
#include <time.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

using namespace std;

char dir[100];
//---------------------------------------------------------------------------
int main(int /*argc*/,char* argv[])
{
   // Greeting
   cerr << "RG-index Builder " << endl
        << "(c) 2012 Internet Database Laboratory. Web site: http://idb.snu.ac.kr" << endl;

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      cerr << "unable to open database " << argv[1] << endl;
      return 1;
   }

   unsigned maxL, minSup;
   maxL = (unsigned) atoi(argv[2]);
   minSup = (unsigned) atoi(argv[3]);

   unsigned maxEdgeCnt=atoi(getenv("MAXEDGECNT"));
   // Output directory
   if (argv[4]) {
      sprintf(dir, "%s", argv[4]);
   }
   else {
      // current time
      time_t t;
      struct tm *t2;
      time(&t);
      t2 = localtime(&t);
      sprintf(dir, "RGINDEX_MAXL%d_MINSUP%d_MAXECNT%d_%d_%d_%d_%d", 
              maxL, minSup, maxEdgeCnt, 
              t2->tm_mon + 1, t2->tm_mday, t2->tm_hour, t2->tm_min);
   }
   cout << dir << endl;
   if (mkdir(dir, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) != 0) {
      cerr << "unable to make foler:" << dir << endl;
   }

   RGindex rgindex(argv[1], dir);
   rgindex.build(db, maxL, minSup);
}
//---------------------------------------------------------------------------

