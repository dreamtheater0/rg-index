#include "rts/database/Database.hpp"
#include "rts/operator/FullyAggregatedIndexScan.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rpath/RPathSegment.hpp"
#include <iostream>
#include <cassert>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
using namespace std;
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Greeting
   cerr << "RPath Reader " << endl
        << "(c) 2011 Internet Database Laboratory. Web site: http://idb.snu.ac.kr" << endl;

   // Check the arguments
   if (argc<4) {
      cerr << "usage: " << argv[0] << " <database> <start> <indexRoot>" << endl;
      return 1;
   }

   // Open the rpath filter 
   MemoryMappedFile file;
   char filename[256];
   unsigned start, indexRoot;

   start = (unsigned) atoi(argv[2]);
   indexRoot = (unsigned) atoi(argv[3]);

   sprintf(filename, "%s.data", argv[1]);
      
   //cout << start << " " << indexRoot << endl;
   if (file.open(filename)) {
      RPathSegment rpathSgmt(&file, start, indexRoot, indexRoot-start);
      RPathSegment::Scan scan;
      unsigned nodeID = 0;
      unsigned prev = 0;
      if (scan.first(rpathSgmt)) {
         prev = nodeID;
         nodeID = scan.getValue1();
         assert (prev <= nodeID);
         cout << nodeID << endl;
         while(scan.next()) {
            prev = nodeID;
            nodeID = scan.getValue1();
            assert (prev <= nodeID);
            cout << nodeID << endl;
         }

      }

      return 0;
   }
   
   cerr << "unable to open " << argv[0] << " rpath" << endl;
   return 1;
}
//---------------------------------------------------------------------------
