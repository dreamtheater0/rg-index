#include "cts/parser/TurtleParser.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/BulkOperation.hpp"
#include "rpath/RPathTreeIndex.hpp"
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include "infra/osdep/Timestamp.hpp"

using namespace std;
//---------------------------------------------------------------------------
static void importStream(DifferentialIndex& diff,istream& in)
   // Import new triples from an input stream
{
   BulkOperation bulk(diff);
   TurtleParser parser(in);
   string subject,predicate,object,objectSubType;
   Type::ID objectType;
   while (true) {
      if (!parser.parse(subject,predicate,object,objectType,objectSubType))
         break;
      bulk.insert(subject,predicate,object,objectType,objectSubType);
   }
   bulk.commit();
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Greeting
   cerr << "RPath Index Updater" << endl;

   // Check the arguments
   if (argc<5) {
      cerr << "usage: " << argv[0] << " <database> <maxL> " 
           << "<insert_triples_file> <delete_triples_file> <rpathDIR>" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      cerr << "unable to open " << argv[1] << endl;
      return 1;
   }

   RPathUpdateInfo insert_info, delete_info;
   DifferentialIndex inserted(db);
   ifstream in(argv[3]);
   if (!in.is_open()) {
      cerr << "unable to open " << argv[3] << endl;
      return 1;
   }
   importStream(inserted, in);
   inserted.getRPathUpdateInfo(insert_info);

   DifferentialIndex deleted(db);
   
   ifstream in2(argv[4]);
   if (!in2.is_open()) {
      cerr << "unable to open " << argv[4] << endl;
      return 1;
   }
   importStream(deleted, in2);
   deleted.getRPathUpdateInfo(delete_info);

   unsigned maxL = atoi(argv[2]);
   RPathTreeIndex rpathTreeIdx(argv[1], argv[5], maxL);
   // load RP-index(old version)

   rpathTreeIdx.rpathUpdate(db, maxL, insert_info, delete_info);
}
//---------------------------------------------------------------------------
