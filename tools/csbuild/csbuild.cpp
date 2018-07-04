#include <iostream>
#include <fstream>
#include <cassert>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sstream>
#include <queue>
#include <set>
#include <map>
#include <algorithm>
#include "rpath/RPathTreeIndex.hpp"
#include "rpath/RPathSegment.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
using namespace std;
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Greeting
   cerr << "Characteristics Set Builder " << endl
        << "(c) 2011 Internet Database Laboratory. Web site: http://idb.snu.ac.kr" << endl;

   // Check the arguments
   if (argc<2) {
      cerr << "usage: " << argv[0] << " <database>" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      cerr << "unable to open database " << argv[1] << endl;
      return 1;
   }

   Register subjectS,predicateS;
   Register predicateO,objectO;
   subjectS.reset(); predicateS.reset(); predicateO.reset(); objectO.reset();
   AggregatedIndexScan *scan_S, *scan_O;
   scan_S = AggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,
         &subjectS,false,&predicateS,false,NULL,false,0);
   scan_O = AggregatedIndexScan::create(db,Database::Order_Object_Predicate_Subject,
         NULL,false,&predicateO,false,&objectO,false,0);

   scan_S->first();
   scan_O->first();

   bool endS=false, endO=false;
   map<string,unsigned> charset;
   map<string,unsigned> charset2;

   while (!endS || !endO) {
      vector<unsigned> cs;
      bool gatherS=false, gatherO=false;
      unsigned nodeId;

      if (endS || (!endO && subjectS.value > objectO.value)) {
         gatherO=true;
         nodeId=objectO.value;
      }
      else if (endO || (!endS && subjectS.value < objectO.value)) {
         gatherS=true;
         nodeId=subjectS.value;
      }
      else {
         gatherO=true;
         gatherS=true;
         nodeId=subjectS.value;
         assert(subjectS.value==objectO.value);
      }
      
      if (gatherO) {
         do {
            cs.push_back(predicateO.value);
            if (!scan_O->next()) {
               endO=true;
               break;
            }
         } while (nodeId==objectO.value);
      }
      if (gatherS) {
         do {
            cs.push_back(predicateS.value+100000000u);
            if (!scan_S->next()) {
               endS=true;
               break;
            }
         } while (nodeId==subjectS.value);
      }

      stringstream convert;
      for (unsigned i=0; i<cs.size(); i++) {
         unsigned p=cs[i];

         if (i!=0)
            convert << "," << p;
         else 
            convert << p;
      }
      string predset = convert.str();
      if (charset.count(predset) > 0) charset[predset]++;
      else charset[predset]=1;

      // special: length=2
      for (unsigned i=0; i<cs.size(); i++) {
         if (i+1 < cs.size()) {
            for (unsigned j=i+1; j<cs.size(); j++) {
               stringstream convert;
               unsigned p1=cs[i];
               unsigned p2=cs[j];
               assert(p1<=p2);
               convert << p1 << "|" << p2;
               string predset = convert.str();
               if (charset2.count(predset) > 0) charset2[predset]++;
               else charset2[predset]=1;
            }
         }
      }
   }

   char statName[256];
   memset(statName, 0, 256);
   sprintf(statName, "%s.cset", argv[1]);
   ofstream ofs_stat(statName, ios::out|ios::trunc);
   for (map<string,unsigned>::iterator it=charset.begin(), limit=charset.end();
        it != limit; it++) {
      ofs_stat << (*it).first <<":";
      unsigned cnt = (*it).second;
      ofs_stat << cnt << endl;
   }
   ofs_stat.flush();
   ofs_stat.close();

   sprintf(statName, "%s.stat2", argv[1]);
   ofstream ofs_stat2(statName, ios::out|ios::trunc);
   for (map<string,unsigned>::iterator it=charset2.begin(), limit=charset2.end();
        it != limit; it++) {
      ofs_stat2 << (*it).first << "|";
      unsigned cnt = (*it).second;
      ofs_stat2 << cnt << "|" << endl;
   }
   ofs_stat2.flush();
   ofs_stat2.close();
   cout << subjectS.value << " " << objectO.value << endl;
}
//---------------------------------------------------------------------------
