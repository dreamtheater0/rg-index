#include <iostream>
#include <fstream>
#include <cassert>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <queue>
#include <set>
#include <map>
#include <algorithm>
#include "rpath/RPathTreeIndex.hpp"
#include "rpath/RPathSegment.hpp"
using namespace std;

//---------------------------------------------------------------------------
void makePPath(vector<unsigned>& predicates,RPathTreeIndex& temp_rpathTreeIdx, unsigned &cnt,
               vector<unsigned>& ppath, unsigned prev_p, unsigned level, unsigned maxL)
{
   unsigned pathsize = ppath.size();
   for (vector<unsigned>::const_iterator iter=predicates.begin(), limit=predicates.end(); iter!=limit;++iter) {
      PPath temp_ppath;

      RPathTreeIndex::Node *node;
      if (pathsize >=2) {
         temp_ppath.Add(ppath[pathsize-2]);
         temp_ppath.Add(ppath[pathsize-1]);
         temp_ppath.Add((*iter));

         node = temp_rpathTreeIdx.SearchNode(temp_ppath);
      }
      else {
         temp_ppath.Add(prev_p);
         temp_ppath.Add((*iter));

         node = temp_rpathTreeIdx.SearchNode(temp_ppath);
      }
      if (node == NULL) continue;

      ppath.push_back((*iter));
      if (level == maxL) {
         for (vector<unsigned>::const_iterator iter2=ppath.begin(), limit2=ppath.end();
              iter2!=limit2;++iter2) {
            cout << (*iter2) << " ";
         }
         cout << endl;
         cnt++;
         ppath.pop_back();
         continue;
      }

      makePPath(predicates, temp_rpathTreeIdx, cnt, ppath, (*iter), level+1, maxL);
      ppath.pop_back();
   }
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Greeting
   cerr << "RPath Query Builder " << endl
        << "(c) 2011 Internet Database Laboratory. Web site: http://idb.snu.ac.kr" << endl;

   // Check the arguments
   if (argc<6) {
      cerr << "usage: " << argv[0] << " <database> <rpathfileterpath> <mincardinality> <maxCardinality> <pathlength>" << endl;
      return 1;
   }

   RPathTreeIndex temp_rpathTreeIdx(argv[1], argv[2], 3);

   vector<RPathTreeIndex::Node *>* nodeList = temp_rpathTreeIdx.GetRootChildren(); 
   vector<unsigned> predicate, ppath; 

   unsigned minCardinality = (unsigned) atoi(argv[3]);
   unsigned maxCardinality = (unsigned) atoi(argv[4]);
   int depth = atoi(argv[5]);

   for (vector<RPathTreeIndex::Node *>::const_iterator iter=nodeList->begin(),
        limit=nodeList->end(); iter!=limit;++iter) {
      RPathTreeIndex::Node* node = *iter;
      if (node->cardinality < minCardinality) continue;
      if (node->cardinality > maxCardinality) continue;
      if (node->predicate >= 1000000) continue;

      predicate.push_back(node->predicate);
   }

   unsigned cnt=0;
   cerr << predicate.size() << endl;
   for (vector<unsigned>::const_iterator iter=predicate.begin(), limit=predicate.end(); iter!=limit;++iter) {
      ppath.push_back((*iter));
      makePPath(predicate, temp_rpathTreeIdx, cnt, ppath, (*iter), 2, depth);
      ppath.pop_back();
   }

   cerr << cnt << " queries generated.\n";
}

//---------------------------------------------------------------------------
