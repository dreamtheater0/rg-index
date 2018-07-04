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
typedef struct {
   unsigned idx;
   unsigned pathID;
   unsigned value;
} HeapEntity;
//---------------------------------------------------------------------------
bool compare(HeapEntity& x, HeapEntity& y)
{
   return x.value > y.value;
}
//---------------------------------------------------------------------------
// N-way merge
//---------------------------------------------------------------------------
static void makeAdditionalStatistics2(RPathTreeIndex *rpathTreeIdx, ofstream& ofs_stat)
{
   map<unsigned, RPathTreeIndex::Node *> *nodeList = rpathTreeIdx->GetRootChildren();
   for (map<unsigned, RPathTreeIndex::Node *>::const_iterator iter=nodeList->begin(),
        limit=nodeList->end(); iter!=limit;++iter) {
      RPathTreeIndex::Node *node = (*iter).second;
      node->rpathSgm = new RPathSegment(&rpathTreeIdx->file, node->startPage, 
            node->startIndexPage, node->startIndexPage-node->startPage);
      RPathSegment *rpathSgmt1 = node->rpathSgm;

      vector <RPathSegment::Scan*> scans;
      vector <RPathSegment*> sgms;
      vector <unsigned> predicates;
      vector <unsigned> cnts;
      vector <unsigned> nodeIDs;
      vector <bool> scanEnds;
      unsigned idx = 0;
      vector<HeapEntity> heap;
      HeapEntity entity;

      cout << "Predicate:" << node->predicate << endl;

      map<unsigned, RPathTreeIndex::Node *>::const_iterator iter2=iter, limit2;
      for (iter2++, limit2=nodeList->end(); iter2!=limit2;++iter2) {
         RPathTreeIndex::Node *node2 = (*iter2).second;
         if (node->predicate == node2->predicate) continue;

         RPathSegment::Scan *scan = new RPathSegment::Scan;
         unsigned nodeID;
         node2->rpathSgm = new RPathSegment(&rpathTreeIdx->file, node2->startPage, 
               node2->startIndexPage, node2->startIndexPage-node2->startPage);

         scan->first(*node2->rpathSgm);
         nodeID = scan->getValue1();
         scans.push_back(scan);
         sgms.push_back(node2->rpathSgm);
         scanEnds.push_back(false);
         predicates.push_back(node2->predicate);
         cnts.push_back(0);
         nodeIDs.push_back(nodeID);

         entity.idx = idx++;
         entity.value = nodeID;
         heap.push_back(entity);
      }

      cout << "Target predicate size:" << cnts.size() << endl;
      std::make_heap(heap.begin(), heap.end(), compare);

      // Merge 시작
      bool allEnd=false;
      unsigned heap_value=0, heap_idx=0, value1;
      RPathSegment::Scan scan1;
      scan1.first(*rpathSgmt1); value1 = scan1.getValue1();

      while (heap.size() > 0) {
         // Heap에서 가장 작은 값을 가져온다
         entity = heap.front();
         heap_value = entity.value;    

         while (value1 != heap_value) {
            while (value1 < heap_value) {
               if (!scan1.next()) {
                  allEnd = true;
                  break;
               }
               value1 = scan1.getValue1();
            }
            if (allEnd) break;

            while (value1 > heap_value) {
               // pop from heap
               entity = heap.front();
               std::pop_heap(heap.begin(), heap.end(), compare);
               heap.pop_back();
               heap_idx = entity.idx;

               if (!scanEnds[heap_idx]) {
                  // 해당 entity의 scan이 안끝났으면 scan
                  while (!scanEnds[heap_idx] && nodeIDs[heap_idx] < value1) {
                     if (!scans[heap_idx]->next()) {
                        scanEnds[heap_idx] = true;
                        break;
                     }
                     nodeIDs[heap_idx] = scans[heap_idx]->getValue1();
                  }

                  if (!scanEnds[heap_idx]) {
                     entity.value = nodeIDs[heap_idx];
                     heap.push_back(entity); 
                     std::push_heap(heap.begin(), heap.end(), compare);
                  }
               }

               if (heap.size() ==0) {
                  allEnd = true;
                  break;
               }
               entity = heap.front();
               heap_value = entity.value;    
            }
            if (allEnd) break;
         }
         if (allEnd) break;

         // 여기서 부터는 value1 == heap_value
         assert(heap_value == value1);
         entity = heap.front();
         pop_heap(heap.begin(), heap.end(), compare);
         heap.pop_back();
         heap_idx = entity.idx;

         cnts[heap_idx]++;

         // 해당 entity의 scan이 안끝났으면 scan에서 한개를 가져와 
         // heap에 넣는다
         if (!scanEnds[heap_idx]) {
            if (!scans[heap_idx]->next()) scanEnds[heap_idx] = true;
            else {
               nodeIDs[heap_idx] = scans[heap_idx]->getValue1();
               entity.value = nodeIDs[heap_idx];
               heap.push_back(entity); 
               push_heap(heap.begin(), heap.end(), compare);
            }
         }
      }

      // Output
      for (unsigned i=0;i<cnts.size();i++) {
         if (cnts[i] > 0)
            ofs_stat << node->predicate << "|" << predicates[i] << "|" << cnts[i] << endl; 
         delete scans[i];
      }

      delete node->rpathSgm;
      node->rpathSgm=NULL;

      for (unsigned i=0;i<sgms.size();i++) {
         delete sgms[i];
      }
   }
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Greeting
   cerr << "RPath Stat Builder " << endl
        << "(c) 2011 Internet Database Laboratory. Web site: http://idb.snu.ac.kr" << endl
        << " *** RPath Stat Builder uses rpathfilter with MaxL=3, reverse mode and variable predicate" << endl;

   // Check the arguments
   if (argc<3) {
      cerr << "usage: " << argv[0] << " <database> <rpathfileterpath> <maxL>" << endl;
      return 1;
   }

   int maxL;
   maxL = atoi(argv[3]);

/*
   if (getenv("REVERSE")) {
      cerr << " turn on REVERSE mode. " << endl;
      return 1;
   }
   */

   char statName[256];
   memset(statName, 0, 256);
   sprintf(statName, "%s.stat2", argv[1]);

   ofstream ofs_stat(statName, ios::out|ios::trunc);

   RPathTreeIndex temp_rpathTreeIdx(argv[1], argv[2], maxL);

   //temp_rpathTreeIdx.Print();
   makeAdditionalStatistics2(&temp_rpathTreeIdx, ofs_stat);
}
//---------------------------------------------------------------------------
