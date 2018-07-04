#include "rts/database/Database.hpp"
#include "rts/operator/FullyAggregatedIndexScan.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rpath/RPathTreeIndex.hpp"
#include "rpath/RPathSegment.hpp"
#include "Sorter.hpp"
#include "TempFile.hpp"
#include "rpath/PPath.hpp"
#include <iostream>
#include <cassert>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <queue>
#include <set>
#include <map>
#include <algorithm>
#include <cmath>
#include <time.h>
#include <sys/stat.h>
#include <unistd.h>
using namespace std;
#define PARTITION ((unsigned) atoi(getenv("PARTITION")))
#define PPATHNUM ((unsigned) atoi(getenv("PPATHNUM")))
#define QUOTEME_(x) #x
#define QUOTEME(x) QUOTEME_(x)

pthread_mutex_t mutex_lock;
pthread_mutex_t mutex_inppath_lock;
unsigned volatile ppath_idx;

char DIR[100];
//---------------------------------------------------------------------------
typedef struct {
   unsigned ppathID;
   set<unsigned> list;
   bool nondiscriminative;
   RPathTreeIndex::Node *suffixNode;
} SortedList;
//---------------------------------------------------------------------------
typedef struct {
   Database* db;
   vector<PPath*>* ppaths;
   RPathTreeIndex* rpathTreeIdx;
   unsigned p;
   unsigned myIDX;
   vector<PPath*>* ppaths_new;
   unsigned maxL;
   unsigned ppathnum;
   double minCnt;
} GetNodeLists3Args;
//---------------------------------------------------------------------------
RPathTreeIndex::Node *getSuffixNode(RPathTreeIndex &rpathTreeIdx, PPath &ppath) 
{
   unsigned i = 1;
   while (true) {
      PPath suffix;
      suffix.path.assign(ppath.path.begin() + i, ppath.path.end());
      RPathTreeIndex::Node *suffixNode = rpathTreeIdx.SearchNode(suffix);
      if (suffixNode) 
         return suffixNode;
      i++;
   }

   assert(false);
   return NULL;
}
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
typedef struct {
   vector<PPath*>* inppaths;
   RPathTreeIndex* rpathTreeIdx;
   unsigned p;
   unsigned myIDX;
   double minCnt;
   vector<PPath*>* remains;
} RemoveInfrequentArgs;
//---------------------------------------------------------------------------
void removeInfrequent(RemoveInfrequentArgs *args)
{
   unsigned myIDX = args->myIDX;
   //unsigned th_mincnt = args->rpathTreeIdx->th_mincnt;
   unsigned p = args->p;

   RPathTreeIndex* rpathTreeIdx = args->rpathTreeIdx;
   vector<PPath*>* inppaths = args->inppaths;
   vector<PPath*>* remains = args->remains;

   vector <PPath> oneBlkPpaths, ppaths;

   unsigned len = (*inppaths)[0]->size();

   for (unsigned i=0, limit=inppaths->size(); i<limit; i++) {
      if ((i % (PARTITION)) != myIDX) continue;

      unsigned lastPred = (*inppaths)[i]->path[len -1];
      if ((p < REVERSE_PRED && p + REVERSE_PRED == lastPred) ||
          (p >= REVERSE_PRED && p - REVERSE_PRED == lastPred))
         continue;

      PPath suffix;
      for (unsigned j=1,limit=(*inppaths)[i]->size(); j<limit; j++) {
         suffix.Add((*inppaths)[i]->path[j]);
      }
      suffix.Add(p);
      RPathTreeIndex::Node *suffixNode = rpathTreeIdx->SearchNode(suffix);

      if (suffixNode && len == suffixNode->len) {
         // if suffixNode is infrequent
         if (suffixNode->skip)
            continue;
         
         if (suffixNode->cardinality < args->minCnt / 2.0f) 
            continue;
      }

      RPathTreeIndex::Node *prefixNode = rpathTreeIdx->SearchNode(*(*inppaths)[i]);
      if (prefixNode->skip)
         continue;

      remains->push_back((*inppaths)[i]);
   }
}
//---------------------------------------------------------------------------
void getNodeLists(GetNodeLists3Args *args)
{
   vector<PPath*>* inppaths = args->ppaths;
   unsigned p = args->p, myIDX = args->myIDX, ppathnum = args->ppathnum;

   Register subject,predicate,object, rpflt_value;
   subject.reset(); predicate.reset(); object.reset(); rpflt_value.reset();
   predicate.value = (p>=(REVERSE_PRED))?p-(REVERSE_PRED):p;

   vector <RPathTreeIndex::Node *> suffixNodes;

   // p sgmt
   IndexScan* scan;
   if (p==ALLPREDICATE) {
      scan = IndexScan::create(*args->db,Database::Order_Subject_Object_Predicate,
                               &subject,false,&predicate,false,&object,false,0);
      scan->addMergeHint(&subject, &rpflt_value);
   }
   else if (p==ALLPREDICATE+(REVERSE_PRED)) {
      scan = IndexScan::create(*args->db,Database::Order_Object_Subject_Predicate,
                               &object,false,&predicate,false,&subject,false,0);
      scan->addMergeHint(&object, &rpflt_value);
   }
   else if (p>=(REVERSE_PRED)) {
      scan = IndexScan::create(*args->db,Database::Order_Predicate_Object_Subject,
                               &object,false,&predicate,true,&subject,false,0);
      scan->addMergeHint(&object, &rpflt_value);
   }
   else {
      scan = IndexScan::create(*args->db,Database::Order_Predicate_Subject_Object,
                               &subject,false,&predicate,true,&object,false,0);
      scan->addMergeHint(&subject, &rpflt_value);
   }

   unsigned len=0, inppath_size = inppaths->size();

   if (inppath_size > 0) 
      len = (*inppaths)[0]->size();

   unsigned start_idx, iter=0;
   while (true) {
      iter++;
      vector<PPath*> ppaths;
      vector<PPath*>::iterator start, end;
      vector<SortedList *> sls;

      pthread_mutex_lock(&mutex_inppath_lock); {
         start_idx=ppath_idx;
         ppath_idx+=ppathnum;
      } pthread_mutex_unlock(&mutex_inppath_lock);

      start=inppaths->begin() + start_idx;
      end=inppaths->begin() + start_idx + ppathnum;
      end=inppaths->end()<end?inppaths->end():end;
      if (start >= end)
         break;
      ppaths.insert(ppaths.end(), start, end); 
      unsigned size = ppaths.size();
      std::cout << "- start getNodeLists. [" << myIDX << "] p: " 
                << p << ", size:" << size << ", start_idx:" << start_idx;
      if (myIDX==0) {
         if (iter%1000==0)
            std::cout << "- p: " << p << ", start_idx:" << start_idx;
      }

      rpflt_value.value = 0;
      if (!scan->first()) {
         delete scan;
         return;
      }

      // Prepare RPathSegments & heap
      RPathSegment::Scan **scans = (RPathSegment::Scan **) malloc(sizeof(RPathSegment::Scan*) * size);
      unsigned *nodeIDs = (unsigned *) malloc(sizeof(unsigned) * size);
      bool *scanEnd = (bool *) malloc (sizeof(bool) * size);
      vector<HeapEntity> heap;
      HeapEntity entity;
      vector<RPathTreeIndex::Node*> nodes;

      unsigned idx = 0;
      for (unsigned i=0; i<size; i++) {
         PPath* ppath = ppaths[i];
         RPathTreeIndex::Node* node = args->rpathTreeIdx->SearchNode(*ppath);
         nodes.push_back(node);
         assert(node); 

         // ppath sgmt
         scans[i] = new RPathSegment::Scan;
         scanEnd[i] = false;
         node->rpathSgm = new RPathSegment(&args->rpathTreeIdx->file, node->startPage, 
                                           node->startIndexPage,
                                           node->startIndexPage-node->startPage);
         if(!scans[i]->first(*node->rpathSgm)) {
            scanEnd[i] = true;
         }
         nodeIDs[i] = scans[i]->getValue1();

         entity.idx = i;
         entity.pathID = ppath->ppathID;
         entity.value = nodeIDs[i];
         heap.push_back(entity);
         idx++;

         PPath newPpath = *ppath;
         newPpath.Add(p);

         SortedList *sl = new SortedList;
         sl->ppathID = i; //ppath->ppathID;
         sl->suffixNode = getSuffixNode(*args->rpathTreeIdx, newPpath);
         sl->nondiscriminative = false;
         sls.push_back(sl);
      }
      std::make_heap(heap.begin(), heap.end(), compare);

      // Merge nodeIDLoader and scan
      bool allEnd=false;
      unsigned prev_value=subject.value, heap_value=0, heap_idx=0;

      std::vector<unsigned>::iterator iter, next;

      while (heap.size() > 0) {
         entity = heap.front();
         heap_value = entity.value;    

         while (subject.value != heap_value) {
            while (subject.value < heap_value) {
               rpflt_value.value = heap_value; // Hint
               if (!scan->next()) {
                  allEnd = true;
                  break;
               }
            }
            if (allEnd) break;

            while (subject.value > heap_value) {
               // pop from heap
               entity = heap.front();
               heap_idx = entity.idx;
               std::pop_heap(heap.begin(), heap.end(), compare);
               heap.pop_back();

               if (!scanEnd[heap_idx]) {
                  while (!scanEnd[heap_idx] && nodeIDs[heap_idx] < subject.value) {
                     if (!scans[heap_idx]->next()) {
                        scanEnd[heap_idx] = true;
                        break;
                     }
                     nodeIDs[heap_idx] = scans[heap_idx]->getValue1();
                  }

                  if (!scanEnd[heap_idx]) {
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
         assert(heap_value == subject.value);

         std::vector<unsigned> buf/* entity.idx*/;
         while (heap_value == heap.front().value) {
            entity = heap.front();
            pop_heap(heap.begin(), heap.end(), compare);
            heap.pop_back();
            heap_idx = entity.idx;
            buf.push_back(entity.idx);

            if (!scanEnd[heap_idx]) {
               if (!scans[heap_idx]->next()) scanEnd[heap_idx] = true;
               else {
                  nodeIDs[heap_idx] = scans[heap_idx]->getValue1();
                  entity.value = nodeIDs[heap_idx];
                  heap.push_back(entity); 
                  push_heap(heap.begin(), heap.end(), compare);
               }
            }
            if (heap.size() ==0) {
               allEnd = true;
               break;
            }
         }

         prev_value=subject.value;
         unsigned prev_object=~0u, buf_size=buf.size();

         while (subject.value == prev_value) {
            if (prev_object == object.value) {
               prev_value = subject.value;
               prev_object = object.value;
               if (!scan->next()) {
                  allEnd = true;
                  break;
               }
               continue;
            }

            // output
            for (unsigned j=0;j<buf_size;j++) {
               unsigned idx = buf[j];
               SortedList *sl = sls[idx];
               //unsigned size = sl->list.size();
               if (sl->nondiscriminative) continue;
               sl->list.insert(object.value);
            }
            prev_value = subject.value;
            prev_object = object.value;
            if (!scan->next()) {
               allEnd = true;
               break;
            }
         }
         if (allEnd) break;
      }
      for (unsigned i=0;i<size;i++) {
         if (scans[i] != NULL)
            delete scans[i];
         
         delete nodes[i]->rpathSgm;
         nodes[i]->rpathSgm=NULL;
      }
      free(scans);
      free(nodeIDs);
      free(scanEnd);

      unsigned cnt=0;
      for (unsigned i=0;i<sls.size();i++) {
         cnt += sls[i]->list.size();
      }

      if (cnt==0) {
         for (unsigned m=0;m<sls.size();m++) {
            SortedList* sl = sls[m];
            delete sl;
         }
         continue;
      }

      // Handle results
      unsigned byte = 0;
      pthread_mutex_lock(&mutex_lock); {
      for (unsigned m=0;m<sls.size();m++) {
         SortedList* sl = sls[m];
         unsigned size = sl->list.size();

         if (size == 0 || sl->nondiscriminative) {
            sl->list.clear();
            delete sl;
            continue;
         }

         PPath *newPpath = new PPath(p, *ppaths[sl->ppathID]);

         if (args->rpathTreeIdx->isDiscriminative(sl->suffixNode, newPpath->size(), size) &&
             size > args->minCnt/4.0f) {
            RPathTreeIndex::NodeIDLoader reader(sl->list);

            unsigned startPage, nextPage;
            args->rpathTreeIdx->packFullyNodeIDLeaves(reader, size, startPage, nextPage, byte);
            args->rpathTreeIdx->outputStat(*newPpath, startPage, nextPage, 
                  byte, size, NULL, NULL);

            if (newPpath->size() < args->maxL)
               args->ppaths_new->push_back(newPpath);
         }
         
         sl->list.clear();
         delete sl;
      }
      } pthread_mutex_unlock(&mutex_lock);
   }
   delete scan;
   return;
}
struct rb_sorter {
   unsigned p;
   unsigned cnt;
};
static bool sorterLessThan (struct rb_sorter sorter1, struct rb_sorter sorter2)
{
   return sorter1.cnt > sorter2.cnt;
}
//---------------------------------------------------------------------------
// BFS with n-way merge (multi thread, not grouping, in-memory merge&sort)
void rpathBuild(Database& db, vector<unsigned>& plist, unsigned maxL, RPathTreeIndex &rpathTreeIdx)
{
   unsigned startPage, nextPage, size, byte;
   std::vector<PPath*> ppathList;
   PPath nullPpath;

   std::cout << "Start building RPathFilter";
   std::cout << "the number of predicates: " << plist.size();

   std::cout << "building iteration: 1";
   map<unsigned, unsigned > nodeCnt;
   map<unsigned, unsigned > tCnt;
   vector<struct rb_sorter> sorterList;
   vector<unsigned> predList;
   pthread_mutex_init(&mutex_lock, NULL);
   pthread_mutex_init(&mutex_inppath_lock, NULL);

   for (std::vector<unsigned>::iterator iter=plist.begin(); iter!=plist.end(); iter++) {
      AggregatedIndexScan* scan;
      unsigned p = *iter, prevNodeID;
      Register subject,predicate,object;
      subject.reset(); predicate.reset(); object.reset();
      set<unsigned> results;
      unsigned cnt=0, ret;

      if (p == ALLPREDICATE)
         scan = AggregatedIndexScan::create(db,Database::Order_Object_Predicate_Subject,
                  NULL,false,&predicate,false,&object,false,0);
      else if (p == ALLPREDICATE+(REVERSE_PRED))
         scan = AggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,
                  &object,false,&predicate,false,NULL,false,0);
      else {
         predicate.value = (p>=(REVERSE_PRED))?p-(REVERSE_PRED):p;
         scan = (p>=(REVERSE_PRED))?
                  AggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,
                     &object,false,&predicate,true,NULL,false,0):
                  AggregatedIndexScan::create(db,Database::Order_Predicate_Object_Subject,
                     NULL,false,&predicate,true,&object,false,0);
      }

      cnt += scan->first();
      results.insert(object.value);
      prevNodeID=object.value;
      while ((ret = scan->next())) { 
         cnt+=ret;
         if (prevNodeID != object.value) {
            results.insert(object.value);
            assert(prevNodeID < object.value);
            prevNodeID = object.value;
         }
      }
      delete scan;

      cout << predicate.value << " " << p << " " << results.size() << endl;
      // Write
      RPathTreeIndex::NodeIDLoader reader(results);
      rpathTreeIdx.packFullyNodeIDLeaves(reader, size, startPage, nextPage, byte);
      nodeCnt[p] = size;
      tCnt[p]=cnt;

      // Output statistics
      PPath newPpath(p, nullPpath);
      rpathTreeIdx.outputStat(newPpath, startPage, nextPage, byte, size, NULL, NULL);

      if (!rpathTreeIdx.is_reverse && p>=REVERSE_PRED) {
         continue;
      }

      struct rb_sorter pred;
      pred.p=p;
      pred.cnt=cnt;
      sorterList.push_back(pred);
   }
   rpathTreeIdx.reopen();

   // determine th_mincnt and predicates to remove
   sort(sorterList.begin(), sorterList.end(), sorterLessThan);
   //std::cout << "skip cnt:" << sorterList.size() * 0.025;
   for (unsigned i=0,limit=sorterList.size(); i<limit; i++) {
      predList.push_back(sorterList[i].p);
      PPath* newPpath = new PPath(sorterList[i].p, nullPpath);
      ppathList.push_back(newPpath);
   }

   // check skip flag for iter-1
   map<unsigned, RPathTreeIndex::Node *> *nodeList = rpathTreeIdx.GetRootChildren();
   for (map<unsigned, RPathTreeIndex::Node *>::const_iterator iter=nodeList->begin(),
        limit=nodeList->end(); iter!=limit;++iter) {
      RPathTreeIndex::Node *node = (*iter).second;

      if (node->cardinality < rpathTreeIdx.getMinCnt(1))
         node->skip=true;
   }
   std::cout << " iteration:1. mincnt:" << rpathTreeIdx.getMinCnt(1);

   unsigned iteration=1;
   unsigned memory = atoi(getenv("MEM"));
   RemoveInfrequentArgs args[PARTITION]; 
   GetNodeLists3Args getnodelist_args[PARTITION]; 
   vector<pthread_t> tid(PARTITION);
   std::vector<PPath*> ppaths_new=ppathList;
   sort(predList.begin(), predList.end());
   do {
      std::vector<PPath*> ppaths = ppaths_new;
      ppaths_new.clear();
      iteration++;

      std::cout << "building iteration: " << iteration 
                << " mincnt:" << rpathTreeIdx.getMinCnt(iteration);
      double th1 = rpathTreeIdx.getMinCnt(iteration-1);
      double th2 = rpathTreeIdx.getMinCnt(iteration);

      for (std::vector<unsigned>::const_iterator iter=predList.begin(),
           limit=predList.end(); iter!=limit; iter++) {
         // Simultaneously merge M ppaths in queue
         unsigned p = *iter;
         std::cout << "- predicate: " << p;

         if (nodeCnt[p] < th1 || nodeCnt[p] < th2/4.0f) {
             //args->rpathTreeIdx->minTh * (iteration - 1)) {
            std::cout << "Infrequent pred:" << p << " size:" << nodeCnt[p] 
                      << " th1:" << th1 << " th2:" << th2/4.0f;
            continue;
         }

         vector<PPath*> remains;
         // Remove infrequent
         vector<PPath*> resultPPaths[PARTITION];
         for (unsigned n=0;n<PARTITION;n++) {
            args[n].inppaths = &ppaths;
            args[n].remains = &resultPPaths[n];
            args[n].rpathTreeIdx = &rpathTreeIdx;
            args[n].p = p;
            args[n].myIDX = n;
            args[n].minCnt = th2;
            if (pthread_create(&tid[n], NULL, (void *(*) (void*)) removeInfrequent, 
                     (void *) &args[n]) < 0) {
               perror("Error:");
               exit(0);
            }
         }
         void* status;
         for (unsigned n=0;n<PARTITION;n++) {
            pthread_join(tid[n], (void **) &status);
            remains.insert(remains.end(), resultPPaths[n].begin(), resultPPaths[n].end());
         }
         std::cout << "ppaths size:" << ppaths.size() << " remains size:" << remains.size();

         unsigned ppathnum;
         long long totalMemory = memory * 1024;
         long long expectedMemory = 
         ((long long)PARTITION * 
          ((long long)60 * (long long)nodeCnt[p] / (long long) (1024 * 1024)) +
          (long long) 2);
         std::cout << "predicate:" << p << " nodeCnt: " << nodeCnt[p]
                   << " totalMemory:" << totalMemory << " expectedMem:" << expectedMemory;
         ppathnum = totalMemory / expectedMemory;

         if (ppathnum > 10000) ppathnum = 10000;
         if (ppathnum > remains.size())
            ppathnum = remains.size() / PARTITION;
         if (ppathnum < 1) ppathnum = 1;
         std::cout << " - PPATH: " << ppathnum;

         ppath_idx = 0;
         for (unsigned n=0;n<PARTITION;n++) {
            getnodelist_args[n].db = &db;
            getnodelist_args[n].ppaths = &remains;
            getnodelist_args[n].rpathTreeIdx = &rpathTreeIdx;
            getnodelist_args[n].p = p;
            getnodelist_args[n].myIDX = n;
            getnodelist_args[n].ppaths_new = &ppaths_new;
            getnodelist_args[n].maxL = maxL;
            getnodelist_args[n].ppathnum = ppathnum;
            getnodelist_args[n].minCnt = th2;
            if (pthread_create(&tid[n], NULL, (void *(*) (void*)) getNodeLists, 
                     (void *) &getnodelist_args[n]) < 0) {
               perror("Error:");
               exit(0);
            }
         }

         for (unsigned n=0;n<PARTITION;n++) {
            pthread_join(tid[n], (void **) &status);
         }
      }
      for (std::vector<PPath*>::iterator iter=ppaths.begin(),
           limit=ppaths.end(); iter!=limit; iter++) {
         PPath *ppath = *iter;
         delete ppath;
      }
      rpathTreeIdx.reopen();
   } while (ppaths_new.size() > 0);

   rpathTreeIdx.close();
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Greeting
   cerr << "RPath Builder " << endl
        << "(c) 2011 Internet Database Laboratory. Web site: http://idb.snu.ac.kr" << endl;

   // Check the arguments
   if (argc<3 || !getenv("TH_MINCNT") || !getenv("TH_DIFF") ||
       !getenv("MEM") || !getenv("PARTITION")) {
      cerr << "usage: " << argv[0] << " <database> <maxL>" << endl;
      cerr << "ENV: TH_DIFF, TH_MINCNT, MEM(GB), PARTITION" << endl;
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
      cerr << "unable to make folder:" << DIR << endl;
   }
   if (chdir(DIR) != 0) {
      cerr << "unable to move to folder:" << DIR << endl;
   }

   std::cout << "Retrieving the predicate list..." << endl;

   sprintf(DIR, ".");
   RPathTreeIndex temp_rpathTreeIdx(argv[1], DIR, maxL, false);
   vector<unsigned> plist = temp_rpathTreeIdx.getPlist(db);
   rpathBuild(db, plist, maxL, temp_rpathTreeIdx);
}
