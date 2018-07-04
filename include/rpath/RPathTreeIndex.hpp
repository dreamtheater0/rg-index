#ifndef H_rpath_RPathTreeIndex
#define H_rpath_RPathTreeIndex

#include <set>
#include <map>
#include <vector>
#include <iostream>
#include <fstream>
#include "infra/osdep/MemoryMappedFile.hpp"
#include "rpath/PPath.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "rpath/RPathSegment.hpp"
#include <pthread.h>

#define REVERSE_PRED (100000000)
#define ALLPREDICATE (50000000)

using namespace std;

class RPathSegment;
class BufferManager;

struct Triple_s {
   unsigned s, p, o;
};
typedef struct Triples_s Triple;

class RPathUpdateInfo {
   public:
   map<unsigned, vector<struct Triple_s> *> triples_pso;
   map<unsigned, vector<struct Triple_s> *> triples_pos;

   vector<unsigned> objs;
   vector<unsigned> subjs;

   set<unsigned> preds;
};

class RPathTreeIndex {
   public:
   class NodeIDLoader {
      protected:
      bool start;
      RPathSegment::Scan scan;
      set<unsigned>::iterator iter;
      set<unsigned>::iterator limit;

      public:
      /// Constructor
      NodeIDLoader(set<unsigned> &results) { 
         iter = results.begin();
         limit = results.end();
      }

      bool next(unsigned& nodeID) {
         if (iter == limit)
            return false;

         nodeID = (*iter);
         iter++;
         return true;
      }
   };
   
   struct NodeUpdateInfo {
      bool isUpdated;
      std::vector<unsigned> insertDelta;
      bool hasDeleted;
   };

   struct Node {
      unsigned predicate;
      unsigned startPage;
      unsigned startIndexPage;
      unsigned cardinality;
      unsigned len;
      bool skip;

      RPathSegment *rpathSgm;

      std::map<unsigned, Node *> *children;
      NodeUpdateInfo *updateInfo;
   };

   ~RPathTreeIndex();
   RPathTreeIndex(char *dataset, char *path, unsigned maxL);
   RPathTreeIndex(char *dataset, char *path, unsigned maxL, bool open);
   unsigned GetStartPage(PPath& pp);
   unsigned GetSelectivity(PPath& pp);
   unsigned GetCost(PPath& pp);
   Node* SearchNode(PPath& ppath);
   unsigned FindCardinality(unsigned predicate);
   std::map<unsigned, Node *> *GetRootChildren() {return root.children;};
   Node* InsertIntoIdx(unsigned startPage, unsigned startIdxPage, 
                       unsigned cardinality, unsigned byte, const char *ppathStr);
   void reopen();
   std::vector<Node *> nodes;
   void Print();

   static void getRPFileName(char *fileName, char *path, char *dataset, unsigned maxL);
   static void getRPStatFileName(char *fileName, char *path, char *dataset, unsigned maxL);

   void rpathBuild(Database& db, unsigned maxL);
   Node* getSuffixNode(PPath &ppath); 
   Node* outputStat(PPath &ppath, unsigned startPage, unsigned startIndex, unsigned byte, 
                    unsigned size, PPath *parentPpath, RPathTreeIndex::Node* curNode);
   //void getNodeLists(Database& db, unsigned p, PPath& ppath, set<unsigned> &results);
   void getNodeLists(Database& db, unsigned p, RPathTreeIndex::Node* node, 
                     PPath& ppath, set<unsigned> &results);
   vector<unsigned> getPlist(Database& db);
   void rpathUpdate(Database& db, unsigned maxL, RPathUpdateInfo& inserted, RPathUpdateInfo& deleted);
   void computeDelta(RPathTreeIndex::Node* curNode, set<unsigned> results, RPathSegment* oldSgm);
   void getNodeListsFromDelta(Database& db, unsigned p, vector<unsigned> delta, 
         RPathTreeIndex::Node *curNode, set<unsigned> &results);
   void getNodeListsFromInserted(unsigned p, PPath& ppath, RPathTreeIndex::Node *node, 
         RPathTreeIndex::Node *curNode, vector<struct Triple_s> *insertedTriples, set<unsigned> &results);
   bool isDiscriminative(RPathTreeIndex::Node* suffixNode, unsigned len, unsigned size);
   bool skipP;
   set<unsigned> skipPreds;

   private:
   void SetParams();
   char fileName[256], statName[256];
   Node root;
   void InsertIntoTree(Node* root, PPath& ppath, unsigned level, Node* newNode);
   Node* SearchNode(Node& root, PPath& ppath, unsigned level);

   void Print(Node& root, unsigned level);
   void NodePrint(Node& node);

   // update
   unsigned lastPage;

   // file output
   ofstream ofs_rf, ofs_stat;
   public:
   MemoryMappedFile file;
   void packFullyNodeIDLeaves(NodeIDLoader& reader, unsigned &size, 
                              unsigned &startPage, unsigned &nextPage, unsigned &byte);
   void close();
   unsigned page;

   // parameters
   unsigned th_mincnt;
   double th_diff;
   bool is_reverse;
   bool is_varp;
   unsigned vlist_cnt;
   unsigned maxL;
   unsigned minTh;

   double getMinCnt(unsigned length);
};

struct CFSEntry_s {
   std::vector<RPathTreeIndex::Node*> nodes;
   double cardinality;
   double costs;
};
typedef struct CFSEntry_s CFSEntry;
#endif
