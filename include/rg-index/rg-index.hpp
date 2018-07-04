#ifndef H_rgindex
#define H_rgindex

#include <set>
#include <map>
#include <iostream>
#include <fstream>
#include "gspan/dfs.hpp"
#include "rg-index/inter_result.hpp"
#include "rpath/RPathSegment.hpp"

using namespace std;

class PredMap: public map<unsigned, unsigned> {

};

class RGindex {
public:
   struct Node {
      vector<unsigned> offsets;
      vector<unsigned> blks;
      vector<unsigned> cardinalities;
      vector<RPathSegment *> segments;
      std::map<GSPAN::DFS, Node *> children;
   };

private:
   unsigned minSup, maxL;
   Database *db;
   map<unsigned, unsigned> suppMap;

   void subgraphMining(unsigned maxL, GSPAN::DFSCode dfscode, InterResultTuple &results);
   void getPossiblePredicates(hash_tbl_t *hash_tbl, set<unsigned> &forward_preds,
                              set<unsigned> &backward_preds, unsigned size);
   // file output
   ofstream ofs_data, ofs_stat;
   unsigned page;
   void materializeVlist(VertexLoader& reader, unsigned &size,
                         unsigned &startPage, unsigned &byte);

   char dataName[256], statName[256]; /* buffers for the file name */
   void InsertIntoTree(Node* root, GSPAN::DFSCode dfscode, unsigned level, Node* newNode);
   void close();
   Pool<table_t> pool;

public:
   PredMap predMap_new, predMap_old;
   Node root;
   MemoryMappedFile file;
   /* open for building */
   RGindex(char *dataset, char *path);
   /* open for querying */
   RGindex(char *dataset, char *path, unsigned maxL);
   /* insert the graph corresponding to the dfscode into RG-index 
      and materialize the Vlists */
   void insert(GSPAN::DFSCode &dfscode, InterResultTuple &results);
   void build(Database &db, unsigned maxL, unsigned minSup);
   void getDataFileName(char *buf, char* path, char *dataset);
   void getStatFileName(char *buf, char* path, char *dataset);
   RGindex::Node* SearchNode(Node& root, GSPAN::DFSCode& dfscode, unsigned level);
};

#endif
