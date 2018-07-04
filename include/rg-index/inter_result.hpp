#ifndef H_inter_result
#define H_inter_result

#include <set>
#include <map>
#include <vector>
#include <algorithm>
#include <cassert>
#include "infra/util/Pool.hpp"
#include "gspan/dfs.hpp"
#include "rts/runtime/Runtime.hpp"

using namespace std;

class Vlist : public set<unsigned> {
   set<unsigned>::iterator iter;

public:
   Vlist() {
      vertexsize=0;
   }
   unsigned vertexsize;
   unsigned offset;
   unsigned first(unsigned &vID) {
      iter = begin(); 
      if (iter == end()) return 0; 

      vID= *iter;
      iter++;
      return 1;
   };
   unsigned next(unsigned &vID)  {
      if (iter == end()) return 0; 
      vID= *iter;
      iter++; 
      return 1;
   }
};

typedef enum {NOTINIT, ONE, SET, REF} ElistVlist_t;
class ElistVlist {

public: 
   ElistVlist();
   ElistVlist(ElistVlist *elistvlist);
   ~ElistVlist();
   ElistVlist_t type;
   union {
      unsigned vertex;
      set<unsigned> *vertices;
   };
   inline void insert (unsigned i_vertex);
   inline unsigned first(unsigned &v, set<unsigned>::iterator &iter);
   inline unsigned next(unsigned &v, set<unsigned>::iterator &iter);
};

class Elist : public map<unsigned, ElistVlist> {
   Elist::iterator iter;
public:
   ~Elist() {
   }
   unsigned offset;
   unsigned first(unsigned &vID) {
      iter = begin(); 
      if (iter == end()) return 0; 

      vID= (*iter).first;
      iter++;
      return 1;
   };
   unsigned next(unsigned &vID)  {
      if (iter == end()) return 0; 
      vID= (*iter).first;
      iter++; 
      return 1;
   }
};

class VertexLoader {
   protected:
      bool start;
      bool vlist;
      set<unsigned>::iterator iter;
      set<unsigned>::iterator limit;

      map<unsigned, ElistVlist>::iterator iter2;
      map<unsigned, ElistVlist>::iterator limit2;

   public:
      /// Constructor
      VertexLoader(Vlist* results) { 
         vlist=true;
         iter = results->begin();
         limit = results->end();
      }
      VertexLoader(Elist* results) { 
         vlist=false;
         iter2 = results->begin();
         limit2 = results->end();
      }

      bool next(unsigned& nodeID) {
         if (vlist) {
            if (iter == limit)
               return false;

            nodeID = (*iter);
            iter++;
            return true;
         }
         else {
            if (iter2 == limit2)
               return false;

            nodeID = (*iter2).first;
            iter2++;
            return true;
         }
      }
};

class Vertex {
public:
   // the neighbors are sorted by the size
   unsigned idx;
   vector<unsigned> neighbors; 
   unsigned size;
   bool terminal;
   Vertex() {
      terminal=true;
      size=0;
      idx=0;
   }
};

class InterResult {
   /**
    * In InterResult, the graph is considered to be a undirected acyclic graph
    */
   vector<Vertex> vertices; // in the interresult graph
   /** 
    * There could be more registers than Vlists when backward edges exist.
    * Therefore, we need mapping information between registers and Vlists
    */ 
   map< pair<unsigned, unsigned>, Elist * > elistmap; // edge --> elist
   /**
    * mapping between original graph and interresult graph 
    * one to many (original graph to interresult graph)
    */
   map<unsigned, vector<unsigned> * > mapFromOriginalToInterResult;
   map<unsigned, unsigned> mapFromInterResultToOriginal;

   inline void findEdges(set<unsigned> &source, unsigned vertexID, InterResult &old_result,
                         unsigned prevVertexID, bool makeElist, bool copyElist);
   void makeBackEdgeList(unsigned v1, unsigned v2);

   map<unsigned, Elist *> map_Elist_Vlist; // Which Elist can we make a Vlist?

   set<Elist*> copiedElist;
   set<Vlist*> copiedVlist;
public:
   InterResult () {};
   ~InterResult ();
   InterResult (InterResult &old_results);
   unsigned support;

   vector<Vlist*> results; // Vlists in the original graph

   bool addForwardEdge(Database &db, unsigned source, unsigned newvertex,
                       InterResult &old_result, unsigned pred, bool reverse,
                       vector<unsigned> &rmvertices, bool makeElist, unsigned minSup);
   Vlist* getVlist(unsigned idx) {
      return results[idx];
   }
   Elist* getElist(unsigned idx);
   void initWithOneSizeGraph(Database &db, GSPAN::DFSCode& dfscode, 
                             unsigned p, unsigned old_p);
   void insertMapping(unsigned original, unsigned interresult);
   vector<unsigned>* getInterResultIDs(unsigned original) {
      return mapFromOriginalToInterResult[original];
   };
   unsigned getOriginalID(unsigned interresult) {
      return mapFromInterResultToOriginal[interresult];
   };
   unsigned getSupport();
   void materializeVlists(ofstream &ofs_data, InterResult* old_result,
         unsigned &page, vector<unsigned> &offsets);
   void materializeVlist(VertexLoader& reader, unsigned &size,
         unsigned &page /*start*/, unsigned &byte, ofstream &ofs_data);
};

class VertexNode {
   bool isMatched;
   unsigned matchID;
   unsigned neighborIdx;
   vector<unsigned> adjVerxIdxs;
   vector<VertexNode> adjVerxNodes;
   vector<vector <unsigned> *> verxlists; // vertex lists for each neighbor

public:
   bool getNext(unsigned source);
};

class Tuple {
   public:
      unsigned value;
      union {
         unsigned v1;
         Tuple *prev_ptr;
      } prev;

      Tuple () {};
      void set(unsigned i_v1, unsigned i_v2) {
         value=i_v2;
         prev.v1=i_v1;
      }
      void set(Tuple *tuple, unsigned v) {
         prev.prev_ptr=tuple;
         value=v;
      }

      inline unsigned getColumn(unsigned get_idx, Tuple **prevTuple) {
         if (get_idx>0) {
            if (get_idx>1) 
               *prevTuple=prev.prev_ptr;
            return value;
         }
         return prev.v1;
      }
      inline unsigned getColumn_i(unsigned get_idx, unsigned size) {
         Tuple *tuple=this;
         if (get_idx==0) {
            for(unsigned i=0;i<size-2; i++) {
               tuple=tuple->prev.prev_ptr;
            }
            return tuple->prev.v1;
         }
         for(unsigned i=0;i<size-get_idx-1; i++) {
            tuple=tuple->prev.prev_ptr;
         }
         return tuple->value;
      }
};

typedef vector <unsigned> tuple_t;
typedef vector <Tuple *> table_t;
typedef map <unsigned, table_t *> hash_tbl_t;

class VertexTupleLoader {
   protected:
     hash_tbl_t::iterator iter, limit;

   public:
      /// Constructor
      VertexTupleLoader(hash_tbl_t *hash_tbl) {
         iter = hash_tbl->begin();
         limit = hash_tbl->end();
      }

      bool next(unsigned& nodeID) {
         if (iter==limit)
            return false;

         nodeID = (*iter).first;
         iter++;
         return true;
      }
};

class InterResultTuple {
public:
   unsigned tuple_size;
   bool backward;
   table_t results; // a set of tuples
   vector<unsigned> offsets;
   vector<unsigned> blks;
   vector<unsigned> cards;
   vector<Vlist> vlists;
   Pool<Tuple> pool;

   InterResultTuple *old_result;
   unsigned getResultCount();

   InterResultTuple (InterResultTuple *i_old_result);
   ~InterResultTuple ();
   void clear();
   unsigned support;

   void addBackwardEdge(InterResultTuple &foreward_results, unsigned v1, unsigned v2);
   bool addForwardEdge(Database &db, hash_tbl_t &hash_tbl, unsigned pred, bool reverse);
   void initWithOneSizeGraph(Database &db, GSPAN::DFSCode& dfscode, unsigned p, unsigned old_p);
   void insertMapping(unsigned original, unsigned interresult);
   unsigned getSupport();
   void materializeVlists(ofstream &ofs_data, unsigned &page);
   void materializeVlist(VertexLoader& reader, unsigned &size,
                         unsigned &page, unsigned &byte, ofstream &ofs_data);
};
#endif
