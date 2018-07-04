#ifndef H_rpath_RPathQueryGraph
#define H_rpath_RPathQueryGraph
#include <vector>
#include "rpath/PPath.hpp"
#include "rpath/RPathTreeIndex.hpp"

class QueryGraph;

class RPathQueryGraph
{
   // incoming edge
   typedef struct {unsigned predicate; unsigned subject; bool constSubject;} Edge;
   std::vector<Edge>* edges;
   unsigned size;

   public:
   RPathQueryGraph(QueryGraph& qg);
   ~RPathQueryGraph();
//   std::vector<RPathTreeIndex::Node*> getCandidateFilterSet(unsigned nodeID, unsigned maxL);
   std::vector<PPath> getCandidateFilterSetPPath(unsigned nodeID, unsigned maxL);
   void traverseGraph(std::vector<PPath>& cfs, PPath &pp, unsigned nodeID, unsigned maxL, unsigned depth);
   void printCFS(unsigned maxL);
};
#endif
