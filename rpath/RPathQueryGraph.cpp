#include "rpath/RPathQueryGraph.hpp"
#include "rpath/RPathTreeIndex.hpp"
#include "cts/infra/QueryGraph.hpp"
#include <cassert>
#include <iostream>
#include <stdlib.h>
#include <math.h>

using namespace std;

RPathQueryGraph::RPathQueryGraph(QueryGraph& qg)
{
   QueryGraph::SubQuery sq = qg.getQuery();
   unsigned maxNodeID = 0;

   // get maximum node ID
   for (vector<QueryGraph::Node>::const_iterator iter=sq.nodes.begin(),limit=sq.nodes.end();
        iter!=limit;++iter) {
      QueryGraph::Node n = *iter;
      if (!(!n.constSubject && !n.constObject))
         continue;
      if (n.subject > maxNodeID) maxNodeID = n.subject;
      if (n.object > maxNodeID) maxNodeID = n.object;
   }

   size = maxNodeID+1;
   edges = new vector<Edge>[size];

   // 각 노드 별 incoming edge를 만든다
   // edges[nodeID]는 nodeID의 incoming edges
   for (vector<QueryGraph::Node>::const_iterator iter=sq.nodes.begin(),limit=sq.nodes.end();
        iter!=limit;++iter) {
      QueryGraph::Node n = *iter;
      Edge edge;

      // constant object에 대해서는 incoming edge를 만들지 않는다
      if (!n.constObject) {
         if (n.constPredicate) 
            edge.predicate = n.predicate;
         else {
            if (!(getenv("VARP") && atoi(getenv("VARP")) == 1)) {
               continue;
            }
            edge.predicate = ALLPREDICATE;
         }

         if (n.constSubject) 
            edge.constSubject = true;
         else
            edge.constSubject = false;
         edge.subject = n.subject;
         edges[n.object].push_back(edge);
      }

      if (getenv("REVERSE") && atoi(getenv("REVERSE")) == 1) {
         // reverse predicate 추가
         if (n.constSubject)
            continue;
         if (n.constObject) 
            edge.constSubject = true;
         else
            edge.constSubject = false;
         edge.predicate = n.predicate + (REVERSE_PRED);
         edge.subject = n.object;
         edges[n.subject].push_back(edge);
      }
   }

   qg.rpathQueryGraph = this;
}

RPathQueryGraph::~RPathQueryGraph()
{
}

vector<PPath> RPathQueryGraph::getCandidateFilterSetPPath(unsigned nodeID, unsigned maxL)
{
   vector<PPath> cfsPPath;
   PPath pp;

   traverseGraph(cfsPPath, pp, nodeID, maxL, 1);

   return cfsPPath;
}

#if 0
vector<RPathTreeIndex::Node*> RPathQueryGraph::getCandidateFilterSet(unsigned nodeID, unsigned maxL)
{
   vector<RPathTreeIndex::Node*> cfsNode;
   vector<PPath> cfsPPath=getCandidateFilterSetPPath(nodeID,maxL);

   for(vector<PPath>::iterator iter=cfsPPath.begin(),limit=cfsPPath.end();
       iter!=limit;++iter) {
      cfsNode.push_back(rpathTreeIdx->SearchNode(*iter));
   }
   
   return cfsNode;
}
#endif

void RPathQueryGraph::traverseGraph(vector<PPath>& cfs, PPath &pp, unsigned nodeID, unsigned maxL, unsigned depth)
{
   vector<Edge> edge = edges[nodeID];
   if (maxL < depth) return;

   for (vector<Edge>::const_iterator iter=edge.begin(),limit=edge.end();
        iter!=limit;++iter) {
      RPathQueryGraph::Edge e = *iter;

      if (getenv("REVERSE") && atoi(getenv("REVERSE")) == 1 && pp.size() > 0) {
         // 너무 많은 ppath 생성을 막기 위해 p, pR 형태 제거
         unsigned p = pp.path[0];
         //cout << p << ", " << e.predicate << " " << abs((long long) (p - e.predicate)) << endl;
         if (abs((long long) (p - e.predicate)) == (REVERSE_PRED))
            continue;
      }

      PPath newpp(pp, e.predicate);
      cfs.push_back(newpp);
      if (!e.constSubject)
         traverseGraph(cfs, newpp, e.subject, maxL, depth+1);
   }
}

void RPathQueryGraph::printCFS(unsigned maxL)
{
   cout << "RPath CFS" << endl;
   for (unsigned i = 0; i < size; i++) {
      vector<Edge> *edge = &edges[i];
      cout << "[" << i << "]";
      if (edge->size() > 0) {
         vector<PPath> cfs = getCandidateFilterSetPPath(i, maxL);
         for(vector<PPath>::const_iterator iter=cfs.begin(),limit=cfs.end();iter!=limit;++iter) {
            PPath ppath = *iter;
            cout << "<";
            ppath.print();
            cout << "> ";
         }
      }
      cout << endl;
   }
}

