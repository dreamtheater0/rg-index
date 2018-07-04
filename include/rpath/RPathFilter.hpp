#ifndef H_rpath_RPathFilter
#define H_rpath_RPathFilter

#include <vector>
#include "rpath/RPathTreeIndex.hpp"
#include "RPathSegment.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rg-index/rg-index.hpp"
#include "gspan/dfs.hpp"

class RPathFilter 
{
   std::vector<RPathSegment *> rpathSgmts;
   std::vector<GSPAN::DFSCode> dfscodes;
   std::vector<double> cards;
   std::vector<unsigned> sizes;
   std::vector<unsigned> nodeIDs;
   unsigned *values;//, curVal;
   unsigned size;
   RPathSegment::Scan *sgmtScans;
   bool GetNextValue(unsigned value);

   public:
   RPathFilter(std::vector<RGindex::Node*>* fltInfo, 
               std::vector<unsigned> &nodeIds,
               std::vector<GSPAN::DFSCode> &dfscodes);
   ~RPathFilter();
   bool CheckFilter(unsigned value);
   bool first();
   void print(PlanPrinter& out);
   bool next(unsigned value);
   Register fltValue;
//   unsigned GetCurVal() {return curVal;}
};

#endif
