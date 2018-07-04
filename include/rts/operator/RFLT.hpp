#ifndef H_rts_operator_RFLT
#define H_rts_operator_RFLT

#include "rts/operator/Operator.hpp"
#include "rpath/RPathTreeIndex.hpp"
#include <vector>
#include "rts/runtime/Runtime.hpp"
#include "rg-index/rg-index.hpp"
#include "gspan/dfs.hpp"

class RPathFilter;

class RFLT : public Operator
{
   std::vector<RGindex::Node*>* fltInfos;
   Operator* input;
   Register* inputValue;
   RPathFilter *rpathFLT;
   Register* fltValue;
   unsigned count;

   public:
   RFLT(Operator* input, std::vector<RGindex::Node*>* fltInfos, 
        std::vector<unsigned> nodeIds, std::vector<GSPAN::DFSCode> &dfscodes,
        Register* const& inputValue, double expectedOutputCardinality);
   ~RFLT();
   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(PlanPrinter& out);
   /// Add a merge join hint
   void addMergeHint(Register* reg1,Register* reg2);
   /// Register parts of the tree that can be executed asynchronous
   void getAsyncInputCandidates(Scheduler& scheduler);
   void getStat(unsigned &final,unsigned &intermediate);
};

#endif
