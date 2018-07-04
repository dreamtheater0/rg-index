#ifndef H_rts_operator_RFLT_M
#define H_rts_operator_RFLT_M

#include "rts/operator/Operator.hpp"
#include "rpath/RPathTreeIndex.hpp"
#include "rg-index/rg-index.hpp"
#include "gspan/dfs.hpp"
#include <vector>

class RPathFilter;

class RFLT_M : public Operator
{
   /// The input
   std::vector<Operator*> childOp;
   /// The join attributes
   std::vector<Register*> childValue;
   /// The non-join attributes
   std::vector<std::vector<Register*>*> childTail;
   std::vector<unsigned> childTailSize;

   unsigned **values;

   /// Buffers for cartesian product
   unsigned **buf_mat;
   unsigned *buf_tail_offset;
   unsigned *buf_next_offset;
   unsigned *buf_max_size;
   unsigned buf_width;

   unsigned *buf_size;
   unsigned *buf_offset;

   /// for iteration
   unsigned sorted_idx_size;

   /// Buffers for next values
   unsigned **shadowValue;
   unsigned *cnts;
   unsigned resultCnt;
   unsigned *currentCnts;

   /// Possible states while scanning the input
   enum ScanState {
      empty, GenerateResult
    };

   /// The current scan state
   ScanState scanState;

   unsigned numOfChildren;
   int *childStatus;

   /// Filter info
   std::vector<RPathTreeIndex::Node*>* fltInfos;
   Register* inputValue;
   RPathFilter *rpathFLT;
   unsigned* fltValue;
   unsigned joinedValue;

   bool hasPrimaryChild;
   unsigned primaryChild;
   unsigned primaryChildrowCnt;

   int generateResults();
   bool isEnd;

   public:
   RFLT_M(std::vector<RGindex::Node*>* fltInfos,
          std::vector<unsigned> &nodeIds, 
          std::vector<GSPAN::DFSCode> &dfscodes_i,
          double expectedOutputCardinality);
   ~RFLT_M();
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

   void addChild(Operator *child, Register *value, std::vector<Register*> *tail);
   void pushIntoBuff(int idx);
   void truncateAllBuff();
   void initBuff();
   void setValues(int idx);
   unsigned getNextMergedValue(unsigned fltNextVal, unsigned &joinedNextVal);
   void buffer();
   void swapChild(unsigned idx);
   void copyChild(unsigned idx);
   void copyFromShadow(unsigned idx);

   class Sorter {
      public:
      Sorter (int idx, int size);
      ~Sorter ();
      int   idx;
      int   size;
   };
   std::vector <Sorter *> sorted_idx;
   std::vector<Sorter> sorters;
   void getStat(unsigned &final,unsigned &intermediate);

   unsigned triplecnt;
};

#endif
