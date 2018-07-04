#ifndef H_rpath_RPathIndexScan
#define H_rpath_RPathIndexScan
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include "rts/database/Database.hpp"
#include "rpath/RPathSegment.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
class BufferManager;
//---------------------------------------------------------------------------
/// An index scan over the fully aggregated facts table
class RPathIndexScan : public Operator
{
   private:
   /// Hints during scanning
   class Hint : public RPathSegment::Scan::Hint {
      private:
      /// The scan
      RPathIndexScan& scan;

      public:
      /// Constructor
      Hint(RPathIndexScan& scan);
      /// Destructor
      ~Hint();
      /// The next hint
      void next(unsigned& value1);
   };
   friend class Hint;

   /// The registers for the different parts of the triple
   Register* value1;
   /// The different boundings
   bool bound1;
   /// The facts segment
   RPathSegment* seg;
   /// The scan
   RPathSegment::Scan scan;
   /// The hinting mechanism
   Hint hint;
   /// Merge hints
   std::vector<Register*> merge1;

   /// Constructor
   RPathIndexScan(BufferManager& rpathBufMgr, unsigned start, unsigned indexRoot, 
                  Register* value1,bool bound);

   // Implementations
   class Scan;
   class ScanPrefix1;

   public:
   /// Destructor
   ~RPathIndexScan();

   /// Produce the first tuple
   virtual unsigned first() = 0;
   /// Produce the next tuple
   virtual unsigned next() = 0;

   /// Print the operator tree. Debugging only.
   void print(DictionarySegment& dict,unsigned indent);
   /// Add a merge join hint
   void addMergeHint(Register* reg1,Register* reg2);
   /// Register parts of the tree that can be executed asynchronous
   void getAsyncInputCandidates(Scheduler& scheduler);

   /// Create a suitable operator
   static RPathIndexScan* create(BufferManager& rpathBufMgr, unsigned start, unsigned indexRoot,
                                 Register* nodeID,bool bound);
};
//---------------------------------------------------------------------------
#endif
