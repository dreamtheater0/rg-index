#include "rpath/RPathIndexScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
#include <cassert>
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
/// Implementation
class RPathIndexScan::Scan : public RPathIndexScan {
   public:
   /// Constructor
   Scan(BufferManager& rpathBufMgr, unsigned start, unsigned indexRoot, Register* value1,bool bound) 
      : RPathIndexScan(rpathBufMgr, start, indexRoot, value1, bound) {}

   /// First tuple
   unsigned first();
   /// Next tuple
   unsigned next();
};
//---------------------------------------------------------------------------
/// Implementation
class RPathIndexScan::ScanPrefix1 : public RPathIndexScan {
   private:
   /// The stop condition
   unsigned stop1;

   public:
   /// Constructor
   ScanPrefix1(BufferManager& rpathBufMgr, unsigned start, unsigned indexRoot, 
               Register* value1,bool bound) 
      : RPathIndexScan(rpathBufMgr, start, indexRoot, value1, bound) {}

   /// First tuple
   unsigned first();
   /// Next tuple
   unsigned next();
};
//---------------------------------------------------------------------------
RPathIndexScan::Hint::Hint(RPathIndexScan& scan)
   : scan(scan)
   // Constructor
{
}
//---------------------------------------------------------------------------
RPathIndexScan::Hint::~Hint()
   // Destructor
{
}
//---------------------------------------------------------------------------
void RPathIndexScan::Hint::next(unsigned& value1)
   // Scanning hint
{
   // First value
   if (scan.bound1) {
      unsigned v=scan.value1->value;
      if ((~v)&&(v>value1)) {
         value1=v;
      }
   }
   for (std::vector<Register*>::const_iterator iter=scan.merge1.begin(),limit=scan.merge1.end();iter!=limit;++iter) {
      unsigned v=(*iter)->value;
      if ((~v)&&(v>value1)) {
         value1=v;
      }
   }
   if (scan.value1->domain) {
      unsigned v=scan.value1->domain->nextCandidate(value1);
      if (v>value1) {
         value1=v;
      }
   }
}
//---------------------------------------------------------------------------
RPathIndexScan::RPathIndexScan(BufferManager& rpathBufMgr, unsigned start, unsigned indexRoot,
                               Register* value1,bool bound1)
   : value1(value1),bound1(bound1),scan(&hint),hint(*this)
   // Constructor
{
   seg  = new RPathSegment(rpathBufMgr, start, indexRoot, indexRoot-start);
}
//---------------------------------------------------------------------------
RPathIndexScan::~RPathIndexScan()
   // Destructor
{
}
//---------------------------------------------------------------------------
void RPathIndexScan::print(DictionarySegment&,unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<RPathIndexScan ";
   indent(level); std::cout << ">" << std::endl;
}
//---------------------------------------------------------------------------
static void handleHints(Register* reg1,Register* reg2,Register* result,std::vector<Register*>& merges)
   // Add hints
{
   bool has1=false,has2=false;
   for (std::vector<Register*>::const_iterator iter=merges.begin(),limit=merges.end();iter!=limit;++iter) {
      if ((*iter)==reg1) has1=true;
      if ((*iter)==reg2) has2=true;
   }
   if (reg1==result) has1=true;
   if (reg2==result) has2=true;

   if (has1&&(!has2)) merges.push_back(reg2);
   if (has2&&(!has1)) merges.push_back(reg1);
}
//---------------------------------------------------------------------------
void RPathIndexScan::addMergeHint(Register* reg1,Register* reg2)
   // Add a merge join hint
{
   handleHints(reg1,reg2,value1,merge1);
}
//---------------------------------------------------------------------------
void RPathIndexScan::getAsyncInputCandidates(Scheduler& /*scheduler*/)
   // Register parts of the tree that can be executed asynchronous
{
}
//---------------------------------------------------------------------------
RPathIndexScan* RPathIndexScan::create(BufferManager& rpathBufMgr, unsigned start, unsigned indexRoot,
                                       Register* nodeID,bool bound)
   // Constructor
{
   // Construct the proper operator
   RPathIndexScan* result;
   if (!bound) {
      result=new Scan(rpathBufMgr,start,indexRoot,nodeID,bound);
   } else {
      result=new ScanPrefix1(rpathBufMgr,start,indexRoot,nodeID,bound);
   }

   return result;
}
//---------------------------------------------------------------------------
unsigned RPathIndexScan::Scan::first()
   // Produce the first tuple
{
   if (!scan.first(*seg))
      return false;
   value1->value=scan.getValue1();
   return 1;
}
//---------------------------------------------------------------------------
unsigned RPathIndexScan::Scan::next()
   // Produce the next tuple
{
   if (!scan.next())
      return false;
   value1->value=scan.getValue1();
   return 1;
}
//---------------------------------------------------------------------------
unsigned RPathIndexScan::ScanPrefix1::first()
   // Produce the first tuple
{
   stop1=value1->value;
   if (!scan.first(*seg,stop1))
      return false;
   if (scan.getValue1()>stop1)
      return false;
   return 1;
}
//---------------------------------------------------------------------------
unsigned RPathIndexScan::ScanPrefix1::next()
   // Produce the next tuple
{
   if (!scan.next())
      return false;
   if (scan.getValue1()>stop1)
      return false;
   return 1;
}
//---------------------------------------------------------------------------
