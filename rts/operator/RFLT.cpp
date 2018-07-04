#include "rts/operator/RFLT.hpp"
#include "rpath/RPathFilter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include <iostream>
#include <stdlib.h>

RFLT::RFLT(Operator* input, std::vector<RGindex::Node*>* fltInfos, 
           std::vector<unsigned> nodeIds, std::vector<GSPAN::DFSCode> &dfscodes,
           Register* const& inputValue, double expectedOutputCardinality) 
   : Operator(expectedOutputCardinality), fltInfos(fltInfos), input(input), inputValue(inputValue)
{
   if (getenv("OFFRPATH")) {
      rpathFLT = NULL;
      return;
   }

   rpathFLT = new RPathFilter(fltInfos, nodeIds, dfscodes);
   fltValue = &rpathFLT->fltValue;
   input->addMergeHint(fltValue,inputValue);
}

RFLT::~RFLT()
{
   delete input;
}

unsigned RFLT::first()
{
   if (!rpathFLT) {
      count = input->first();
      observedOutputCardinality+=count;
      return count;
   }

   if (!rpathFLT->first())
      return 0;

   count=input->first();
   if (count == 0)
      return 0;

   while(true) {
      if (fltValue->value < inputValue->value) {
         if(!rpathFLT->next(inputValue->value))
            return 0;
      }
      if (fltValue->value > inputValue->value) {
         count = input->next();
         if (count == 0)
            return 0;
      }
      if (fltValue->value == inputValue->value) {
         observedOutputCardinality+=count;
         return count;
      }
   }
}

unsigned RFLT::next()
{
   if (!rpathFLT) {
      count = input->next();
      observedOutputCardinality+=count;
      return count;
   }

   count = input->next();
   if (count == 0)
      return 0;

   while(true) {
      if (fltValue->value < inputValue->value) {
         if(!rpathFLT->next(inputValue->value))
            return 0;
      }
      if (fltValue->value > inputValue->value) {
         count = input->next();
         if (count == 0)
            return 0;
      }
      if (fltValue->value == inputValue->value) {
         observedOutputCardinality+=count;
         return count;
      }
   }

   return 0;
}

void RFLT::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   (void)fltInfos;
   out.beginOperator("RFLT",expectedOutputCardinality,observedOutputCardinality);
   if (rpathFLT) rpathFLT->print(out);
   input->print(out);
   out.endOperator();
}
void RFLT::addMergeHint(Register* reg1,Register* reg2)
{
   input->addMergeHint(reg1,reg2);
}
void RFLT::getAsyncInputCandidates(Scheduler& scheduler)
{
   input->getAsyncInputCandidates(scheduler);
}
void RFLT::getStat(unsigned &final,unsigned &intermediate)
{
   input->getStat(final, intermediate);
//   intermediate+=observedOutputCardinality; 
}
//---------------------------------------------------------------------------
