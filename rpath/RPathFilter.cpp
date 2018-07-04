#include "rpath/RPathFilter.hpp"
#include <stdio.h>
#include <iostream>
#include <cassert>
#include <sstream>
#include "rg-index/rg-index.hpp"

RPathFilter::RPathFilter(std::vector<RGindex::Node*>* fltInfos,
                         std::vector<unsigned> &nodeIds,
                         std::vector<GSPAN::DFSCode> &dfscodes_i)
{
   size=fltInfos->size();
   values = new unsigned[size];
   sgmtScans = new RPathSegment::Scan[size];
   for(unsigned i=0,limit=size;i<limit;i++) {
      RGindex::Node *fltInfo=(*fltInfos)[i];
      rpathSgmts.push_back(fltInfo->segments[nodeIds[i]]);
      assert(fltInfo->segments[nodeIds[i]]);
      cards.push_back(fltInfo->cardinalities[nodeIds[i]]);
      sizes.push_back(fltInfo->blks[nodeIds[i]]);
   }
   fltValue.reset();
   dfscodes=dfscodes_i;
   nodeIDs=nodeIds;
}

RPathFilter::~RPathFilter()
{
}

bool RPathFilter::first()
{
   bool merged = true;
   unsigned maxValue = 0;

   for(unsigned i = 0,limit = size;i < limit;i++) {
      if (!sgmtScans[i].first(*rpathSgmts[i]))
         return false;
      values[i] = sgmtScans[i].getValue1();

      if (i > 0 && maxValue != values[i]) {
         merged = false;
      }

      if (maxValue < values[i])
         maxValue = values[i];
   }

   if (merged) {
      //curVal = maxValue;
      fltValue.value = maxValue;
      return true;
   }

   return GetNextValue(maxValue);
}

inline bool RPathFilter::GetNextValue(unsigned value)
{
   bool merged;
//   fltValue.value = value;
//   return true;
   // Merge rpath sgmts and return merged value larger than 'value' or same with 'value'
   do {
      merged = true;
      for(unsigned i = 0,limit = size;i < limit;i++) {
         while(values[i] < value) {
            if(!sgmtScans[i].next())
               return false;
            values[i]=sgmtScans[i].getValue1();
         }

         if (value < values[i]) {
            merged=false;
            value=values[i];
         }
      }
   } while(!merged);

   fltValue.value = value;
   return true;
}

bool RPathFilter::next(unsigned value)
{
   if (value == fltValue.value)
      return true;

   return GetNextValue(value);
}

void RPathFilter::print(PlanPrinter& out)
{
   char buf[1000];
   for(unsigned i = 0,limit = size;i < limit;i++) {
      stringstream dfscode;
      dfscode << dfscodes[i];
      sprintf(buf, "VLIST[%u]<%s> nodeId:%u, card:%f, blks:%u", (unsigned) dfscodes[i].size(), 
              dfscode.str().c_str(), nodeIDs[i], cards[i], sizes[i]);
      out.addArgumentAnnotation(buf);
   }
}
