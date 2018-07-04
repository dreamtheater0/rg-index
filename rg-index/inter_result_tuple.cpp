#include <iostream>
#include <fstream>
#include <cassert>
#include <algorithm>
#include "rpath/RPathSegment.hpp"
#include "rg-index/inter_result.hpp"
#include "rts/operator/IndexScan.hpp"

//---------------------------------------------------------------------------
InterResultTuple::InterResultTuple(InterResultTuple *i_old_result) {
   tuple_size=0;
   old_result=i_old_result; 
   backward=false;

   cout << "create inter_result:" << this << endl;
   cout.flush();
};
//---------------------------------------------------------------------------
InterResultTuple::~InterResultTuple() {
   cout << "start destruct inter_result:" << this;
   clear();
   cout << " end. " << endl;
   cout.flush();
}

//---------------------------------------------------------------------------
void InterResultTuple::clear() {
   results.clear();
   offsets.clear();
   blks.clear();
   for(unsigned i=0; i<tuple_size; i++) {
      vlists[i].clear();
   }
   vlists.clear();
   cards.clear();
   pool.freeAll();
}
//---------------------------------------------------------------------------
unsigned InterResultTuple::getResultCount() {
   unsigned resultCnt=results.size();
   if(old_result) {
      return old_result->getResultCount() + resultCnt;
   }
   return resultCnt;
}
//---------------------------------------------------------------------------
bool InterResultTuple::addForwardEdge(Database &db, hash_tbl_t &hash_tbl,
                                      unsigned pred, bool reverse)
{
   tuple_size=old_result->tuple_size+1;
   vlists.resize(tuple_size);

   Register predicate,value1,value2,vlistReg;
   predicate.reset(); value1.reset(); value2.reset(); vlistReg.reset();
   predicate.value=pred;
   IndexScan* scan;
   if (!reverse) {
      // will be joined with Subject and store Objects in Vlist
      scan = IndexScan::create(db,Database::Order_Predicate_Subject_Object,
                               &value1,false,&predicate,true,&value2,false, 0);
      scan->addMergeHint(&value1, &vlistReg);
   }
   else {
      // will be joined with Object and store Subjects in Vlist
      scan = IndexScan::create(db,Database::Order_Predicate_Object_Subject,
                               &value2,false,&predicate,true,&value1,false, 0);
      scan->addMergeHint(&value1, &vlistReg);
   }

   // Hash join
   table_t *tuples=NULL;
   scan->first();
   unsigned v1, v2, prev=0;
   unsigned newKeyIdx=tuple_size-1;
   hash_tbl_t::iterator iter;
   do {
      v1=value1.value;
      v2=value2.value;

      // Give up if the results are too large
      if (getResultCount() > 200000000) {
         delete scan;
         return false;
      }

      if (tuples && v1==prev) {
         for(table_t::iterator iter2=tuples->begin(),
             limit2=tuples->end(); iter2!=limit2; iter2++) {
            Tuple *newtuple=pool.alloc();
            newtuple->set(*iter2, v2);
            results.push_back(newtuple);
            vlists[newKeyIdx].insert(v2);
         }
      }
      else {
         iter=hash_tbl.find(v1);
         if(iter!=hash_tbl.end()) {
            tuples=(*iter).second;
            for(table_t::iterator iter2=tuples->begin(),
                limit2=tuples->end(); iter2!=limit2; iter2++) {
               Tuple *newtuple=pool.alloc();
               newtuple->set(*iter2, v2);
               results.push_back(newtuple);

               Tuple *prevTuple=newtuple;
               for(unsigned i=tuple_size; i>0; i--) {
                  unsigned key=prevTuple->getColumn(i-1,&prevTuple);
                  vlists[i-1].insert(key);
               }
            }
         }
         else {
            tuples=NULL;
         }
      }
      prev=v1;
   } while(scan->next());
   delete scan;

   cout << "resultcnt: " << getResultCount();
   getSupport();

   return true;
}
//---------------------------------------------------------------------------
void InterResultTuple::addBackwardEdge(InterResultTuple &foreward_results, 
                                       unsigned v1 /*dest*/, 
                                       unsigned v2 /*source*/) {
   backward=true;
   tuple_size=foreward_results.tuple_size-1;
   vlists.resize(tuple_size);

   for(table_t::iterator iter=foreward_results.results.begin(), 
       limit=foreward_results.results.end(); iter!=limit; iter++) {
      Tuple *tuple=(*iter);
      unsigned key1=tuple->getColumn_i(v2,foreward_results.tuple_size);
      unsigned key2=tuple->getColumn_i(v1,foreward_results.tuple_size);
      if(key1==key2) {
         Tuple *newtuple=tuple->prev.prev_ptr;
         results.push_back(newtuple);

         Tuple *prevTuple=newtuple;
         for(unsigned i=tuple_size; i>0; i--) {
            unsigned key=prevTuple->getColumn(i-1,&prevTuple);
            vlists[i-1].insert(key);
         }
      }
   }
   getSupport();
}
//---------------------------------------------------------------------------
void InterResultTuple::initWithOneSizeGraph(Database &db, GSPAN::DFSCode& dfscode, 
                                            unsigned p, unsigned old_p) {
   // make Vlists for 1-size graph 
   cout << "Make 1-size graph. predcate: " << p << ", old_p:" << old_p << endl;
   cout.flush();
   dfscode.push(0, 1, p, GSPAN::EDGE_TYPE_NORMAL);

   Register subject,predicate,object;
   subject.reset(); predicate.reset(); object.reset();
   predicate.value=old_p;
   IndexScan* scan= IndexScan::create(db,Database::Order_Predicate_Subject_Object,
            &subject,false,&predicate,true,&object,false, 0);
   scan->first();

   tuple_size=2;
   vlists.resize(2);
   while(1) {
      unsigned v1=subject.value, v2=object.value;
      Tuple *tuple=pool.alloc();
      tuple->set(v1, v2);
      results.push_back(tuple);
      vlists[0].insert(v1);
      vlists[1].insert(v2);

      if (!scan->next())
         break;
   };
   delete scan;

   // support
   cout << "resultcnt: " << getResultCount();
   getSupport();
}
//---------------------------------------------------------------------------
static void writePage(ofstream& out,unsigned page,const void* data)
   // Write a page to the file
{
   unsigned long long ofs=static_cast<unsigned long long>(page)*
                          static_cast<unsigned long long>(RPathSegment::pageSize);
   if (static_cast<unsigned long long>(out.tellp())!=ofs) {
      cout << "internal error: tried to write page " << page << " (ofs " << ofs << ") at position " << out.tellp() << endl;
      throw;
   }
   out.write(static_cast<const char*>(data),RPathSegment::pageSize);
}
//---------------------------------------------------------------------------
static void writeUint32(unsigned char* target,unsigned value)
   // Write a 32bit value
{
   target[0]=value>>24;
   target[1]=(value>>16)&0xFF;
   target[2]=(value>>8)&0xFF;
   target[3]=value&0xFF;
}
//---------------------------------------------------------------------------
static unsigned bytes(unsigned v)
   // Compute the number of bytes required to encode a value
{
   if (v>=(1<<24))
      return 4; else
   if (v>=(1<<16))
      return 3; else
   if (v>=(1<<8)) return 2; else
      return 1;
}
//---------------------------------------------------------------------------
static unsigned writeDelta(unsigned char* buffer,unsigned ofs,unsigned value)
   // Write an integer with varying size
{
   if (value>=(1<<24)) {
      writeUint32(buffer+ofs,value);
      return ofs+4;
   } else if (value>=(1<<16)) {
      buffer[ofs]=value>>16;
      buffer[ofs+1]=(value>>8)&0xFF;
      buffer[ofs+2]=value&0xFF;
      return ofs+3;
   } else if (value>=(1<<8)) {
      buffer[ofs]=value>>8;
      buffer[ofs+1]=value&0xFF;
      return ofs+2;
   } else {
      buffer[ofs]=value;
      return ofs+1;
   }
}
//---------------------------------------------------------------------------
void InterResultTuple::materializeVlist(VertexLoader& reader, unsigned &size,
                               unsigned &page /*start*/, unsigned &byte,
                               ofstream &ofs_data)
{
   // Pack the fully aggregated facts into leaves using prefix compression
   const unsigned headerSize = 4; // Next pointer
   unsigned char buffer[RPathSegment::pageSize];
   unsigned bufferPos=headerSize;

   unsigned lastNodeID=0, nodeID;
   size = 0;
   byte = 0;

   while (reader.next(nodeID)) {
      // Try to pack it on the current page
      unsigned len;

      size++;

      if ((nodeID-lastNodeID)<127)
         len=1; else
         len=1+bytes(nodeID-lastNodeID);

      // Tuple too big or first element on the page?
      if ((bufferPos==headerSize)||(bufferPos+len>RPathSegment::pageSize)) {
         // Write the partial page
         if (bufferPos>headerSize) {
            writeUint32(buffer,page+1);
            memset(buffer+bufferPos,0,RPathSegment::pageSize-bufferPos);
            writePage(ofs_data,page,buffer);
            ++page;
         }
         // Write the first element fully
         bufferPos=headerSize;
         writeUint32(buffer+bufferPos,nodeID); bufferPos+=4;
      } else {
         // No, pack them
         if ((nodeID-lastNodeID)<127) {
            buffer[bufferPos++]=nodeID-lastNodeID;
         } else {
            buffer[bufferPos++]=0x80|(bytes(nodeID-lastNodeID));
            bufferPos=writeDelta(buffer,bufferPos,nodeID-lastNodeID);
         }
      }
      // Update the values
      lastNodeID=nodeID;
   }
   // Flush the last page
   writeUint32(buffer,0);
   memset(buffer+bufferPos,0,RPathSegment::pageSize-bufferPos);
   byte+=bufferPos;
   writePage(ofs_data,page,buffer);
   ++page;
}
//---------------------------------------------------------------------------
void InterResultTuple::materializeVlists(ofstream &ofs_data, unsigned &page) {
   for(unsigned i=0; i<tuple_size; i++) {
      if (old_result && i<(tuple_size-2) && 
          (double) old_result->cards[i]*0.7f < (double) cards[i]) {
         offsets.push_back(old_result->offsets[i]);
         blks.push_back(old_result->blks[i]);
         cards[i]=old_result->cards[i];
         continue;
      }

      VertexLoader vloader(&vlists[i]);
      unsigned size, byte, page1;
      offsets.push_back(page);
      page1=page;
      materializeVlist(vloader, size, page, byte, ofs_data);
      blks.push_back(page-page1);
      i++;
   }
}
//---------------------------------------------------------------------------
unsigned InterResultTuple::getSupport() {
   unsigned minSupport=~0u, sum=0;
   for(unsigned i=0; i<tuple_size; i++) {
      unsigned size=vlists[i].size();
      vlists[i].clear();
      cards.push_back(size);
      minSupport=min(minSupport, size);
      sum+=size;
   }
   support=minSupport;
   cout << " Sum: " << sum <<  " support: " << support << endl;
   return minSupport;
};
//---------------------------------------------------------------------------
