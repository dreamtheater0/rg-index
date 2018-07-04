#include <iostream>
#include <fstream>
#include <cassert>
#include <glog/logging.h>
#include "rpath/RPathSegment.hpp"
#include "rg-index/inter_result.hpp"
#include "rts/operator/IndexScan.hpp"

//---------------------------------------------------------------------------
InterResult::InterResult(InterResult& old_results) {
   vertices=old_results.vertices;
   
   mapFromOriginalToInterResult=old_results.mapFromOriginalToInterResult;
   for(map<unsigned, vector<unsigned> * >::iterator iter=old_results.mapFromOriginalToInterResult.begin(),
       limit=old_results.mapFromOriginalToInterResult.end(); iter!=limit; iter++) {
      vector<unsigned>* vertices=new vector<unsigned>;
      mapFromOriginalToInterResult[(*iter).first]=vertices;
      (*vertices)=(*(*iter).second);
   }
   mapFromInterResultToOriginal=old_results.mapFromInterResultToOriginal;
   results.resize(vertices.size()+1);
}
//---------------------------------------------------------------------------
InterResult::~InterResult() {
   // delete mapping info.
   for(map<unsigned, vector<unsigned> * >::iterator iter=mapFromOriginalToInterResult.begin(),
       limit=mapFromOriginalToInterResult.end(); iter!=limit; iter++) {
      delete (*iter).second;   
   }
   // delete Elists
   cout << this << " delete elist: ";
   for(map< pair<unsigned, unsigned>, Elist *>::iterator iter=elistmap.begin(),
       limit=elistmap.end(); iter!=limit; iter++) {
      if (copiedElist.find((*iter).second)==copiedElist.end()) {
         cout << (*iter).second << " ";
         delete (*iter).second;
      }
   }
   for(vector<Vlist*>::iterator iter=results.begin(), limit=results.end();
       iter!=limit; iter++) {
      if (copiedVlist.find((*iter))==copiedVlist.end()) 
         delete (*iter);
   }
   cout << endl;
}
//---------------------------------------------------------------------------
Elist* InterResult::getElist(unsigned idx)
{
   unsigned sourceID=(*getInterResultIDs(idx))[0];
   unsigned neighborID=vertices[sourceID].neighbors[0];
   Elist *elist=elistmap[pair<unsigned, unsigned> (sourceID,neighborID)];
   return elist;
}
//---------------------------------------------------------------------------
bool InterResult::addForwardEdge(Database &db, unsigned source, unsigned newvertex,
                                 InterResult &old_result, unsigned pred, bool reverse,
                                 vector<unsigned> &rmvertices, bool makeElist, unsigned /*minSup*/)
{
   // we use only the first interresult ID as vertex ID
   // because the remains are vertices created from backward edges
   unsigned sourceID=(*getInterResultIDs(source))[0];

   // Create new vertex
   unsigned newVertexID=vertices.size();
   vertices.resize(newVertexID+1);
   Vertex *newVertex=&vertices[newVertexID];
   newVertex->idx=newVertexID;
   insertMapping(newvertex, newVertexID);
   vertices[sourceID].neighbors.push_back(newVertexID);
   vertices[newVertexID].neighbors.push_back(sourceID);
   vertices[sourceID].terminal=false;

   for(unsigned i=0;i<vertices.size();i++) {
      cout << i << " vertex:" << &vertices[i] << " vertexIdx:" << vertices[i].idx 
           << "neighbors: ";
      for(unsigned j=0;j<vertices[i].neighbors.size();j++) {
         cout << vertices[i].neighbors[j] << " ";
      }
      cout << endl;
   }
   
   // Add edge lists
   Elist *forwardElist=new Elist; // key: source
   Elist *backwardElist=new Elist; // key: newEdgeID

   elistmap[pair<unsigned, unsigned> (sourceID,newVertexID)]=forwardElist;
   elistmap[pair<unsigned, unsigned> (newVertexID,sourceID)]=backwardElist;

   // Make Elist
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

   unsigned neighborID=vertices[sourceID].neighbors[0];
   Elist *old_elist=old_result.elistmap[pair<unsigned, unsigned> (sourceID,neighborID)];
   //cout << old_elist << " " << old_elist->size() << endl;
   Elist::iterator iter=old_elist->begin(), limit=old_elist->end();
   unsigned vID=(*iter).first, v1, v2;
   scan->first(); 
   bool isEnd=false;
   vlistReg.value=vID;
   set<unsigned> *vlist1=results[sourceID]=new Vlist, *vlist2=results[newVertexID]=new Vlist;
   assert(iter!=limit);
   do {
      v1=value1.value;
      v2=value2.value;
      if (vID==v1) {
         ElistVlist* elistvlist=&(*forwardElist)[v1];
         unsigned prev=v1;
         vlist1->insert(v1);
         while (prev==v1) {
            elistvlist->insert(v2);
            vlist2->insert(v2);
            // back edge list
            (*backwardElist)[v2].insert(v1);
            if (!scan->next()) {
               isEnd=true;
               break;
            }
            v1=value1.value;
            v2=value2.value;
         }
      }
      else if (v1 < vID) {
         if (!scan->next()) isEnd=true;
      }
      else if (v1 > vID) {
         iter++;
         if (iter==limit) isEnd=true;
         vlistReg.value=vID=(*iter).first;
      }
   } while (!isEnd);
   delete scan;
   // set support
   support=min(forwardElist->size(), backwardElist->size());
   if (vlist1->size()==old_result.results[sourceID]->size()) {
      findEdges(*vlist1, sourceID, old_result, newVertexID, makeElist, true); 
   }
   else {
      findEdges(*vlist1, sourceID, old_result, newVertexID, makeElist, false); 
   }
   // make back-edge lists for rmpath 
   for(vector<unsigned>::iterator iter=rmvertices.begin(), limit=rmvertices.end()-1;
       iter!=limit; iter++) {
      vector<unsigned>::iterator iter2=iter+1;
      unsigned v1=(*iter), v2=(*iter2);
      makeBackEdgeList(v1, v2);
   }
   for(map< pair<unsigned, unsigned>, Elist * >::iterator iter=elistmap.begin(),
       limit=elistmap.end(); iter!=limit; iter++) {

      cout << (*iter).second << " " << (*iter).first.first << "," << (*iter).first.second 
           << ", size:" << (*iter).second->size() << endl;
   }
   
   for(unsigned i=0;i<vertices.size();i++) {
      cout << "vertex:" << vertices[i].idx << ", terminal:" << vertices[i].terminal 
           << " result:" << results[i]->size() << endl;
   }
   return true;
}
//---------------------------------------------------------------------------
void InterResult::makeBackEdgeList(unsigned v1, unsigned v2)
{
   /* make elist(v2,v1) from elist (v1, v2) */
   Elist *source_elist=elistmap[pair<unsigned, unsigned> (v1, v2)];
   Elist *target_elist=new Elist;
   elistmap[pair<unsigned, unsigned> (v2, v1)]=target_elist;
   
   for(Elist::iterator iter=source_elist->begin(), limit=source_elist->end();
       iter!=limit; iter++) {
      ElistVlist &elistvlist=(*iter).second;
      unsigned v1, v2=(*iter).first;
      set<unsigned>::iterator iter2;
      elistvlist.first(v1, iter2);
      (*target_elist)[v1].insert(v2);
      while(elistvlist.next(v1,iter2)) {
         (*target_elist)[v1].insert(v2);
      }
   }
}
//---------------------------------------------------------------------------
inline void InterResult::findEdges(set<unsigned> &sources, unsigned vertexID, InterResult &old_result,
                                   unsigned prevVertexID, bool makeElist, bool copyElist)
{
   vector<unsigned> &neighborVs=vertices[vertexID].neighbors;
   Elist *elists, *old_elist;

   cout << "findedges. sourcesSize:" << sources.size() << ", vertexID:" 
        << vertexID << ", neighborSize:" << neighborVs.size() << endl;

   if (copyElist) {
      cout << "copy edges.." << endl;
      for (unsigned i=0,limit=neighborVs.size();i<limit;i++) {
         unsigned neighborID=neighborVs[i];
         if (neighborID==prevVertexID)
            continue;
         Elist *elist=old_result.elistmap[pair<unsigned, unsigned>(vertexID, neighborID)];
         elistmap[pair<unsigned, unsigned>(vertexID, neighborID)]=elist;
         copiedElist.insert(elist);
         Vlist *neighbors=results[neighborID]=old_result.results[neighborID];
         copiedVlist.insert(neighbors);
         if (support>neighbors->size())
            support=neighbors->size();
         if (!vertices[neighborVs[i]].terminal) {
            findEdges(*neighbors, neighborID, old_result, vertexID, makeElist, copyElist);
         }
      }
      return;
   }

   for (unsigned i=0,limit=neighborVs.size();i<limit;i++) {
      unsigned neighborID=neighborVs[i];
      cout << "neighborID:" << neighborID << ", prevVertexID:" << prevVertexID 
           << ", terminal:" << vertices[neighborVs[i]].terminal << endl;
      if (neighborID==prevVertexID)
         continue;
      elistmap[pair<unsigned, unsigned>(vertexID, neighborID)]=elists=new Elist;
      old_elist=old_result.elistmap[pair<unsigned, unsigned>(vertexID, neighborID)];
      Vlist *neighbors=results[neighborID]=new Vlist;

      if (sources.size()<=old_elist->size()) { 
         for (set<unsigned>::iterator iter=sources.begin(), limit=sources.end();
              iter!=limit; iter++) {
            unsigned vertex=(*iter);
            Elist::iterator elistiter=old_elist->find(vertex); 
            if (elistiter!=elists->end()) {
               ElistVlist &elistvlist=(*elistiter).second;
               switch (elistvlist.type) {
                  case ONE:
                     neighbors->insert(elistvlist.vertex);
                     break;
                  case SET:
                  case REF:
                     neighbors->insert(elistvlist.vertices->begin(), elistvlist.vertices->end());
                     break;
                  default:
                     assert(false);
               }
               if(makeElist) (*elists)[vertex]=ElistVlist(&elistvlist);
            }
         }
      }
      else {
         for(Elist::iterator elistiter=old_elist->begin(), elistlimit=old_elist->end();
             elistiter!=elistlimit;elistiter++) {
            unsigned vertex=(*elistiter).first;
            if (sources.find(vertex)!=sources.end()) {
               ElistVlist &elistvlist=(*elistiter).second;
               switch (elistvlist.type) {
                  case ONE:
                     neighbors->insert(elistvlist.vertex);
                     break;
                  case SET:
                  case REF:
                     neighbors->insert(elistvlist.vertices->begin(), elistvlist.vertices->end());
                     break;
                  default:
                     assert(false);
               }
               if(makeElist) (*elists)[vertex]=ElistVlist(&elistvlist);
            }
         }
      }
      if (support>neighbors->size())
         support=neighbors->size();
      if (!vertices[neighborVs[i]].terminal) {
         unsigned copyElist2=false;
         if (neighbors->size()==old_result.results[neighborID]->size())
            copyElist2=true;
         findEdges(*neighbors, neighborID, old_result, vertexID, makeElist, copyElist2);
      }
   }
   cout << "end findedges." << endl;
}
//---------------------------------------------------------------------------
void InterResult::insertMapping(unsigned original, unsigned interresult) {
   if (mapFromOriginalToInterResult.find(original)==mapFromOriginalToInterResult.end()) {
      mapFromOriginalToInterResult[original]=new vector<unsigned>;
   }
   mapFromOriginalToInterResult[original]->push_back(interresult);
   mapFromInterResultToOriginal[interresult]=original;
}
//---------------------------------------------------------------------------
void InterResult::initWithOneSizeGraph(Database &db, GSPAN::DFSCode& dfscode, 
                                       unsigned p, unsigned old_p) {
   Vlist *vlist1=new Vlist, *vlist2=new Vlist;
   results.push_back(vlist1);
   results.push_back(vlist2);
   vertices.resize(2);
   vertices[0].idx=0; 
   vertices[0].neighbors.push_back(1); 
   vertices[1].idx=1; 
   vertices[1].neighbors.push_back(0); 
   insertMapping(0,0);
   insertMapping(1,1);
   Elist *elist1, *elist2;
   elistmap[pair<unsigned, unsigned> (0, 1)]=elist1=new Elist;
   elistmap[pair<unsigned, unsigned> (1, 0)]=elist2=new Elist;

   // make Vlists for 1-size graph 
   LOG(INFO) << "Make 1-size graph. predcate: " << p << ", old_p:" << old_p;
   cout << "Make 1-size graph. predcate: " << p << ", old_p:" << old_p << endl;
   dfscode.push(0, 1, p, GSPAN::EDGE_TYPE_NORMAL);

   Register subject,predicate,object;
   subject.reset(); predicate.reset(); object.reset();

   predicate.value=old_p;
   IndexScan* scan= IndexScan::create(db,Database::Order_Predicate_Subject_Object,
            &subject,false,&predicate,true,&object,false, 0);
   scan->first();
   bool isEnd=false;
   unsigned v1=subject.value, v2=object.value;
   unsigned prev=v1;
   do {
      ElistVlist* elistvlist=&(*elist1)[v1];
      results[0]->insert(v1);
      while (prev==v1) {
         results[1]->insert(v2);
         elistvlist->insert(v2);
         // backward elist
         (*elist2)[v2].insert(v1);
   
         if (!scan->next()) {
            isEnd=true;
            break;
         }
         v1=subject.value;
         v2=object.value;
      }
      prev=v1;
   } while (!isEnd);

   // TODO: sort the neighbors by .....what?

   LOG(INFO) << "size1: " << results[0]->size() << ", size2: " << results[1]->size() << endl;
   delete scan;

   // support
   support=min(results[0]->size(), results[1]->size());
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
void InterResult::materializeVlist(VertexLoader& reader, unsigned &size,
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
void InterResult::materializeVlists(ofstream &ofs_data, InterResult* old_result,
                                    unsigned &page, vector<unsigned> &offsets) {
   unsigned vertexsize=vertices.size();
   for(vector<Vertex>::iterator iter=vertices.begin(), limit=vertices.end();
       iter!=limit;iter++) {
      Vertex *vertex=&(*iter);
      unsigned vertexID=vertex->idx;
      Vlist *vlist=results[vertexID];

      if (old_result && vertexID<(vertexsize-1) && 
          old_result->results[vertexID]->size()*0.8f<vlist->size()) {
         offsets.push_back(old_result->results[vertexID]->offset);
         vlist->offset=old_result->results[vertexID]->offset;
         continue;
      }

      VertexLoader vloader(vlist);
      unsigned size, byte;
      offsets.push_back(page);
      vlist->offset=page;
      materializeVlist(vloader, size, page, byte, ofs_data);
   }
}
//---------------------------------------------------------------------------
unsigned InterResult::getSupport() {
   unsigned minSupport=~0u;
   for(vector<Vertex>::iterator iter=vertices.begin(), limit=vertices.end();
       iter!=limit;iter++) {
      Vertex *vertex=&(*iter);
      unsigned vertexID=vertex->idx;
      Vlist *vlist=results[vertexID];
      unsigned size=vlist->size();
      if (size<minSupport)
          minSupport=size;
   }
   support=minSupport;
   return minSupport;
};
//---------------------------------------------------------------------------
ElistVlist::ElistVlist() {
   type=NOTINIT;
   vertices=NULL;
}
//---------------------------------------------------------------------------
ElistVlist::ElistVlist(ElistVlist *elistvlist) {
   switch(elistvlist->type) {
      case NOTINIT:
         assert(false);
         break;
      case ONE:
         type=ONE;
         vertex=elistvlist->vertex;
         break;
      case REF:
      case SET:
         type=REF;
         vertices=elistvlist->vertices;
         break;
   }
}
//---------------------------------------------------------------------------
ElistVlist::~ElistVlist() {
   switch(type) {
      case NOTINIT:
      case ONE:
      case REF:
         break;
      case SET:
         delete vertices;
         break;
   }
}
//---------------------------------------------------------------------------
inline void ElistVlist::insert(unsigned i_vertex) {
   switch(type) {
      case NOTINIT:
         vertex=i_vertex;
         type=ONE;
         break;
      case ONE: {
         unsigned temp=vertex;
         vertices=new set<unsigned>;
         vertices->insert(temp);
         vertices->insert(i_vertex);
         type=SET;
         break;
      }
      case SET:
         vertices->insert(i_vertex);
         break;
      case REF:
         assert(false);
         break; 
   }
}
//---------------------------------------------------------------------------
inline unsigned ElistVlist::first(unsigned &v, set<unsigned>::iterator &iter) {
   switch(type) {
      case NOTINIT:
         return 0;
         break;
      case ONE:
         v=vertex;
         return 1;
         break;
      case SET:
      case REF:
         iter=vertices->begin();
         v=(*iter);
         iter++;
         return 1;
         break;
   }
}
//---------------------------------------------------------------------------
inline unsigned ElistVlist::next(unsigned &v, set<unsigned>::iterator &iter) {
   switch(type) {
      case NOTINIT:
         return 0;
         break;
      case ONE:
         return 0;
         break;
      case SET:
      case REF:
         if (iter==vertices->end())
            return 0;
         v=(*iter);
         iter++;
         return 1;
         break;
   }
}
//---------------------------------------------------------------------------
