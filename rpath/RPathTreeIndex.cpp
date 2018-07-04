#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <cassert>
#include <math.h>
#include <queue>
#include <glog/logging.h>
#include "rpath/PPath.hpp"
#include "rpath/RPathTreeIndex.hpp"
#include "rts/buffer/BufferManager.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/FullyAggregatedIndexScan.hpp"
#include "infra/osdep/Timestamp.hpp"

using namespace std;

void RPathTreeIndex::getRPFileName(char *buf, char* path, char *dataset, unsigned /*maxL*/)
{
   sprintf(buf, "%s/%s.rpath", path, dataset);
}

void RPathTreeIndex::getRPStatFileName(char *buf, char *path, char *dataset, unsigned /*maxL*/)
{
   sprintf(buf, "%s/%s.stat", path, dataset);
}

RPathTreeIndex::RPathTreeIndex(char *dataset, char *path, unsigned maxL, bool ) {
   // open for building
   root.predicate = 0;
   root.startPage = 0;
   root.startIndexPage = 0;
   root.cardinality = 0;
   root.updateInfo = NULL;
//   root.updateInfo->hasDeleted = false;
//   root.updateInfo->isUpdated = false;
   root.children = NULL;

   memset(fileName, 0, 256);
   memset(statName, 0, 256);

   getRPFileName(fileName, path, dataset, maxL);
   getRPStatFileName(statName, path, dataset, maxL);

   ofs_rf.open(fileName, ios::out|ios::trunc|ios::binary);
   ofs_stat.open(statName, ios::out|ios::trunc);

   lastPage = 0;
   page = 0;

   SetParams();
   //pthread_mutex_init(&mutex_lock, NULL);
   //pthread_mutex_init(&mutex_lock_idx, NULL);
}

RPathTreeIndex::~RPathTreeIndex() {
}

void RPathTreeIndex::reopen() {
   ofs_rf.flush();
   file.close();
   if (!file.open(fileName)) {
      cerr << "Fail to open " << fileName << endl;
      exit(1);
   };
}

void RPathTreeIndex::SetParams() {
   th_mincnt = 0;
   th_diff = 1;
   is_reverse = false;
   is_varp = false;
   minTh = 1500;

   if (getenv("TH_MINCNT")) {
      th_mincnt = atoi(getenv("TH_MINCNT"));
   }
   if (getenv("TH_DIFF")) {
      th_diff = atof(getenv("TH_DIFF"));
   }
   if (getenv("REVERSE") != NULL && atoi(getenv("REVERSE")) == 1)
      is_reverse = true;
   if (getenv("VARP") != NULL && atoi(getenv("VARP")) == 1)
      is_varp = true;

   if (getenv("MINTH") != NULL) 
      minTh = atoi(getenv("MINTH"));

   maxL=atoi(getenv("MAXL"));

   skipP=false;
   if (getenv("SKIPPRED")) {
      skipP=true;
   }
   
   LOG(INFO) << "th_mincnt:" << th_mincnt;
   LOG(INFO) << "th_diff:" << th_diff;
   LOG(INFO) << "is_reverse:" << is_reverse;
   LOG(INFO) << "is_varp:" << is_varp;
}

RPathTreeIndex::RPathTreeIndex(char *dataset, char *path, unsigned maxL) {
   memset(fileName, 0, 256);
   memset(statName, 0, 256);

   getRPFileName(fileName, path, dataset, maxL);
   getRPStatFileName(statName, path, dataset, maxL);

   if (!file.open(fileName)) {
      cerr << "Fail to open " << fileName << endl;
      exit(1);
   };

   char ppathInfo[256];
   memset(ppathInfo, 0, 256);

   lastPage = 0;
   ifstream ifs_stat(statName, ios::in);

   root.predicate = 0;
   root.startPage = 0;
   root.startIndexPage = 0;
   root.cardinality = 0;
   root.children = NULL;

   SetParams();
   vlist_cnt = 0;

   char *len, *ppathStr, *saveptr;
   unsigned startPage, startIdxPage, cardinality, byte;

   while(ifs_stat >> ppathInfo && !ifs_stat.eof()) {
      // PPath length (필요없음)
      len = strtok_r(ppathInfo, "|", &saveptr);
      // PPath 
      ppathStr = strtok_r(NULL, "|", &saveptr);
      // Start page
      startPage = (unsigned) atoi(strtok_r(NULL, "|", &saveptr));
      // Start index page
      startIdxPage = (unsigned) atoi(strtok_r(NULL, "|", &saveptr));
      // cardinality
      cardinality = (unsigned) atoi(strtok_r(NULL, "|", &saveptr));
      // byte (for update) 
      byte = (unsigned) atoi(strtok_r(NULL, "|", &saveptr));

      // check the discriminativeness
      unsigned l=atoi(len);
      if (l > maxL)
         break;
      /*

      RPathTreeIndex::Node *suffixNode = getSuffixNode(ppath);

      if (ppath.size() > 1 && !suffixNode)
         continue;

      if (!this->isDiscriminative(suffixNode, ppath.size(), cardinality))
         continue;
      */

      //LOG(INFO) << ppathStr << " " << cardinality;
      InsertIntoIdx(startPage, startIdxPage, cardinality, byte, ppathStr);
      vlist_cnt++;
   }
   page=lastPage;
   LOG(INFO) << "vlist cnt:" << vlist_cnt;
   LOG(INFO) << "last page:" << lastPage;
}

RPathTreeIndex::Node* RPathTreeIndex::InsertIntoIdx(unsigned startPage, 
                                                    unsigned startIdxPage, unsigned cardinality, 
                                                    unsigned /*byte*/, const char *ppathStr) 
{
   PPath ppath(ppathStr);

   Node *newNode = new Node();
   newNode->startPage = startPage;
   newNode->startIndexPage = startIdxPage;
   newNode->cardinality = cardinality;
   newNode->rpathSgm = NULL; // create when needed 
   newNode->children = NULL;

   newNode->skip=false;
   if (newNode->cardinality < getMinCnt(ppath.path.size()))
      newNode->skip=true;

//   newNode->updateInfo->hasDeleted = false;
//   newNode->ppath.change(ppathStr);
   newNode->predicate = ppath.path.back();
   newNode->len=ppath.path.size();

   if (lastPage < newNode->startIndexPage)
      lastPage = newNode->startIndexPage;

   InsertIntoTree(&root, ppath, 0, newNode);
   nodes.push_back(newNode);
   return newNode;
}

void RPathTreeIndex::InsertIntoTree(Node* root, PPath& ppath, unsigned level,
                                    Node* newNode)
{
   // same predicate path exists already
   if (level == ppath.path.size()) assert(false);
   unsigned predicate = ppath.path[level];

   if (root->children==NULL) {
      root->children=new std::map<unsigned, Node *>;
   }
   else {
      map<unsigned, Node*>::iterator it;
      it = root->children->find(predicate);

      if (it != root->children->end()) {
         InsertIntoTree(it->second, ppath, level+1, newNode);
         return;
      }

      if (level != ppath.path.size() - 1) {
         // the parent node is not inserted because it is not discriminative
         // let's ignore this node
         return;
      }
   }
   root->children->insert(pair<unsigned,Node*>(newNode->predicate, newNode));
}

unsigned RPathTreeIndex::GetStartPage(PPath& pp) {
   pp.print();
   return 0;
}

unsigned RPathTreeIndex::GetSelectivity(PPath& pp) {
   pp.print();
   return 0;
}

unsigned RPathTreeIndex::GetCost(PPath& pp) {
   pp.print();
   return 0;
}

RPathTreeIndex::Node* RPathTreeIndex::SearchNode(PPath& ppath) {
   return SearchNode(root, ppath, 0);
}

RPathTreeIndex::Node* RPathTreeIndex::SearchNode(Node& root, PPath& ppath, unsigned level) {
   unsigned predicate;

   if (level == ppath.path.size())
      return &root;

   predicate = ppath.path[level];

   if (!is_reverse && predicate >= REVERSE_PRED)
      return NULL;

   if (root.children==NULL)
      return NULL;

   map<unsigned, Node*>::iterator it;
   it = root.children->find(predicate);

   if (it != root.children->end()) {
      return SearchNode(*it->second, ppath, level+1);
   }

   return NULL;
}

void RPathTreeIndex::NodePrint(Node& node) {
   cerr /*<< node.ppath.getStr()*/ << "|" << node.startPage 
        << "|" << node.startIndexPage << "|" << node.cardinality << endl;
}

void RPathTreeIndex::Print(Node& /*root*/, unsigned /*level*/) {
//  for (unsigned i=0;i<level;i++) cout << " ";
   /*
   for (vector<Node *>::const_iterator iter=root.children.begin(),
        limit=root.children.end(); iter!=limit;++iter) {
      NodePrint(*(*iter));
   }
   for (vector<Node *>::const_iterator iter=root.children.begin(),
        limit=root.children.end(); iter!=limit;++iter) {
      Print(*(*iter), level+1);
   }
   */
}

unsigned RPathTreeIndex::FindCardinality(unsigned predicate)
{
   map<unsigned, Node*>::iterator it;
   it = root.children->find(predicate);

   if (it != root.children->end()) {
      Node *node = (*it).second;
      return node->cardinality;
   }

   assert(false);
   return 0;
}

void RPathTreeIndex::Print()
{
   /*
   for (vector<Node *>::const_iterator iter=root.children.begin(),
        limit=root.children.end(); iter!=limit;++iter) {
      Node *node = *iter;
      NodePrint(*node);
   }

   for (vector<Node *>::const_iterator iter=root.children.begin(),
        limit=root.children.end(); iter!=limit;++iter) {
      Node *node = *iter;
      Print(*node, 0);
   }
   */
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
RPathTreeIndex::Node* RPathTreeIndex::getSuffixNode(PPath &ppath) 
{
   unsigned i = 1;
   while (true) {
      PPath suffix;
      suffix.path.assign(ppath.path.begin() + i, ppath.path.end());
      RPathTreeIndex::Node *suffixNode = SearchNode(suffix);
      if (suffixNode) {
         if (suffixNode==&root) return NULL;
         return suffixNode;
      }
      i++;
   }

   assert(false);
   return NULL;
}
//---------------------------------------------------------------------------
void RPathTreeIndex::packFullyNodeIDLeaves(NodeIDLoader& reader, unsigned &size,
                                           unsigned &startPage, unsigned &nextPage, unsigned &byte)
   // Pack the fully aggregated facts into leaves using prefix compression
{
   const unsigned headerSize = 4; // Next pointer
   unsigned char buffer[RPathSegment::pageSize];
   unsigned bufferPos=headerSize;

   unsigned lastNodeID=0, nodeID;
   //bool islocked = false;
   size = 0;
   byte = 0;

   startPage = page;
   while (reader.next(nodeID)) {
      // Try to pack it on the current page
      // 차이가 127(2^7-1)까지는 1 byte에 저장 (길이 정보 없이, MSB:0)
      // 차이가 127(2^7)보다 크면 길이 정보와 함께 저장 (MSB:1)
      // MSB:0, data도 0인 경우는 end를 의미한다
      unsigned len;

      size++;

      if ((nodeID-lastNodeID)<127)
         len=1; else
         len=1+bytes(nodeID-lastNodeID);

      // Tuple too big or first element on the page?
      if ((bufferPos==headerSize)||(bufferPos+len>RPathSegment::pageSize)) {
         // Write the partial page
         if (bufferPos>headerSize) {
            /*if (!islocked) {
               // Critical section ***********
               pthread_mutex_lock(&mutex_lock);
               islocked = true;
            }*/
            writeUint32(buffer,page+1);
            memset(buffer+bufferPos,0,RPathSegment::pageSize-bufferPos);
            //byte+=bufferPos/1024;
            writePage(ofs_rf,page,buffer);
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
   //assert(bufferPos <= RPathSegment::pageSize);
//   for (unsigned index=bufferPos;index<RPathSegment::pageSize;index++)
//      buffer[index]=0;
   memset(buffer+bufferPos,0,RPathSegment::pageSize-bufferPos);
   byte+=bufferPos;
   /*
   if (!islocked) {
      // Critical section ***********
      pthread_mutex_lock(&mutex_lock);
      islocked = true;
      startPage = page;
   }*/
   writePage(ofs_rf,page,buffer);
   ++page;
   nextPage = page;
   //pthread_mutex_unlock(&mutex_lock);
   // *********** Critical section 
}
//---------------------------------------------------------------------------
static char* getStrPpath(vector<unsigned>& ppath, char *buf) {
   for (vector<unsigned>::const_iterator iter=ppath.begin(),limit=ppath.end();
        iter!=limit;++iter) {
      if (iter == ppath.begin())
         buf += sprintf(buf, "%u", (*iter));
      else
         buf += sprintf(buf, ",%u", (*iter));
   }
   return buf;
}
//---------------------------------------------------------------------------
static inline int cmpValue(uint64_t l,uint64_t r) { return (l<r)?-1:((l>r)?1:0); }
//---------------------------------------------------------------------------
RPathTreeIndex::Node* RPathTreeIndex::outputStat(PPath &ppath, 
                                 unsigned startPage, unsigned startIndex, unsigned byte,
                                 unsigned size, PPath *parentPpath, 
                                 RPathTreeIndex::Node* curNode)
{
   // Output statistics
   char buf[1024], *offset=buf;
   offset += sprintf(buf, "%u|", (unsigned) ppath.size());
   offset = getStrPpath(ppath.path, offset);
   offset += sprintf(offset, "|%u|%u|%u|%u|", startPage, startIndex, size, byte);
   if (parentPpath)
      sprintf(offset, "%s", parentPpath->getStr().c_str());
   ofs_stat << buf << endl;

   if (curNode) {
      curNode->startPage = startPage;
      curNode->startIndexPage = startIndex;
      curNode->cardinality = size;
      curNode->rpathSgm = NULL; /* new RPathSegment(&file, startPage, startIndex, startIndex-startPage);*/
      return curNode;
   }
   return InsertIntoIdx(startPage, startIndex, size, byte, ppath.getStr().c_str());
}
//---------------------------------------------------------------------------
void RPathTreeIndex::getNodeLists(Database& db, unsigned p, RPathTreeIndex::Node* node, 
                                  PPath& ppath, set<unsigned> &results)
{
   // Result
   //LOG(INFO) << "- start getNodeLists. p: " << p;

   if (ppath.size() == 0) {
      AggregatedIndexScan* scan;
      Register subject,predicate,object;
      subject.reset(); predicate.reset(); object.reset();

      if (p == ALLPREDICATE)
         scan = AggregatedIndexScan::create(db,Database::Order_Object_Predicate_Subject,
                  NULL,false,&predicate,false,&object,false, 0);
      else if (p == ALLPREDICATE+(REVERSE_PRED))
         scan = AggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,
                  &object,false,&predicate,false,NULL,false, 0);
      else {
         predicate.value = (p>=(REVERSE_PRED))?p-(REVERSE_PRED):p;
         scan = (p>=(REVERSE_PRED))?
                  AggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,
                     &object,false,&predicate,true,NULL,false, 0):
                  AggregatedIndexScan::create(db,Database::Order_Predicate_Object_Subject,
                     NULL,false,&predicate,true,&object,false, 0);
      }

      scan->first();
      results.insert(object.value);
      while (scan->next()) { 
         results.insert(object.value);
      }
      delete scan;
      return;
   }

   // p sgmt
   IndexScan* scan;
   Register subject,predicate,object, rpflt_value;
   subject.reset(); predicate.reset(); object.reset(); rpflt_value.reset();
   predicate.value = (p>=(REVERSE_PRED))?p-(REVERSE_PRED):p;
   if (p==ALLPREDICATE) {
      scan = IndexScan::create(db,Database::Order_Subject_Object_Predicate,
                               &subject,false,&predicate,false,&object,false, 0);
      scan->addMergeHint(&subject, &rpflt_value);
   }
   else if (p==ALLPREDICATE+(REVERSE_PRED)) {
      scan = IndexScan::create(db,Database::Order_Object_Subject_Predicate,
                               &object,false,&predicate,false,&subject,false, 0);
      scan->addMergeHint(&object, &rpflt_value);
   }
   else if (p>=(REVERSE_PRED)) {
      scan = IndexScan::create(db,Database::Order_Predicate_Object_Subject,
                               &object,false,&predicate,true,&subject,false, 0);
      scan->addMergeHint(&object, &rpflt_value);
   }
   else {
      scan = IndexScan::create(db,Database::Order_Predicate_Subject_Object,
                               &subject,false,&predicate,true,&object,false, 0);
      scan->addMergeHint(&subject, &rpflt_value);
   }

   unsigned nodeID;
   RPathSegment::Scan prefixScan;
   if(!prefixScan.first(*node->rpathSgm)) {
      delete scan;
      return;
   }
   rpflt_value.value = nodeID = prefixScan.getValue1();

   if (!scan->first()) {
      delete scan;
      return;
   }

   // Merge nodeIDLoader and scan
   while (true) {
      if (subject.value < nodeID) {
         if (!scan->next())
            break;
      }
      else if (subject.value > nodeID) {
         if (!prefixScan.next())
            break;
         rpflt_value.value = nodeID = prefixScan.getValue1();
      }
      else {
         // Find a match
         results.insert(object.value);
         if (!scan->next())
            break;
      }
   }

   //LOG(INFO) << "- end getNodeLists. p: " << p << " cnt:" << results.size();
   delete scan;
   return;
}
//---------------------------------------------------------------------------
vector<unsigned> RPathTreeIndex::getPlist(Database& db)
{
   // Get Predicate List
   Register subject,predicate,object;
   subject.reset(); predicate.reset(); object.reset();
   FullyAggregatedIndexScan* scan=
      FullyAggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,
            NULL,false,&predicate,false,NULL,false, 0);

   vector<unsigned> predList, predList2;
   vector<unsigned> ppath;

   if (scan->first()) {
      predList.push_back(predicate.value);
      // And all others
      while (scan->next()) {
         predList.push_back(predicate.value);
      }
   }
   delete scan;

   if (is_varp)
      predList.push_back(ALLPREDICATE);

//   if (is_reverse) { // we should always add reverse predicates for statistics
      for (vector<unsigned>::iterator iter=predList.begin(), limit=predList.end(); iter!=limit; ++iter) {
         assert ((*iter) < (REVERSE_PRED));
         predList2.push_back((*iter) + (REVERSE_PRED));
      }
      predList.insert(predList.end(), predList2.begin(), predList2.end());
//   }

   return predList;
}
//---------------------------------------------------------------------------
/*
static unsigned getBlkSize(RPathTreeIndex::NodeIDLoader& reader)
{
   const unsigned headerSize = 4; // Next pointer
   unsigned bufferPos=headerSize;

   unsigned lastNodeID=0, nodeID, blkCnt=0;

   while (reader.next(nodeID)) {
      // Try to pack it on the current page
      // 차이가 127(2^7-1)까지는 1 byte에 저장 (길이 정보 없이, MSB:0)
      // 차이가 127(2^7)보다 크면 길이 정보와 함께 저장 (MSB:1)
      // MSB:0, data도 0인 경우는 end를 의미한다
      unsigned len;

      if ((nodeID-lastNodeID)<127)
         len=1; else
         len=1+bytes(nodeID-lastNodeID);

      // Tuple too big or first element on the page?
      if ((bufferPos==headerSize)||(bufferPos+len>RPathSegment::pageSize)) {
         // Write the partial page
         if (bufferPos>headerSize) {
            blkCnt++;
         }
         // Write the first element fully
         bufferPos=headerSize;
         bufferPos+=4;
      } else {
         // No, pack them
         if ((nodeID-lastNodeID)<127) {
            bufferPos++;
         } else {
            bufferPos++;
            unsigned value = nodeID-lastNodeID;
            if (value>=(1<<24)) {
               bufferPos += 4;
            } else if (value>=(1<<16)) {
               bufferPos += 3;
            } else if (value>=(1<<8)) {
               bufferPos += 2;
            } else {
               bufferPos += 1;
            }
         }
      }
      // Update the values
      lastNodeID=nodeID;
   }
   // Flush the last page
   blkCnt++;

   return blkCnt;
} */
//---------------------------------------------------------------------------
void RPathTreeIndex::rpathBuild(Database& db, unsigned maxL)
{
   unsigned startPage = 0, nextPage = 0, byte = 0;
   std::queue<PPath> queue;
   PPath nullPpath;

   LOG(INFO) << "Start building RPathFilter";

   vector<unsigned> plist = getPlist(db);
   LOG(INFO) << "the number of predicates: " << plist.size();

   unsigned iteration=0, maxSize=0, vlistcnt=0, prev_len=0;
   queue.push(nullPpath);

   while (!queue.empty()) {
      PPath ppath = queue.front();
      queue.pop();

      LOG(INFO) << "building iteration: " << ++iteration;

      unsigned len = ppath.size();

      if (len > prev_len) {
         reopen();
         prev_len=len;
      }

      RPathTreeIndex::Node* node = SearchNode(ppath);
      node->rpathSgm = new RPathSegment(&file, node->startPage, node->startIndexPage,
            node->startIndexPage-node->startPage);

      for (std::vector<unsigned>::iterator iter=plist.begin();
           iter!=plist.end();) {
         unsigned p = *iter;

         PPath newPpath(p, ppath);

         if (len > 0) {
            unsigned lastPred = ppath.path[ppath.size()-1];
            if ((p < REVERSE_PRED && p + REVERSE_PRED == lastPred) ||
                (p >= REVERSE_PRED && p - REVERSE_PRED == lastPred)) {
               iter++;
               continue;
            }
         }

         // check THRESHOLD_MINBLKCNT 
         RPathTreeIndex::Node *suffixNode = getSuffixNode(newPpath);
         if (ppath.size() > 0) {
            if (suffixNode == NULL || (suffixNode && suffixNode->len != len)) {
               iter++;
               continue;
            }
            if (suffixNode->cardinality < getMinCnt(len+1)) {
               iter++;
               continue;
            }
         }

         set<unsigned> results;

         getNodeLists(db, p, node, ppath, results);
         vlistcnt++;

         unsigned size = results.size();
         if (maxSize < size)
            maxSize = size;
//         LOG(INFO) << "- ppath: " << newPpath.getStr() << " size:" << size << " maxSize:" << maxSize;

         if (size == 0) {
            iter++;
            continue;
         }

         // get (N-1) suffix 
         bool add = false;
         if (isDiscriminative(suffixNode, len+1, size)) {
            NodeIDLoader reader(results);
            packFullyNodeIDLeaves(reader, size, startPage, nextPage, byte);
            outputStat(newPpath, startPage, nextPage, byte, size, NULL, NULL);
            add = true;
            if (newPpath.size() < maxL)
               queue.push(newPpath);
            /*
            if (suffixNode) 
            cout << newPpath.getStr() << " is discriminative. blkcnt:" << nextPage - startPage 
                 << " size: " << size << " suffixSize:" << suffixNode->cardinality 
                 << " th_diff:" << th_diff << " real_diff:" << (double) size/ (double) suffixNode->cardinality << endl;
                 */
         }

         if (ppath.size() == 0) {
            if (size < getMinCnt(len+1)) {
               iter = plist.erase(iter);
               continue;
            }
         }

         iter++;
      }
      delete node->rpathSgm;
      node->rpathSgm=NULL;
   };
   close();
   cout << "vlist cnt:" << vlistcnt << endl;
}
//---------------------------------------------------------------------------
void RPathTreeIndex::close()
{
   ofs_rf.flush();
   ofs_stat.flush();
   ofs_rf.close();
   ofs_stat.close();
}
//---------------------------------------------------------------------------
bool RPathTreeIndex::isDiscriminative(RPathTreeIndex::Node* suffixNode, 
                                      unsigned /*len*/, unsigned size)
{
   if (th_diff == 1 && th_mincnt == 0)
      return true;

   if (!suffixNode)
      return true;

   if ((double) size / (double)suffixNode->cardinality < th_diff)
      return true; 

   return false;
}
//---------------------------------------------------------------------------
void RPathTreeIndex::rpathUpdate(Database& db, unsigned maxL, 
                                 RPathUpdateInfo& inserted, RPathUpdateInfo& deleted)
{
   unsigned startPage = 0, nextPage = lastPage, byte = 0;
   std::queue<PPath> queue;
   PPath nullPpath;

   ofs_rf.open(fileName, ios::out|ios::app|ios::binary);
   ofs_stat.open(statName, ios::out|ios::trunc);

   LOG(INFO) << "Start updating RPathFilter";

   vector<unsigned> plist = getPlist(db);
   LOG(INFO) << "the number of predicates: " << plist.size();

   unsigned iteration=0, maxSize=0;
   queue.push(nullPpath);

   for(set<unsigned>::iterator iter=inserted.preds.begin(),
       limit=inserted.preds.end(); iter!=limit; iter++)
      LOG(INFO) << "inserted predicates: " << *iter;
   for(set<unsigned>::iterator iter=deleted.preds.begin(),
       limit=deleted.preds.end(); iter!=limit; iter++)
      LOG(INFO) << "deleted predicates: " << *iter;

   Timestamp start;
   unsigned updatedVlist=0, prev_len=0;
   while (!queue.empty()) {
      std::vector<PPath> ppaths;

      PPath ppath = queue.front();
      queue.pop();

      LOG(INFO) << "updating iteration: " << ++iteration;

      bool isPInInserted, isPInDeleted; 
      RPathTreeIndex::Node *prefixNode = SearchNode(ppath);
      if (prefixNode==&root){
         // null ppath인 경우임
         prefixNode=NULL;
      }

      unsigned len = ppath.size();
      if (len > prev_len) {
         reopen();
         prev_len=len;
      }
      RPathTreeIndex::Node* node = SearchNode(ppath);
      node->rpathSgm = new RPathSegment(&file, node->startPage, node->startIndexPage,
            node->startIndexPage-node->startPage);

      for (std::vector<unsigned>::iterator iter=plist.begin();
           iter!=plist.end();) {
         unsigned p = *iter;
         //LOG(INFO) << "- predicate: " << p;

         PPath newPpath = ppath;
         newPpath.Add(p);

         if (len > 0) {
            unsigned lastPred = ppath.path[ppath.size()-1];
            if ((p < REVERSE_PRED && p + REVERSE_PRED == lastPred) ||
                  (p >= REVERSE_PRED && p - REVERSE_PRED == lastPred)) {
               iter++;
               continue;
            }
         }

         // check THRESHOLD_MINBLKCNT  - shortcut
         RPathTreeIndex::Node *suffixNode = getSuffixNode(newPpath);
         if (ppath.size() > 0) {
            if (suffixNode == NULL ||
                suffixNode->len != ppath.size()) {
               iter++;
               //LOG(INFO) << "ppath: " << newPpath.getStr() << " skipped. 1. " << p;
               continue;
            }
            /*
            else if (th_mincnt > 0 && 
                     suffixNode->startIndexPage - suffixNode->startPage <= th_mincnt+len-1) {
               iter++;
               LOG(INFO) << "ppath: " << newPpath.getStr() << " skipped. 2. " << p;
               continue;
            }
            */
         }

         // Checking update conditions
         isPInInserted = inserted.preds.count(p);
         isPInDeleted = deleted.preds.count(p);

         RPathTreeIndex::Node *curNode = SearchNode(newPpath);
         set<unsigned> results;
         bool newlyCreated=true;
         if (!prefixNode) {
            // 길이 1짜리 predicate path
            if (isPInDeleted || isPInInserted) {
               getNodeLists(db, p, node, ppath, results);
            }
            else
               newlyCreated=false;
         }
         else {
            if (!isPInDeleted && !prefixNode->updateInfo->hasDeleted) {
               // Delta를 체크해 계산
               bool hasInserted=prefixNode->updateInfo->insertDelta.size();
               if (isPInInserted && hasInserted) {
                  // TODO::
                  getNodeLists(db, p, node, ppath, results);
               }
               else if (!isPInInserted && hasInserted) {
                  // TODO:: non-discriminative인 경우
                  getNodeListsFromDelta(db, p, prefixNode->updateInfo->insertDelta, curNode, results);
               }
               else if (isPInInserted && !hasInserted) {
                  // TODO:: non-discriminative인 경우
                  getNodeListsFromInserted(p, ppath, node, curNode, inserted.triples_pso[p], results);
               }
               else if (!isPInInserted && !hasInserted) {
                  // non-discriminative여서 없었던 경우인지 체크하자
                  if (suffixNode->updateInfo->isUpdated & !curNode) {
                     getNodeLists(db, p, node, ppath, results);
                  }
                  else {
                     // none
                     newlyCreated=false;
                  }
               }
            }
            else { // rebuild
               getNodeLists(db, p, node, ppath, results);
            }
         }

         bool discriminative = false;
         unsigned size = results.size();

         if (newlyCreated) {
            // for debugging
            if (maxSize < size)
               maxSize = size;
//            LOG(INFO) << "- ppath: " << newPpath.getStr() << " size:" << size << " maxSize:" << maxSize;

            if (size == 0) {
               iter++;
               continue;
            }
            updatedVlist++;
            if (isDiscriminative(suffixNode, newPpath.size(), size)) {
               // discriminative
               // store the new Vlist in new segment
               NodeIDLoader reader(results);
               packFullyNodeIDLeaves(reader, size, startPage, nextPage, byte);
               
               RPathSegment *oldSgm;
               if (curNode) {
                  oldSgm = new RPathSegment(&file, curNode->startPage, curNode->startIndexPage,
                                            curNode->startIndexPage-curNode->startPage);
               }
               else oldSgm = NULL; 

               curNode = outputStat(newPpath, startPage, nextPage, byte, size, NULL, curNode);
               curNode->updateInfo->isUpdated = true;
               discriminative = true;

               // compute the delta
               if (oldSgm) {
                  computeDelta(curNode, results, oldSgm);
                  delete oldSgm;
               }
               else {
                  // newly created V-list
                  curNode->updateInfo->insertDelta.assign(results.begin(), results.end());
               }
            }
         }
         else if (curNode) {
            // 여기는 curNode가 있는 상태임
            // Vlist에 변화는 없으나 discriminative인지는 다시 확인해야 함
            // suffixNode가 변경된경우
            if (isDiscriminative(suffixNode, newPpath.size(), curNode->cardinality)) {
               discriminative = true;
               outputStat(newPpath, curNode->startPage, curNode->startIndexPage, 0,
                          curNode->cardinality, NULL, curNode);
            }
            // delta는 없다
         }

         if (ppath.size() == 0 && size < th_mincnt) {
            iter = plist.erase(iter);
            // we regard 1-length ppath as discriminative, always.
            // so, we push newPpath into queue
            queue.push(newPpath);
            continue;
         }

         if (discriminative && newPpath.size() < maxL)
            queue.push(newPpath);
         iter++;
      }
   };
   close();

   Timestamp stop;

   cout << "Execution time: " << (stop-start) << " ms" << endl;
   cout << "Updated Vlist: " << updatedVlist << endl;
}
//---------------------------------------------------------------------------
void RPathTreeIndex::computeDelta(RPathTreeIndex::Node* curNode, set<unsigned> results,
                                  RPathSegment* oldSgm)
{
   vector<unsigned> &inserted = curNode->updateInfo->insertDelta;

   RPathSegment::Scan oldScan;
   unsigned curNodeID, oldNodeID;

   NodeIDLoader curScan(results);
   curScan.next(curNodeID);
   oldScan.first(*oldSgm);
   oldNodeID = oldScan.getValue1();

   bool isCurScanEnd=false, isOldScanEnd=false;
   while(true) {
      if (curNodeID > oldNodeID) {
         curNode->updateInfo->hasDeleted = true;
         if (!oldScan.next()) {
            break;
         }
         oldNodeID = oldScan.getValue1();
      }
      else if (curNodeID < oldNodeID) {
         inserted.push_back(curNodeID);
         if (!curScan.next(curNodeID)) {
            curNode->updateInfo->hasDeleted = true;
            isCurScanEnd=true;
            break;
         }
      }
      else {
         isCurScanEnd = !curScan.next(curNodeID);
         isOldScanEnd = !oldScan.next();
         oldNodeID = oldScan.getValue1();

         if (isCurScanEnd && !isOldScanEnd)
            curNode->updateInfo->hasDeleted = true;
         
         if (isCurScanEnd || isOldScanEnd) {
            break;
         }
      }
   }

   // curScan에 남은 노드는 모두 inserted로
   while(!isCurScanEnd) {
      inserted.push_back(curNodeID);
      if(!curScan.next(curNodeID))
         isCurScanEnd=true;
   }

/*
   LOG(INFO) << "compute delta: " << curNode->ppath.getStr() 
             << ", inserted: " << inserted.size() 
             << ", hasDeleted: " << curNode->updateInfo->hasDeleted;
             */
}
//---------------------------------------------------------------------------
void RPathTreeIndex::getNodeListsFromDelta(Database& db, unsigned p, vector<unsigned> delta, 
      RPathTreeIndex::Node *curNode, set<unsigned> &results)
{
   // Result
//   LOG(INFO) << "- start getNodeListsFromDelta. p: " << p;

   // p sgmt
   IndexScan* scan;
   Register subject,predicate,object, rpflt_value;
   subject.reset(); predicate.reset(); object.reset(); rpflt_value.reset();
   predicate.value = (p>=(REVERSE_PRED))?p-(REVERSE_PRED):p;
   if (p==ALLPREDICATE) {
      scan = IndexScan::create(db,Database::Order_Subject_Object_Predicate,
                               &subject,false,&predicate,false,&object,false, 0);
      scan->addMergeHint(&subject, &rpflt_value);
   }
   else if (p==ALLPREDICATE+(REVERSE_PRED)) {
      scan = IndexScan::create(db,Database::Order_Object_Subject_Predicate,
                               &object,false,&predicate,false,&subject,false, 0);
      scan->addMergeHint(&object, &rpflt_value);
   }
   else if (p>=(REVERSE_PRED)) {
      scan = IndexScan::create(db,Database::Order_Predicate_Object_Subject,
                               &object,false,&predicate,true,&subject,false, 0);
      scan->addMergeHint(&object, &rpflt_value);
   }
   else {
      scan = IndexScan::create(db,Database::Order_Predicate_Subject_Object,
                               &subject,false,&predicate,true,&object,false, 0);
      scan->addMergeHint(&subject, &rpflt_value);
   }
   if (!scan->first()) {
      delete scan;
      return;
   }

   if (curNode) {
      RPathSegment::Scan oldScan;
      curNode->rpathSgm = new RPathSegment(&file, curNode->startPage, curNode->startIndexPage,
                                           curNode->startIndexPage-curNode->startPage);
      if(!oldScan.first(*curNode->rpathSgm)) {
         delete scan;
         delete curNode->rpathSgm;
         return;
      }
      while (true) {
         unsigned nodeID = oldScan.getValue1();
         results.insert(nodeID);
         if(!oldScan.next()) {
            break;
         }
      }
      delete curNode->rpathSgm;
   }

   unsigned deltaNodeID;
   vector<unsigned>::iterator iter=delta.begin(), limit=delta.end();
   deltaNodeID=*iter;

   // Merge nodeIDLoader and scan
   while (true) {
      if (subject.value < deltaNodeID) {
         if (!scan->next())
            break;
      }
      else if (subject.value > deltaNodeID) {
         iter++;
         if (iter==limit)
            break;
         deltaNodeID=*iter;
      }
      else {
         // Find a match
         results.insert(object.value);
         if (!scan->next())
            break;
      }
   }

//   LOG(INFO) << "- end getNodeListsFromDelta. p: " << p << " cnt:" << results.size();
   delete scan;
   return;
}
//---------------------------------------------------------------------------
void RPathTreeIndex::getNodeListsFromInserted(unsigned /*p*/, PPath& /*ppath*/, 
      RPathTreeIndex::Node *node,
      RPathTreeIndex::Node *curNode, vector<struct Triple_s> *insertedTriples, set<unsigned> &results)
{
   // Result
//   LOG(INFO) << "- start getNodeListsFromInserted. p: " << p;

   if (curNode) {
      RPathSegment::Scan oldScan;
      curNode->rpathSgm = new RPathSegment(&file, curNode->startPage, curNode->startIndexPage,
                                           curNode->startIndexPage-curNode->startPage);
      if(!oldScan.first(*curNode->rpathSgm)) {
         delete curNode->rpathSgm;
         return;
      }
      while (true) {
         unsigned nodeID = oldScan.getValue1();
         results.insert(nodeID);
         if(!oldScan.next()) {
            break;
         }
      }
      delete curNode->rpathSgm;
   }

   unsigned nodeID;
//   RPathTreeIndex::Node* node = SearchNode(ppath);
   RPathSegment::Scan prefixScan;
//   node->rpathSgm = new RPathSegment(&file, node->startPage, node->startIndexPage,
//                                     node->startIndexPage-node->startPage);
   if(!prefixScan.first(*node->rpathSgm)) {
//      delete node->rpathSgm;
      return;
   }
   nodeID = prefixScan.getValue1();

   vector<struct Triple_s>::iterator iter=insertedTriples->begin(),
                                     limit=insertedTriples->end();
   unsigned subject, object;
   subject = (*iter).s;
   object = (*iter).o;

   // Merge nodeIDLoader and scan
   while (true) {
      if (subject < nodeID) {
         iter++;
         if (iter==limit)
            break;
         subject = (*iter).s;
         object = (*iter).o;
      }
      else if (subject > nodeID) {
         if (!prefixScan.next())
            break;
         nodeID = prefixScan.getValue1();
      }
      else {
         // Find a match
         results.insert(object);
         iter++;
         if (iter==limit)
            break;
         subject = (*iter).s;
         object = (*iter).o;
      }
   }

//   delete node->rpathSgm;
//   LOG(INFO) << "- end getNodeListsFromInserted. p: " << p << " cnt:" << results.size();
   return;
}
//---------------------------------------------------------------------------
double RPathTreeIndex::getMinCnt(unsigned length) {
   return (double) pow(((double) (length-1)/(double)maxL),2) * (double) th_mincnt;
}
//---------------------------------------------------------------------------
