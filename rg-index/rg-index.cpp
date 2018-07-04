#include "rts/operator/FullyAggregatedIndexScan.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rg-index/rg-index.hpp"
#include "gspan/dfs.hpp"
#include <cassert>
#include <algorithm>
#include <glog/logging.h>

//---------------------------------------------------------------------------
typedef struct {
   unsigned predicate;
   unsigned support;
   unsigned max;
   unsigned edgecnt;
} predicate_support_t;
//---------------------------------------------------------------------------
static bool compare1(predicate_support_t c1, predicate_support_t c2) {
   return (c1.edgecnt > c2.edgecnt); // desc: >
}
//---------------------------------------------------------------------------
void RGindex::getDataFileName(char *buf, char* path, char *dataset)
{
   sprintf(buf, "%s/%s.data", path, dataset);
};
//---------------------------------------------------------------------------
void RGindex::getStatFileName(char *buf, char* path, char *dataset)
{
   sprintf(buf, "%s/%s.stat", path, dataset);
};
//---------------------------------------------------------------------------
RGindex::RGindex(char *dataset, char *path) {
   // open for building

   memset(dataName, 0, 256);
   memset(statName, 0, 256);

   getDataFileName(dataName, path, dataset);
   getStatFileName(statName, path, dataset);

   cout << dataName << endl;
   cout << statName << endl;

   ofs_data.open(dataName, ios::out|ios::trunc|ios::binary);
   if (ofs_data.fail()) {
      cerr << "couldn't make file " << dataName << endl; 
      exit(1);
   }
   ofs_stat.open(statName, ios::out|ios::trunc);
   if (ofs_stat.fail()) {
      cerr << "couldn't make file " << statName << endl; 
      exit(1);
   }

   page = 0;
}
//---------------------------------------------------------------------------
RGindex::RGindex(char *dataset, char *path, unsigned /*maxL*/) {
   // open for querying 

   memset(dataName, 0, 256);
   memset(statName, 0, 256);

   getDataFileName(dataName, path, dataset);
   getStatFileName(statName, path, dataset);

   cout << dataName << endl;
   cout << statName << endl;

   if (!file.open(dataName)) {
      cerr << "Fail to open file " << dataName << endl; 
      exit(1);
   }

   ifstream ifs_stat(statName, ios::in);

   char buf[1024];
   memset(buf, 0, 1024);

   // read predicate count
   ifs_stat >> buf;
   unsigned pred;
   sscanf(buf, "Predicate:%u", &pred);

   unsigned oldID, newID;
   for (unsigned i=0; i<pred; i++) {
      ifs_stat >> buf;
      sscanf(buf, "%u|%u", &newID, &oldID);
      predMap_new[newID]=oldID;
      predMap_old[oldID]=newID;
      //cout << oldID << "->" << newID << endl;
   }

   char buf2[10], buf3[10];
   unsigned len, source, dest, offset, card;

   ifs_stat >> len;
   while(!ifs_stat.eof()) {
      ifs_stat.ignore(12);
      GSPAN::DFSCode dfscode;
      Node *newNode=new Node();
      unsigned offsetlen=1;

      for(unsigned i=0; i<len; i++) {
         ifs_stat >> source;
         ifs_stat.ignore(2);
         ifs_stat >> dest;
         ifs_stat.ignore(2);
         ifs_stat >> pred;
         ifs_stat.ignore(2);
         ifs_stat.get(buf2, 3); // direction
         ifs_stat.ignore(3);
         ifs_stat.get(buf3, 2); // edge type: f or b 
         ifs_stat.ignore(3);

       //  cout << source << " " << dest << " " << pred << " " << buf2 << " ";

         if (strcmp(buf2, "->")==0)
            dfscode.push(source, dest, pred, GSPAN::EDGE_TYPE_NORMAL);
         else 
            dfscode.push(source, dest, pred, GSPAN::EDGE_TYPE_REVERSE);

         if (buf3[0]=='f') 
            offsetlen++;
      }
      // cardinality 
      for(unsigned i=0; i<offsetlen; i++) {
         ifs_stat >> card;
       //  cout << card << " ";
         newNode->cardinalities.push_back(card);
      }
      ifs_stat.ignore(2);
      // offsets
      for(unsigned i=0; i<offsetlen; i++) {
         ifs_stat >> offset;
       //  cout << offset << " ";
         newNode->offsets.push_back(offset);
         newNode->segments.push_back(NULL);
      }
      ifs_stat.ignore(2);
      // blks
      for(unsigned i=0; i<offsetlen; i++) {
         ifs_stat >> offset;
       //  cout << offset << " ";
         newNode->blks.push_back(offset);
      }

      ifs_stat >> len;

      InsertIntoTree(&root, dfscode, 0, newNode);
   }
   page = 0;
}
//---------------------------------------------------------------------------
void RGindex::insert(GSPAN::DFSCode &dfscode, InterResultTuple &results) 
{
   // materialize the vlists
   Node *newNode=new Node();

   results.materializeVlists(ofs_data, page);

   // output to stat
   // dfscode size | dfs code | offsets....
   ofs_stat << dfscode.size() << '|' << dfscode << "|";

   for (vector<unsigned>::iterator iter=results.cards.begin(),
        limit=results.cards.end(); iter!=limit; iter++) {
      ofs_stat << (*iter) << " ";
   }
   ofs_stat << "|";

   for (vector<unsigned>::iterator iter=results.offsets.begin(),
        limit=results.offsets.end(); iter!=limit; iter++) {
      ofs_stat << (*iter) << " ";
   }
   ofs_stat << "|";

   for (vector<unsigned>::iterator iter=results.blks.begin(),
        limit=results.blks.end(); iter!=limit; iter++) {
      ofs_stat << (*iter) << " ";
   }
   ofs_stat << endl;

   InsertIntoTree(&root, dfscode, 0, newNode);
}
//---------------------------------------------------------------------------
void RGindex::close()
{
   ofs_data.flush();
   ofs_stat.flush();
   ofs_data.close();
   ofs_stat.close();
}
//---------------------------------------------------------------------------
void RGindex::InsertIntoTree(Node* root, GSPAN::DFSCode dfscode, unsigned level,
                             Node* newNode)
{
   if (level==dfscode.size()) assert(false);

   map<GSPAN::DFS, Node*>::iterator it;
   it = root->children.find(dfscode[level]);

   if (it!=root->children.end()) {
      InsertIntoTree(it->second, dfscode, level+1, newNode);
      return;
   }

   if (level != dfscode.size() - 1) {
      // the parent node is not inserted because it is not discriminative
      // let's ignore this node
      return;
   }
   root->children.insert(pair<GSPAN::DFS,Node*>(dfscode[level], newNode));
}
//---------------------------------------------------------------------------
void RGindex::build(Database &db, unsigned maxL, unsigned minSup)
{
   this->minSup=minSup;
   this->maxL=maxL;
   this->db=&db;

   // Get Predicate List
   Register subject1,predicate1,object1;
   Register subject2,predicate2,object2;
   subject1.reset(); predicate1.reset(); object1.reset();
   subject2.reset(); predicate2.reset(); object2.reset();
   AggregatedIndexScan* scan1=
      AggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,
            &subject1,false,&predicate1,false,NULL,false, 0);
   AggregatedIndexScan* scan2=
      AggregatedIndexScan::create(db,Database::Order_Predicate_Object_Subject,
            NULL,false,&predicate2,false,&object2,false, 0);

   vector<unsigned> predList;
   vector<unsigned> ppath;
   unsigned maxEdgecnt=0, cnt;

   cnt=scan1->first();
   scan2->first();
   bool isEnd=false;
   unsigned prev_p1, prev_p2;
   vector<predicate_support_t> plist;

   LOG(INFO) << "Retrieving the predicate list..." << endl;
   unsigned maxEdgeCnt=atoi(getenv("MAXEDGECNT"));

   while (!isEnd) {
      unsigned cnt1=0, cnt2=0,edgecnt=0;
      predicate_support_t pred_sup;

      prev_p1=predicate1.value;
      prev_p2=predicate2.value;
      assert(prev_p1==prev_p2);
      // get support
      while (prev_p1==predicate1.value) {
         cnt1++; // number of Subject
         edgecnt+=cnt;
         cnt=scan1->next();
         if (!cnt) {
            isEnd=true;
            break;
         }
      }
      while (prev_p2==predicate2.value) {
         cnt2++; // number of Object
         if (!scan2->next()) {
            assert(isEnd);
            break;
         }
      }

      if (prev_p1==9) // lubm
         continue; 
      // Skip too frequent predicates
      if (edgecnt >= maxEdgeCnt)
         continue;

      pred_sup.predicate=prev_p1; 
      pred_sup.support=min(cnt1, cnt2);
      pred_sup.edgecnt=edgecnt;
      maxEdgecnt=max(maxEdgecnt,edgecnt);

      if (pred_sup.support < minSup)
         continue;

      plist.push_back(pred_sup);
   }
   delete scan1;
   delete scan2;

   sort(plist.begin(), plist.end(), compare1);

   // Make predicate mapping table
   unsigned predID=0;
   ofs_stat << "Predicate:" << plist.size() << endl;
   cout << maxEdgecnt << endl;
   for(vector<predicate_support_t>::const_iterator iter=plist.begin(), end=plist.end();
       iter!=end; iter++) {
      LOG(INFO) << "Predicate:" << (*iter).predicate << " Support:" << (*iter).support 
                << " edgecnt:" << (*iter).edgecnt;
      predMap_new[predID]=(*iter).predicate;
      predMap_old[(*iter).predicate]=predID;
      suppMap[predID]=(*iter).support;
      ofs_stat << predID << "|" << (*iter).predicate << endl;
      predID++;
   }

   set<unsigned> rmvertices;
   rmvertices.insert(0);
   rmvertices.insert(1);

   for(map<unsigned, unsigned>::iterator iter=predMap_new.begin(), end=predMap_new.end(); 
       iter!=end;) {
      GSPAN::DFSCode dfscode;
      unsigned p=(*iter).first, old_p=(*iter).second;
      InterResultTuple results(NULL);
      results.initWithOneSizeGraph(db, dfscode, p, old_p);
      insert(dfscode, results);
      subgraphMining(maxL, dfscode, results);
      // project
      predMap_new.erase(iter++);
   }

   close();
}
//---------------------------------------------------------------------------
void RGindex::getPossiblePredicates(hash_tbl_t *hash_tbl, set<unsigned> &forward_preds,
                                    set<unsigned> &backward_preds, unsigned curMinSup) 
{
   Register subject,predicate,object,elistReg;
   subject.reset(); predicate.reset(); object.reset(); elistReg.reset();

   // forward 
   {
      AggregatedIndexScan* scan= 
         AggregatedIndexScan::create(*db,Database::Order_Subject_Predicate_Object,
               &subject,false,&predicate,false,NULL,false, 0);
      scan->addMergeHint(&subject, &elistReg);
      unsigned v1=0, v2;

      VertexTupleLoader vloader(hash_tbl);
      vloader.next(v1);
      elistReg.value=v1;
      scan->first(); 
      v2=subject.value;
      bool isEnd=false;
      while(!isEnd) {
         if (v1==v2) {
            if (predMap_old.find(predicate.value)!=predMap_old.end()) {
               unsigned newP=predMap_old[predicate.value];
               if(suppMap[newP]>=curMinSup /* size-increasing support */) {
                  forward_preds.insert(newP);
               }
            }
            if (!scan->next()) isEnd=true;
            v2=subject.value;
         }
         if (v1>v2) {
            if (!scan->next()) isEnd=true; 
            v2=subject.value;
         }
         if (v1<v2) {
            if (!vloader.next(v1)) isEnd=true; 
            elistReg.value=v1;
         }
      }
      delete scan;
   }

   // backward
   {
      AggregatedIndexScan* scan= 
         AggregatedIndexScan::create(*db,Database::Order_Object_Predicate_Subject,
               NULL,false,&predicate,false,&object,false, 0);
      scan->addMergeHint(&object, &elistReg);
      unsigned v1=0, v2;
      VertexTupleLoader vloader(hash_tbl);
      vloader.next(v1);
      elistReg.value=v1;
      scan->first(); 
      v2=object.value;
      bool isEnd=false;
      while(!isEnd) {
         if (v1==v2) {
            if (predMap_old.find(predicate.value)!=predMap_old.end()) {
               unsigned newP=predMap_old[predicate.value];
               if (suppMap[newP]>=curMinSup /* size-increasing support */) {
                  backward_preds.insert(newP);
               }
            }
            if (!scan->next()) isEnd=true;
            v2=object.value;
         }
         if (v1>v2) {
            if (!scan->next()) isEnd=true; 
            v2=object.value;
         }
         if (v1<v2) {
            if (!vloader.next(v1)) isEnd=true; 
            elistReg.value=v1;
         }
      }
      delete scan;
   }
}
//---------------------------------------------------------------------------
void RGindex::subgraphMining(unsigned maxL, GSPAN::DFSCode dfscode, InterResultTuple &old_results) 
{
   VLOG(1) << "subgraphMining. old_result.offsets:" << old_results.offsets.size();
   // get the right most path
   GSPAN::RMPath rmpath=dfscode.buildRMPath();

   unsigned newVertexID=dfscode.nodeCount();

   vector<unsigned> rmvertices;
   cout << "rmvertices: ";
   for (unsigned i=0; i<rmpath.size(); i++) {
      rmvertices.push_back(dfscode[rmpath[i]].dest);
      cout << dfscode[rmpath[i]].dest << " ";
   }
   rmvertices.push_back(dfscode[rmpath[rmpath.size()-1]].src);
   cout << dfscode[rmpath[rmpath.size()-1]].src << endl;

   // conduct right most extension
   unsigned curMinSup=minSup*(dfscode.size()+1);
   // before extension, we check the support
   if (old_results.support<curMinSup) {
      VLOG(1) << "Old Infrequent Pattern!. " << "minSup:" << curMinSup
         << " Support:" << old_results.support;
      return;
   }

   for (vector<unsigned>::const_iterator iter=rmvertices.begin(), end=rmvertices.end(); 
        iter!=end; iter++) {
      unsigned source=(*iter);

      // make hash table
      cout << "make hashtbl... "; cout.flush();
      hash_tbl_t hash_tbl; 
      hash_tbl_t::iterator hash_tbl_iter;
      for(table_t::iterator iter2=old_results.results.begin(),
          limit2=old_results.results.end(); iter2!=limit2; iter2++) {
         Tuple *tuple=*iter2;
         unsigned key=tuple->getColumn_i(source,old_results.tuple_size);
         assert(tuple);

         hash_tbl_iter=hash_tbl.find(key);
         if (hash_tbl_iter==hash_tbl.end()) {
            table_t *tbl=new table_t;
            assert(tbl->size()==0);
            tbl->push_back(tuple);
            hash_tbl.insert(pair<unsigned, table_t *>(key, tbl)); 
         }
         else {
            (*hash_tbl_iter).second->push_back(tuple);
         }
      }
      cout << "finish. size:" << hash_tbl.size() << endl; cout.flush();

      // get possible predicates 
      set<unsigned> forward_preds, backward_preds;
      getPossiblePredicates(&hash_tbl, forward_preds, backward_preds, curMinSup);
      VLOG(1) << "source:" << source << ", forward_preds:" << forward_preds.size()
              << ", backward_preds:" << backward_preds.size();

      // add forward edges
      for (set<unsigned>::iterator iter2=forward_preds.begin(),
           end2=forward_preds.end(); iter2!=end2; iter2++) {
         unsigned pred=(*iter2);
         if (dfscode.hasSameDFS(source, pred, GSPAN::EDGE_TYPE_NORMAL))
            continue;
         dfscode.push(source, newVertexID, pred, GSPAN::EDGE_TYPE_NORMAL);
         VLOG(1) << "New DFS Code: ";
         VLOG(1) << dfscode;
         if (!dfscode.is_min()) { 
            VLOG(1) << "Non-minimum DFS Code!!!";   
            GSPAN::Graph g;
            GSPAN::DFSCode dfscode2;
            dfscode.toGraph(g);
            dfscode2.fromGraph(g);
            VLOG(1) << "This is the minimum DFS Code!!!";
            VLOG(1) << dfscode2;
            dfscode.pop();
            continue;
         }
         cout << dfscode << endl;

         // extension
         InterResultTuple new_results(&old_results);
         if (!new_results.addForwardEdge(*db, hash_tbl, predMap_new[pred], false)) {
            VLOG(1) << "Too large results!! We give up.";
            dfscode.pop();
            continue;
         }
         if (new_results.support<curMinSup) {
            VLOG(1) << "Infrequent Pattern!. " << "minSup:" << curMinSup
                    << " Support:" << new_results.support;
            dfscode.pop();
            continue;
         }
         VLOG(1) << "Frequent Pattern!. " << "minSup:" << curMinSup 
                 << " Support:" << new_results.support;
         insert(dfscode, new_results);

         if (maxL>dfscode.size())
            subgraphMining(maxL, dfscode, new_results);

         // Backward extension
         if (iter==rmvertices.begin()) {
            for(unsigned dest=(*iter)-1; dest<=0; dest--) { 
               dfscode.pop();
               dfscode.push(source, dest, pred, GSPAN::EDGE_TYPE_NORMAL);

               VLOG(1) << "Backward expansion. New DFS Code: ";
               VLOG(1) << dfscode;
               if (!dfscode.is_min()) { 
                  VLOG(1) << "Non-minimum DFS Code!!!";   
                  GSPAN::Graph g;
                  GSPAN::DFSCode dfscode2;
                  dfscode.toGraph(g);
                  dfscode2.fromGraph(g);
                  VLOG(1) << "This is the minimum DFS Code!!!";
                  VLOG(1) << dfscode2;
                  continue;
               }
               cout << "Backward expansion. " << dfscode << endl;
               InterResultTuple back_new_results(&old_results);
               back_new_results.addBackwardEdge(new_results, dest, newVertexID);

               if (back_new_results.support<curMinSup) {
                  VLOG(1) << "Infrequent Pattern!. " << "minSup:" << curMinSup
                     << " Support:" << back_new_results.support;
                  continue;
               }
               VLOG(1) << "Frequent Pattern!. " << "minSup:" << curMinSup 
                       << " Support:" << back_new_results.support;
               insert(dfscode, back_new_results);

               if (maxL>dfscode.size())
                  subgraphMining(maxL, dfscode, back_new_results);
            }
         }

         dfscode.pop();
      }
      for (set<unsigned>::iterator iter2=backward_preds.begin(),
           end2=backward_preds.end(); iter2!=end2; iter2++) {
         unsigned pred=(*iter2);
         if (dfscode.hasSameDFS(source, pred, GSPAN::EDGE_TYPE_REVERSE))
            continue;
         dfscode.push(source, newVertexID, pred, GSPAN::EDGE_TYPE_REVERSE);
         VLOG(1) << "New DFS Code: ";
         VLOG(1) << dfscode;
         if (!dfscode.is_min()) { 
            VLOG(1) << "Non-minimum DFS Code!!!";   
            GSPAN::Graph g;
            GSPAN::DFSCode dfscode2;
            dfscode.toGraph(g);
            dfscode2.fromGraph(g);
            VLOG(1) << "This is the minimum DFS Code!!!";
            VLOG(1) << dfscode2;
            dfscode.pop();
            continue;
         }
         cout << dfscode << endl;

         // extension
         InterResultTuple new_results(&old_results);
         if (!new_results.addForwardEdge(*db, hash_tbl, predMap_new[pred], true)) {
            VLOG(1) << "Too large results!! We give up.";
            dfscode.pop();
            continue;
         }
         if (new_results.support<curMinSup) {
            VLOG(1) << "Infrequent Pattern!. " << "minSup:" << curMinSup
                    << " Support:" << new_results.support;
            dfscode.pop();
            continue;
         }
         VLOG(1) << "Frequent Pattern!. " << "minSup:" << curMinSup 
                 << " Support:" << new_results.support;
         insert(dfscode, new_results);

         if (maxL>dfscode.size())
            subgraphMining(maxL, dfscode, new_results);

         // Backward extension
         if (iter==rmvertices.begin()) {
            for(unsigned dest=(*iter)-1; dest<=0; dest--) { 
               dfscode.pop();
               dfscode.push(source, dest, pred, GSPAN::EDGE_TYPE_REVERSE);

               VLOG(1) << "Backward expansion. New DFS Code: ";
               VLOG(1) << dfscode;
               if (!dfscode.is_min()) { 
                  VLOG(1) << "Non-minimum DFS Code!!!";   
                  GSPAN::Graph g;
                  GSPAN::DFSCode dfscode2;
                  dfscode.toGraph(g);
                  dfscode2.fromGraph(g);
                  VLOG(1) << "This is the minimum DFS Code!!!";
                  VLOG(1) << dfscode2;
                  continue;
               }
               cout << "Backward expansion. " << dfscode << endl;

               InterResultTuple back_new_results(&old_results);
               back_new_results.addBackwardEdge(new_results, dest, newVertexID);

               if (back_new_results.support<curMinSup) {
                  VLOG(1) << "Infrequent Pattern!. " << "minSup:" << curMinSup
                     << " Support:" << back_new_results.support;
                  continue;
               }
               VLOG(1) << "Frequent Pattern!. " << "minSup:" << curMinSup 
                       << " Support:" << back_new_results.support;
               insert(dfscode, back_new_results);

               if (maxL>dfscode.size())
                  subgraphMining(maxL, dfscode, back_new_results);
            }
         }
         dfscode.pop();
      }

      cout << "delete hashtbl.... "; cout.flush();
      for(hash_tbl_t::iterator iter2=hash_tbl.begin(),
          limit2=hash_tbl.end(); iter2!=limit2; iter2++) {
         table_t *tbl=(*iter2).second;
         tbl->clear();
         delete tbl;
      }
      hash_tbl.clear();
      cout << "finish." << endl; cout.flush();
   }
}
//---------------------------------------------------------------------------
RGindex::Node* RGindex::SearchNode(Node& root, GSPAN::DFSCode& dfscode, unsigned level) {
   GSPAN::DFS dfs;
   dfs=dfscode[level];

   if (level==dfscode.size()) // leaf node
      return &root;

   if (root.children.size()==0)
      return NULL;

   map<GSPAN::DFS, Node*>::iterator it;
   it=root.children.find(dfs);

   if (it != root.children.end()) {
      return SearchNode(*it->second, dfscode, level+1);
   }

   return NULL;
}
//---------------------------------------------------------------------------
