#include "rts/database/Database.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "cts/parser/TurtleParser.hpp"
#include <sys/stat.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <fstream>
#include <set>
#include <libgen.h>

using namespace std;
//---------------------------------------------------------------------------
static void writeURI(ofstream& to, const char* start,const char* stop)
   // Write a URI
{
   to << "<";
   for (const char* iter=start;iter<stop;++iter) {
      char c=*iter;
      switch (c) {
         case '\t': to << "\\t"; break;
         case '\n': to << "\\n"; break;
         case '\r': to << "\\r"; break;
         case '>': to << "\\>"; break;
         case '\\': to << "\\\\"; break;
         default: to << c; break;
      }
   }
   to << ">";
}
//---------------------------------------------------------------------------
static void writeLiteral(ofstream& to, const char* start,const char* stop)
   // Write a literal
{
   to << "\"";
   for (const char* iter=start;iter<stop;++iter) {
      char c=*iter;
      switch (c) {
         case '\t': to << "\\t"; break;
         case '\n': to << "\\n"; break;
         case '\r': to << "\\r"; break;
         case '\"': to << "\\\""; break;
         case '\\': to << "\\\\"; break;
         default: to << c; break;
      }
   }
   to << "\"";
}
//---------------------------------------------------------------------------
static void dumpSubject(ofstream& to, DictionarySegment& dic,unsigned id)
   // Write a subject entry
{
   const char* start,*stop; Type::ID type; unsigned subType;
   if (!dic.lookupById(id,start,stop,type,subType)) {
      cerr << "consistency error: encountered unknown id " << id << endl;
      throw;
   }
   if (type!=Type::URI)
      cerr << "consistency error: subjects must be URIs" << endl;
   writeURI(to, start,stop);
}
//---------------------------------------------------------------------------
static void dumpPredicate(ofstream& to, DictionarySegment& dic,unsigned id)
   // Write a predicate entry
{
   const char* start,*stop; Type::ID type; unsigned subType;
   if (!dic.lookupById(id,start,stop,type,subType)) {
      cerr << "consistency error: encountered unknown id " << id << endl;
      throw;
   }
   if (type!=Type::URI)
      cerr << "consistency error: subjects must be URIs" << endl;
   writeURI(to, start,stop);
}
//---------------------------------------------------------------------------
static void dumpObject(ofstream& to, DictionarySegment& dic,unsigned id)
   // Write an object entry
{
   const char* start,*stop; Type::ID type; unsigned subType;
   if (!dic.lookupById(id,start,stop,type,subType)) {
      cerr << "consistency error: encountered unknown id " << id << endl;
      throw;
   }
   switch (type) {
      case Type::URI: writeURI(to, start,stop); break;
      case Type::Literal: writeLiteral(to, start,stop); break;
      case Type::CustomLanguage: {
         const char* start2,*stop2; Type::ID type2; unsigned subType2;
         if (!dic.lookupById(subType,start2,stop2,type2,subType2)) {
            cerr << "consistency error: encountered unknown language " << subType << endl;
            throw;
         }
         writeLiteral(to, start,stop);
         to << "@";
         for (const char* iter=start2;iter!=stop2;++iter)
            to << (*iter);
         } break;
      case Type::CustomType: {
         const char* start2,*stop2; Type::ID type2; unsigned subType2;
         if (!dic.lookupById(subType,start2,stop2,type2,subType2)) {
            cerr << "consistency error: encountered unknown type " << subType << endl;
            throw;
         }
         writeLiteral(to, start,stop);
         to << "^^";
         writeURI(to, start2,stop2);
         } break;
      case Type::String: writeLiteral(to, start,stop); to << "^^<http://www.w3.org/2001/XMLSchema#string>"; break;
      case Type::Integer: writeLiteral(to, start,stop); to << "^^<http://www.w3.org/2001/XMLSchema#integer>"; break;
      case Type::Decimal: writeLiteral(to, start,stop); to << "^^<http://www.w3.org/2001/XMLSchema#decimal>"; break;
      case Type::Double: writeLiteral(to, start,stop); to << "^^<http://www.w3.org/2001/XMLSchema#double>"; break;
      case Type::Boolean: writeLiteral(to, start,stop); to << "^^<http://www.w3.org/2001/XMLSchema#boolean>"; break;
   }
}
//---------------------------------------------------------------------------
static void dumpTriple(ofstream& to, DictionarySegment& dic, Register subject, Register predicate, Register object)
{
   dumpSubject(to, dic,subject.value);
   to << " ";
   dumpPredicate(to, dic,predicate.value);
   to << " ";
   dumpObject(to, dic,object.value);
   to << "." << "\n";
}

//---------------------------------------------------------------------------
/*
int main(int argc,char* argv[])
{
   if (argc < 6) {
      cerr << "usage: " << argv[0] << " database directory ratio(all) ratio(insert) ratio(delete) " << endl;
      return 1;
   }

   string dataset = argv[1];
   string directory = argv[2];

   double ratio_all, ratio_insert, ratio_delete;
   ratio_all = atof(argv[3]);
   ratio_insert = atof(argv[4]);
   ratio_delete = atof(argv[5]);

   unsigned pcnt=0, pcnt_all=0; // no limit
   if (argc>7) {
      pcnt=atoi(argv[6]);
      pcnt_all=atoi(argv[7]);
   }

   mkdir(directory.c_str(), 0755);
   string filename = directory + "/" + dataset + ".n3";
   ofstream ofs_original(filename.c_str(), ios::out|ios::trunc);
   filename = directory + "/" + dataset + "_deleted.n3";
   ofstream ofs_deleted(filename.c_str(), ios::out|ios::trunc);
   filename = directory + "/" + dataset + "_inserted.n3";
   ofstream ofs_inserted(filename.c_str(), ios::out|ios::trunc);
   filename = directory + "/" + dataset + "_updated.n3";
   ofstream ofs_updated(filename.c_str(), ios::out|ios::trunc);

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cerr << "unable to open database " << argv[1] << endl;
      return 1;
   }

   DictionarySegment& dic=db.getDictionary();
   Register subject,predicate,object;
   subject.reset(); predicate.reset(); object.reset();
   IndexScan* scan=IndexScan::create(db,Database::Order_Subject_Predicate_Object,&subject,false,&predicate,false,&object,false,0);
   set<unsigned> resources;
   set<unsigned> predicates;
   set<unsigned> predicates_all;

   if (scan->first()) do {
      double dice = (double) rand() / (double) RAND_MAX;
      if (resources.count(predicate.value) == 0) {
         if (pcnt_all > 0 && pcnt_all == predicates_all.size() && 
             predicates_all.count(predicate.value) == 0) 
            continue;

         dumpTriple(ofs_original, dic, subject, predicate, object);
         dumpTriple(ofs_updated, dic, subject, predicate, object);
         resources.insert(subject.value);
         resources.insert(predicate.value);
         resources.insert(object.value);
         predicates_all.insert(predicate.value);
         continue;
      }
      if (pcnt_all > 0 && pcnt_all == predicates_all.size() && 
          predicates_all.count(predicate.value) == 0) {
         continue;
      }
      if (dice < ratio_all) {
         dumpTriple(ofs_original, dic, subject, predicate, object);

         dice = (double) rand() / (double) RAND_MAX;
         if (dice < ratio_delete) {
            if (pcnt > 0 && pcnt == predicates.size() && predicates.count(predicate.value) == 0) {
               dumpTriple(ofs_updated, dic, subject, predicate, object);
               resources.insert(subject.value);
               resources.insert(predicate.value);
               resources.insert(object.value);
               continue;
            }
            if (resources.count(subject.value) > 0 && 
                resources.count(predicate.value) > 0 &&
                resources.count(object.value) > 0) {
               dumpTriple(ofs_deleted, dic, subject, predicate, object);
               predicates.insert(predicate.value); 
               continue;
            }
         }

         dumpTriple(ofs_updated, dic, subject, predicate, object);
         resources.insert(subject.value);
         resources.insert(predicate.value);
         resources.insert(object.value);
      }
      else {
         if (pcnt > 0 && pcnt == predicates.size() && predicates.count(predicate.value) == 0) {
            continue;
         }
         dice = (double) rand() / (double) RAND_MAX;
         if (dice < ratio_insert * ratio_all) {
            dumpTriple(ofs_inserted, dic, subject, predicate, object);
            predicates.insert(predicate.value); 
//          dumpTriple(ofs_updated, dic, subject, predicate, object);
         }
      }
   } while (scan->next());
   delete scan;

   cerr << "concatenate updated and inserted..." << endl;
   ofs_inserted.close();

   char ch;
   filename = directory + "/" + dataset + "_inserted.n3";
   ifstream ifs_inserted(filename.c_str());
   while(!ifs_inserted.eof()) {
      ifs_inserted.get(ch);
      if (ifs_inserted.good()) 
         ofs_updated.put(ch);
   }

   cerr << "predicate size:" << predicates.size() << endl;
   cerr << "predicate_all size:" << predicates_all.size() << endl;

   ifs_inserted.close();
   ofs_updated.close();
   ofs_deleted.close();
}
*/
//---------------------------------------------------------------------------

//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc < 6) {
      cerr << "usage: " << argv[0] << " database directory ratio(all) ratio(insert) ratio(delete) " << endl;
      return 1;
   }

   char *dataset=strdup(argv[1]);
   dataset=basename(dataset);
   string directory = argv[2];

   unsigned max_all, max_insert, max_delete;
   max_all = atoi(argv[3]);
   max_insert = atoi(argv[4]);
   max_delete = atoi(argv[5]);

   unsigned pred_max_all=0, pred_max=0; // no limit
   if (argc>7) {
      pred_max_all=atoi(argv[6]);
      pred_max=atoi(argv[7]);
   }

   mkdir(directory.c_str(), 0755);
   string filename = directory + "/" + dataset + ".n3";
   ofstream ofs_original(filename.c_str(), ios::out|ios::trunc);
   filename = directory + "/" + dataset + "_deleted.n3";
   ofstream ofs_deleted(filename.c_str(), ios::out|ios::trunc);
   filename = directory + "/" + dataset + "_inserted.n3";
   ofstream ofs_inserted(filename.c_str(), ios::out|ios::trunc);
   filename = directory + "/" + dataset + "_updated.n3";
   ofstream ofs_updated(filename.c_str(), ios::out|ios::trunc);

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cerr << "unable to open database " << argv[1] << endl;
      return 1;
   }

   DictionarySegment& dic=db.getDictionary();
   Register subject,predicate,object;
   subject.reset(); predicate.reset(); object.reset();
   IndexScan* scan=IndexScan::create(db,Database::Order_Subject_Predicate_Object,&subject,false,&predicate,false,&object,false,0);
   set<unsigned> resources;
   set<unsigned> pred_insert, pred_all, pred_delete;

   unsigned cnt_all=0, cnt_insert=0, cnt_delete=0;
   if (scan->first()) do {
      if (pred_max_all > 0 && 
          pred_max_all == pred_all.size() && 
          pred_all.count(predicate.value) == 0) 
         continue;

      // to preserve predicate IDs, 
      // the first triple of any predicate is saved
      if (resources.count(predicate.value) == 0) {
         dumpTriple(ofs_original, dic, subject, predicate, object);
         dumpTriple(ofs_updated, dic, subject, predicate, object);
         resources.insert(subject.value);
         resources.insert(predicate.value);
         resources.insert(object.value);
         pred_all.insert(predicate.value);
         continue;
      }

      if (cnt_all < max_all) {
         dumpTriple(ofs_original, dic, subject, predicate, object);
         cnt_all++;

         if (! (pred_max > 0 && pred_max == pred_delete.size() && 
                pred_delete.count(predicate.value) == 0)) {
            if (cnt_delete < max_delete && 
                resources.count(subject.value) > 0 && 
                resources.count(predicate.value) > 0 &&
                resources.count(object.value) > 0) {
               dumpTriple(ofs_deleted, dic, subject, predicate, object);
               pred_delete.insert(predicate.value); 
               cnt_delete++;
               continue;
            }
         }

         dumpTriple(ofs_updated, dic, subject, predicate, object);
         resources.insert(subject.value);
         resources.insert(predicate.value);
         resources.insert(object.value);
      }
      else {
         if (cnt_insert == max_insert)
            break;

         if (resources.count(predicate.value) == 0)
            continue;

         if (pred_max > 0 && pred_max == pred_insert.size() && 
             pred_insert.count(predicate.value) == 0) 
            continue;

         dumpTriple(ofs_inserted, dic, subject, predicate, object);
         dumpTriple(ofs_updated, dic, subject, predicate, object);
         pred_insert.insert(predicate.value); 
         cnt_insert++;
      }
   } while (scan->next());
   delete scan;

   cerr << "pred_all size:" << pred_all.size() << endl;
   cerr << "pred_insert size:" << pred_insert.size() << endl;
   cerr << "pred_delete size:" << pred_delete.size() << endl;
   cerr << "cnt_all size:" << cnt_all << endl;
   cerr << "cnt_insert size:" << cnt_insert << endl;
   cerr << "cnt_delete size:" << cnt_delete << endl;

   ofs_inserted.close();
   ofs_updated.close();
   ofs_deleted.close();
}
//---------------------------------------------------------------------------
