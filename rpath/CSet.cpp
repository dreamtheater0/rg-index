#include "rpath/CSet.hpp"
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <cassert>
#include <sstream>

CSet::CSet(char *dataset) {
   char fileName[256];
   memset(fileName, 0, 256);
   sprintf(fileName, "%s.cset", dataset);
   ifstream ifs_stat(fileName, ios::in);

   char *stat=(char*)malloc(sizeof(char)*1024*1024);
   while(ifs_stat >> stat && !ifs_stat.eof()) {
      char *cs = strtok(stat, ":");
      unsigned cnt = atoi(strtok(NULL, "|"));

      vector<unsigned> *vec=new vector<unsigned>;
      
      char *tok, *str, *saveptr;
      str = cs;
      unsigned predicate;
      while((tok = strtok_r(str, ",", &saveptr)) != NULL) {
         predicate = (unsigned) atoi(tok);
         vec->push_back(predicate);
         str = NULL;
      }
      cset[vec] = cnt;
   }
}

unsigned CSet::getCnt(vector<unsigned> preds) {
   unsigned cnt=0;
   
   stringstream convert;
   for (unsigned i=0; i<preds.size(); i++) {
      unsigned p=preds[i];
      if (i!=0) convert << "," << p;
      else convert << p;
   }
   string predset = convert.str();

   if (cache.count(predset)>0) {
      return cache[predset];
   }

   for (map<vector<unsigned>*,unsigned>::iterator it=cset.begin(), limit=cset.end();
        it != limit; it++) {
      vector<unsigned> *target_vecs=(*it).first;

      // check containment
      bool contained=true;

      vector<unsigned>::iterator iter2=target_vecs->begin(),limit2=target_vecs->end();
      for (vector<unsigned>::iterator iter1=preds.begin(),limit1=preds.end(); 
           iter1!=limit1; iter1++) {
         bool has=false;
         for (; iter2!=limit2; iter2++) {
            if ((*iter1)==(*iter2)) {
               has=true;
               break;
            }
         }
         if (!has) {
            contained=false;
            break;
         }
      }
      if (contained)
         cnt+=(*it).second;
   }
   if (cnt == 0) {
      cout << "CSet error!!: " << predset << endl;
      assert(false);
   }
   cache[predset]=cnt;
   return cnt;
}
