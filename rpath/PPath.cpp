#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include "rpath/PPath.hpp"
#include <string.h>

using namespace std;

PPath::PPath(const char *ppathStr) {
   change((char *) ppathStr);
}

void PPath::change(const char *ppathStr) {
   char *tok, *str, *saveptr;
   unsigned predicate;
   char tempStr[256];

   strcpy(tempStr, ppathStr);
   path.clear();

   str = tempStr;
   while((tok = strtok_r(str, ",", &saveptr)) != NULL) {
      predicate = (unsigned) atoi(tok);
      Add(predicate);
      str = NULL;
   }
}

PPath::PPath(PPath& pp, unsigned predicate) {
   Add(predicate);
   for (vector<unsigned>::const_iterator iter=pp.path.begin(),limit=pp.path.end();
        iter!=limit;++iter) {
      Add(*iter);
   }
}

PPath::PPath(unsigned predicate, PPath& pp) {
   for (vector<unsigned>::const_iterator iter=pp.path.begin(),limit=pp.path.end();
        iter!=limit;++iter) {
      Add(*iter);
   }
   Add(predicate);
}


void PPath::Add(unsigned predicate) {
   path.push_back(predicate);
}

void PPath::print() {
   for (vector<unsigned>::const_iterator iter=path.begin(),limit=path.end();
        iter!=limit;++iter) {
      cout << *iter << " ";
   }
}

char* PPath::str(char *buf) {
   for (vector<unsigned>::const_iterator iter=path.begin(),limit=path.end();
        iter!=limit;++iter) {
      if (iter == path.begin())
         buf += sprintf(buf, "%u", (*iter));
      else
         buf += sprintf(buf, ",%u", (*iter));
   }
   return buf;
}

bool PPath::contained(PPath& ppath2) {
   int delta = ppath2.path.size() - path.size();
   if (delta >= 0) {
      for (unsigned i=0,limit=path.size();i<limit;i++) {
         if (path[i] != ppath2.path[i+delta])
            return false;
      }
      return true;
   }
   return false;
}


string PPath::getStr() {
   char buf[256];
   str(buf);
   return string(buf);
}

