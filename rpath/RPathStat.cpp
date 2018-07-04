#include "rpath/RPathStat.hpp"
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <cassert>

RPathStat::RPathStat(char *dataset, char *path) {
   char fileName[256], stat[256];
   memset(fileName, 0, 256);
   memset(stat, 0, 256);
#if 0
   if (getenv("REVERSE"))
      sprintf(fileName, "%s_%u.stat2_r", dataset, maxL);
   else
      sprintf(fileName, "%s_%u.stat2", dataset, maxL);
#endif
   sprintf(fileName, "%s.stat2",dataset);

   ifstream ifs_stat(fileName, ios::in);

   unsigned p1, p2, cnt=0;
   while(ifs_stat >> stat && !ifs_stat.eof()) {
      p1 = atoi(strtok(stat, "|"));
      p2 = atoi(strtok(NULL, "|"));
      cnt = atoi(strtok(NULL, "|"));
      Pair pair;
      pair.p1 = p1; pair.p2 = p2;
      stats[pair] = cnt;
   }
}

unsigned RPathStat::getStat(unsigned p1, unsigned p2) {
   Pair pair;
   assert(p1 != p2);
   pair.p1 = p1; pair.p2 = p2;
   map<Pair, unsigned>::iterator iter;
   iter = stats.find(pair);
   if (iter == stats.end()) 
   {
      pair.p1 = p2; pair.p2 = p1;
      iter = stats.find(pair);
      if (iter == stats.end()) 
         return 0;
   }      

   return iter->second;
}
