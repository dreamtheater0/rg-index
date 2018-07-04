#ifndef H_rpath_RPathStat
#define H_rpath_RPathStat

#include <map>

using namespace std;

class RPathStat
{
   class Pair
   {
      public:
         unsigned p1, p2;

         Pair() {};
         ~Pair() {};
         bool operator<(const Pair& other) const
         {
            if (p1 < other.p1)
               return true;
            else if (p1 == other.p1 && p2 < other.p2)
               return true;
            return false;
         };
   };

   map<Pair, unsigned> stats;

   public:
   RPathStat(char *dataset, char *path);
   ~RPathStat();

   unsigned getStat(unsigned p1, unsigned p2);
};

#endif

