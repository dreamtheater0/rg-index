#ifndef H_rpath_CSet
#define H_rpath_CSet

#include <map>
#include <string>
#include <vector>

using namespace std;

class CSet
{
   map<vector<unsigned>*, unsigned> cset;
   map<string, unsigned> cache;

   public:
   CSet(char *dataset);
   ~CSet();

   unsigned getCnt(vector<unsigned> preds);
};

#endif

