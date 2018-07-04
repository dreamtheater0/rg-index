#ifndef H_rpath_PPath
#define H_rpath_PPath

#include <vector>
#include <string>

class PPath {
   public:
   PPath() {};
   PPath(const char *ppathStr);
   PPath(PPath& pp, unsigned predicate);
   PPath(unsigned predicate, PPath& pp);
   void Add(unsigned predicate);
   void print();
   bool contained(PPath& ppath2);
   char* str(char*);
   unsigned size() {return path.size();};
   void change(const char *ppathStr);
   std::string getStr();

   std::vector<unsigned> path;
   unsigned ppathID; // build시에 사용
};

#endif
