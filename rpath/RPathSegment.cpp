#include "rpath/RPathSegment.hpp"
#include "infra/osdep/MemoryMappedFile.hpp"
#include <cassert>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
/// The size of the header on each fact page
static const unsigned headerSize = 4;
//---------------------------------------------------------------------------
/// Helper functions
static inline unsigned readInner1(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+16+8*slot); }
static inline unsigned readInnerPage(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+16+8*slot+4); }
//---------------------------------------------------------------------------
RPathSegment::RPathSegment(MemoryMappedFile* file,unsigned tableStart,unsigned indexRoot,unsigned pages)
   : tableStart(tableStart),indexRoot(indexRoot),
     pages(pages),file(file)
   // Constructor
{
}
//---------------------------------------------------------------------------
RPathSegment::Scan::Scan()
   : seg(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
RPathSegment::Scan::~Scan()
   // Destructor
{
   close();
}
//---------------------------------------------------------------------------
const unsigned char* RPathSegment::getPage(unsigned page)
{
   return reinterpret_cast<const unsigned char*>(file->getBegin())+
            (unsigned long long) page* (unsigned long long) pageSize;
}
//---------------------------------------------------------------------------
bool RPathSegment::Scan::first(RPathSegment& segment)
   // Start a new scan over the whole segment
{
   current=segment.getPage(segment.tableStart);
   seg=&segment;
   pos=posLimit=0;

   return next();
}
//---------------------------------------------------------------------------
static inline unsigned readDelta1(const unsigned char* pos) { return pos[0]; }
static inline unsigned readDelta2(const unsigned char* pos) { return (pos[0]<<8)|pos[1]; }
static inline unsigned readDelta3(const unsigned char* pos) { return (pos[0]<<16)|(pos[1]<<8)|pos[2]; }
static inline unsigned readDelta4(const unsigned char* pos) { return (pos[0]<<24)|(pos[1]<<16)|(pos[2]<<8)|pos[3]; }
//---------------------------------------------------------------------------
bool RPathSegment::Scan::readNextPage()
   // Read the next page
{
   // Already read the first page? Then read the next one
   if (pos-1) {
      const unsigned char* page=static_cast<const unsigned char*>(current);
      unsigned nextPage=readUint32Aligned(page);
      if (!nextPage)
         return false;
      current=seg->getPage(nextPage);
   }

   // Decompress the first triple
   const unsigned char* page=static_cast<const unsigned char*>(current);
   const unsigned char* reader=page+headerSize,*limit=page+RPathSegment::pageSize;
   unsigned nodeID=readUint32Aligned(reader); reader+=4;
   unsigned* writer=nodeIDs;
   (*writer)=nodeID;
   ++writer;

   // Decompress the remainder of the page
   while (reader<limit) {
      // Decode the header byte
      unsigned info=*(reader++);
      // Small gap only?
      if (info<0x80) {
         if (!info)
            break;
         nodeID+=info;
         (*writer)=nodeID;
         ++writer;
         continue;
      }
      // Decode the parts
      switch (info&127) {
         case 1: nodeID+=readDelta1(reader); reader+=1; break;
         case 2: nodeID+=readDelta2(reader); reader+=2; break;
         case 3: nodeID+=readDelta3(reader); reader+=3; break;
         case 4: nodeID+=readDelta4(reader); reader+=4; break;
         default:
            assert(false);
      }
      (*writer)=nodeID;
      ++writer;
   }

   // Update the entries
   pos=nodeIDs;
   posLimit=writer;

#if 0
   // Check if we should make a skip
   if (hint) {
      unsigned next1=triples[0].value1;
      while (true) {
         // Compute the next hint
         hint->next(next1);

         // No entry on this page?
         const Triple* oldPos=pos;
         if (!find(next1)) {
            if (!seg->lookup(next1,current))
               return false;
            pos=posLimit=0;
            ++pos;
            return readNextPage();
         }

         // Stop if we are at a suitable position
         if (oldPos==pos)
            break;
      }
   }
#endif

   return true;
}
//---------------------------------------------------------------------------
void RPathSegment::Scan::close()
   // Close the scan
{
   seg=0;
   current=0;
}
//---------------------------------------------------------------------------
