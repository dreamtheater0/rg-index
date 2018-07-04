#ifndef H_rpath_RPathSegment
#define H_rpath_RPathSegment
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
#include "rts/segment/Segment.hpp"
#include "infra/osdep/MemoryMappedFile.hpp"
#include "rts/buffer/BufferReference.hpp"
//---------------------------------------------------------------------------
/// RPathSegment 
class RPathSegment
{
   private:
   /// The start of the raw facts table
   unsigned tableStart;
   /// The root of the index b-tree
   unsigned indexRoot;
   /// Statistics
   unsigned pages;

   /// Lookup the first page contains entries >= the start condition
   bool lookup(unsigned start1,BufferReference& ref);

   RPathSegment(const RPathSegment&);
   void operator=(const RPathSegment&);

   MemoryMappedFile *file;
   const unsigned char* getPage(unsigned page);

   public:
   static const unsigned pageSize = 512 * 1;
   /// Constructor
   RPathSegment(MemoryMappedFile *file,unsigned tableStart,unsigned indexRoot,unsigned pages);

   /// Get the number of pages in the segment
   unsigned getPages() const { return pages; }

   /// A scan over the facts segment
   class Scan {
      private:
      /// The maximum number of entries per page
      static const unsigned maxCount = pageSize;

      /// The current page
      const unsigned char* current;
      /// The segment
      RPathSegment *seg;
      /// The position on the current page
      const unsigned* pos,*posLimit;
      /// The decompressed triples
      unsigned nodeIDs[maxCount];

      Scan(const Scan&);
      void operator=(const Scan&);

      /// Read the next page
      bool readNextPage();

      public:
      /// Constructor
      explicit Scan();
      /// Destructor
      ~Scan();

      /// Start a new scan over the whole segment and reads the first entry
      bool first(RPathSegment& segment);

      /// Read the next entry
      bool next() { if ((++pos)>=posLimit) return readNextPage(); else return true; }
      /// Get the first value
      unsigned getValue1() const { return (*pos); }

      /// Close the scan
      void close();
   };
   friend class Scan;

   /// Change the byte order
   static inline unsigned flipByteOrder(unsigned value) { return (value<<24)|((value&0xFF00)<<8)|((value&0xFF0000)>>8)|(value>>24); }

   /// Helper function. Reads a 32bit big-endian value
   static inline unsigned readUint32(const unsigned char* data) { return (data[0]<<24)|(data[1]<<16)|(data[2]<<8)|data[3]; }
   /// Helper function. Reads a 32bit big-endian value that is guaranteed to be aligned. This assumes little-endian order! (needs a define for big-endian)
   static inline unsigned readUint32Aligned(const unsigned char* data) { return flipByteOrder(*reinterpret_cast<const unsigned*>(data)); }
};
//---------------------------------------------------------------------------
#endif
