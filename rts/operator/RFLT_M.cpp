#include "rts/operator/RFLT_M.hpp"
#include "rpath/RPathFilter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include <iostream>
#include <stdlib.h>
#include <algorithm>
#include <cassert>
#include <string.h>

//---------------------------------------------------------------------------
static bool sorterLessThan (RFLT_M::Sorter *sorter1, RFLT_M::Sorter *sorter2)
{
   return sorter1->size < sorter2->size;
}
//---------------------------------------------------------------------------
RFLT_M::RFLT_M(std::vector<RGindex::Node*>* fltInfos,
               std::vector<unsigned> &nodeIds, std::vector<GSPAN::DFSCode> &dfscodes_i,
               double expectedOutputCardinality) : Operator(expectedOutputCardinality)
{
   if (getenv("OFFRPATH")) {
      rpathFLT = NULL;
      return;
   }

   rpathFLT = new RPathFilter(fltInfos, nodeIds, dfscodes_i);
   numOfChildren=0;
   fltValue = &rpathFLT->fltValue.value;
   scanState = empty;
   sorted_idx_size = 0;
   hasPrimaryChild = false;
   isEnd = false;
   joinedValue = 0;
   triplecnt = 0;
}
//---------------------------------------------------------------------------
RFLT_M::~RFLT_M()
{
   for(unsigned i=0; i < numOfChildren; i++)
      delete childOp[i];
}
//---------------------------------------------------------------------------
unsigned RFLT_M::first()
{
   initBuff();

   if (!rpathFLT) {
      return next();
   }

   if (!rpathFLT->first())
      return 0;

   for (unsigned i = 0; i < numOfChildren; i++) {
      copyChild(i);
   }
   return next();
}
//---------------------------------------------------------------------------
unsigned RFLT_M::next()
{
   if (hasPrimaryChild) {
      for (unsigned i = sorted_idx_size; i > 0; i--) {
         unsigned idx;
         idx = sorted_idx[i-1]->idx;

         if (buf_offset[idx] == buf_size[idx] - 1) {
            buf_offset[idx] = 0;
            setValues(idx);
         }
         else {
            buf_offset[idx]++;
            setValues(idx);
            observedOutputCardinality += 1;
            return 1;
         }
      }

      // primaryChild의 next row를 읽는다
      if (primaryChildrowCnt==0){
         swapChild(primaryChild); // 두번째 row를 register에 기록
         primaryChildrowCnt=1;
         observedOutputCardinality += 1;
         return 1;
      }
      else {
         //unsigned prev_val=childValue[primaryChild]->value;
         unsigned cnt = childOp[primaryChild]->next();
         if (cnt == 0) {
            return 0;
         }
         if (childValue[primaryChild]->value == joinedValue) {
            observedOutputCardinality += 1;
            return 1;
         }
      }

      // primaryChild가 끝나면 결과 생성 종료
      // End cartesian product
      if (sorted_idx_size > 0) {
         memset(buf_offset, 0, sizeof(unsigned) * numOfChildren);
         memset(buf_size, 0, sizeof(unsigned) * numOfChildren);
      }
   }

   /* init */
   if (isEnd) 
      return 0;

   unsigned max_val;
   if (joinedValue != 0) {
      max_val = joinedValue + 1;
      if(!rpathFLT->next(max_val))
         return 0;
   }
   else { 
      // called from first()
      max_val = 0;
   }

   max_val = *fltValue;
   bool is_match = true;
   for (unsigned i = 0; i < numOfChildren; i++) {
      if (hasPrimaryChild && i == primaryChild) {
         hasPrimaryChild = false;
      }
      else
         copyFromShadow(i);
         
      if (*values[i] != max_val) {
         is_match = false;
         if (*values[i] > max_val) {
            max_val = *values[i];
         }
      }
   }

   // get Next Merged Value
   unsigned cnt;
   while (!is_match) {
      is_match = true;
      for (unsigned i = 0; i < numOfChildren; i++) {
         Operator *op = childOp[i];
         unsigned *value = values[i];

         while (*value < max_val) {
            cnt = op->next();
            if (cnt == 0) {
               return 0;
            }
         }
         if (*value > max_val) {
            // childValue[i]->value가 rpathFLT의 값보다 크기 때문에
            // filter의 다음값을 읽는다
            is_match = false;
            max_val = *value;
            break;
         }
      }
      if (!is_match) {
         if(!rpathFLT->next(max_val))
            return 0;
         max_val = *fltValue;
      }
   }
   joinedValue = max_val;

   // Buffering of rows which have the same key value
   // buffering이 필요한 child 파악하기
   // row가 2개 이상인 경우에만 buffering을 한다==>childToBuffer
   std::vector<unsigned> childToBuffer; 
   bool buffer=false;
   for (unsigned i = 0; i < numOfChildren; i++) {
      copyChild(i);
      cnt = childOp[i]->next();
      //triplecnt+=cnt;
      if (cnt == 0) {
         isEnd = true;
         // row 1개
         swapChild(i);
         continue;
      }
      if (*values[i] != max_val) {
         // row 1개
         // register: 1st row of next block, shadow: 1st row
         swapChild(i);
         // register: 1st row, shadow: 1st row of next block
         continue;
      }
      //swapChild(i); 
      // register: 2nd row, shadow: 1st row
      childToBuffer.push_back(i);   
      buffer=true;
   }

   if (buffer) {
      // childToBuffer에서 한개 제외하기
      // cartesian product를 생성 할 때에 한 child는 buffer를 하지 않아도 된다
      // 이상적으로는 가장 많은 수의 buffer를 갖고 있는 child를 선택해야 한다
      // 그러나 아직 buffer를 시작하지 않아 이를 파악 할 수 없다
      // 그냥 마지막 child 선택
      hasPrimaryChild = true;
      primaryChild = childToBuffer.back();
      childToBuffer.pop_back();
      primaryChildrowCnt = 0;
      sorted_idx.clear();
      sorted_idx_size = 0;

      for (std::vector<unsigned>::iterator iter=childToBuffer.begin(),
           limit=childToBuffer.end(); iter!=limit; ++iter) {
         unsigned child=*iter;
         // register: 2nd row, shadow: 1st row
         pushIntoBuff(child); // buffer 2nd row
         swapChild(child); 
         // register: 1st row, shadow: 2nd row

         while (1) {
            pushIntoBuff(child);
            cnt = childOp[child]->next();
            //triplecnt+=cnt;
            if (cnt == 0) {
               isEnd=true;
               break;
            }

            if (*values[child] != max_val) {
               copyChild(child);
               break;
            }
         }
         
         buf_offset[child] = 0;
         setValues(child);
         *values[child] = max_val;
         if (childTailSize[child] > 0) {
            sorters[child].size = buf_size[child] * childTailSize[child];
            sorted_idx.push_back(&sorters[child]);
            sorted_idx_size++;
         }
      }
      sort(sorted_idx.begin(), sorted_idx.end(), sorterLessThan);  

      /*
      cout << "buffer:" << primaryChild << endl;
      cout << "hasPrimaryChild:" << hasPrimaryChild;
      cout << " sorted_idx_size:" << sorted_idx_size; 
      for (unsigned i = 0; i < numOfChildren; i++) {
         cout << " " << buf_size[i];
      }
      cout << endl;
      */
   }

   observedOutputCardinality++;
   return 1;
}
//---------------------------------------------------------------------------
void RFLT_M::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("RFLT_M",expectedOutputCardinality,observedOutputCardinality);
   if (rpathFLT) rpathFLT->print(out);
   for(unsigned i=0; i < numOfChildren-1; i++)
      out.addEqualPredicateAnnotation(childValue[i],childValue[i+1]);
   for(unsigned i=0; i < numOfChildren; i++)
      out.addMaterializationAnnotation(*childTail[i]);
   for(unsigned i=0; i < numOfChildren; i++)
      childOp[i]->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
#if 0
void RFLT_M::print(DictionarySegment& dict,unsigned level)
{
   indent(level); std::cout << "<RFLT_M row_cnt:" << observedOutputCardinality << " ";
   for(unsigned i=0; i < numOfChildren; i++) {
      if (i!=0) std::cout << "=";
      printRegister(dict,childValue[i]);
   }

   for(unsigned i=0; i < numOfChildren; i++) {
      std::cout << " [";
      for (std::vector<Register*>::const_iterator iter=childTail[i]->begin(),
           limit=childTail[i]->end();iter!=limit;++iter) {
         std::cout << " "; printRegister(dict,*iter);
      }
      std::cout << "]";
   }

   if (rpathFLT) rpathFLT->print(); std::cout << std::endl;
   for(unsigned i=0; i < numOfChildren; i++) 
      childOp[i]->print(dict,level+1);
   indent(level); std::cout << ">" << std::endl;
}
#endif
//---------------------------------------------------------------------------
void RFLT_M::addMergeHint(Register* reg1,Register* reg2)
{
   for(unsigned i=0; i < numOfChildren; i++) 
      childOp[i]->addMergeHint(reg1, reg2);
}
//---------------------------------------------------------------------------
void RFLT_M::getAsyncInputCandidates(Scheduler& scheduler)
{
   for(unsigned i=0; i < numOfChildren; i++) 
      childOp[i]->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
void RFLT_M::addChild(Operator *child, Register *value, std::vector<Register*> *tail)
{
   // Add Operator
   childOp.push_back(child);

   // Add join attribute
   childValue.push_back(value);

   // Add non-join attribute
   childTail.push_back(tail);
   childTailSize.push_back(tail->size());

   numOfChildren++;
}
//---------------------------------------------------------------------------
void RFLT_M::initBuff()
{
   int i, tail_size = 0;

   buf_size = (unsigned *) calloc (numOfChildren, sizeof(unsigned));
   buf_offset = (unsigned *) calloc (numOfChildren, sizeof(unsigned));
   cnts = (unsigned *) calloc (numOfChildren, sizeof(unsigned));
   currentCnts = (unsigned *) calloc (numOfChildren, sizeof(unsigned));
   values = (unsigned **) malloc(sizeof(unsigned *) * numOfChildren);

   resultCnt = 1;

   for (i = 0; i < (int) numOfChildren; i++) {
      Sorter sorter(i,0);

      sorters.push_back(sorter);
      cnts[i] = childOp[i]->first();
      if (cnts[i] == 0) {
         isEnd=true;
      }
      values[i] = &(childValue[i]->value);
   }

   buf_max_size = (unsigned *) malloc(sizeof(unsigned) * numOfChildren);
   buf_mat = (unsigned **) malloc (sizeof(unsigned *) * numOfChildren);
   buf_tail_offset = (unsigned *) calloc(numOfChildren, sizeof(unsigned));
   shadowValue = (unsigned **) malloc (sizeof(unsigned*) * numOfChildren);

   for (i = 0; i < (int) numOfChildren; i++) {
      buf_tail_offset[i] = tail_size;
      tail_size += childTail[i]->size();
      tail_size += 2; // value, cnt

      buf_max_size[i] = 1024 * 4;
      buf_mat[i] = (unsigned *) malloc (sizeof(unsigned) * (childTailSize[i] + 1) * buf_max_size[i]);
      shadowValue[i] = (unsigned *) malloc (sizeof(unsigned) * (childTailSize[i] + 2));
   }

   //shadowValue = (unsigned *) malloc (sizeof (unsigned) * tail_size);

   // filter hint 설정
   if (rpathFLT) {
      for(unsigned i=0; i < numOfChildren; i++)  {
         childOp[i]->addMergeHint(&rpathFLT->fltValue, childValue[i]);
      }
   }
   for(unsigned i=0; i < numOfChildren; i++)  {
      for(unsigned j=0; j < numOfChildren; j++)  {
         childOp[i]->addMergeHint(childValue[i], childValue[j]);
      }
   }
}
//---------------------------------------------------------------------------
inline void RFLT_M::pushIntoBuff(int idx)
{
   unsigned *buf;

   if (buf_max_size[idx] == buf_size[idx]) {
      buf_max_size[idx] = buf_max_size[idx] * 2;
      buf_mat[idx] = (unsigned *) realloc (buf_mat[idx], 
                     sizeof (unsigned) * (childTailSize[idx] + 1)  * buf_max_size[idx]);
      std::cerr << "realloc !!" << std::endl;
   }

   buf = &buf_mat[idx][buf_size[idx] * (childTailSize[idx] + 1)];
   buf_size[idx]++;

   //buf[0] = cnts[idx];

   std::vector<Register*> &cTail = *childTail[idx];

   for (unsigned i = 0, limit=childTailSize[idx]; i < limit; i++)
      buf[i + 1] = cTail[i]->value;
}
//---------------------------------------------------------------------------
void RFLT_M::truncateAllBuff()
{
   int i;

   for (i = 0; i < (int) numOfChildren; i++) {
      buf_offset[i] = 0;
      buf_size[i] = 0;
   }
}
//---------------------------------------------------------------------------
inline void RFLT_M::setValues(int idx)
{
   unsigned *buf;

   /* Setting values in buffers to registers */
   unsigned offset = buf_offset[idx] * (childTailSize[idx] + 1);
   buf = &buf_mat[idx][offset];

   //unsigned buf_toffset = buf_tail_offset[idx];

   //resultCnt *= currentCnts[idx] = buf[0];

   std::vector<Register*> &cTail = *childTail[idx];
   for (unsigned i = 0, limit=childTailSize[idx]; i < limit; i++) {
      cTail[i]->value = buf[i + 1];
   }
}
//---------------------------------------------------------------------------
RFLT_M::Sorter::Sorter (int idx, int size) : idx(idx), size(size)
   // Constructor
{
}
//---------------------------------------------------------------------------
RFLT_M::Sorter::~Sorter ()
   // Destructor
{
}
//---------------------------------------------------------------------------
inline void RFLT_M::swapChild(unsigned idx)
{
   // shadow와 register 값을 swap
//   unsigned *shadow = &shadowValue[buf_tail_offset[idx]];
   unsigned *shadow = shadowValue[idx];

//   std::swap(shadow[0], cnts[idx]);
   std::swap(shadow[1], childValue[idx]->value);
   std::vector<Register*> &cTail = *childTail[idx];
   for (unsigned j = 0, limit=childTailSize[idx]; j < limit; j++) {
      std::swap(shadow[j+2], cTail[j]->value);
   }
}
//---------------------------------------------------------------------------
inline void RFLT_M::copyFromShadow(unsigned idx)
{
   // copy shadows to regs
   unsigned *shadow = shadowValue[idx];

   childValue[idx]->value=shadow[1];
   std::vector<Register*> &cTail = *childTail[idx];
   for (unsigned j = 0, limit=childTailSize[idx]; j < limit; j++) {
      cTail[j]->value=shadow[j+2];
   }
}
//---------------------------------------------------------------------------
inline void RFLT_M::copyChild(unsigned idx)
{
   // copy regs to shadows
   unsigned *shadow = shadowValue[idx];

   shadow[1]=childValue[idx]->value;
   std::vector<Register*> &cTail = *childTail[idx];
   for (unsigned j = 0, limit=childTailSize[idx]; j < limit; j++) {
      shadow[j+2]=cTail[j]->value;
   }
}
//---------------------------------------------------------------------------
void RFLT_M::getStat(unsigned &final,unsigned &intermediate)
{
   intermediate += observedOutputCardinality;
   for(unsigned i=0; i < numOfChildren; i++)
      childOp[i]->getStat(final, intermediate);
}
