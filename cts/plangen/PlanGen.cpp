#include "cts/plangen/PlanGen.hpp"
#include "cts/plangen/Costs.hpp"
#include "cts/codegen/CodeGen.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/segment/ExactStatisticsSegment.hpp"
#include <map>
#include <set>
#include <algorithm>
#include "rpath/PPath.hpp"
#include "rpath/RPathQueryGraph.hpp"
#include "rpath/RPathTreeIndex.hpp"
#include "rpath/RPathStat.hpp"
#include "rpath/combination.h"
#include <iostream>
#include <cassert>
#include "rpath/CSet.hpp"
#include "gspan/graph.hpp"
#include "gspan/dfs.hpp"
#include "rg-index/rg-index.hpp"

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
using namespace stdcomb;
//---------------------------------------------------------------------------
// XXX integrate DPhyper-based optimization, query path statistics
//---------------------------------------------------------------------------
/// Description for a join
struct PlanGen::JoinDescription
{
   /// Sides of the join
   BitSet left,right;
   /// Required ordering
   unsigned ordering;
   /// Selectivity
   double selectivity;
   /// Table function
   const QueryGraph::TableFunction* tableFunction;
};
//---------------------------------------------------------------------------
PlanGen::PlanGen()
   // Constructor
{
}
//---------------------------------------------------------------------------
PlanGen::~PlanGen()
   // Destructor
{
}
//---------------------------------------------------------------------------
bool PlanGen::addPlan(Problem* problem,Plan* plan)
   // Add a plan to a subproblem
{
    // Check for dominance
    if (~plan->ordering) {
        Plan* last=0;
        for (Plan* iter=problem->plans,*next;iter;iter=next) {
            next=iter->next;
            if (iter->ordering==plan->ordering) {
                // Dominated by existing plan?
                if (iter->costs<=plan->costs) {
                    plans.free(plan);
                    return false;
                }
                // No, remove the existing plan
                if (last)
                    last->next=iter->next; else
                        problem->plans=iter->next;
                plans.free(iter);
            } else if ((!~iter->ordering)&&(iter->costs>=plan->costs)) {
                // Dominated by new plan
                if (last)
                    last->next=iter->next; else
                        problem->plans=iter->next;
                plans.free(iter);
            } else last=iter;
        }
    } else {
        Plan* last=0;
        for (Plan* iter=problem->plans,*next;iter;iter=next) {
            next=iter->next;
            // Dominated by existing plan?
            if (iter->costs<=plan->costs) {
                plans.free(plan);
                return false;
            }
            // Dominates existing plan?
            if (iter->ordering==plan->ordering) {
                if (last)
                    last->next=iter->next; else
                        problem->plans=iter->next;
                plans.free(iter);
            } else last=iter;
        }
    }

    // Add the plan to the problem set
    plan->next=problem->plans;
    problem->plans=plan;
    return true;
}
//---------------------------------------------------------------------------
static Plan* buildFilters(PlanContainer& plans,const QueryGraph::SubQuery& query,Plan* plan,unsigned value1,unsigned value2,unsigned value3)
   // Apply filters to index scans
{
   // Collect variables
   set<unsigned> orderingOnly,allAttributes;
   if (~(plan->ordering))
      orderingOnly.insert(plan->ordering);
   allAttributes.insert(value1);
   allAttributes.insert(value2);
   allAttributes.insert(value3);

   // Apply a filter on the ordering first
   for (vector<QueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
      if ((*iter).isApplicable(orderingOnly)) {
         Plan* p2=plans.alloc();
         double cost1=plan->costs+Costs::filter(plan->cardinality);
         p2->op=Plan::Filter;
         p2->costs=cost1;
         p2->opArg=(*iter).id;
         p2->left=plan;
         p2->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::Filter*>(&(*iter)));
         p2->next=0;
         p2->cardinality=plan->cardinality*0.5;
         p2->ordering=plan->ordering;
         plan=p2;
      }
   // Apply all other applicable filters
   for (vector<QueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
      if ((*iter).isApplicable(allAttributes)&&(!(*iter).isApplicable(orderingOnly))) {
         Plan* p2=plans.alloc();
         p2->op=Plan::Filter;
         p2->opArg=(*iter).id;
         p2->left=plan;
         p2->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::Filter*>(&(*iter)));
         p2->next=0;
         p2->cardinality=plan->cardinality*0.5;
         p2->costs=plan->costs+Costs::filter(plan->cardinality);
         p2->ordering=plan->ordering;
         plan=p2;
      }
   return plan;
}
//---------------------------------------------------------------------------
static void normalizePattern(Database::DataOrder order,unsigned& c1,unsigned& c2,unsigned& c3)
    // Extract subject/predicate/object order
{
   unsigned s=~0u,p=~0u,o=~0u;
   switch (order) {
      case Database::Order_Subject_Predicate_Object: s=c1; p=c2; o=c3; break;
      case Database::Order_Subject_Object_Predicate: s=c1; o=c2; p=c3; break;
      case Database::Order_Object_Predicate_Subject: o=c1; p=c2; s=c3; break;
      case Database::Order_Object_Subject_Predicate: o=c1; s=c2; p=c3; break;
      case Database::Order_Predicate_Subject_Object: p=c1; s=c2; o=c3; break;
      case Database::Order_Predicate_Object_Subject: p=c1; o=c2; s=c3; break;
   }
   c1=s; c2=p; c3=o;
}
//---------------------------------------------------------------------------
static unsigned getCardinality(Database& db,Database::DataOrder order,unsigned c1,unsigned c2,unsigned c3)
   // Estimate the cardinality of a predicate
{
   normalizePattern(order,c1,c2,c3);
   return db.getExactStatistics().getCardinality(c1,c2,c3);
}
//---------------------------------------------------------------------------
void PlanGen::buildIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* result,unsigned value1,unsigned value1C,unsigned value2,unsigned value2C,unsigned value3,unsigned value3C)
   // Build an index scan
{
   // Initialize a new plan
   Plan* plan=plans.alloc();
   plan->op=Plan::IndexScan;
   plan->opArg=order;
   plan->left=0;
   plan->right=0;
   plan->next=0;
   plan->subject=false;

   // Compute the statistics
   unsigned scanned;
   plan->original_cardinality=plan->cardinality=scanned=getCardinality(*db,order,value1C,value2C,value3C);
   if (!~value1) {
      if (!~value2) {
         plan->ordering=value3;
      } else {
         scanned=getCardinality(*db,order,value1C,value2C,~0u);
         plan->ordering=value2;
      }
   } else {
      scanned=getCardinality(*db,order,value1C,~0u,~0u);
      plan->ordering=value1;
   }
   unsigned pages=1+static_cast<unsigned>(db->getFacts(order).getPages()*(static_cast<double>(scanned)/static_cast<double>(db->getFacts(order).getCardinality())));
   plan->costs=Costs::seekBtree()+Costs::scan(pages);

   // Apply filters
   plan=buildFilters(plans,query,plan,value1,value2,value3);

   // Setting additional information for RP-Filter
   unsigned predicate;
   // Find predicate value
   switch(order) {
      case Database::Order_Predicate_Subject_Object: 
      case Database::Order_Predicate_Object_Subject: predicate = value1C; break;
      case Database::Order_Object_Predicate_Subject: 
      case Database::Order_Subject_Predicate_Object: 
           predicate =  (!~value2)?value2C:500000; 
           break;
      case Database::Order_Subject_Object_Predicate:
      case Database::Order_Object_Subject_Predicate: 
           predicate =  (!~value3)?value3C:500000; 
           break;
      default:
           assert(false);
   }
   plan->predicate = predicate;

   if (~value1) {
      if (order == Database::Order_Subject_Predicate_Object ||
            order == Database::Order_Subject_Object_Predicate)
         plan->subject = true;
   }
   if (!~value1 && ~value2) {
      if (order == Database::Order_Predicate_Subject_Object || 
            order == Database::Order_Object_Subject_Predicate)
         plan->subject = true;
   }
   if (!~value1 && !~value2 && ~value3) {
      if (order == Database::Order_Object_Predicate_Subject || 
            order == Database::Order_Predicate_Object_Subject)
         plan->subject = true;
   }

   // And store it
   if (db->rpathTreeIdx && !getenv("HEURISTIC")) {
      plan=buildRFLT(result, plans, plan, predicate, plan->subject);
   }
   if (db->rgindex) {
      plan=buildRFLT_RGINDEX(result, plans, plan, predicate, plan->subject);
   }
   addPlan(result,plan);
}
//---------------------------------------------------------------------------
void PlanGen::buildAggregatedIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* result,unsigned value1,unsigned value1C,unsigned value2,unsigned value2C)
   // Build an aggregated index scan
{
   // Refuse placing constants at the end
   // They should be pruned out anyway, but sometimes are not due to misestimations
   if ((!~value2)&&(~value1))
      return;

   // Initialize a new plan
   Plan* plan=plans.alloc();
   plan->op=Plan::AggregatedIndexScan;
   plan->opArg=order;
   plan->left=0;
   plan->right=0;
   plan->next=0;
   plan->subject=false;

   // Compute the statistics
   unsigned scanned=getCardinality(*db,order,value1C,value2C,~0u);
   unsigned fullSize=db->getFacts(order).getCardinality();
   if (scanned>fullSize)
      scanned=fullSize;
   if (!scanned) scanned=1;
   plan->original_cardinality=plan->cardinality=scanned;
   if (!~value1) {
      plan->ordering=value2;
   } else {
      plan->ordering=value1;
   }
   unsigned pages=1+static_cast<unsigned>(db->getAggregatedFacts(order).getPages()*(static_cast<double>(scanned)/static_cast<double>(fullSize)));
   plan->costs=Costs::seekBtree()+Costs::scan(pages);

   // Apply filters
   plan=buildFilters(plans,query,plan,value1,value2,~0u);

   // And store it
   if (addPlan(result,plan)) {
      if (db->rpathTreeIdx) {
         unsigned predicate;
         // Find predicate value
         switch(order) {
            case Database::Order_Predicate_Subject_Object: 
            case Database::Order_Predicate_Object_Subject: predicate = value1C; break;
            case Database::Order_Object_Predicate_Subject: 
            case Database::Order_Subject_Predicate_Object: 
               predicate = (!~value2)?value2C:500000; 
               break;
            case Database::Order_Subject_Object_Predicate:
            case Database::Order_Object_Subject_Predicate: 
               predicate = 500000; 
               break;
            default:
               assert(false);
         }
         plan->predicate = predicate;

         if (~value1) {
            if (order == Database::Order_Subject_Predicate_Object ||
                order == Database::Order_Subject_Object_Predicate)
               plan->subject = true;
         }
         if (!~value1 && ~value2) {
            if (order == Database::Order_Predicate_Subject_Object || 
                order == Database::Order_Object_Subject_Predicate)
               plan->subject = true;
         }
         buildRFLT(result, plans, plan, predicate, plan->subject);
      }
   }
}
//---------------------------------------------------------------------------
void PlanGen::buildFullyAggregatedIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* result,unsigned value1,unsigned value1C)
   // Build an fully aggregated index scan
{
   // Initialize a new plan
   Plan* plan=plans.alloc();
   plan->op=Plan::FullyAggregatedIndexScan;
   plan->opArg=order;
   plan->left=0;
   plan->right=0;
   plan->next=0;
   plan->subject=false;

   // Compute the statistics
   unsigned scanned=getCardinality(*db,order,value1C,~0u,~0u);
   unsigned fullSize=db->getFacts(order).getCardinality();
   if (scanned>fullSize)
      scanned=fullSize;
   if (!scanned) scanned=1;
   plan->original_cardinality=plan->cardinality=scanned;
   if (!~value1) {
      plan->ordering=~0u;
   } else {
      plan->ordering=value1;
   }
   unsigned pages=1+static_cast<unsigned>(db->getFullyAggregatedFacts(order).getPages()*(static_cast<double>(scanned)/static_cast<double>(fullSize)));
   plan->costs=Costs::seekBtree()+Costs::scan(pages);

   // Apply filters
   plan=buildFilters(plans,query,plan,value1,~0u,~0u);

   // And store it
   if (addPlan(result,plan)) {
      if (db->rpathTreeIdx) {
         unsigned predicate;
         // Find predicate value
         switch(order) {
            case Database::Order_Predicate_Subject_Object: 
            case Database::Order_Predicate_Object_Subject: predicate = value1C; break;
            case Database::Order_Object_Predicate_Subject: 
            case Database::Order_Subject_Predicate_Object: 
            case Database::Order_Subject_Object_Predicate:
            case Database::Order_Object_Subject_Predicate: 
               predicate = 500000; 
               break;
            default:
               assert(false);
         }
         plan->predicate = predicate;

         if (~value1) {
            if (order == Database::Order_Subject_Predicate_Object ||
                order == Database::Order_Subject_Object_Predicate)
               plan->subject = true;
         }
         buildRFLT(result, plans, plan, predicate, plan->subject);
      }
   }
}
//---------------------------------------------------------------------------
static bool isUnused(const QueryGraph::Filter* filter,unsigned val)
   // Check if a variable is unused
{
   if (!filter)
      return true;
   if (filter->type==QueryGraph::Filter::Variable)
      if (filter->id==val)
         return false;
   return isUnused(filter->arg1,val)&&isUnused(filter->arg2,val)&&isUnused(filter->arg3,val);
}
//---------------------------------------------------------------------------
static bool isUnused(const QueryGraph::SubQuery& query,const QueryGraph::Node& node,unsigned val)
   // Check if a variable is unused outside its primary pattern
{
   for (vector<QueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
      if (!isUnused(&(*iter),val))
         return false;
   for (vector<QueryGraph::Node>::const_iterator iter=query.nodes.begin(),limit=query.nodes.end();iter!=limit;++iter) {
      const QueryGraph::Node& n=*iter;
      if ((&n)==(&node))
         continue;
      if ((!n.constSubject)&&(val==n.subject)) return false;
      if ((!n.constPredicate)&&(val==n.predicate)) return false;
      if ((!n.constObject)&&(val==n.object)) return false;
   }
   for (vector<QueryGraph::SubQuery>::const_iterator iter=query.optional.begin(),limit=query.optional.end();iter!=limit;++iter)
      if (!isUnused(*iter,node,val))
         return false;
   for (vector<vector<QueryGraph::SubQuery> >::const_iterator iter=query.unions.begin(),limit=query.unions.end();iter!=limit;++iter)
      for (vector<QueryGraph::SubQuery>::const_iterator iter2=(*iter).begin(),limit2=(*iter).end();iter2!=limit2;++iter2)
         if (!isUnused(*iter2,node,val))
            return false;
   return true;
}
//---------------------------------------------------------------------------
static bool isUnused(const QueryGraph& query,const QueryGraph::Node& node,unsigned val)
   // Check if a variable is unused outside its primary pattern
{
   for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
      if ((*iter)==val)
         return false;

   return isUnused(query.getQuery(),node,val);
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildScan(const QueryGraph::SubQuery& query,const QueryGraph::Node& node,unsigned id)
   // Generate base table accesses
{
   // Create new problem instance
   Problem* result=problems.alloc();
   result->next=0;
   result->plans=0;
   result->relations=BitSet();
   result->relations.set(id);

   // Check which parts of the pattern are unused
   bool unusedSubject=(!node.constSubject)&&isUnused(*fullQuery,node,node.subject);
   bool unusedPredicate=(!node.constPredicate)&&isUnused(*fullQuery,node,node.predicate);
   bool unusedObject=(!node.constObject)&&isUnused(*fullQuery,node,node.object);

   // Lookup variables
   unsigned s=node.constSubject?~0u:node.subject,p=node.constPredicate?~0u:node.predicate,o=node.constObject?~0u:node.object;
   unsigned sc=node.constSubject?node.subject:~0u,pc=node.constPredicate?node.predicate:~0u,oc=node.constObject?node.object:~0u;

   // Build all relevant scans
   if ((unusedSubject+unusedPredicate+unusedObject)>=2) {
      if (!unusedSubject) {
         buildFullyAggregatedIndexScan(query,Database::Order_Subject_Predicate_Object,result,s,sc);
      } else if (!unusedObject) {
         buildFullyAggregatedIndexScan(query,Database::Order_Object_Subject_Predicate,result,o,oc);
      } else {
         buildFullyAggregatedIndexScan(query,Database::Order_Predicate_Subject_Object,result,s,sc);
      }
   } else {
      if (unusedObject)
         buildAggregatedIndexScan(query,Database::Order_Subject_Predicate_Object,result,s,sc,p,pc); else
         buildIndexScan(query,Database::Order_Subject_Predicate_Object,result,s,sc,p,pc,o,oc);
      if (unusedPredicate)
         buildAggregatedIndexScan(query,Database::Order_Subject_Object_Predicate,result,s,sc,o,oc); else
         buildIndexScan(query,Database::Order_Subject_Object_Predicate,result,s,sc,o,oc,p,pc);
      if (unusedSubject)
         buildAggregatedIndexScan(query,Database::Order_Object_Predicate_Subject,result,o,oc,p,pc); else
         buildIndexScan(query,Database::Order_Object_Predicate_Subject,result,o,oc,p,pc,s,sc);
      if (unusedPredicate)
         buildAggregatedIndexScan(query,Database::Order_Object_Subject_Predicate,result,o,oc,s,sc); else
         buildIndexScan(query,Database::Order_Object_Subject_Predicate,result,o,oc,s,sc,p,pc);
      if (unusedObject)
         buildAggregatedIndexScan(query,Database::Order_Predicate_Subject_Object,result,p,pc,s,sc); else
         buildIndexScan(query,Database::Order_Predicate_Subject_Object,result,p,pc,s,sc,o,oc);
      if (unusedSubject)
         buildAggregatedIndexScan(query,Database::Order_Predicate_Object_Subject,result,p,pc,o,oc); else
         buildIndexScan(query,Database::Order_Predicate_Object_Subject,result,p,pc,o,oc,s,sc);
   }

   // Update the child pointers as info for the code generation
   for (Plan* iter=result->plans;iter;iter=iter->next) {
      Plan* iter2=iter;
      while (iter2->op==Plan::Filter || iter2->op==Plan::RFLT)
         iter2=iter2->left;
      iter2->left=static_cast<Plan*>(0)+id;
      iter2->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::Node*>(&node));
   }

   return result;
}
//---------------------------------------------------------------------------
PlanGen::JoinDescription PlanGen::buildJoinInfo(const QueryGraph::SubQuery& query,const QueryGraph::Edge& edge)
   // Build the informaion about a join
{
   // Fill in the relations involved
   JoinDescription result;
   result.left.set(edge.from);
   result.right.set(edge.to);

   // Compute the join selectivity
   const QueryGraph::Node& l=query.nodes[edge.from],&r=query.nodes[edge.to];
   result.selectivity=db->getExactStatistics().getJoinSelectivity(l.constSubject,l.subject,l.constPredicate,l.predicate,l.constObject,l.object,r.constSubject,r.subject,r.constPredicate,r.predicate,r.constObject,r.object);

   // Look up suitable orderings
   if (!edge.common.empty()) {
      result.ordering=edge.common.front(); // XXX multiple orderings possible
   } else {
      // Cross product
      result.ordering=(~0u)-1;
      result.selectivity=-1;
   }
   result.tableFunction=0;

   return result;
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildOptional(const QueryGraph::SubQuery& query,unsigned id)
   // Generate an optional part
{
   // Solve the subproblem
   Plan* p=translate(query);

   // Create new problem instance
   Problem* result=problems.alloc();
   result->next=0;
   result->plans=p;
   result->relations=BitSet();
   result->relations.set(id);

   return result;
}
//---------------------------------------------------------------------------
static void collectVariables(const QueryGraph::Filter* filter,set<unsigned>& vars,const void* except)
   // Collect all variables used in a filter
{
   if ((!filter)||(filter==except))
      return;
   if (filter->type==QueryGraph::Filter::Variable)
      vars.insert(filter->id);
   collectVariables(filter->arg1,vars,except);
   collectVariables(filter->arg2,vars,except);
   collectVariables(filter->arg3,vars,except);
}
//---------------------------------------------------------------------------
static void collectVariables(const QueryGraph::SubQuery& query,set<unsigned>& vars,const void* except)
   // Collect all variables used in a subquery
{
   for (vector<QueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
      collectVariables(&(*iter),vars,except);
   for (vector<QueryGraph::Node>::const_iterator iter=query.nodes.begin(),limit=query.nodes.end();iter!=limit;++iter) {
      const QueryGraph::Node& n=*iter;
      if (except==(&n))
         continue;
      if (!n.constSubject) vars.insert(n.subject);
      if (!n.constPredicate) vars.insert(n.predicate);
      if (!n.constObject) vars.insert(n.object);
   }
   for (vector<QueryGraph::SubQuery>::const_iterator iter=query.optional.begin(),limit=query.optional.end();iter!=limit;++iter)
      if (except!=(&(*iter)))
         collectVariables(*iter,vars,except);
   for (vector<vector<QueryGraph::SubQuery> >::const_iterator iter=query.unions.begin(),limit=query.unions.end();iter!=limit;++iter)
      if (except!=(&(*iter)))
         for (vector<QueryGraph::SubQuery>::const_iterator iter2=(*iter).begin(),limit2=(*iter).end();iter2!=limit2;++iter2)
            if (except!=(&(*iter2)))
               collectVariables(*iter2,vars,except);
}
//---------------------------------------------------------------------------
static void collectVariables(const QueryGraph& query,set<unsigned>& vars,const void* except)
   // Collect all variables used in a query
{
   for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
      vars.insert(*iter);
   if (except!=(&(query.getQuery())))
      collectVariables(query.getQuery(),vars,except);
}
//---------------------------------------------------------------------------
static Plan* findOrdering(Plan* root,unsigned ordering)
   // Find a plan with a specific ordering
{
   for (;root;root=root->next)
      if (root->ordering==ordering)
         return root;
   return 0;
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildUnion(const vector<QueryGraph::SubQuery>& query,unsigned id)
   // Generate a union part
{
   // Solve the subproblems
   vector<Plan*> parts,solutions;
   for (unsigned index=0;index<query.size();index++) {
      Plan* p=translate(query[index]),*bp=p;
      for (Plan* iter=p;iter;iter=iter->next)
         if (iter->costs<bp->costs)
            bp=iter;
      parts.push_back(bp);
      solutions.push_back(p);
   }

   // Compute statistics
   Plan::card_t card=0;
   Plan::cost_t costs=0;
   for (vector<Plan*>::const_iterator iter=parts.begin(),limit=parts.end();iter!=limit;++iter) {
      card+=(*iter)->cardinality;
      costs+=(*iter)->costs;
   }

   // Create a new problem instance
   Problem* result=problems.alloc();
   result->next=0;
   result->plans=0;
   result->relations=BitSet();
   result->relations.set(id);

   // And create a union operator
   Plan* last=plans.alloc();
   last->op=Plan::Union;
   last->opArg=0;
   last->left=parts[0];
   last->right=parts[1];
   last->cardinality=card;
   last->costs=costs;
   last->ordering=~0u;
   last->next=0;
   result->plans=last;
   for (unsigned index=2;index<parts.size();index++) {
      Plan* nextPlan=plans.alloc();
      nextPlan->left=last->right;
      last->right=nextPlan;
      last=nextPlan;
      last->op=Plan::Union;
      last->opArg=0;
      last->right=parts[index];
      last->cardinality=card;
      last->costs=costs;
      last->ordering=~0u;
      last->next=0;
   }

   // Could we also use a merge union?
   set<unsigned> otherVars,unionVars;
   vector<unsigned> commonVars;
   collectVariables(*fullQuery,otherVars,&query);
   for (vector<QueryGraph::SubQuery>::const_iterator iter=query.begin(),limit=query.end();iter!=limit;++iter)
      collectVariables(*iter,unionVars,0);
   set_intersection(otherVars.begin(),otherVars.end(),unionVars.begin(),unionVars.end(),back_inserter(commonVars));
   if (commonVars.size()==1) {
      unsigned resultVar=commonVars[0];
      // Can we get all plans sorted in this way?
      bool canMerge=true;
      costs=0;
      for (vector<Plan*>::const_iterator iter=solutions.begin(),limit=solutions.end();iter!=limit;++iter) {
         Plan* p;
         if ((p=findOrdering(*iter,resultVar))==0) {
            canMerge=false;
            break;
         }
         costs+=p->costs;
      }
      // Yes, build the plan
      if (canMerge) {
         Plan* last=plans.alloc();
         last->op=Plan::MergeUnion;
         last->opArg=0;
         last->left=findOrdering(solutions[0],resultVar);
         last->right=findOrdering(solutions[1],resultVar);
         last->cardinality=card;
         last->costs=costs;
         last->ordering=resultVar;
         last->next=0;
         result->plans->next=last;
         for (unsigned index=2;index<solutions.size();index++) {
            Plan* nextPlan=plans.alloc();
            nextPlan->left=last->right;
            last->right=nextPlan;
            last=nextPlan;
            last->op=Plan::MergeUnion;
            last->opArg=0;
            last->right=findOrdering(solutions[index],resultVar);
            last->cardinality=card;
            last->costs=costs;
            last->ordering=resultVar;
            last->next=0;
         }
      }
   }

   return result;
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildTableFunction(const QueryGraph::TableFunction& /*function*/,unsigned id)
   // Generate a table function access
{
   // Create new problem instance
   Problem* result=problems.alloc();
   result->next=0;
   result->plans=0;
   result->relations=BitSet();
   result->relations.set(id);

   return result;
}
//---------------------------------------------------------------------------
static void findFilters(Plan* plan,set<const QueryGraph::Filter*>& filters)
   // Find all filters already applied in a plan
{
   switch (plan->op) {
      case Plan::Union:
      case Plan::MergeUnion:
         // A nested subquery starts here, stop
         break;
      case Plan::IndexScan:
      case Plan::AggregatedIndexScan:
      case Plan::FullyAggregatedIndexScan:
      case Plan::Singleton:
         // We reached a leaf.
         break;
      case Plan::Filter:
         filters.insert(reinterpret_cast<QueryGraph::Filter*>(plan->right));
         findFilters(plan->left,filters);
         break;
      case Plan::NestedLoopJoin:
      case Plan::MergeJoin:
      case Plan::HashJoin:
         findFilters(plan->left,filters);
         findFilters(plan->right,filters);
         break;
      case Plan::HashGroupify: case Plan::TableFunction:
         findFilters(plan->left,filters);
         break;
      case Plan::RFLT:
         break;
      case Plan::RFLT_M:
         break;
   }
}
//---------------------------------------------------------------------------
Plan* PlanGen::translate(const QueryGraph::SubQuery& query)
   // Translate a query into an operator tree
{
   bool singletonNeeded=(!(query.nodes.size()+query.optional.size()+query.unions.size()))&&query.tableFunctions.size();

   // Check if we could handle the query
   if ((query.nodes.size()+query.optional.size()+query.unions.size()+query.tableFunctions.size()+singletonNeeded)>BitSet::maxWidth)
      return 0;

   // Seed the DP table with scans
   vector<Problem*> dpTable;
   dpTable.resize(query.nodes.size()+query.optional.size()+query.unions.size()+query.tableFunctions.size()+singletonNeeded);
   Problem* last=0;
   unsigned id=0;
   for (vector<QueryGraph::Node>::const_iterator iter=query.nodes.begin(),limit=query.nodes.end();iter!=limit;++iter,++id) {
      Problem* p=buildScan(query,*iter,id);
      if (last)
         last->next=p; else
         dpTable[0]=p;
      last=p;
   }
   for (vector<QueryGraph::SubQuery>::const_iterator iter=query.optional.begin(),limit=query.optional.end();iter!=limit;++iter,++id) {
      Problem* p=buildOptional(*iter,id);
      if (last)
         last->next=p; else
         dpTable[0]=p;
      last=p;
   }
   for (vector<vector<QueryGraph::SubQuery> >::const_iterator iter=query.unions.begin(),limit=query.unions.end();iter!=limit;++iter,++id) {
      Problem* p=buildUnion(*iter,id);
      if (last)
         last->next=p; else
         dpTable[0]=p;
      last=p;
   }
   unsigned functionIds=id;
   for (vector<QueryGraph::TableFunction>::const_iterator iter=query.tableFunctions.begin(),limit=query.tableFunctions.end();iter!=limit;++iter,++id) {
      Problem* p=buildTableFunction(*iter,id);
      if (last)
         last->next=p; else
         dpTable[0]=p;
      last=p;
   }
   unsigned singletonId=id;
   if (singletonNeeded) {
      Plan* plan=plans.alloc();
      plan->op=Plan::Singleton;
      plan->opArg=0;
      plan->left=0;
      plan->right=0;
      plan->next=0;
      plan->cardinality=1;
      plan->ordering=~0u;
      plan->costs=0;

      Problem* problem=problems.alloc();
      problem->next=0;
      problem->plans=plan;
      problem->relations=BitSet();
      problem->relations.set(id);
      if (last)
         last->next=problem; else
         dpTable[0]=problem;
      last=problem;
   }

   // Construct the join info
   vector<JoinDescription> joins;
   for (vector<QueryGraph::Edge>::const_iterator iter=query.edges.begin(),limit=query.edges.end();iter!=limit;++iter)
      joins.push_back(buildJoinInfo(query,*iter));
   id=functionIds;
   for (vector<QueryGraph::TableFunction>::const_iterator iter=query.tableFunctions.begin(),limit=query.tableFunctions.end();iter!=limit;++iter,++id) {
      JoinDescription join;
      set<unsigned> input;
      for (vector<QueryGraph::TableFunction::Argument>::const_iterator iter2=(*iter).input.begin(),limit2=(*iter).input.end();iter2!=limit2;++iter2)
         if (~(*iter2).id)
            input.insert((*iter2).id);
      for (unsigned index=0;index<query.nodes.size();index++) {
         unsigned s=query.nodes[index].constSubject?~0u:query.nodes[index].subject;
         unsigned p=query.nodes[index].constPredicate?~0u:query.nodes[index].predicate;
         unsigned o=query.nodes[index].constObject?~0u:query.nodes[index].object;
         if (input.count(s)||input.count(p)||input.count(o)||input.empty())
            join.left.set(index);
      }
      if (singletonNeeded&&input.empty())
         join.left.set(singletonId);
      for (unsigned index=0;index<query.tableFunctions.size();index++) {
         bool found=false;
         for (vector<unsigned>::const_iterator iter=query.tableFunctions[index].output.begin(),limit=query.tableFunctions[index].output.end();iter!=limit;++iter)
            if (input.count(*iter)) {
               found=true;
               break;
            }
         if (found)
            join.left.set(functionIds+index);
      }
      join.right.set(id);
      join.ordering=~0u;
      join.selectivity=1;
      join.tableFunction=&(*iter);
      joins.push_back(join);
   }

   // Build larger join trees
   vector<unsigned> joinOrderings;
   for (unsigned index=1;index<dpTable.size();index++) {
      map<BitSet,Problem*> lookup;
      for (unsigned index2=0;index2<index;index2++) {
         for (Problem* iter=dpTable[index2];iter;iter=iter->next) {
            BitSet leftRel=iter->relations;
            for (Problem* iter2=dpTable[index-index2-1];iter2;iter2=iter2->next) {
               // Overlapping subproblem?
               BitSet rightRel=iter2->relations;
               if (leftRel.overlapsWith(rightRel))
                  continue;

               // Investigate all join candidates
               Problem* problem=0;
               double selectivity=1;
               for (vector<JoinDescription>::const_iterator iter3=joins.begin(),limit3=joins.end();iter3!=limit3;++iter3)
                  if (((*iter3).left.subsetOf(leftRel))&&((*iter3).right.subsetOf(rightRel))) {
                     if (!iter->plans) break;
                     // We can join it...
                     BitSet relations=leftRel.unionWith(rightRel);
                     if (lookup.count(relations)) {
                        problem=lookup[relations];
                     } else {
                        lookup[relations]=problem=problems.alloc();
                        problem->relations=relations;
                        problem->plans=0;
                        problem->next=dpTable[index];
                        dpTable[index]=problem;
                     }
                     // Table function call?
                     if ((*iter3).tableFunction) {
                        for (Plan* leftPlan=iter->plans;leftPlan;leftPlan=leftPlan->next) {
                           Plan* p=plans.alloc();
                           p->op=Plan::TableFunction;
                           p->opArg=0;
                           p->left=leftPlan;
                           p->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::TableFunction*>((*iter3).tableFunction));
			   p->next=0;
                           p->cardinality=leftPlan->cardinality;
                           p->costs=leftPlan->costs+Costs::tableFunction(leftPlan->cardinality);
                           p->ordering=leftPlan->ordering;
                           addPlan(problem,p);
                        }

                        problem=0;
                        break;
                     }
                     // Collect selectivities and join order candidates
                     joinOrderings.clear();
                     joinOrderings.push_back((*iter3).ordering);
                     selectivity=(*iter3).selectivity;
                     for (++iter3;iter3!=limit3;++iter3) {
                        joinOrderings.push_back((*iter3).ordering);
                        // selectivity*=(*iter3).selectivity;
                     }
                     break;
                  }
               if (!problem) continue;

               // Combine phyiscal plans
               for (Plan* leftPlan=iter->plans;leftPlan;leftPlan=leftPlan->next) {
                  for (Plan* rightPlan=iter2->plans;rightPlan;rightPlan=rightPlan->next) {
                     // Try a merge joins
                     if (leftPlan->ordering==rightPlan->ordering) {
                        for (vector<unsigned>::const_iterator iter=joinOrderings.begin(),limit=joinOrderings.end();iter!=limit;++iter) {
                           if (leftPlan->ordering==(*iter)) {
                              // if the leftPlan or rightPlan is RFLT or RFLT_M, build RFLT_M
                              if (db->rgindex && getenv("USE_RFLTM")) {
                                 if (leftPlan->op==Plan::RFLT || leftPlan->op==Plan::RFLT_M ||
                                     rightPlan->op==Plan::RFLT || rightPlan->op==Plan::RFLT_M) {
                                    buildRFLT_M_RGINDEX(problem, plans, leftPlan->ordering, 
                                                leftPlan, rightPlan, selectivity);
                                    break;
                                 }
                              }
                              Plan* p=plans.alloc();
                              p->op=Plan::MergeJoin;
                              p->opArg=*iter;
                              p->left=leftPlan;
                              p->right=rightPlan;
                              p->next=0;
                              p->original_cardinality=leftPlan->original_cardinality*rightPlan->original_cardinality*selectivity;
                              if (p->original_cardinality<1) p->original_cardinality=1;
                              if ((p->cardinality=leftPlan->cardinality*rightPlan->cardinality*selectivity*1)<1) p->cardinality=1;
                              p->costs=leftPlan->costs+rightPlan->costs+Costs::mergeJoin(leftPlan->cardinality,rightPlan->cardinality);
                              p->ordering=leftPlan->ordering;
                              addPlan(problem,p);
                              break;
                           }
                        }
                     }
                     // Try a hash join
                     if (selectivity>=0) {
                        Plan* p=plans.alloc();
                        p->op=Plan::HashJoin;
                        p->opArg=0;
                        p->left=leftPlan;
                        p->right=rightPlan;
               			p->next=0;
                        p->original_cardinality=leftPlan->original_cardinality*rightPlan->original_cardinality*selectivity*1;
                        if (p->original_cardinality<1) p->original_cardinality=1;
                        if ((p->cardinality=leftPlan->cardinality*rightPlan->cardinality*selectivity*1)<1) p->cardinality=1;
                        p->costs=leftPlan->costs+rightPlan->costs+Costs::hashJoin(leftPlan->cardinality,rightPlan->cardinality);
                        p->ordering=~0u;
                        addPlan(problem,p);
                        // Second order
                        p=plans.alloc();
                        p->op=Plan::HashJoin;
                        p->opArg=0;
                        p->left=rightPlan;
                        p->right=leftPlan;
               			p->next=0;
                        p->original_cardinality=leftPlan->original_cardinality*rightPlan->original_cardinality*selectivity*1;
                        if (p->original_cardinality<1) p->original_cardinality=1;
                        if ((p->cardinality=leftPlan->cardinality*rightPlan->cardinality*selectivity*1)<1) p->cardinality=1;
                        p->costs=leftPlan->costs+rightPlan->costs+Costs::hashJoin(rightPlan->cardinality,leftPlan->cardinality);
                        p->ordering=~0u;
                        addPlan(problem,p);
                     } else {
                        // Nested loop join
                        Plan* p=plans.alloc();
                        p->op=Plan::NestedLoopJoin;
                        p->opArg=0;
                        p->left=leftPlan;
                        p->right=rightPlan;
               			p->next=0;
                        p->original_cardinality=leftPlan->original_cardinality*rightPlan->original_cardinality;
                        if ((p->cardinality=leftPlan->cardinality*rightPlan->cardinality)<1) p->cardinality=1;
                        p->costs=leftPlan->costs+rightPlan->costs+leftPlan->cardinality*rightPlan->costs;
                        p->ordering=leftPlan->ordering;
                        addPlan(problem,p);
                     }
                  }
               }
            }
         }
      }
   }
   // Extract the bestplan
   if (dpTable.empty()||(!dpTable.back()))
      return 0;
   Plan* plan=dpTable.back()->plans;
   if (!plan)
      return 0;

   // Add all remaining filters
   set<const QueryGraph::Filter*> appliedFilters;
   findFilters(plan,appliedFilters);
   for (vector<QueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
      if (!appliedFilters.count(&(*iter))) {
         Plan* p=plans.alloc();
         p->op=Plan::Filter;
         p->opArg=0;
         p->left=plan;
         p->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::Filter*>(&(*iter)));
         p->next=0;
         p->cardinality=plan->cardinality; // XXX real computation
         p->costs=plan->costs;
         p->ordering=plan->ordering;
         plan=p;
      }

   // Return the complete plan
   return plan;
}
//---------------------------------------------------------------------------
static void buildGraph(QueryGraph& query, GSPAN::Graph &graph, RGindex *rg_index) 
{
   unsigned maxNodeID = 0;

   // assume a single subquery
   QueryGraph::SubQuery sq = query.getQuery();

   // get maximum node ID
   unsigned constCnt=0;
   for (vector<QueryGraph::Node>::const_iterator iter=sq.nodes.begin(),
        limit=sq.nodes.end(); iter!=limit;++iter) {
      QueryGraph::Node n = *iter;
      if (n.constSubject || n.constObject) {
         constCnt++;
         continue;
      }
      maxNodeID=max(maxNodeID, n.subject);
      maxNodeID=max(maxNodeID, n.object);
   }
   graph.resize(maxNodeID+constCnt+1);

   unsigned indexedE=0, edgeCnt=0, constNodeID=maxNodeID+1;
   for (vector<QueryGraph::Node>::const_iterator iter=sq.nodes.begin(),limit=sq.nodes.end();
         iter!=limit;++iter) {
      QueryGraph::Node n=(*iter);
      unsigned objID=n.object;

      if (n.constObject)
         objID=constNodeID++;

      if (rg_index->predMap_old.count(n.predicate)) {
         graph[n.subject].push(n.subject, objID, rg_index->predMap_old[n.predicate],
               GSPAN::EDGE_TYPE_NORMAL);
         indexedE++; 
         edgeCnt++;
      }
   }
   cout << "graphEdge:" << edgeCnt << " indexed:" << indexedE << endl;
   graph.buildEdge();
   graph.write(cout);
}
//---------------------------------------------------------------------------
static bool getSuperGraphs(GSPAN::Graph &G, unsigned v, GSPAN::Graph &g, RGindex *rg_index,
                           map<GSPAN::DFSCode, unsigned> &subgraphs,
                           set<GSPAN::DFSCode> &examinedSubgraphs, unsigned maxL)
{
   if (maxL==(unsigned)atoi(getenv("MAXL"))) {
      GSPAN::DFSCode dfscode;
      g.buildEdge();
      dfscode.fromGraph(g);
      subgraphs.insert(pair<GSPAN::DFSCode, unsigned>(dfscode, dfscode.idToSubsMap[v]));
      return true;
   }

   set<GSPAN::DFS> edges;
   for (unsigned from=0; from<g.size(); from++) {
      for (GSPAN::Vertex::edge_iterator iter=g[from].edge.begin(),
           limit=g[from].edge.end(); iter!=limit; iter++) {
         GSPAN::DFS dfs;
         dfs.src=iter->from;
         dfs.dest=iter->to;
         dfs.elabel=iter->elabel;
         dfs.type=iter->type;
         edges.insert(dfs);
      }
   }

   vector<GSPAN::DFS> adj_edges;
   for (int from=0; from<(int) g.size(); from++) {
      if (!g[from].set)
         continue;

      for (GSPAN::Vertex::edge_iterator it = G[from].edge.begin();
           it != G[from].edge.end(); ++it) {
         GSPAN::DFS dfs;

         if (it->elabel==500000)
            continue;

         dfs.src=it->from;
         dfs.dest=it->to;
         dfs.elabel=it->elabel;
         dfs.type=it->type;

         if (edges.count(dfs)==0) {
            adj_edges.push_back(dfs);
         }
      }
   }
   cout << "adj_edges:" << adj_edges.size() << endl;

   bool haveSuperGraph=false;
   for (vector<GSPAN::DFS>::iterator iter=adj_edges.begin(),
        limit=adj_edges.end(); iter!=limit; iter++) {
      GSPAN::Graph g1=g, g2;
      GSPAN::DFSCode dfscode;
      g1[iter->src].push(iter->src, iter->dest, iter->elabel, iter->type);
      g1.resize(std::max(std::max(iter->src, iter->dest) + 1, (int) g1.size()));
      g1[iter->src].set=true;
      g1[iter->dest].set=true;
      g2=g1;
      g2.buildEdge();
      dfscode.fromGraph(g2);
      if (examinedSubgraphs.find(dfscode)==examinedSubgraphs.end()) {
         examinedSubgraphs.insert(dfscode);
         if (rg_index->SearchNode(rg_index->root, dfscode, 0)) {
            getSuperGraphs(G, v, g1, rg_index, subgraphs, examinedSubgraphs, maxL+1);
            haveSuperGraph=true; 
         }
      }
   }

   g.buildEdge();
   if (!haveSuperGraph && g.edge_size()>0) {
      GSPAN::DFSCode dfscode;
      dfscode.fromGraph(g);
      subgraphs.insert(pair<GSPAN::DFSCode, unsigned>(dfscode, dfscode.idToSubsMap[v]));
      return true;
   }
   return false;
}
//---------------------------------------------------------------------------
static void getCandidateSubgraphs(GSPAN::Graph &G, unsigned v, RGindex *rg_index,
                                  map<GSPAN::DFSCode, unsigned> &subgraphs) 
{
   GSPAN::Graph g;
   g.resize(v+1);
   g[v].set=true;
   set<GSPAN::DFSCode> examinedSubgraphs;
   getSuperGraphs(G, v, g, rg_index, subgraphs, examinedSubgraphs,0);
   cout << "examinedSubgraphs:" << examinedSubgraphs.size() 
        << " candidates:" << subgraphs.size() << endl;
}
//---------------------------------------------------------------------------
Plan* PlanGen::translate(Database& db,QueryGraph& query)
   // Translate a query into an operator tree
{
   // Reset the plan generator
   plans.clear();
   problems.freeAll();
   this->db=&db;
   fullQuery=&query;

   if (getenv("PRINT"))
      query.print();

   if (getenv("RPATH")) {
      RPathQueryGraph rpathqg(query);
      if (getenv("PRINT"))
         rpathqg.printCFS(atoi(getenv("MAXL")));
   }

      
   if (getenv("RGINDEX")) {
      // build GSPAN::Graph from QueryGraph
      buildGraph(query, gspan_graph, db.rgindex);
   }

   // Retrieve the base plan
   Plan* plan=translate(query.getQuery());
   if (!plan)
      return 0;
   Plan* best=plan;
   if (!getenv("RGINDEX") || getenv("HEURISTIC")) {
      for (Plan* iter=plan;iter;iter=iter->next)
         if ((!best)||(iter->costs<best->costs)||((iter->costs==best->costs)&&(iter->cardinality<best->cardinality)))
            best=iter;
      if (!best)
         return 0;

      if (getenv("RPATH") && getenv("HEURISTIC")) {
         addRPFLTbyHeuristic(best);
      }
   }

   // Aggregate, if required
   if ((query.getDuplicateHandling()==QueryGraph::CountDuplicates)||(query.getDuplicateHandling()==QueryGraph::NoDuplicates)||(query.getDuplicateHandling()==QueryGraph::ShowDuplicates)) {
      Plan* p=plans.alloc();
      p->op=Plan::HashGroupify;
      p->opArg=0;
      p->left=best;
      p->right=0;
      p->next=0;
      p->cardinality=best->cardinality; // not correct, be we do not use this value anyway
      p->costs=best->costs; // the same here
      p->ordering=~0u;
      best=p;
   }

   return best;
}
//---------------------------------------------------------------------------
bool PlanGen::getRPathFLTSelectivityCosts(vector<RPathTreeIndex::Node*>& rflts, 
                                          unsigned predicate, bool subject, 
                                          double &selectivity, double &costs)
{
   selectivity = 1.0f;
   costs = 0;
   if (subject) predicate += (REVERSE_PRED);
   unsigned denominator = db->rpathTreeIdx->FindCardinality(predicate);

   for(vector<RPathTreeIndex::Node*>::iterator iter=rflts.begin(),limit=rflts.end();
       iter!=limit;++iter) {
      RPathTreeIndex::Node *node = (*iter);
      if (node->predicate == predicate) {
         double prev_selectivity = selectivity;
         selectivity *= (double) node->cardinality / (double) denominator;
         if (selectivity == 0)
            cout << "selectivity1: " <<  selectivity << 
                    " node->cardinality:" << node->cardinality << 
                    " prev_selectivity:" << prev_selectivity << 
                    " denominator:" << denominator << endl;
      }
      else {
         unsigned intersect = db->rpathStat->getStat(predicate, node->predicate);
         //assert (intersect);
         unsigned denominator2 = db->rpathTreeIdx->FindCardinality(node->predicate);
         double prev_selectivity = selectivity;
         selectivity *= ((double) node->cardinality / (double) denominator2 * 
               (double) intersect / (double) denominator);
         if (selectivity == 0)
            cout << "selectivity2: " <<  selectivity << 
                    " node->cardinality:" << node->cardinality << 
                    " prev_selectivity:" << prev_selectivity << 
                    " denominator:" << denominator << 
                    " denominator2:" << denominator2 <<
                    " intersect:" << intersect << endl;
      }
      costs += Costs::scan((node->startIndexPage-node->startPage)/4);
   }
   return true;
}
//---------------------------------------------------------------------------
/*
static bool compare(CFSEntry *entry1, CFSEntry *entry2)
{
   return entry1->cardinality < entry2->cardinality;
   //return entry1->costs < entry2->costs;
}
*/
//---------------------------------------------------------------------------
static double getEffect(Database *db, Plan* plan, RPathTreeIndex::Node *node) 
{
   unsigned predicate = plan->predicate;
   bool subject = plan->subject;
   double effect;

   if (subject) predicate += (REVERSE_PRED);
   unsigned originalcnt = db->rpathTreeIdx->FindCardinality(predicate);

   if (node->predicate == predicate) {
      effect = (double) node->cardinality / (double) originalcnt;
   }
   else {
      unsigned intersect = db->rpathStat->getStat(predicate, node->predicate);
      unsigned othercnt = db->rpathTreeIdx->FindCardinality(node->predicate);
      effect = ((double) node->cardinality / (double) othercnt * 
                (double) intersect / (double) originalcnt);

   }
   return effect;
}
//---------------------------------------------------------------------------
static double getFilteredCardinality(Database *db, Plan* plan, unsigned csetCnt,
   vector<RPathTreeIndex::Node*> &nodes) {
   // calculate filtered output cardinality for an index scan

   // effect : the number of vertex in filter / the number of distinct vertex in sortkey column
   double effect;
   double cardinality=plan->cardinality;
   unsigned predicate = plan->predicate;
   if (plan->subject) predicate += (REVERSE_PRED);
   unsigned originalcnt = db->rpathTreeIdx->FindCardinality(predicate);

   for(vector<RPathTreeIndex::Node*>::iterator iter=nodes.begin(),limit=nodes.end();
       iter!=limit;++iter) {
      RPathTreeIndex::Node *node = (*iter);
      effect = getEffect(db, plan, node);
      double filtered=(double)plan->cardinality*effect;
      if (filtered < cardinality)
         cardinality=filtered;
   }
   double filtered=(double)plan->cardinality*((double) csetCnt / (double) originalcnt);
   if (filtered < cardinality)
      cardinality=filtered;

   if (cardinality < 1) cardinality=1;
   return cardinality;
}
//---------------------------------------------------------------------------
Plan* PlanGen::buildRFLT_RGINDEX(Problem* /*result*/,PlanContainer& plans, Plan* plan,
                                 unsigned predicate, bool subject)
{
   map<GSPAN::DFSCode, unsigned> subgraphs;
   getCandidateSubgraphs(gspan_graph, plan->ordering, db->rgindex, subgraphs);

   cout << "predicate:" << predicate << " ordering:" << plan->ordering << endl;

   set<unsigned> preds;
   for (GSPAN::Vertex::edge_iterator iter=gspan_graph[plan->ordering].edge.begin(),
        limit=gspan_graph[plan->ordering].edge.end(); iter!=limit; iter++) {
      GSPAN::Edge edge=*iter;
      if (edge.type==GSPAN::EDGE_TYPE_NORMAL)
         preds.insert(db->rgindex->predMap_new[edge.elabel]+REVERSE_PRED);
      else
         preds.insert(db->rgindex->predMap_new[edge.elabel]);
   }

   unsigned minCard=~0u;//, card=0;
   vector<RGindex::Node*> nodes;
   vector<unsigned> nodeIds;
   vector<double> cards;
   vector<GSPAN::DFSCode> dfscodes;
   for (map<GSPAN::DFSCode, unsigned>::iterator iter=subgraphs.begin(),
         limit=subgraphs.end(); iter!=limit; iter++) {
      GSPAN::DFSCode dfscode=iter->first;
      unsigned vID=iter->second;
      RGindex::Node *node=db->rgindex->SearchNode(db->rgindex->root, dfscode, 0);
      minCard=min(node->cardinalities[vID], minCard);
      nodes.push_back(node);
      cards.push_back(node->cardinalities[vID]);
      nodeIds.push_back(vID);
      dfscodes.push_back(dfscode);
   }
   vector<unsigned> pred_vec(preds.begin(), preds.end());
   unsigned csetCard=db->cset->getCnt(pred_vec);
   minCard=min(csetCard, minCard);
   cards.push_back(plan->cardinality);

   vector<unsigned> tmp;
   if (subject)
      tmp.push_back(predicate+REVERSE_PRED);
   else
      tmp.push_back(predicate);
   unsigned values=db->cset->getCnt(tmp);

   Plan* p2=plans.alloc();
   p2->op=Plan::RFLT;
   p2->opArg=0;
   p2->left=plan;
   p2->right=0;
   p2->rgindexNodes=nodes;
   p2->nodeIds=nodeIds;
   p2->next=0;
   p2->cardinality=(double) plan->cardinality * (double) minCard / (double) values;
   p2->original_cardinality=plan->cardinality;
   p2->costs=plan->costs+Costs::merge(cards);
   p2->ordering=plan->ordering;
   p2->dfscodes=dfscodes;

   return p2; 
}
//---------------------------------------------------------------------------
Plan* PlanGen::buildRFLT(Problem* /*result*/,PlanContainer& plans,Plan* plan,unsigned predicate,
                        bool subject)
{
   if (subject) predicate += (REVERSE_PRED);

   // find incoming predicate paths and their nodes
   vector<PPath> cfs = 
      fullQuery->rpathQueryGraph->getCandidateFilterSetPPath(plan->ordering, db->maxL);

   vector<PPath> cfs_noredun;
   for (int i=0,limit=cfs.size();i<limit;i++) {
      PPath ppath1 = cfs[i];
      bool isRedundant=false;

      if (ppath1.size()==1 && ppath1.path[0]==plan->predicate)
         continue;

      if (ppath1.size()==1 && plan->subject && ppath1.path[0]==plan->predicate+REVERSE_PRED)
         continue;

      for (int j=0;j<limit;j++) {
         if (i==j) continue;
         PPath ppath2 = cfs[j];

         if (ppath1.contained(ppath2)) {
            RPathTreeIndex::Node *node = db->rpathTreeIdx->SearchNode(ppath2);
            if (node) {
               isRedundant=true;
               break;
            }
         }
      }
      if (isRedundant)
         continue;
      cfs_noredun.push_back(ppath1);
   }

   vector<RPathTreeIndex::Node*> nodes;
   double costs = 0, effect;
   vector<PPath> ppaths;
   for(vector<PPath>::iterator iter=cfs_noredun.begin(),limit=cfs_noredun.end();iter!=limit;++iter) {
      RPathTreeIndex::Node *node = db->rpathTreeIdx->SearchNode((*iter));
      if (node) {
         nodes.push_back(node);
         ppaths.push_back((*iter));
      }
   }

   // effect : the number of vertex in filter / the number of distinct vertex in sortkey column
   vector<RPathTreeIndex::Node*> effectiveNodes;
   vector<PPath> effectivePPaths;
   set<unsigned> preds;
   preds.insert(predicate);
   costs=plan->costs;
   vector<double> cards;
   cards.push_back(plan->cardinality);
   for(unsigned i=0, limit=nodes.size(); i<limit;i++) {
      RPathTreeIndex::Node *node = nodes[i];
      effect = getEffect(db, plan, node);

      if (node->cardinality >= 1000000 && effect > db->alpha)
         continue;

      if (node->cardinality >= plan->cardinality)
         continue;

      effectiveNodes.push_back(node);
      effectivePPaths.push_back(ppaths[i]);
      preds.insert(node->predicate);
      cards.push_back(node->cardinality);
   }

   assert(effectiveNodes.size()==effectivePPaths.size());

   vector<unsigned> vec(preds.begin(), preds.end());
   Plan* p2=plans.alloc();
   p2->op=Plan::RFLT;
   p2->opArg=0;
   p2->left=plan;
   p2->right=0;
   p2->rpathIdxNodes=effectiveNodes;
   p2->ppaths=effectivePPaths;
   p2->next=0;
   unsigned csetCard=db->cset->getCnt(vec);
   p2->cardinality=getFilteredCardinality(db, plan, csetCard, effectiveNodes);
   p2->original_cardinality=plan->cardinality;
   p2->costs=costs+Costs::merge(cards);
   p2->ordering=plan->ordering;
   return p2; 
}
//---------------------------------------------------------------------------
static void addVectorUnique(vector<RPathTreeIndex::Node*>& source, 
                            vector<RPathTreeIndex::Node*>& dest)
{
   for(vector<RPathTreeIndex::Node*>::iterator iter=source.begin(),limit=source.end();
       iter!=limit; ++iter) {
      bool skip = false;
      for(vector<RPathTreeIndex::Node*>::iterator iter2=dest.begin(),limit2=dest.end();
            iter2!=limit2; ++iter2) {

         if ((*iter)==(*iter2)) {
            skip = true;
            break;
         }
      }
      if (skip) continue;
      dest.push_back((*iter));
   }
}
//---------------------------------------------------------------------------
static vector<RPathTreeIndex::Node*> intersectVector(
       vector<RPathTreeIndex::Node*>& source, vector<PPath>& sourcePPaths,
       vector<RPathTreeIndex::Node*>& dest, vector<PPath>& destPPaths,
       vector<PPath>& ppaths)
{
   vector<RPathTreeIndex::Node*> intersect;
   if (source.size() < dest.size()) {
      for(unsigned i=0, limit=source.size(); i<limit; i++) {
         bool exist = false;
         for(unsigned j=0, limit2=dest.size(); j<limit2; j++) {
            if (source[i]==dest[j]) {
               exist = true;
               break;
            }
         }
         if (exist) {
            intersect.push_back(source[i]);
            ppaths.push_back(sourcePPaths[i]);
         }
      }
   }
   else {
      for(unsigned i=0, limit=dest.size(); i<limit; i++) {
         bool exist = false;
         for(unsigned j=0, limit2=source.size(); j<limit2; j++) {
            if (dest[i]==source[j]) {
               exist = true;
               break;
            }
         }
         if (exist) {
            intersect.push_back(dest[i]);
            ppaths.push_back(destPPaths[i]);
         }
      }
   }

   return intersect;
}
//---------------------------------------------------------------------------
static void gatherInputOfRFLT_M(PlanContainer& plans,vector<RPathTreeIndex::Node*>& rpathIdxNodes, 
                                vector<Plan*> &inputs, Plan* plan, double& costs)
{
   switch (plan->op) {
      case Plan::RFLT:
         inputs.push_back(plan->left); 
         costs+=plan->left->costs;
         addVectorUnique(plan->rpathIdxNodes, rpathIdxNodes);
         break;
      case Plan::RFLT_M:
         for(vector<Plan*>::iterator iter=plan->inputPlans.begin(),limit=plan->inputPlans.end();
               iter!=limit; ++iter) {
            inputs.push_back((*iter));
         }
         addVectorUnique(plan->rpathIdxNodes, rpathIdxNodes);
         break;
      case Plan::IndexScan:
      case Plan::AggregatedIndexScan:
      case Plan::FullyAggregatedIndexScan:
         inputs.push_back(plan); 
         costs+=plan->costs;
         break;
      case Plan::MergeJoin:
         gatherInputOfRFLT_M(plans, rpathIdxNodes, inputs, plan->left, costs);
         gatherInputOfRFLT_M(plans, rpathIdxNodes, inputs, plan->right, costs);
         break;
      default:
         assert(0);
   }
}
//---------------------------------------------------------------------------
static void gatherInputOfRFLT_M2(vector<Plan*> &inputs, Plan* plan)
{
   switch (plan->op) {
      case Plan::RFLT:
         inputs.push_back(plan->left); 
         break;
      case Plan::RFLT_M:
         for(vector<Plan*>::iterator iter=plan->inputPlans.begin(),limit=plan->inputPlans.end();
               iter!=limit; ++iter) {
            inputs.push_back((*iter));
         }
         break;
      case Plan::IndexScan:
      case Plan::AggregatedIndexScan:
      case Plan::FullyAggregatedIndexScan:
         inputs.push_back(plan); 
         break;
      case Plan::MergeJoin:
         gatherInputOfRFLT_M2(inputs, plan->left);
         gatherInputOfRFLT_M2(inputs, plan->right);
         break;
      default:
         assert(0);
   }
}
//---------------------------------------------------------------------------
void PlanGen::buildRFLT_M_RGINDEX(Problem* result,PlanContainer& plans,unsigned joinOrdering,
                                  Plan* left, Plan *right, double joinSelectivity)
{
   Plan* p=plans.alloc();
   p->op=Plan::RFLT_M;
   p->opArg=joinOrdering;
   p->left=NULL;
   p->right=NULL;

   vector<double> cardinalities;
   set<unsigned> preds;
   vector<double> cards;

   /// Gather inputs
   gatherInputOfRFLT_M2(p->inputPlans,left);
   gatherInputOfRFLT_M2(p->inputPlans,right);

   map<GSPAN::DFSCode, unsigned> subgraphs;
   getCandidateSubgraphs(gspan_graph, joinOrdering, db->rgindex, subgraphs);
   for (GSPAN::Vertex::edge_iterator iter=gspan_graph[joinOrdering].edge.begin(),
        limit=gspan_graph[joinOrdering].edge.end(); iter!=limit; iter++) {
      GSPAN::Edge edge=*iter;
      if (edge.elabel==500000) continue;
      if (edge.type==GSPAN::EDGE_TYPE_NORMAL)
         preds.insert(db->rgindex->predMap_new[edge.elabel]+REVERSE_PRED);
      else
         preds.insert(db->rgindex->predMap_new[edge.elabel]);
   }

   double joinkeycnt=0; // minimum among input cardinality and csets and vlists --> distinct join key 
   unsigned minCard=~0u;
   double costs=0;
   vector<RGindex::Node*> nodes;
   vector<unsigned> nodeIds;
   vector<GSPAN::DFSCode> dfscodes;
   for (map<GSPAN::DFSCode, unsigned>::iterator iter=subgraphs.begin(),
         limit=subgraphs.end(); iter!=limit; iter++) {
      GSPAN::DFSCode dfscode=iter->first;
      unsigned vID=iter->second;
      RGindex::Node *node=db->rgindex->SearchNode(db->rgindex->root, dfscode, 0);
      minCard=min(node->cardinalities[vID], minCard);
      assert(minCard>0);
      costs+=Costs::scan(node->blks[vID]); // cost for reading Vlist
      nodes.push_back(node);
      cards.push_back(node->cardinalities[vID]);
      nodeIds.push_back(vID);
      dfscodes.push_back(dfscode);
   }
   vector<unsigned> pred_vec(preds.begin(), preds.end());
   unsigned csetCard=db->cset->getCnt(pred_vec);
   minCard=min(csetCard, minCard);
   assert(minCard>0);

   // add costs of child operators
   for(vector<Plan*>::iterator iter=p->inputPlans.begin(),limit=p->inputPlans.end();
       iter!=limit;iter++) {
      Plan* input = (*iter);
      costs+=input->costs;
      
      vector<unsigned> tmp;
      if (input->subject)
         tmp.push_back(input->predicate+REVERSE_PRED);
      else
         tmp.push_back(input->predicate);
      unsigned values=db->cset->getCnt(tmp);
      minCard=min(values, minCard);
      assert(minCard>0);
   }

   // for each input plan, calculate filtered cardinality
   double minFilteredCard=~0u; // filtered input cardinality
   for(vector<Plan*>::iterator iter=p->inputPlans.begin(),limit=p->inputPlans.end();
       iter!=limit;iter++) {
      Plan *plan = (*iter);

      vector<unsigned> tmp;
      if (plan->subject)
         tmp.push_back(plan->predicate+REVERSE_PRED);
      else
         tmp.push_back(plan->predicate);
      unsigned values=db->cset->getCnt(tmp);
      double cardinality = (double) plan->cardinality * (double) minCard / (double) values;

      p->filteredCard.push_back(cardinality);
      cards.push_back(cardinality);
      assert(cardinality>0);
      minFilteredCard=min(minFilteredCard, cardinality);
   }
   joinkeycnt=min((double)minCard, (double)minFilteredCard);
   if (joinkeycnt<1) joinkeycnt=1;

   double cardinality=joinkeycnt;
   for(vector<Plan*>::iterator iter=p->inputPlans.begin(),limit=p->inputPlans.end();
       iter!=limit;iter++) {
      Plan *plan = (*iter);
      unsigned predicate=plan->predicate;
      if (plan->subject) predicate += (REVERSE_PRED);
      vector<unsigned> tmp;
      tmp.push_back(predicate);
      unsigned denominator=db->cset->getCnt(tmp);
      double cnt= plan->cardinality/(double) denominator;
      if (cnt<1) cnt=1;
      cardinality*=cnt;
   }

   /// Compute cardinality and costs
   p->joinkeycnt=joinkeycnt;
   p->original_cardinality=left->original_cardinality*right->original_cardinality*joinSelectivity;
   p->cardinality=cardinality;//*10;
   p->costs=costs+Costs::merge(cards);
   p->ordering=joinOrdering;
   p->rgindexNodes=nodes;
   p->nodeIds=nodeIds;
   p->dfscodes=dfscodes;
   addPlan(result,p);
}
//---------------------------------------------------------------------------
void PlanGen::buildRFLT_M(Problem* result,PlanContainer& plans,unsigned joinOrdering,
                          Plan* left, Plan *right, double joinSelectivity)
{
   Plan* p=plans.alloc();
   p->op=Plan::RFLT_M;

   /// Gather inputs
   double costs=0;
   vector<double> cardinalities;
   vector<PPath> ppaths;

   p->rpathIdxNodes.clear();
   p->rpathIdxNodes=left->rpathIdxNodes;
   p->ppaths=left->ppaths;
   p->rpathIdxNodes=intersectVector(p->rpathIdxNodes,p->ppaths, 
                                    right->rpathIdxNodes, right->ppaths,
                                    ppaths);
   p->ppaths=ppaths;
   assert(p->ppaths.size()==p->rpathIdxNodes.size());
   gatherInputOfRFLT_M2(p->inputPlans,left);
   gatherInputOfRFLT_M2(p->inputPlans,right);

   for(vector<Plan*>::iterator iter=p->inputPlans.begin(),limit=p->inputPlans.end();
         iter!=limit; ++iter) {
      for(vector<Plan*>::iterator iter2=(iter+1); iter2!=limit; ++iter2) {
         assert ((*iter) != (*iter2));
      }
   }

   p->opArg=joinOrdering;
   p->left=NULL;
   p->right=NULL;

   set<unsigned> preds;
   vector<double> cards;

   // make preds
   for(vector<Plan*>::iterator iter=p->inputPlans.begin(),limit=p->inputPlans.end();
       iter!=limit;iter++) {
      Plan* input = (*iter);
      unsigned predicate = input->predicate;
      if (input->subject) predicate += (REVERSE_PRED);
      preds.insert(predicate);
      costs+=input->costs;
   }

   double joinkeycnt=0; // minimum among input cardinality and csets and vlists --> distinct join key 
   for(vector<RPathTreeIndex::Node*>::iterator iter=p->rpathIdxNodes.begin(),
       limit=p->rpathIdxNodes.end(); iter!=limit;++iter) {
      RPathTreeIndex::Node *node = (*iter);
      preds.insert(node->predicate);
      costs+=Costs::scan((node->startIndexPage-node->startPage)/4);
      if (joinkeycnt==0 || joinkeycnt > node->cardinality)
         joinkeycnt=node->cardinality;
   }
   vector<unsigned> vec(preds.begin(), preds.end());
   unsigned csetCard=db->cset->getCnt(vec);
   p->csetcnt=csetCard;
   if (joinkeycnt==0 || joinkeycnt>csetCard)
      joinkeycnt=csetCard;

   // for each input plan, calculate filtered cardinality
   double minFilteredCard=0; // filtered input cardinality
   for(vector<Plan*>::iterator iter=p->inputPlans.begin(),limit=p->inputPlans.end();
       iter!=limit;iter++) {
      Plan *plan = (*iter);
      double cardinality = getFilteredCardinality(db, plan, csetCard, p->rpathIdxNodes);
      p->filteredCard.push_back(cardinality);
      cards.push_back(cardinality);
      assert(cardinality>0);
      if (minFilteredCard==0 || minFilteredCard > cardinality)
         minFilteredCard=cardinality;
   }
   joinkeycnt=minFilteredCard<joinkeycnt?minFilteredCard:joinkeycnt;
   if (joinkeycnt<1) joinkeycnt=1;

   double cardinality=joinkeycnt;
   for(vector<Plan*>::iterator iter=p->inputPlans.begin(),limit=p->inputPlans.end();
       iter!=limit;iter++) {
      Plan *plan = (*iter);
      unsigned predicate=plan->predicate;
      if (plan->subject) predicate += (REVERSE_PRED);
      unsigned denominator = db->rpathTreeIdx->FindCardinality(predicate);
      double cnt= plan->cardinality/(double) denominator;
      if (cnt<1) cnt=1;
      cardinality*=cnt;
   }

   /// Compute cardinality and costs
   p->joinkeycnt=joinkeycnt;
   p->original_cardinality=left->original_cardinality*right->original_cardinality*joinSelectivity;
   p->cardinality=cardinality*10;
   p->costs=costs+Costs::merge(cards);
   p->ordering=joinOrdering;

   assert(p->ppaths.size()==p->rpathIdxNodes.size());
   addPlan(result,p);
}
//---------------------------------------------------------------------------
Plan* PlanGen::addRPFLTbyHeuristic(Plan* plan)
{
   if (plan->op == Plan::IndexScan) {
      // Add RPFLT
      vector<PPath> cfs = 
         fullQuery->rpathQueryGraph->getCandidateFilterSetPPath(plan->ordering, db->maxL);
      vector<PPath> cfs_noredun;
      // Remove redundant filter
      for (int i=0,limit=cfs.size();i<limit;i++) {
         PPath ppath1 = cfs[i];
         bool isRedundant=false;

         if (ppath1.size()==1 && ppath1.path[0]==plan->predicate)
            continue;

         if (ppath1.size()==1 && plan->subject && ppath1.path[0]==plan->predicate+REVERSE_PRED)
            continue;

         for (int j=0;j<limit;j++) {
            if (i==j) continue;
            PPath ppath2 = cfs[j];

            if (ppath1.contained(ppath2)) {
               RPathTreeIndex::Node *node = db->rpathTreeIdx->SearchNode(ppath2);
               if (node) {
                  isRedundant=true;
                  break;
               }
            }
         }
         if (isRedundant)
            continue;
         cfs_noredun.push_back(ppath1);
      }

      vector<RPathTreeIndex::Node*> tempNodes;
      for(vector<PPath>::iterator iter=cfs_noredun.begin(),limit=cfs_noredun.end();
         iter!=limit;++iter) {
         RPathTreeIndex::Node *node = db->rpathTreeIdx->SearchNode(*iter);
         if (node)
            tempNodes.push_back(node);
      }
      if (tempNodes.size() == 0)
         return NULL;

      Plan* RPFLTPlan=plans.alloc();
      RPFLTPlan->op=Plan::RFLT;
      RPFLTPlan->opArg=0;
      RPFLTPlan->left=plan;
      RPFLTPlan->right=0;
      RPFLTPlan->rpathIdxNodes=tempNodes;
      RPFLTPlan->next=0;
      RPFLTPlan->cardinality=0;
      RPFLTPlan->original_cardinality=0;
      RPFLTPlan->costs=0;
      RPFLTPlan->ordering=plan->ordering;

      return RPFLTPlan;
   }

   // The others(not IndexScan)
   if (plan->left) {
      Plan* newPlan = addRPFLTbyHeuristic(plan->left);
      if (newPlan)
         plan->left = newPlan;
   }

   if (plan->right) {
      Plan* newPlan = addRPFLTbyHeuristic(plan->right);
      if (newPlan)
         plan->right = newPlan;
   }

   if (plan->op == Plan::MergeJoin) {
      if (getenv("USE_RFLTM")) {
         if (plan->left->op==Plan::RFLT || plan->left->op==Plan::RFLT_M ||
             plan->right->op==Plan::RFLT || plan->right->op==Plan::RFLT_M) {

            plan->op = Plan::RFLT_M;
            double costs=0;
            vector<double> cardinalities;
            gatherInputOfRFLT_M(plans, plan->rpathIdxNodes,plan->inputPlans, plan->left, costs);
            gatherInputOfRFLT_M(plans, plan->rpathIdxNodes,plan->inputPlans, plan->right, costs);

            // remove redundant Vlists
            for(vector<Plan*>::iterator iter=plan->inputPlans.begin(),limit=plan->inputPlans.end();
                iter!=limit; ++iter) {
            }

            plan->opArg=plan->ordering;
            plan->left=NULL;
            plan->right=NULL;
         }
      }
   }
   return NULL;
}
//---------------------------------------------------------------------------
