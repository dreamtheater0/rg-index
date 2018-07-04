#ifndef H_GSPAN_DFS
#define H_GSPAN_DFS

#include "gspan/graph.hpp"
#include <iostream>
#include <vector>
#include <map>

namespace GSPAN {

class DFS {
public:
    /**
     * XXX src/dest vs. from/to 차이점
     *
     * src/dest: DFS code 생성 중에 탐색 순서대로 붙이는 
     *           노드의 subscript #를 지칭.
     * from/to: edge가 연결된 각 노드의 원래 ID를 지칭.
     */
    int src;
    int dest;
    int elabel;
    edge_type_t type;

    friend bool operator == (const DFS &d1, const DFS &d2) {
        return (d1.src == d2.src && d1.dest == d2.dest 
                && d1.elabel == d2.elabel && d1.type == d2.type);
    }

    friend bool operator != (const DFS &d1, const DFS &d2) { 
        return (!(d1 == d2)); 
    }

    friend bool operator < (const DFS &d1, const DFS &d2) { 
        if (d1.src<d2.src || 
            (d1.src==d2.src && d1.dest<d2.dest) || 
            (d1.src==d2.src && d1.dest==d2.dest && d1.elabel<d2.elabel) ||
            (d1.src==d2.src && d1.dest==d2.dest && d1.elabel==d2.elabel && d1.type<d2.type))
           return true;
        return false;
    }

    DFS(): src(0), dest(0), elabel(0), type(EDGE_TYPE_UNDEFINED) {};
};

typedef std::vector<int> RMPath;    /* right-most path */


struct PDFS {
    Edge *edge;
    PDFS *prev;
    PDFS(): edge(0), prev(0) {};
};

class Projected: public std::vector<PDFS> {
public:
    void push(Edge *edge, PDFS *prev) {
        resize(size() + 1);
        PDFS &d = (*this)[size()-1];
        d.edge = edge; 
        d.prev = prev;
    }
};

typedef std::map<int, Projected> Projected_map;
typedef std::map<int, Projected>::iterator Projected_iterator;

typedef std::map<int, Projected_map> Projected_map2;
typedef std::map<int, Projected_map>::iterator Projected_iterator2;

typedef std::map<int, Projected_map2> Projected_map3;
typedef std::map<int, Projected_map2>::iterator Projected_iterator3;

class History: public std::vector<Edge*> {
private:
    std::vector<int> edge;  /* history에 포함하고 있는 모든 edge id 저장 */
    std::vector<int> vertex;  /* history에 포함하고 있는 모든 vertex id 저장 */

public:
    bool hasEdge   (unsigned int id) { return (bool)edge[id]; }
    bool hasVertex (unsigned int id) { return (bool)vertex[id]; }
    void build     (Graph &, PDFS *);
    History() {};
    History (Graph& g, PDFS *p) { build (g, p); }

};

struct DFSCode: public std::vector<DFS> {
private:
    RMPath rmpath;

public:
    const RMPath& buildRMPath();

    bool extendMinCode(Graph &g, Projected &p, DFSCode *check);

    bool is_min(void);

    /* Convert current DFS code into a graph. */
    void toGraph(Graph &g);

    /* Clear current DFS code and build code from the given graph. */
    void fromGraph(Graph &g);

    /* Return number of nodes in the graph. */
    unsigned int nodeCount(void);

    void push(int src, int dest, int elabel, edge_type_t type) {
        resize(size() + 1);
        DFS &d = (*this)[size()-1];

        d.src = src;
        d.dest = dest;
        d.elabel = elabel;
        d.type = type;
    }
    void pop() { resize(size()-1); }
    bool hasSameDFS(int src, int elabel, edge_type_t type) {
       for(std::vector<DFS>::iterator iter=begin(), limit=end(); iter!=limit; iter++) { 
          DFS dfs=(*iter);
          if(dfs.src==src && dfs.elabel==elabel && dfs.type==type)
             return true;
          if(dfs.dest==src && dfs.elabel==elabel && dfs.type!=type)
             return true;
       }
       return false;
    }

    friend std::ostream& operator<<(std::ostream& os, const DFSCode& dfscode); // write

    /* subscript # -> original vertex ID mapping */
    std::map<int, int> subsToIdMap;
    /* original vertex ID -> subscript # mapping */
    std::map<int, int> idToSubsMap;

};

typedef std::vector<Edge*> EdgeList;

}; /* end of namespace GSPAN */

#endif /* H_GSPAN_DFS */
