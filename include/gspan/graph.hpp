#ifndef H_GSPAN_GRAPH
#define H_GSPAN_GRAPH

#include <iostream>
#include <vector>

namespace GSPAN {

typedef enum edge_type_e {
    EDGE_TYPE_NORMAL,
    EDGE_TYPE_REVERSE,
    EDGE_TYPE_UNDEFINED
} edge_type_t;

struct Edge {
    int from;
    int to;
    int elabel;
    edge_type_t type;
    unsigned int id;
    Edge(): from(0), to(0), elabel(0), type(EDGE_TYPE_UNDEFINED), id(0) {};
};

class Vertex {
public:
    Vertex() {
       set=false;
    };
    typedef std::vector<Edge>::iterator edge_iterator;
    std::vector<Edge> edge;

    void push(int from, int to, int elabel, edge_type_t type) {
        edge.resize(edge.size()+1);
        edge[edge.size()-1].from = from;
        edge[edge.size()-1].to = to;
        edge[edge.size()-1].elabel = elabel;
        edge[edge.size()-1].type = type;
        return;
    }

    void push(int from, int to, int elabel, edge_type_t type, unsigned int id) {
        edge.resize(edge.size()+1);
        edge[edge.size()-1].from = from;
        edge[edge.size()-1].to = to;
        edge[edge.size()-1].elabel = elabel;
        edge[edge.size()-1].type = type;
        edge[edge.size()-1].id = id;
        return;
    }

    bool set;
};

class Graph: public std::vector<Vertex> {
private:
    unsigned int edge_size_; /* XXX inverse edges not included */

public:
    typedef std::vector<Vertex>::iterator vertex_iterator;

    unsigned int edge_size() { return edge_size_; }
    unsigned int vertex_size() { return (unsigned int)size(); } // wrapper
    void buildEdge(void);
    std::istream& read(std::istream&); // read
    std::ostream& write(std::ostream&); // write
    void check(void);

    Graph(): edge_size_(0) {};
};


}; /* end of namespace GSPAN */

#endif /* H_GSPAN_GRAPH */
