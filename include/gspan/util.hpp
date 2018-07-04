#ifndef H_GSPAN_UTIL
#define H_GSPAN_UTIL

#include "gspan/graph.hpp"
#include "gspan/dfs.hpp"

namespace GSPAN {

typedef std::vector <Edge*> EdgeList;

bool get_forward_pure(Graph &g, Edge *e, History &history, EdgeList &result);
bool get_forward_rmpath(Graph &g, Edge *e, History &history, EdgeList &result);
bool get_forward_root(Graph &g, EdgeList &result);
Edge *get_backward(Graph &g, Edge *e1, Edge *e2, History &history);

}; /* end of namespace GSPAN */

#endif /* H_GSPAN_UTIL */
