#include "gspan/graph.hpp"
#include "gspan/dfs.hpp"
#include <assert.h>

namespace GSPAN {

/**
 * @brief  graph에서 edge e2의 도착 vertex가 가지고 있는 edge들 중
 *         edge e1의 시작 vertex를 향하는 edge를 찾는다.
 */
Edge*
get_backward (Graph &g, Edge* e1, Edge* e2, History& history) {
    if (e1 == e2)
        return 0;

    assert(e1->from >= 0 && e1->from < (int)g.size());
    assert(e1->to >= 0 && e1->to < (int)g.size());
    assert(e2->to >= 0 && e2->to < (int)g.size());

    for (Vertex::edge_iterator it = g[e2->to].edge.begin();
         it != g[e2->to].edge.end(); ++it) {
        /* 이미 DFS에 포함된 edge는 제외.
         * normal edge와 reverse edge의 id가 서로 같으므로,
         * 둘 중 하나가 포함되면 다른 하나는 처리되지 않음. */
        if (history.hasEdge(it->id))
            continue;

        if (it->to == e1->from && e1->elabel <= it->elabel) {
            return &(*it);
        }
    }

    return 0;
}

/**
 * @brief  graph에서 edge e의 시작 vertex가 가지고 있는 edge들 중
 *         아직 발견되지 않은 vertex를 향하는 edge를 찾는다.
 *         rightmost path를 backtracking해 가며 forward edge를 찾을 때 쓰임.
 */
bool 
get_forward_rmpath(Graph &g, Edge *e, History& history, EdgeList &result) {
    result.clear();
    assert(e->to >= 0 && e->to < (int)g.size());
    assert(e->from >= 0 && e->from < (int)g.size());

    for (Vertex::edge_iterator it = g[e->from].edge.begin();
         it != g[e->from].edge.end(); ++it) {
        if (e->to == it->to || history.hasVertex (it->to))
            continue;

        if (e->elabel <= it->elabel) {
            result.push_back (&(*it));
        }
    }

    return (!result.empty());
}

/**
 * @brief  graph에서 edge e의 도착 vertex가 가지고 있는 edge들 중
 *         아직 발견되지 않은 vertex를 향하는 edge를 찾는다.
 */
bool 
get_forward_pure(Graph &g, Edge *e, History& history, EdgeList &result) {
    result.clear();

    assert(e->to >= 0 && e->to < (int)g.size());

    /* Walk all edges leaving from vertex e->to. */
    for (Vertex::edge_iterator it = g[e->to].edge.begin();
        it != g[e->to].edge.end(); ++it) {
        assert(it->to >= 0 && it->to < (int)g.size());

        if (history.hasVertex(it->to))
            continue;

        result.push_back (&(*it));
    }

    return (!result.empty());
}

/**
 * @brief  graph에서 label이 가장 작은 normal edge들을 root edge로 선택하여 
 *         result에 넣어준다.
 */
bool 
get_forward_root(Graph &g, EdgeList &result) {
    result.clear();

    for (unsigned int i = 0; i < g.size(); ++i) {
        Vertex &v = g[i];
        for (Vertex::edge_iterator it = v.edge.begin(); 
             it != v.edge.end(); ++it) {
            assert(it->to >= 0 && it->to < (int)g.size());

            if (it->type == EDGE_TYPE_REVERSE)
                continue;

            if (result.empty() || it->elabel < result[0]->elabel) {
                result.clear();
                result.push_back(&(*it));
            }
            else if (it->elabel == result[0]->elabel) {
                result.push_back(&(*it));
            }
        }
    }

    return (!result.empty());
}

} /* end of namespace GSPAN */
