#include "gspan/dfs.hpp"
#include "gspan/util.hpp"
#include <algorithm>
#include <assert.h>
#include <cstring>
#include <iterator>
#include <set>
#include <string>
#include <glog/logging.h>

namespace GSPAN {

void 
History::build(Graph &graph, PDFS *e) {
    // first build history
    clear();
    edge.clear();
    edge.resize(graph.edge_size());
    vertex.clear();
    vertex.resize(graph.size());

    if (e) {
        push_back(e->edge);
        edge[e->edge->id] = vertex[e->edge->from] = vertex[e->edge->to] = 1;

        for (PDFS *p = e->prev ; p ; p = p->prev) {
            push_back (p->edge); // this line eats 8% of overall instructions(!)
            edge[p->edge->id] = vertex[p->edge->from] = vertex[p->edge->to] = 1;
        }
        std::reverse(begin(), end());
    }
}


/**
 * @brief  right-most path를 구성하는 edge의 DFS element를 DFSCode에서 찾아
 *         각 DFS element의 index를 맨 뒤쪽 edge부터 역순으로 
 *         rmpath vector에 저장한다. 
 */
const RMPath&
DFSCode::buildRMPath(void) {
    rmpath.clear();

    int old_src = -1;

    for (int i = size() - 1; i >= 0; --i) {
        if ((*this)[i].src < (*this)[i].dest // forward
            && (rmpath.empty() || old_src == (*this)[i].dest)) {

            rmpath.push_back(i);
            old_src = (*this)[i].src;
        }
    }

    return rmpath;
}

void print_edge(int from, int to, int elabel, edge_type_t type) {
    if (type == EDGE_TYPE_NORMAL)
       VLOG(1) << from << " " << to << " " << elabel << " ->";
    else 
       VLOG(1) << from << " " << to << " " << elabel << " <-";
}

/**
 * @brief  edge를 탐색해 나가며 minimum DFS code를 확장한다.
 *   check 인자가 null인 경우 - minimum DFS code 생성. (return 값은 항상 true)
 *   check 인자가 null이 아닌 경우 - check가 minimum DFS code인지 검사.
 *   (return 값은 minimum이면 true, 아니면 false)
 *
 * forward edges: DFS spanning tree에 속하는 edge들.  (vi -> vj, i < j)
 * backward edges: 그 외의 edge들.  (vi -> vj,  i > j)
 */
bool
DFSCode::extendMinCode(Graph &g, Projected &p, DFSCode *check) {
    const RMPath& rmpath = buildRMPath();
    int max_dest = (*this)[rmpath[0]].dest;
    //std::cout << "\nmax_dest=" << max_dest << "\n";
    VLOG(1) << "max_dest=" << max_dest;

    Projected_map2 root;
    EdgeList edges;
    bool found = false;
    int new_dest = 0;

    /* backward edge */
    for (int i = rmpath.size()-1; !found && i >= 1; --i) {
        for (unsigned int n = 0; n < p.size(); ++n) {
            PDFS *cur = &p[n];
            History history(g, cur);
            /* rightmost path에 속한 vertex들 중 가장 오래전에 탐색한 것부터 
             * 순서대로 다음 edge가 향하는 vertex가 있는지 찾음. */
            Edge *e = get_backward(g, history[rmpath[i]], history[rmpath[0]], 
                                   history);
            if (e) {
                found = true;
                root[e->elabel][e->type].push(e, cur);
                new_dest = (*this)[rmpath[i]].src;
                //std::cout << "\nextended backward edges\n";
                VLOG(1) << "extended backward edges";
                print_edge(e->from, e->to, e->elabel, e->type);
            }
        }
    }

    if (found) {
        Projected_iterator2 elabel = root.begin();
        Projected_iterator typelabel = elabel->second.begin();
        push(max_dest, new_dest, elabel->first, (edge_type_t)typelabel->first);

        if (check && (*check)[(*this).size()-1] != (*this)[(*this).size()-1]) {
            return false;
        }
        return extendMinCode(g, typelabel->second, check);
    }

    Projected_map3 fwd;
    found = false;
    bool cur_found;
    for (unsigned int n = 0; n < p.size(); ++n) {
        PDFS *cur = &p[n];
        History history(g, cur);

        cur_found = false;
        /* pure forward edge */
        if (get_forward_pure(g, history[rmpath[0]], history, edges)) {
            found = true; cur_found = true;
            //std::cout << "\nextended forward edges\n";
            VLOG(1) << "extended forward edges";
            for (EdgeList::iterator it = edges.begin(); 
                 it != edges.end(); ++it) {
                fwd[max_dest][(*it)->elabel][(*it)->type].push(*it, cur);
                print_edge((*it)->from, (*it)->to, (*it)->elabel, (*it)->type);
            }
        }

        /* backtracked forward edge */
        for (int i = 0; !cur_found && i < (int)rmpath.size(); ++i) {
            if (get_forward_rmpath(g, history[rmpath[i]], history, edges)) {
                found = true; cur_found = true;
                //std::cout << "\nextended backtracked forward edges\n";
                VLOG(1) << "extended backtracked forward edges";
                for (EdgeList::iterator it = edges.begin(); 
                     it != edges.end(); ++it) {
                    fwd[(*this)[rmpath[i]].src][(*it)->elabel][(*it)->type].push(*it, cur);
                    print_edge((*it)->from, (*it)->to, (*it)->elabel, 
                               (*it)->type);
                }
            }
        }
    }

    if (found) {
        Projected_iterator3 new_src = fwd.begin();
        Projected_iterator2 elabel = new_src->second.begin();
        Projected_iterator typelabel = elabel->second.begin();
        push(new_src->first, max_dest+1, elabel->first, 
             (edge_type_t)typelabel->first);
        if (check && (*check)[(*this).size()-1] != (*this)[(*this).size()-1]) {
            return false;
        }
        return extendMinCode(g, typelabel->second, check);
    }

    /* minimum DFS code를 원래 노드의 ID대로 출력 */
    //std::cout << "\nDFS code (w/ original vertex id)\n";
    VLOG(1) << "DFS code (w/ original vertex id)";
    int sn = 0;
    bool *is_set = (bool*)malloc(sizeof(bool) * g.size());
    for (unsigned int i = 0; i < g.size(); i++) 
        is_set[i] = false;

    for (unsigned int n = 0; n < p.size(); n++) {
        History h(g, &p[n]);
        for (unsigned int i = 0; i < h.size(); ++i) {
            if (is_set[h[i]->from] == false) {
                subsToIdMap[sn] = h[i]->from;
                idToSubsMap[h[i]->from] = sn;
                sn++;
                is_set[h[i]->from] = true;
            }
            if (is_set[h[i]->to] == false) {
                subsToIdMap[sn] = h[i]->to;
                idToSubsMap[h[i]->to] = sn;
                sn++;
                is_set[h[i]->to] = true;
            }

            print_edge(h[i]->from, h[i]->to, h[i]->elabel, h[i]->type);
        }
        //std::cout << "----\n";
        VLOG(1) << "----";
    }
    free(is_set);
    return true;
}

/**
 * @brief  DFS code가 minimum DFS code인지 검사한다.
 */
bool 
DFSCode::is_min(void) {
    if ((*this).size() == 1)
        return true;

    Graph g;
    (*this).toGraph(g);

    Projected_map root;    /* map(elabel -> Projected) */
    EdgeList root_edges;

    get_forward_root(g, root_edges);
    //std::cout << "\nroot edges\n";
    VLOG(1) << "root edges";
    for (EdgeList::iterator it = root_edges.begin(); 
         it != root_edges.end(); ++it) {
        root[(*it)->elabel].push(*it, 0);
        print_edge((*it)->from, (*it)->to, (*it)->elabel, (*it)->type);
    }

    Projected_iterator elabel = root.begin();
    DFSCode min_dc;
    min_dc.push(0, 1, elabel->first, EDGE_TYPE_NORMAL);

    return min_dc.extendMinCode(g, elabel->second, this);
}

/**
 * @brief  input graph의 minimum DFS code를 생성한다.
 */
void
DFSCode::fromGraph(Graph &g) {
    clear();

    Projected_map root;    /* map(elabel -> Projected) */
    EdgeList root_edges;

    get_forward_root(g, root_edges);
    //std::cout << "\nroot edges\n";
    VLOG(1) << "root edges";
    for (EdgeList::iterator it = root_edges.begin(); 
         it != root_edges.end(); ++it) {
        root[(*it)->elabel].push(*it, 0);
        print_edge((*it)->from, (*it)->to, (*it)->elabel, (*it)->type);
    }

    Projected_iterator elabel = root.begin();
    push(0, 1, elabel->first, EDGE_TYPE_NORMAL);

    extendMinCode(g, elabel->second, 0 /* minimum DFS code 생성 */);
}

void
DFSCode::toGraph(Graph &g) {
    g.clear();

    for (DFSCode::iterator it = begin(); it != end(); ++it) {
        g.resize(std::max(it->src, it->dest) + 1);
        g[it->src].push(it->src, it->dest, it->elabel, it->type);
    }

    g.buildEdge();
}

unsigned int
DFSCode::nodeCount(void) {
    unsigned int nodecount = 0;

    for (DFSCode::iterator it = begin() ; it != end() ; ++it)
        nodecount = std::max(nodecount, 
                             (unsigned int)(std::max(it->src, it->dest) + 1));

    return nodecount;
}


std::ostream&
operator<< (std::ostream& os, const DFSCode& dfscode) {
    if (dfscode.size() == 0) 
        return os;

    os << "DFS code: ";
    for (unsigned int i = 0; i < dfscode.size(); ++i) {
        os << "(" << dfscode[i].src << ", " << dfscode[i].dest 
           << ", " << dfscode[i].elabel << ", ";

        if (dfscode[i].type == EDGE_TYPE_NORMAL) {
            os << "->)";
        } 
        else {
            os << "<-)";
        }

        if (dfscode[i].src < dfscode[i].dest) {
            os << " [f] ";
        } 
        else {
            os << " [b] ";
        }
    }

    return os;
}

} /* end of namespace GSPAN */
