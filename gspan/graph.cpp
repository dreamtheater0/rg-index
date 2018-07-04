#include "gspan/graph.hpp"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iterator>
#include <strstream>
#include <set>
#include <map>

#include <assert.h>

namespace GSPAN {

template <class T, class Iterator> void 
tokenize(const char *str, Iterator iterator) {
    std::istrstream is(str, std::strlen(str));
    std::copy(std::istream_iterator <T> (is), 
              std::istream_iterator <T> (), iterator);
}

/**
 * @brief  graph의 edge에 ID를 부여하고, 같은 ID를 갖는 inverse edge를 생성한다.
 */
void 
Graph::buildEdge(void) {
    Vertex inv_edges;   /* XXX Vertex 용도는 아님... 단순 Edge vector 용도 */
    edge_type_t inv_type;

    /* give an ID to each edges */
    unsigned int id = 0;
    for (int from = 0; from < (int)size(); ++from) {
        for (Vertex::edge_iterator it = (*this)[from].edge.begin();
             it != (*this)[from].edge.end(); ++it) {
            it->id = id;
            inv_type = it->type == EDGE_TYPE_NORMAL ? EDGE_TYPE_REVERSE 
                                                    : EDGE_TYPE_NORMAL;
            /* generate inverse edges which have the same ID with their edges */
            inv_edges.push(it->to, from, it->elabel, inv_type, id);
            id++;
        }
    }

    /* merge inverse edges to this graph */
    for (Vertex::edge_iterator it = inv_edges.edge.begin(); 
         it != inv_edges.edge.end(); ++it) {
        (*this)[it->from].push(it->from, it->to, it->elabel, it->type, it->id);
    }

    /* XXX inverse edge는 빼고 원래 edge 개수로 지정.. 
     * History에서 사용되는데 문제 없으려나? */
    edge_size_ = id;
}

std::istream&
Graph::read(std::istream& is) {
    std::vector<std::string> result;
    char line[1024];

    clear();

    while (true) {
        if (!is.getline(line, 1024))
            break;

        result.clear();
        tokenize<std::string>(line, std::back_inserter(result));

        if (result.empty()) {
            // do nothing
        } 
        else if (result[0] == "v" && result.size() >= 2) {
            unsigned int id = atoi(result[1].c_str());
            this->resize(id + 1);
        } 
        else if (result[0] == "e" && result.size() >= 4) {
            int from = atoi(result[1].c_str());
            int to = atoi(result[2].c_str());
            int elabel = atoi(result[3].c_str());

            if ((int)size() <= from || (int)size() <= to) {
                std::cerr << "Format Error: " 
                          << "define vertex lists before edges" << std::endl;
                exit(-1);
            }

            (*this)[from].push(from, to, elabel, EDGE_TYPE_NORMAL);
        }
    }

    buildEdge();

    return is;
}

std::ostream&
Graph::write(std::ostream& os) {
    char buf[512];
    std::set<std::string> tmp;

    for (int from = 0; from < (int)size(); ++from) {
        os << "v " << from << std::endl;

        for (Vertex::edge_iterator it = (*this)[from].edge.begin ();
             it != (*this)[from].edge.end (); ++it) {
            if (it->type == EDGE_TYPE_NORMAL)
                std::sprintf(buf, "%d %d %d -> [%d]", 
                             from, it->to, it->elabel, it->id);
            else // EDGE_TYPE_REVERSE
                std::sprintf(buf, "%d %d %d <- [%d]", 
                             from, it->to, it->elabel, it->id);
            tmp.insert(buf);
        }
    }

    for (std::set<std::string>::iterator it = tmp.begin(); 
         it != tmp.end(); ++it) {
        os << "e " << *it << std::endl;
    }

    return os;
}


} /* end of namespace GSPAN */
