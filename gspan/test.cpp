#include "gspan/graph.hpp"
#include "gspan/dfs.hpp"
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <glog/logging.h>

void 
usage(void) {
    std::cout << "usage: gspan_test [-f filename]" << std::endl;
    std::cout << std::endl;
    std::cout << "options" << std::endl;
    std::cout << "  -h, show this usage help" << std::endl;
    std::cout << "  -f filename, set the filename for input graph" << std::endl;
    std::cout << std::endl;

    std::cout << "The input graph has to be in this format:" << std::endl;
    std::cout << "t" << std::endl;
    std::cout << "v <vertex-index>" << std::endl;
    std::cout << "..." << std::endl;
    std::cout << "e <edge-from> <edge-to> <edge-label>" << std::endl;
    std::cout << "..." << std::endl;
    std::cout << "<empty line>" << std::endl;
    std::cout << std::endl;

    std::cout << "Indices start at zero, labels are arbitrary unsigned integers." << std::endl;
    std::cout << std::endl;
}

int 
main(int argc, char **argv) {
    char *filename = 0;

    int opt;
    while ((opt = getopt(argc, argv, "f:")) != -1) {
        switch (opt) {
            case 'f':
                filename = optarg;
                break;
            case 'h':
            default:
                usage ();
                return -1;
        }
    }

    if (filename == 0) {
        std::cout << "Error: no input file specified\n";
        return 1;
    }

    GSPAN::Graph g;
    std::ifstream fs;

    fs.open(filename, std::ios_base::in);
    if (!fs.is_open()) {
        std::cout << "Error: '" << filename << "' not exist\n";
        return 1;
    }

    std::istream& is = fs;

    g.read((std::istream&) is);
    g.write((std::ostream&) std::cout);

    GSPAN::DFSCode dc;
    dc.fromGraph(g);

    std::cout << std::endl << "== Subscript # -> original vertex ID mapping ==" 
              << std::endl;
    std::map<int, int> m = dc.subsToIdMap;
    for (unsigned int i=0; i<m.size(); i++) {
        std::cout << i << " -> " << m[i] << std::endl;
    }

    std::cout << std::endl << "== Minimum DFS code ==" << std::endl;
    std::cout << dc << std::endl;

#if 0
    if (dc.is_min()) {
        std::cout << "this is the minimum DFS code!\n";
    }
#endif

    fs.close();
    return 0;
}

