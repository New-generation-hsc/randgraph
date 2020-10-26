#ifndef _GRAPH_CONFIG_H_
#define _GRAPH_CONFIG_H_

#include <string>
#include "api/types.hpp"

/** config
 * 
 * This file contribute to define the graph config structure
 */

struct graph_config {
    std::string base_name;
    int fnum;
    size_t blocksize;
    tid_t nthreads;

    vid_t nvertices;
    eid_t nedges;
};

#endif