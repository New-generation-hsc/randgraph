#ifndef _GRAPH_DRIVER_H_
#define _GRAPH_DRIVER_H_

#include "cache.hpp"
#include "util/io.hpp"

/** graph_driver
 * This file contribute to define the operations of how to read from disk
 * or how to write graph data into disk
 */

class graph_driver {
public:
    graph_driver() { }
    
    void load_block_vertex(int fd, eid_t *buf, const block_t &block) { 
        load_block_range(fd, buf, block.nverts + 1, block.start_vert);
    }

    void load_block_degree(int fd, vid_t *buf, const block_t &block) { 
        load_block_range(fd, buf, block.nverts, block.start_vert);
    }

    void load_block_edge(int fd, vid_t *buf, const block_t &block) {
        load_block_range(fd, buf, block.nedges, block.start_edge);
    }

    void load_walk(int fd, size_t cnt, graph_buffer<walk_t> &walks) {
        load_block_range(fd, walks.buffer_begin(), cnt, 0);
        walks.set_size(cnt);
    }

    void dump_walk(int fd, graph_buffer<walk_t> &walks) {
        dump_block_range(fd, walks.buffer_begin(), walks.size(), 0);
        walks.set_size(0);
    }
};

#endif