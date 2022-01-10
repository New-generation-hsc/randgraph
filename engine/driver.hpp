#ifndef _GRAPH_DRIVER_H_
#define _GRAPH_DRIVER_H_

#include "cache.hpp"
#include "util/io.hpp"
#include "api/graph_buffer.hpp"
#include "api/types.hpp"

/** graph_driver
 * This file contribute to define the operations of how to read from disk
 * or how to write graph data into disk
 */

class graph_driver {
public:
    graph_driver() { }
    
    void load_block_vertex(int fd, eid_t *buf, const block_t &block) { 
        load_block_range(fd, buf, block.nverts + 1, block.start_vert * sizeof(eid_t));
    }

    void load_block_degree(int fd, vid_t *buf, const block_t &block) { 
        load_block_range(fd, buf, block.nverts, block.start_vert * sizeof(vid_t));
    }

    void load_block_edge(int fd, vid_t *buf, const block_t &block) {
        load_block_range(fd, buf, block.nedges, block.start_edge * sizeof(vid_t));
    }

    void load_block_weight(int fd, real_t* buf, const block_t& block) {
        load_block_range(fd, buf, block.nedges, block.start_edge * sizeof(real_t));
    }

    void load_block_prob(int fd, real_t* buf, const block_t& block) {
        load_block_range(fd, buf, block.nedges, block.start_edge * sizeof(real_t));
    }

    void load_block_alias(int fd, vid_t* buf, const block_t& block) {
        load_block_range(fd, buf, block.nedges, block.start_edge * sizeof(vid_t));
    }

    template<typename walk_data_t>
    void load_walk(int fd, size_t cnt, graph_buffer<walk_data_t> &walks) {
        load_block_range(fd, walks.buffer_begin(), cnt, 0);
        walks.set_size(cnt);
    }

    template<typename walk_data_t>
    void dump_walk(int fd, graph_buffer<walk_data_t> &walks) {
        dump_block_range(fd, walks.buffer_begin(), walks.size(), 0);
        walks.set_size(0);
    }
};

#endif