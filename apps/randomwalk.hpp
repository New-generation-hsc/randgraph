#ifndef _GRAPH_RANDOMWALK_H_
#define _GRAPH_RANDOMWALK_H_

#include <omp.h>
#include <cstdlib>
#include <ctime>

#include "api/types.hpp"
#include "engine/walk.hpp"
#include "engine/context.hpp"

class randomwalk_t {
protected:
    wid_t numsources;   /* the number of source start to walk */
    hid_t steps;        /* the number of hops */
    float teleport;   /* the probability teleport to source vertex */

public:
    randomwalk_t(wid_t num, hid_t hops, float prob) { 
        numsources = num;
        steps = hops;
        teleport = prob;
    }

    void update_walk(walk_t walk, cache_block* cache, graph_walk *walk_manager) {
        tid_t tid = omp_get_thread_num();
        vid_t dst = walk.pos;
        hid_t hop = walk.hop;

        srand(time(0));
        vid_t start_vert = cache->block->start_vert, end_vert = cache->block->start_vert + cache->block->nverts;
        while(dst >= start_vert && dst < end_vert && hop > 0) {
            vid_t off = dst - start_vert;
            eid_t adj_head = cache->beg_pos[off] - cache->block->start_edge, adj_tail = cache->beg_pos[off + 1] - cache->block->start_edge;
            graph_context ctx(dst, cache->csr + adj_head, cache->csr + adj_tail, teleport, walk_manager->nvertices);
            dst = choose_next(ctx);
            hop--;
        }

        if(hop > 0) {
            bid_t blk = walk_manager->global_blocks->get_block(dst);
            assert(blk < walk_manager->global_blocks->nblocks);
            walk_manager->move_walk(walk, blk, tid, dst, hop);
            walk_manager->set_max_hop(blk, hop);
        }
    }

    vid_t choose_next(context& ctx) {
        return ctx.transition();
    }

    wid_t get_numsources() { return numsources; }
    hid_t get_hops() { return steps; }
};

#endif