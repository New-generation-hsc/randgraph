#ifndef _GRAPH_RANDOMWALK_H_
#define _GRAPH_RANDOMWALK_H_

#include <omp.h>
#include <cstdlib>
#include <ctime>

#include "api/types.hpp"
#include "engine/walk.hpp"
#include "engine/context.hpp"
#include "engine/sample.hpp"
#include "metrics/metrics.hpp"

struct randomwalk_conf_t {
    wid_t numsources;
    hid_t steps;
    float teleport;
};

class randomwalk_t {
protected:
    wid_t numsources;   /* the number of source start to walk */
    hid_t steps;        /* the number of hops */
    float teleport;     /* the probability teleport to source vertex */

public:
    randomwalk_t(wid_t num, hid_t hops, float prob) {
        numsources = num;
        steps = hops;
        teleport = prob;
    }

    template<typename AppConfig>
    randomwalk_t(AppConfig& conf) { }

    randomwalk_t(randomwalk_conf_t& conf) {
        numsources = conf.numsources;
        steps = conf.steps;
        teleport = conf.teleport;
    }

    template <typename walk_data_t, WalkType walk_type>
    void prologue(graph_walk<walk_data_t, walk_type> *walk_manager)
    {
    }

    template <typename walk_data_t, WalkType walk_type, typename SampleType>
    void update_walk(const walker_t<walk_data_t> &walker, graph_cache *cache, graph_walk<walk_data_t, walk_type> *walk_manager, SampleType *sampler)
    {
        logstream(LOG_ERROR) << "you are using a generic method." << std::endl;
    }

    wid_t get_numsources() { return numsources; }
    hid_t get_hops() { return steps; }
};

template <>
void randomwalk_t::prologue<empty_data_t, FirstOrder>(graph_walk<empty_data_t, FirstOrder> *walk_manager)
{
#pragma omp parallel for schedule(static)
    for (wid_t idx = 0; idx < this->numsources; idx++)
    {
        vid_t s = rand() % walk_manager->nvertices;
        walker_t<empty_data_t> walker = walker_makeup(idx, s, s, this->steps);
        walk_manager->move_walk(walker);
    }

    for (bid_t blk = 0; blk < total_blocks<FirstOrder>(walk_manager->nblocks); blk++)
    {
        walk_manager->set_max_hop(blk, steps);
    }
}

template <>
void randomwalk_t::update_walk(const walker_t<empty_data_t> &walker, graph_cache *cache, graph_walk<empty_data_t, FirstOrder> *walk_manager, sample_policy_t *sampler)
{
    tid_t tid = omp_get_thread_num();
    vid_t dst = WALKER_POS(walker);
    hid_t hop = WALKER_HOP(walker);
    bid_t p = walk_manager->global_blocks->get_block(dst);
    cache_block *run_block = &(cache->cache_blocks[(*(walk_manager->global_blocks))[p].cache_index]);

    unsigned seed = (unsigned)(dst + hop + tid + time(NULL));
    vid_t start_vert = run_block->block->start_vert, end_vert = run_block->block->start_vert + run_block->block->nverts;
    while (dst >= start_vert && dst < end_vert && hop > 0)
    {
        vid_t off = dst - start_vert;
        eid_t adj_head = run_block->beg_pos[off] - run_block->block->start_edge, adj_tail = run_block->beg_pos[off + 1] - run_block->block->start_edge;
        if (run_block->weights == NULL) {
            walk_context<UNBAISEDCONTEXT> ctx(dst, walk_manager->nvertices, run_block->csr + adj_head, run_block->csr + adj_tail, &seed, teleport);
            dst = vertex_sample(ctx, sampler);
        } else {
            walk_context<BIASEDCONTEXT> ctx(dst, walk_manager->nvertices, run_block->csr + adj_head, run_block->csr + adj_tail, &seed, run_block->weights + adj_head, run_block->weights + adj_tail, teleport);
            dst = vertex_sample(ctx, sampler);
        }
        hop--;
    }

    if(hop > 0) {
        bid_t blk = walk_manager->global_blocks->get_block(dst);
        assert(blk < walk_manager->global_blocks->nblocks);
        walker_t<empty_data_t> next_walker = walker_makeup(WALKER_ID(walker), WALKER_SOURCE(walker), dst, hop);
        walk_manager->move_walk(next_walker);
        walk_manager->set_max_hop(next_walker);
    }
}

#endif
