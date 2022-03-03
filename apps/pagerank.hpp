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
#include "userprogram.hpp"

struct pagerank_conf_t
{
    vid_t firstsource;
    vid_t numsources;
    wid_t walkpersource;
    hid_t steps;
    real_t teleport;
};

class pagerank_t {
protected:
    pagerank_conf_t _conf;

public:
    pagerank_t(vid_t num, hid_t hops, vid_t firstsource, wid_t walkpersource)
    {
        _conf.numsources = num;
        _conf.steps = hops;
        _conf.firstsource = firstsource;
        _conf.walkpersource = walkpersource;
        _conf.teleport = 0.0;
    }

    template<typename AppConfig>
    pagerank_t(AppConfig& conf) { }

    pagerank_t(pagerank_conf_t& conf) {
        _conf = conf;
    }

    template <typename walk_data_t, WalkType walk_type>
    void prologue(graph_walk<walk_data_t, walk_type> *walk_manager)
    {
    }

    template <typename walk_data_t, WalkType walk_type>
    void update_walk(const walker_t<walk_data_t> &walker, graph_cache *cache, graph_walk<walk_data_t, walk_type> *walk_manager, sample_policy_t *sampler, unsigned int *seed, bool dynamic)
    {
        logstream(LOG_ERROR) << "you are using a generic method." << std::endl;
        // update_strategy_t<randomwalk_conf_t, walk_data_t, walk_type, SampleType>::update_walk(_conf, walker, cache, walk_manager, sampler);
    }

    void epilogue() {  }

    wid_t get_numsources() { return _conf.numsources; }
    hid_t get_hops() { return _conf.steps; }
};

template <>
void pagerank_t::prologue<empty_data_t, FirstOrder>(graph_walk<empty_data_t, FirstOrder> *walk_manager)
{
    wid_t total_walks = 0;
    for(vid_t vertex = _conf.firstsource; (vertex < _conf.firstsource + _conf.numsources) && (vertex < walk_manager->nvertices); vertex++) {
        // #pragma omp parallel for schedule(static)
        for(wid_t idx = 0; idx < _conf.walkpersource; idx++) {
            walker_t<empty_data_t> walker = walker_makeup(idx, vertex, vertex, this->_conf.steps);
            walk_manager->move_walk(walker);
            total_walks++;
        }
    }

    logstream(LOG_INFO) << "total walks num = " << total_walks << std::endl;

    for (bid_t blk = 0; blk < total_blocks<FirstOrder>(walk_manager->nblocks); blk++)
    {
        walk_manager->set_max_hop(blk, _conf.steps);
        logstream(LOG_DEBUG) << "block [ " << blk << " ]  walks num = " << walk_manager->nblockwalks(blk) << std::endl;
    }
}

template <>
void pagerank_t::update_walk<empty_data_t, FirstOrder>(const walker_t<empty_data_t> &walker, graph_cache *cache, graph_walk<empty_data_t, FirstOrder> *walk_manager, sample_policy_t *sampler, unsigned int * seed, bool dynamic)
{
        // tid_t tid = (tid_t)omp_get_thread_num();
        vid_t dst = WALKER_POS(walker);
        hid_t hop = WALKER_HOP(walker);
        bid_t p = walk_manager->global_blocks->get_block(dst);
        cache_block *run_block = &(cache->cache_blocks[(*(walk_manager->global_blocks))[p].cache_index]);

         // unsigned seed = (unsigned)(dst + hop + tid + time(NULL));
        vid_t start_vert = run_block->block->start_vert;

        vid_t off = dst - start_vert;
        eid_t adj_head = run_block->beg_pos[off] - run_block->block->start_edge, adj_tail = run_block->beg_pos[off + 1] - run_block->block->start_edge;
        vid_t next_vertex;
        if (run_block->weights == NULL)
        {
            walk_context<UNBAISEDCONTEXT> ctx(dst, walk_manager->nvertices, run_block->csr + adj_head, run_block->csr + adj_tail, seed, _conf.teleport);
            next_vertex = vertex_sample(ctx, sampler, nullptr, dynamic);
        }
        else
        {
            walk_context<BIASEDCONTEXT> ctx(dst, walk_manager->nvertices, run_block->csr + adj_head, run_block->csr + adj_tail, seed, run_block->weights + adj_head, run_block->weights + adj_tail, _conf.teleport);
            next_vertex = vertex_sample(ctx, sampler, nullptr, dynamic);
        }
        hop--;

        // logstream(LOG_DEBUG) << "prev_vertex : " << dst << ", next_vertex : " << next_vertex << std::endl;

        if (hop > 0)
        {
            bid_t blk = walk_manager->global_blocks->get_block(next_vertex);
            assert(blk < walk_manager->global_blocks->nblocks);
            walker_t<empty_data_t> next_walker = walker_makeup(WALKER_ID(walker), WALKER_SOURCE(walker), next_vertex, hop);
            walk_manager->move_walk(next_walker);
            walk_manager->set_max_hop(next_walker);
        }
}
#endif
