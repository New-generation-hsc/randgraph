#ifndef _SECOND_ORDER_H_
#define _SECOND_ORDER_H_

#include <omp.h>
#include <unordered_set>
#include "api/types.hpp"
#include "engine/walk.hpp"
#include "engine/sample.hpp"
#include "engine/context.hpp"

template <>
inline vid_t get_vertex_from_walk<vid_t>(const vid_t &data)
{
    return data;
}

struct second_order_conf_t {
    wid_t numsources;
    hid_t hops;
    second_order_param_t param;
};

class second_order_app_t
{
private:
    wid_t _numsources;
    hid_t _hops;
    second_order_param_t param;

public:
    second_order_app_t(wid_t nsources, hid_t steps, second_order_param_t app_param)
    {
        _numsources = nsources;
        _hops = steps;
        param = app_param;
    }

    template <typename AppConfig>
    second_order_app_t(AppConfig &conf) {}

    second_order_app_t(second_order_conf_t &conf)
    {
        _numsources = conf.numsources;
        _hops = conf.hops;
        param = conf.param;
    }

    template <typename walk_data_t, WalkType walk_type>
    void prologue(graph_walk<walk_data_t, walk_type> *walk_manager)
    {
    }

    template <typename walk_data_t, WalkType walk_type>
    void update_walk(const walker_t<walk_data_t> &walker, graph_cache *cache, graph_walk<walk_data_t, walk_type> *walk_manager, sample_policy_t *sampler)
    {
        logstream(LOG_ERROR) << "you are using a generic method." << std::endl;
    }

    void epilogue()
    {
    }

    wid_t get_numsources() { return _numsources; }
    hid_t get_hops() { return _hops; }
};

template <>
void second_order_app_t::prologue<vid_t, SecondOrder>(graph_walk<vid_t, SecondOrder> *walk_manager)
{
#pragma omp parallel for schedule(static)
    for (wid_t idx = 0; idx < this->_numsources; idx++)
    {
        vid_t s = rand() % walk_manager->nvertices;
        walker_t<vid_t> walker = walker_makeup<vid_t>(s, idx, s, s, this->_hops);
        walk_manager->move_walk(walker);
    }

    for (bid_t blk = 0; blk < total_blocks<SecondOrder>(walk_manager->nblocks); blk++)
    {
        walk_manager->set_max_hop(blk, this->_hops);
    }
}

template <>
void second_order_app_t::update_walk<vid_t, SecondOrder>(const walker_t<vid_t> &walker, graph_cache *cache, graph_walk<vid_t, SecondOrder> *walk_manager, sample_policy_t *sampler)
{
    tid_t tid = omp_get_thread_num();
    vid_t cur_vertex = WALKER_POS(walker), prev_vertex = get_vertex_from_walk(walker.data);
    hid_t hop = WALKER_HOP(walker);
    bid_t cur_blk = walk_manager->global_blocks->get_block(cur_vertex);
    bid_t prev_blk = walk_manager->global_blocks->get_block(prev_vertex);
    unsigned seed = (unsigned)(cur_vertex + prev_vertex + hop + tid + time(NULL));

    bid_t cur_cache_index = (*(walk_manager->global_blocks))[cur_blk].cache_index, prev_cache_index = (*(walk_manager->global_blocks))[prev_blk].cache_index;
    bid_t nblocks = walk_manager->global_blocks->nblocks;
    assert(cur_cache_index != nblocks && prev_cache_index != nblocks);

    while (cur_cache_index != nblocks && hop > 0)
    {
        cache_block *cur_block = &(cache->cache_blocks[cur_cache_index]);
        cache_block *prev_block = &(cache->cache_blocks[prev_cache_index]);

        vid_t start_vertex = cur_block->block->start_vert, off = cur_vertex - start_vertex;
        vid_t prev_start_vertex = prev_block->block->start_vert, prev_off = prev_vertex - prev_start_vertex;
        eid_t adj_head = cur_block->beg_pos[off] - cur_block->block->start_edge, adj_tail = cur_block->beg_pos[off + 1] - cur_block->block->start_edge;
        eid_t prev_adj_head = prev_block->beg_pos[prev_off] - prev_block->block->start_edge, prev_adj_tail = prev_block->beg_pos[prev_off + 1] - prev_block->block->start_edge;

        vid_t next_vertex = 0;
        if (cur_block->weights == nullptr && cur_block->acc_weights == nullptr)
        {
            walk_context<SECONDORDERCTX> ctx(param, cur_vertex, walk_manager->nvertices, cur_block->csr + adj_head, cur_block->csr + adj_tail,
                                                    &seed, prev_vertex, prev_block->csr + prev_adj_head, prev_block->csr + prev_adj_tail);
            next_vertex = vertex_sample(ctx, sampler);
        }
        else
        {
            if (sampler->use_alias && sampler->use_acc_weight)
            {
                walk_context<BIASEDACCSECONDORDERCTX> ctx(param, cur_vertex, walk_manager->nvertices, cur_block->csr + adj_head, cur_block->csr + adj_tail,
                                                          &seed, prev_vertex, prev_block->csr + prev_adj_head, prev_block->csr + prev_adj_tail,
                                                          cur_block->acc_weights + adj_head, cur_block->acc_weights + adj_tail, prev_block->acc_weights + prev_adj_head, prev_block->acc_weights + prev_adj_tail, cur_block->prob, cur_block->alias);
                next_vertex = vertex_sample(ctx, sampler);
            }
            else if (sampler->use_acc_weight)
            {
                walk_context<BIASEDACCSECONDORDERCTX> ctx(param, cur_vertex, walk_manager->nvertices, cur_block->csr + adj_head, cur_block->csr + adj_tail,
                                                          &seed, prev_vertex, prev_block->csr + prev_adj_head, prev_block->csr + prev_adj_tail,
                                                          cur_block->acc_weights + adj_head, cur_block->acc_weights + adj_tail, prev_block->acc_weights + prev_adj_head, prev_block->acc_weights + prev_adj_tail);
                next_vertex = vertex_sample(ctx, sampler);
            }
            else
            {
                walk_context<BIASEDSECONDORDERCTX> ctx(param, cur_vertex, walk_manager->nvertices, cur_block->csr + adj_head, cur_block->csr + adj_tail,
                                                       &seed, prev_vertex, prev_block->csr + prev_adj_head, prev_block->csr + prev_adj_tail,
                                                       cur_block->weights + adj_head, cur_block->weights + adj_tail, prev_block->acc_weights + prev_adj_head, prev_block->acc_weights + prev_adj_tail);
                next_vertex = vertex_sample(ctx, sampler);
            }
        }
        prev_vertex = cur_vertex;
        cur_vertex = next_vertex;

        prev_cache_index = cur_cache_index;
        if (!(cur_vertex >= cur_block->block->start_vert && cur_vertex < cur_block->block->start_vert + cur_block->block->nverts))
        {
            cur_blk = walk_manager->global_blocks->get_block(cur_vertex);
            cur_cache_index = (*(walk_manager->global_blocks))[cur_blk].cache_index;
        }
        hop--;
    }

    if (hop > 0)
    {
        walker_t<vid_t> next_walker = walker_makeup(prev_vertex, WALKER_ID(walker), WALKER_SOURCE(walker), cur_vertex, hop);
        walk_manager->move_walk(next_walker);
        walk_manager->set_max_hop(next_walker);
    }
}

#endif