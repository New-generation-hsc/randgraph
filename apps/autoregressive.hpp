#ifndef _GRAPH_AUTOREGRESSIVE_H_
#define _GRAPH_AUTOREGRESSIVE_H_

#include "api/types.hpp"
#include "userprogram.hpp"
#include "engine/context.hpp"

struct autoregressive_conf_t
{
    wid_t numsources;
    hid_t hops;
    real_t alpha;
};

template <typename SampleType>
class update_strategy_t<autoregressive_conf_t, vid_t, SecondOrder, SampleType>
{
public:
    static void update_walk(const autoregressive_conf_t &conf, const walker_t<vid_t> &walker, graph_cache *cache, graph_walk<vid_t, SecondOrder> *walk_manager, SampleType *sampler)
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
                autoregressive_context<SECONDORDERCTX> ctx(cur_vertex, walk_manager->nvertices, cur_block->csr + adj_head, cur_block->csr + adj_tail,
                                                     &seed, prev_vertex, prev_block->csr + prev_adj_head, prev_block->csr + prev_adj_tail, conf.alpha);
                next_vertex = vertex_sample(ctx, sampler);
            }
            else
            {
                if (sampler->use_alias && sampler->use_acc_weight)
                {
                    autoregressive_context<BIASEDACCSECONDORDERCTX> ctx(cur_vertex, walk_manager->nvertices, cur_block->csr + adj_head, cur_block->csr + adj_tail,
                                                                        &seed, prev_vertex, prev_block->csr + prev_adj_head, prev_block->csr + prev_adj_tail,
                                                                        cur_block->acc_weights + adj_head, cur_block->acc_weights + adj_tail,
                                                                        cur_block->prob, cur_block->alias, conf.alpha, prev_block->acc_weights + prev_adj_head, prev_block->acc_weights + prev_adj_tail);
                    next_vertex = vertex_sample(ctx, sampler);
                }
                else if (sampler->use_acc_weight)
                {
                    autoregressive_context<BIASEDACCSECONDORDERCTX> ctx(cur_vertex, walk_manager->nvertices, cur_block->csr + adj_head, cur_block->csr + adj_tail,
                                                                        &seed, prev_vertex, prev_block->csr + prev_adj_head, prev_block->csr + prev_adj_tail,
                                                                        cur_block->acc_weights + adj_head, cur_block->acc_weights + adj_tail, conf.alpha, prev_block->acc_weights + prev_adj_head, prev_block->acc_weights + prev_adj_tail);
                    next_vertex = vertex_sample(ctx, sampler);
                }
                else
                {
                    autoregressive_context<BIASEDSECONDORDERCTX> ctx(cur_vertex, walk_manager->nvertices, cur_block->csr + adj_head, cur_block->csr + adj_tail,
                                                                     &seed, prev_vertex, prev_block->csr + prev_adj_head, prev_block->csr + prev_adj_tail,
                                                                     cur_block->weights + adj_head, cur_block->weights + adj_tail, conf.alpha, prev_block->weights + prev_adj_head, prev_block->weights + prev_adj_tail);
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
};

class autoregressive_t
{
private:
    autoregressive_conf_t _conf;

    void post_query_weights(cache_block *cur_block, vid_t cur_vertex, cache_block *prev_block, vid_t prev_vertex, std::vector<real_t> &adj_weights);

public:
    autoregressive_t(wid_t nsources, hid_t steps, real_t alpha)
    {
        _conf.numsources = nsources;
        _conf.hops = steps;
        _conf.alpha = alpha;
    }

    template <typename AppConfig>
    autoregressive_t(AppConfig &conf) {}

    autoregressive_t(autoregressive_conf_t &conf)
    {
        _conf = conf;
    }

    template <typename walk_data_t, WalkType walk_type>
    void prologue(graph_walk<walk_data_t, walk_type> *walk_manager)
    {
    }

    template <typename walk_data_t, WalkType walk_type, typename SampleType>
    void update_walk(const walker_t<walk_data_t> &walker, graph_cache *cache, graph_walk<walk_data_t, walk_type> *walk_manager, SampleType *sampler)
    {
        update_strategy_t<autoregressive_conf_t, vid_t, SecondOrder, SampleType>::update_walk(_conf, walker, cache, walk_manager, sampler);
    }

    void epilogue()
    {
    }

    wid_t get_numsources() { return _conf.numsources; }
    hid_t get_hops() { return _conf.hops; }
};

template <>
void autoregressive_t::prologue<vid_t, SecondOrder>(graph_walk<vid_t, SecondOrder> *walk_manager)
{
#pragma omp parallel for schedule(static)
    for (wid_t idx = 0; idx < this->_conf.numsources; idx++)
    {
        vid_t s = rand() % walk_manager->nvertices;
        walker_t<vid_t> walker = walker_makeup<vid_t>(s, idx, s, s, this->_conf.hops);
        walk_manager->move_walk(walker);
    }

    for (bid_t blk = 0; blk < total_blocks<SecondOrder>(walk_manager->nblocks); blk++)
    {
        walk_manager->set_max_hop(blk, this->_conf.hops);
    }
}

#endif