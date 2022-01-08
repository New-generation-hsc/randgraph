#ifndef _GRAPH_NODE2VEC_H_
#define _GRAPH_NODE2VEC_H_

#include <omp.h>
#include <unordered_set>
#include "api/types.hpp"
#include "engine/walk.hpp"
#include "engine/sample.hpp"

template <>
inline vid_t get_vertex_from_walk<vid_t>(const vid_t &data)
{
    return data;
}

struct node2vec_conf_t {
    wid_t numsources;
    hid_t hops;
    real_t p, q; 
    bool weighted;
};

class node2vec_t {
private:
    wid_t numsources;
    hid_t hops;
    real_t p, q; // hyper-parameter
    bool weighted;

    void post_query_weights(cache_block *cur_block, vid_t cur_vertex, cache_block *prev_block, vid_t prev_vertex, std::vector<real_t> &adj_weights);
public: 
    node2vec_t(wid_t nsources, hid_t steps, real_t _p, real_t _q, bool _weighted)
    {
        numsources = nsources;
        hops = steps;
        p = _p;
        q = _q;
        weighted = _weighted;
    }

    template<typename AppConfig>
    node2vec_t(AppConfig& conf) { }

    node2vec_t(node2vec_conf_t& conf) {
        numsources = conf.numsources;
        hops = conf.hops;
        p = conf.p;
        q = conf.q;
        weighted = conf.weighted;
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

    void epilogue() {
        
    }

    wid_t get_numsources() { return numsources; }
    hid_t get_hops() { return hops; }
};


// void node2vec_t::post_query_weights(cache_block *cur_block, vid_t cur_vertex, cache_block *prev_block, vid_t prev_vertex, std::vector<real_t> &adj_weights)
// {
//     vid_t start_vertex = cur_block->block->start_vert, off = cur_vertex - start_vertex;
//     eid_t adj_head = cur_block->beg_pos[off] - cur_block->block->start_edge, adj_tail = cur_block->beg_pos[off + 1] - cur_block->block->start_edge;
//     eid_t deg = adj_tail - adj_head;
//     adj_weights.resize(deg);

//     start_vertex = prev_block->block->start_vert;
//     off = prev_vertex - start_vertex;
//     eid_t prev_adj_head = prev_block->beg_pos[off] - prev_block->block->start_edge, prev_adj_tail = prev_block->beg_pos[off + 1] - prev_block->block->start_edge;
//     std::unordered_set<vid_t> prev_neighbors(prev_block->csr + prev_adj_head, prev_block->csr + prev_adj_tail);
//     for(eid_t index = adj_head; index < adj_tail; index++) {
//         if(cur_block->csr[index] == prev_vertex) {
//             if(weighted) adj_weights[index - adj_head] = cur_block->weights[index];
//             else adj_weights[index - adj_head] = 1.0;
//         }else if(prev_neighbors.find(cur_block->csr[index]) != prev_neighbors.end()) {
//             if(weighted) adj_weights[index - adj_head] = 1.0 / p * cur_block->weights[index];
//             else adj_weights[index - adj_head] = 1.0 / p;
//         }else {
//             if(weighted) adj_weights[index - adj_head] = p * cur_block->weights[index];
//             else adj_weights[index - adj_head] = p;
//         }
//     }
// }

template<>
void node2vec_t::prologue<vid_t, SecondOrder>(graph_walk<vid_t, SecondOrder> *walk_manager) {
#pragma omp parallel for schedule(static)
    for (wid_t idx = 0; idx < this->numsources; idx++)
    {
        vid_t s = rand() % walk_manager->nvertices;
        walker_t<vid_t> walker = walker_makeup<vid_t>(s, idx, s, s, this->hops);
        walk_manager->move_walk(walker);
    }

    for (bid_t blk = 0; blk < total_blocks<SecondOrder>(walk_manager->nblocks); blk++)
    {
        walk_manager->set_max_hop(p, this->hops);
    }
}

template<>
void node2vec_t::update_walk(const walker_t<vid_t> &walker, graph_cache *cache, graph_walk<vid_t, SecondOrder> *walk_manager, second_order_sample_t *sampler)
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

    while (cur_cache_index != nblocks && hop > 0) {
        cache_block *cur_block = &(cache->cache_blocks[cur_cache_index]);
        cache_block *prev_block = &(cache->cache_blocks[prev_cache_index]);

        // std::vector<real_t> adj_weights;
        // post_query_weights(cur_block, cur_vertex, prev_block, prev_vertex, adj_weights);
        // vid_t next_vertex;
        // if(adj_weights.empty()) next_vertex = (vid_t)rand_r(&seed) % walk_manager->nvertices;
        // else next_vertex = sample(sampler, adj_weights);
        vid_t start_vertex = cur_block->block->start_vert, off = cur_vertex - start_vertex;
        eid_t adj_head = cur_block->beg_pos[off] - cur_block->block->start_edge, adj_tail = cur_block->beg_pos[off + 1] - cur_block->block->start_edge;
        real_t *weight_start = nullptr, *weight_end = nullptr;
        if(cur_block->weights != nullptr) {
            weight_start = cur_block->weights + adj_head;
            weight_end = cur_block->weights + adj_tail;
        }
        node2vec_context ctx(cur_vertex, prev_vertex, walk_manager->nvertices, p, q,
            cur_block->csr +  adj_head, cur_block->csr + adj_tail,
            prev_block->csr + adj_head, prev_block->csr + adj_tail,
            weight_start, weight_end, &seed                
        );
        vid_t next_vertex = vertex_sample(ctx, sampler);
        prev_vertex = cur_vertex;
        cur_vertex = next_vertex;

        prev_cache_index = cur_cache_index;
        if(!(cur_vertex >= cur_block->block->start_vert && cur_vertex < cur_block->block->start_vert + cur_block->block->nverts)) {
            cur_blk = walk_manager->global_blocks->get_block(cur_vertex);
            cur_cache_index = (*(walk_manager->global_blocks))[cur_blk].cache_index;
        }
        hop--;
    }

    if(hop > 0) {
        walker_t<vid_t> next_walker = walker_makeup(prev_vertex, WALKER_ID(walker), WALKER_SOURCE(walker), cur_vertex, hop);
        walk_manager->move_walk(next_walker);
        walk_manager->set_max_hop(next_walker);
    }
}

#endif