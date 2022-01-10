#ifndef _GRAPH_CONTEXT_H_
#define _GRAPH_CONTEXT_H_

#include <cstdlib>
#include <ctime>
#include <unordered_set>
#include "api/types.hpp"
#include "logger/logger.hpp"

enum CtxType
{
    UNBAISEDCONTEXT,        /* the simple first-order context -- unbaised */
    BIASEDCONTEXT,          /* the baised simple first-order context */
    SECONDORDERCTX,         /* the simple second-order context -- unbaised */
    BIASEDSECONDORDERCTX,   /* the baised second-order context */
    BIASEDACCSECONDORDERCTX /* the baised second-order context, using accmulute weight */
};

class context
{
public:
    vid_t cur_vertex;
    vid_t nvertices;
    vid_t *adj_start, *adj_end;
    unsigned *local_seed;

    context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed)
    {
        this->cur_vertex = vertex;
        this->nvertices = num_vertices;
        this->adj_start = start,
        this->adj_end = end;
        this->local_seed = seed;
    }
};

template <CtxType ctx_type>
class walk_context : public context
{
public:
    walk_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed) : context(vertex, num_vertices, start, end, seed)
    {
    }
};

template <>
class walk_context<UNBAISEDCONTEXT> : public context
{
public:
    float teleport;
    walk_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed, float p) : context(vertex, num_vertices, start, end, seed)
    {
        this->teleport = p;
    }
};

template <>
class walk_context<BIASEDCONTEXT> : public context
{
public:
    float teleport;
    real_t *weight_start, *weight_end;
    walk_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
                 real_t *wht_start, real_t *wht_end, float p) : context(vertex, num_vertices, start, end, seed)
    {
        this->teleport = p;
        this->weight_start = wht_start;
        this->weight_end = wht_end;
    }
};

template <>
class walk_context<SECONDORDERCTX> : public context
{
public:
    vid_t prev_vertex;
    vid_t *prev_adj_start, *prev_adj_end;
    walk_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
                 vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e) : context(vertex, num_vertices, start, end, seed)
    {
        this->prev_vertex = prev;
        this->prev_adj_start = p_adj_s;
        this->prev_adj_end = p_adj_e;
    }
};

template <>
class walk_context<BIASEDSECONDORDERCTX> : public context
{
public:
    vid_t prev_vertex;
    vid_t *prev_adj_start, *prev_adj_end;
    real_t *weight_start, *weight_end;

    walk_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
                 vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, real_t *wht_start, real_t *wht_end) : context(vertex, num_vertices, start, end, seed)
    {
        this->prev_vertex = prev;
        this->prev_adj_start = p_adj_s;
        this->prev_adj_end = p_adj_e;
        this->weight_start = wht_start;
        this->weight_end = wht_end;
    }
};

template <>
class walk_context<BIASEDACCSECONDORDERCTX> : public context
{
public:
    vid_t prev_vertex;
    vid_t *prev_adj_start, *prev_adj_end;
    real_t *acc_weight_start, *acc_weight_end;

    walk_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
                 vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, real_t *acc_wht_start, real_t *acc_wht_end) : context(vertex, num_vertices, start, end, seed)
    {
        this->prev_vertex = prev;
        this->prev_adj_start = p_adj_s;
        this->prev_adj_end = p_adj_e;
        this->acc_weight_start = acc_wht_start;
        this->acc_weight_end = acc_wht_end;
    }
};

template <CtxType ctx_type>
class node2vec_context : public walk_context<ctx_type>
{
public:
    node2vec_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
                     vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e) : walk_context<ctx_type>(vertex, num_vertices, start, end, seed)
    {
        logstream(LOG_ERROR) << "you should use a specialized node2vec context" << std::endl;
    }

};

template <>
class node2vec_context<SECONDORDERCTX> : public walk_context<SECONDORDERCTX>
{
public:
    float p, q;
    node2vec_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
                     vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, float param_p, float param_q) : walk_context<SECONDORDERCTX>(vertex, num_vertices, start, end, seed, prev, p_adj_s, p_adj_e)
    {
        this->p = param_p;
        this->q = param_q;
    }

    void query_neigbors_weight(std::vector<real_t> &adj_weights)
    {
        size_t deg = static_cast<size_t>(adj_end - adj_start);
        adj_weights.resize(deg);
        std::unordered_set<vid_t> prev_neighbors(prev_adj_start, prev_adj_end);
        for(size_t index = 0; index < deg; ++index) {
            if(*(adj_start + index) == prev_vertex) {
                adj_weights[index] = 1.0;
            }else if(prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end()) {
                adj_weights[index] = 1.0 / p;
            }else {
                adj_weights[index] = 1.0 / q;
            }
        }
    }
};

template <>
class node2vec_context<BIASEDSECONDORDERCTX> : public walk_context<BIASEDSECONDORDERCTX>
{
public:
    float p, q;
    node2vec_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
                     vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, real_t *wht_start, real_t *wht_end, float param_p, float param_q) : walk_context<BIASEDSECONDORDERCTX>(vertex, num_vertices, start, end, seed, prev, p_adj_s, p_adj_e, wht_start, wht_end)
    {
        this->p = param_p;
        this->q = param_q;
    }

    void query_neigbors_weight(std::vector<real_t> &adj_weights)
    {
        size_t deg = static_cast<size_t>(adj_end - adj_start);
        adj_weights.resize(deg);
        std::unordered_set<vid_t> prev_neighbors(prev_adj_start, prev_adj_end);
        for(size_t index = 0; index < deg; ++index) {
            if(*(adj_start + index) == prev_vertex) {
                adj_weights[index] = *(weight_start + index);
            }else if(prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end()) {
                adj_weights[index] = *(weight_start + index) / p;
            }else {
                adj_weights[index] = *(weight_start + index) / q;
            }
        }
    }
};

template <>
class node2vec_context<BIASEDACCSECONDORDERCTX> : public walk_context<BIASEDACCSECONDORDERCTX>
{
public:
    float p, q;
    node2vec_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
                     vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, real_t *acc_wht_start, real_t *acc_wht_end, float param_p, float param_q) : walk_context<BIASEDACCSECONDORDERCTX>(vertex, num_vertices, start, end, seed, prev, p_adj_s, p_adj_e, acc_wht_start, acc_wht_end)
    {
        this->p = param_p;
        this->q = param_q;
    }

    void query_neigbors_weight(std::vector<real_t> &adj_weights)
    {
        size_t deg = static_cast<size_t>(adj_end - adj_start);
        adj_weights.resize(deg);
        std::unordered_set<vid_t> prev_neighbors(prev_adj_start, prev_adj_end);
        for(size_t index = 0; index < deg; ++index) {
            if(*(adj_start + index) == prev_vertex) {
                if(index == 0) adj_weights[index] = *acc_weight_start;
                else adj_weights[index] = *(acc_weight_start + index) - *(acc_weight_start + index - 1);
            }else if(prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end()) {
                if(index == 0) adj_weights[index] = (*acc_weight_start) / p;
                else adj_weights[index] = (*(acc_weight_start + index) - *(acc_weight_start + index - 1)) / p;
            }else {
                if(index == 0) adj_weights[index] = (*acc_weight_start) / q;
                else adj_weights[index] = (*(acc_weight_start + index) - *(acc_weight_start + index - 1)) / q;
            }
        }
    }
};

#endif