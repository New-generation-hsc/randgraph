#ifndef _GRAPH_CONTEXT_H_
#define _GRAPH_CONTEXT_H_

#include <cstdlib>
#include <ctime>
#include <unordered_set>
#include "api/types.hpp"
#include "logger/logger.hpp"

/** graph context
 * 
 * This file define when vertex choose the next hop, the tranisition context
 */

class context {
public:
    vid_t cur_vertex;
    vid_t nvertices;
    vid_t *adj_start, *adj_end;
    real_t *weight_start, *weight_end;
    unsigned *local_seed;

    /* the preprocessed alias table for fast selection */
    real_t *prob;
    vid_t  *alias;

    context(vid_t _pos, vid_t _nvertices, vid_t *_adj_start, vid_t *_adj_end, real_t *_weight_start, real_t *_weight_end, unsigned *seed)
    {
        this->cur_vertex = _pos;
        this->adj_start = _adj_start;
        this->adj_end = _adj_end;
        this->weight_start = _weight_start;
        this->weight_end = _weight_end;
        this->nvertices = _nvertices;
        this->local_seed = seed;
        this->prob = nullptr;
        this->alias = nullptr;
    }

    void set_alias_table(real_t *pre_prob, vid_t *pre_alias) {
        assert(pre_prob != nullptr && pre_alias != nullptr);
        this->prob = pre_prob;
        this->alias = pre_alias;
    }
};


class graph_context : public context {
public:
    float teleport;

    graph_context(vid_t _pos, vid_t _nvertices, float _teleport,
                  vid_t *_adj_start, vid_t *_adj_end, 
                  real_t *_weight_start, real_t *_weight_end, 
                  unsigned *seed
                 ) : context(_pos, _nvertices, _adj_start, _adj_end, _weight_start, _weight_end, seed)
    {
        this->teleport = _teleport;
    }
};


class second_order_context : public context {
public:
    vid_t prev_vertex;
    vid_t *prev_adj_start, *prev_adj_end;
    real_t *prev_weight_start, *prev_weight_end;

    second_order_context(vid_t cur, vid_t prev, vid_t num_vertices,
                         vid_t *adj_s, vid_t *adj_e,
                         vid_t *p_adj_s, vid_t *p_adj_e,
                         real_t *wht_s, real_t *wht_e,
                         unsigned *seed
                        ) : context(cur, num_vertices, adj_s, adj_e, wht_s, wht_e, seed)
    {
        this->prev_vertex = prev;
        this->prev_adj_start = p_adj_s;
        this->prev_adj_end = p_adj_e;
    }

    virtual void query_neighbors_weights(std::vector<real_t> &adj_weights) { }
    virtual void query_common_neighbors_weights(std::vector<real_t> &comm_adj_weights, std::vector<vid_t> &comm_neighbors) { }
};

class node2vec_context : public second_order_context {
public:
    real_t p, q;

    node2vec_context(vid_t cur, vid_t prev, vid_t num_vertices, 
                     real_t param_p, real_t param_q,
                     vid_t *adj_s, vid_t *adj_e,
                     vid_t *p_adj_s, vid_t *p_adj_e,
                     real_t *wht_s, real_t *wht_e,
                     unsigned *seed
    ) : second_order_context(cur, prev, num_vertices, adj_s, adj_e, p_adj_s, p_adj_e, wht_s, wht_e, seed) 
    {
        this->p = param_p;
        this->q = param_q;
    }

    virtual void query_neighbors_weights(std::vector<real_t>& adj_weights) {
        size_t deg = static_cast<size_t>(adj_end - adj_start);
        adj_weights.resize(deg);
        std::unordered_set<vid_t> prev_neighbors(prev_adj_start, prev_adj_end);
        for(size_t index = 0; index < deg; ++index) {
            if(*(adj_start + index) == prev_vertex) {
                if(weight_start == nullptr || weight_end == nullptr) adj_weights[index] = 1.0;
                else adj_weights[index] = *(weight_start + index);
            }else if(prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end()) {
                if(weight_start == nullptr || weight_end == nullptr) adj_weights[index] = 1.0 / p;
                else adj_weights[index] = *(weight_start + index) / p;
            }else {
                if(weight_start == nullptr || weight_end == nullptr) adj_weights[index] = 1.0 / q;
                else adj_weights[index] = *(weight_start + index) / q;
            }
        }
    }

    virtual void query_common_neighbors_weights(std::vector<real_t>& comm_adj_weights, std::vector<vid_t>& comm_neighbors) {
        size_t deg = static_cast<size_t>(adj_end - adj_start);
        std::unordered_set<vid_t> prev_neighbors(prev_adj_start, prev_adj_end);
        for (size_t index = 0; index < deg; ++index)
        {
            if (*(adj_start + index) == prev_vertex)
            {
                if (weight_start == nullptr || weight_end == nullptr) {
                    comm_adj_weights.push_back(1.0);
                    comm_neighbors.push_back(*(adj_start + index));
                }
                else {
                    comm_adj_weights.push_back(*(weight_start + index));
                    comm_neighbors.push_back(*(adj_start + index));
                }
            }
            else if (prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end())
            {
                if (weight_start == nullptr || weight_end == nullptr) {
                    comm_adj_weights.push_back(1.0 / p);
                    comm_neighbors.push_back(*(adj_start + index));
                }
                else {
                    comm_adj_weights.push_back(*(weight_start + index) / p);
                    comm_neighbors.push_back(*(adj_start + index));
                }
            }
        }
    }
};

class autoregressive_context : public second_order_context {
public:
    real_t alpha;

    autoregressive_context(vid_t cur, vid_t prev, vid_t num_vertices,
                           real_t param_alpha,
                           vid_t *adj_s, vid_t *adj_e,
                           vid_t *p_adj_s, vid_t *p_adj_e,
                           real_t *wht_s, real_t *wht_e,
                           unsigned *seed
    ) : second_order_context(cur, prev, num_vertices, adj_s, adj_e, p_adj_s, p_adj_e, wht_s, wht_e, seed)
    {
        this->alpha = param_alpha;
    }
};

#endif