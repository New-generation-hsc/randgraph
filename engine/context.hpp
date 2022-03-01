#ifndef _GRAPH_CONTEXT_H_
#define _GRAPH_CONTEXT_H_

#include <cstdlib>
#include <ctime>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include "api/types.hpp"
#include "logger/logger.hpp"
#include "util/hash.hpp"

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
    second_order_param_t app_param;
    vid_t prev_vertex;
    vid_t *prev_adj_start, *prev_adj_end;
    std::unordered_set<vid_t> prev_neighbors;
    BloomFilter *bf;
    real_t w_equal, w_comm, w_other, w_max, w_min;

    walk_context(second_order_param_t param, vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
                 vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, second_order_func_t app_func, BloomFilter *filter = nullptr) : context(vertex, num_vertices, start, end, seed)
    {
        this->app_param = param;
        this->prev_vertex = prev;
        this->prev_adj_start = p_adj_s;
        this->prev_adj_end = p_adj_e;
        this->bf = filter;
        vertex_t c_vertex = { vertex, static_cast<vid_t>(end - start) };
        vertex_t p_vertex = { prev, static_cast<vid_t>(p_adj_e - p_adj_s) };
        this->w_equal = app_func.query_equal_func(p_vertex, c_vertex);
        this->w_comm = app_func.query_comm_neighbor_func(p_vertex, c_vertex);
        this->w_other = app_func.query_other_vertex_func(p_vertex, c_vertex);
        this->w_max = app_func.query_upper_bound_func(p_vertex, c_vertex);
        this->w_min = app_func.query_lower_bound_func(p_vertex, c_vertex);
    }

    void query_neigbors_weight(std::vector<real_t> &adj_weights)
    {
        size_t deg = static_cast<size_t>(adj_end - adj_start);
        adj_weights.resize(deg);
        prev_neighbors = std::unordered_set<vid_t>(prev_adj_start, prev_adj_end);
        for(size_t index = 0; index < deg; ++index) {
            if(*(adj_start + index) == prev_vertex) {
                // adj_weights[index] = app_param.gamma;
                adj_weights[index] = this->w_equal;
            }else if(prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end()) {
                // adj_weights[index] = app_param.alpha + app_param.beta;
                adj_weights[index] = this->w_comm;
            }else {
                // adj_weights[index] = app_param.delta;
                adj_weights[index] = this->w_other;
            }
        }
    }

    real_t query_max_weight() {
        // if(cur_vertex == prev_vertex) return app_param.alpha + app_param.beta;
        // return std::max(app_param.gamma, std::max(app_param.alpha + app_param.beta, app_param.delta));
        if(cur_vertex == prev_vertex) return this->w_equal;
        return this->w_max;
    }

    real_t query_min_weight() { return this->w_min; }

    real_t query_vertex_weight(size_t index) {
        // if(*(adj_start + index) == prev_vertex) {
        //     return app_param.gamma;
        // }else if(prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end()) {
        //     return app_param.alpha + app_param.beta;
        // }else {
        //     return app_param.delta;
        // }

        vid_t next_vertex = *(adj_start + index);
        // if(next_vertex == prev_vertex) return app_param.gamma;
        if(next_vertex == prev_vertex) return this->w_equal;

        if(bf && !bf->exist(prev_vertex, next_vertex)) {
            // return app_param.delta;
            return this->w_other;
        }

        // if(prev_neighbors.empty()) {
        //     prev_neighbors = std::unordered_set<vid_t>(prev_adj_start, prev_adj_end);
        // }

        // if(prev_neighbors.find(next_vertex) != prev_neighbors.end()) {
        //     return app_param.alpha + app_param.beta;
        // }else {
        //     return app_param.delta;
        // }
        bool exist = std::binary_search(prev_adj_start, prev_adj_end, next_vertex);
        // if(exist) return app_param.alpha + app_param.beta;
        // else return app_param.delta;
        if(exist) return this->w_comm;
        else return this->w_other;
    }

    void query_comm_neigbors_weight(std::vector<real_t> &adj_weights, std::vector<vid_t> &comm_neighbors, real_t &total_weight)
    {
        size_t deg = static_cast<size_t>(adj_end - adj_start);
        real_t comm_weight_sum = 0;
        prev_neighbors = std::unordered_set<vid_t>(prev_adj_start, prev_adj_end);
        for (size_t index = 0; index < deg; ++index)
        {
            if (*(adj_start + index) == prev_vertex)
            {
                comm_weight_sum += app_param.gamma;
                adj_weights.push_back(comm_weight_sum);
                comm_neighbors.push_back(*(adj_start + index));
            }
            else if (prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end())
            {
                comm_weight_sum += app_param.alpha + app_param.beta;
                adj_weights.push_back(comm_weight_sum);
                comm_neighbors.push_back(*(adj_start + index));
            }
        }
        total_weight = comm_weight_sum + (deg - comm_neighbors.size()) * app_param.delta;
    }

    real_t query_pivot_weight(const std::vector<real_t> &adj_weights, const std::vector<vid_t> &comm_neighbors, eid_t pivot)
    {
        size_t pos = std::upper_bound(comm_neighbors.begin(), comm_neighbors.end(), *(adj_start + pivot)) - comm_neighbors.begin();
        real_t pivot_weight = 0.0;
        if (pos > 0)
        {
            pivot_weight += adj_weights[pos - 1];
        }
        pivot_weight += (pivot + 1 - pos) * app_param.delta;
        return pivot_weight;
    }
};

template <>
class walk_context<BIASEDSECONDORDERCTX> : public context
{
public:
    second_order_param_t app_param;
    vid_t prev_vertex;
    vid_t *prev_adj_start, *prev_adj_end;
    real_t *weight_start, *weight_end;
    real_t *prev_weight_start, *prev_weight_end;

    walk_context(second_order_param_t param, vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
                 vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, real_t *wht_start, real_t *wht_end,
                 real_t *p_wht_s, real_t *p_wht_e) : context(vertex, num_vertices, start, end, seed)
    {
        this->app_param = param;
        this->prev_vertex = prev;
        this->prev_adj_start = p_adj_s;
        this->prev_adj_end = p_adj_e;
        this->weight_start = wht_start;
        this->weight_end = wht_end;
        this->prev_weight_start = p_wht_s;
        this->prev_weight_end = p_wht_e;
    }

    void query_neigbors_weight(std::vector<real_t> &adj_weights) const
    {
        size_t deg = static_cast<size_t>(adj_end - adj_start);
        adj_weights.resize(deg);
        std::unordered_map<vid_t, size_t> neighbor_index;
        size_t prev_deg = static_cast<size_t>(prev_adj_end - prev_adj_start);
        for(size_t index = 0; index < prev_deg; ++index) neighbor_index[*(prev_adj_start + index)] = index;
        for(size_t index = 0; index < deg; ++index) {
            vid_t nxt_vertex = *(adj_start + index);
            if( nxt_vertex == prev_vertex) {
                adj_weights[index] = *(weight_start + index) * app_param.gamma;
            }else if(neighbor_index.find(nxt_vertex) != neighbor_index.end()) {
                adj_weights[index] = *(prev_weight_start + neighbor_index[nxt_vertex]) * app_param.alpha + *(weight_start + index) * app_param.beta;
            }else {
                adj_weights[index] = *(weight_start + index) * app_param.delta;
            }
        }
    }
};

template <>
class walk_context<BIASEDACCSECONDORDERCTX> : public context
{
public:
    second_order_param_t app_param;
    vid_t prev_vertex;
    vid_t *prev_adj_start, *prev_adj_end;
    real_t *acc_weight_start, *acc_weight_end;
    real_t *prev_acc_weight_start, *prev_acc_weight_end;
    real_t *prob;
    vid_t  *alias;
    real_t gamma_factor, beta_factor, alpha_factor;

    walk_context(second_order_param_t param, vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
                 vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, real_t *acc_wht_start, real_t *acc_wht_end,
                 real_t *p_acc_wht_s, real_t *p_acc_wht_e) : context(vertex, num_vertices, start, end, seed)
    {
        this->app_param = param;
        this->prev_vertex = prev;
        this->prev_adj_start = p_adj_s;
        this->prev_adj_end = p_adj_e;
        this->acc_weight_start = acc_wht_start;
        this->acc_weight_end = acc_wht_end;
        this->prev_acc_weight_start = p_acc_wht_s;
        this->prev_acc_weight_end = p_acc_wht_e;
        this->prob = nullptr;
        this->alias = nullptr;
        this->gamma_factor = app_param.gamma / app_param.delta;
        this->beta_factor  = app_param.beta / app_param.delta;
        this->alpha_factor  = app_param.alpha / app_param.delta;
    }

    walk_context(second_order_param_t param, vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
                 vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, real_t *acc_wht_start, real_t *acc_wht_end,
                 real_t *p_acc_wht_s, real_t *p_acc_wht_e,  real_t *pb, vid_t *alias_index) : context(vertex, num_vertices, start, end, seed)
    {
        this->app_param = param;
        this->prev_vertex = prev;
        this->prev_adj_start = p_adj_s;
        this->prev_adj_end = p_adj_e;
        this->acc_weight_start = acc_wht_start;
        this->acc_weight_end = acc_wht_end;
        this->prev_acc_weight_start = p_acc_wht_s;
        this->prev_acc_weight_end = p_acc_wht_e;
        this->prob = pb;
        this->alias = alias_index;
        this->gamma_factor = app_param.gamma / app_param.delta;
        this->beta_factor  = app_param.beta / app_param.delta;
        this->alpha_factor  = app_param.alpha / app_param.delta;
    }

    void query_neigbors_weight(std::vector<real_t> &adj_weights) const
    {
        size_t deg = static_cast<size_t>(adj_end - adj_start);
        adj_weights.resize(deg);
        std::unordered_map<vid_t, size_t> neighbor_index;
        size_t prev_deg = static_cast<size_t>(prev_adj_end - prev_adj_start);
        for(size_t index = 0; index < prev_deg; ++index) neighbor_index[*(prev_adj_start + index)] = index;

        for (size_t index = 0; index < deg; ++index)
        {
            if(*(adj_start + index) == prev_vertex) {
                if (index == 0)
                    adj_weights[index] = (*acc_weight_start) * app_param.gamma;
                else
                    adj_weights[index] = (*(acc_weight_start + index) - *(acc_weight_start + index - 1)) * app_param.gamma;
            }
            else if (neighbor_index.find(*(adj_start + index)) != neighbor_index.end())
            {
                real_t cur_w = 0.0, prev_w = 0.0;
                if (index == 0) cur_w = *acc_weight_start;
                else cur_w = *(acc_weight_start + index) - *(acc_weight_start + index - 1);

                size_t prev_index = neighbor_index[*(adj_start + index)];
                if (prev_index == 0) prev_w = *prev_acc_weight_start;
                else {
                    prev_w = *(prev_acc_weight_start + prev_index) - *(prev_acc_weight_start + prev_index - 1);
                }
                adj_weights[index] = app_param.alpha * prev_w + app_param.beta * cur_w;
            }
            else
            {
                if (index == 0)
                    adj_weights[index] = (*acc_weight_start) * app_param.delta;
                else
                    adj_weights[index] = (*(acc_weight_start + index) - *(acc_weight_start + index - 1)) * app_param.delta;
            }
        }
    }

    void query_comm_neigbors_weight(std::vector<real_t> &adj_weights, std::vector<vid_t> &comm_neighbors, real_t &total_weight) const
    {
        size_t deg = static_cast<size_t>(adj_end - adj_start);
        adj_weights.resize(deg);
        comm_neighbors.resize(deg);
        std::unordered_map<vid_t, size_t> neighbor_index;
        size_t prev_deg = static_cast<size_t>(prev_adj_end - prev_adj_start);
        for(size_t index = 0; index < prev_deg; ++index) neighbor_index[*(prev_adj_start + index)] = index;

        real_t comm_weight_sum = 0;
        size_t off = 0;
        for (size_t index = 0; index < deg; ++index)
        {
            if(*(adj_start + index) == prev_vertex) {
                real_t cur_w = 0.0;
                if (index == 0) cur_w = *acc_weight_start;
                else cur_w = *(acc_weight_start + index) - *(acc_weight_start + index - 1);
                real_t tmp_wht = (this->gamma_factor - 1.0) * cur_w;
                if(tmp_wht > 0.0) {
                    comm_weight_sum += tmp_wht;
                    // adj_weights.push_back(comm_weight_sum);
                    // comm_neighbors.push_back(*(adj_start + index));
                    adj_weights[off] = comm_weight_sum;
                    comm_neighbors[off++] = *(adj_start + index);
                }
            }
            else if (neighbor_index.find(*(adj_start + index)) != neighbor_index.end())
            {
                real_t prev_w = 0.0, cur_w = 0.0;
                if (index == 0) cur_w = *acc_weight_start;
                else cur_w = *(acc_weight_start + index) - *(acc_weight_start + index - 1);

                size_t prev_index = neighbor_index[*(adj_start + index)];
                if (prev_index == 0) prev_w = *prev_acc_weight_start;
                else {
                    prev_w = *(prev_acc_weight_start + prev_index) - *(prev_acc_weight_start + prev_index - 1);
                }
                comm_weight_sum += this->alpha_factor * prev_w + (this->beta_factor - 1.0) * cur_w;
                // adj_weights.push_back(comm_weight_sum);
                // comm_neighbors.push_back(*(adj_start + index));
                adj_weights[off] = comm_weight_sum;
                comm_neighbors[off++] = *(adj_start + index);
            }
        }
        adj_weights.resize(off);
        comm_neighbors.resize(off);
        total_weight = comm_weight_sum + (*(acc_weight_start + (deg - 1)));
    }

//    void query_comm_neigbors_weight(std::vector<real_t> &adj_weights, std::vector<vid_t> &comm_neighbors, real_t &total_weight) const
//    {
//        size_t deg = static_cast<size_t>(adj_end - adj_start);
//        std::unordered_map<vid_t, size_t> neighbor_index;
//        size_t prev_deg = static_cast<size_t>(prev_adj_end - prev_adj_start);
//        for(size_t index = 0; index < prev_deg; ++index) neighbor_index[*(prev_adj_start + index)] = index;
//
//        real_t comm_weight_sum = 0;
//        for (size_t index = 0; index < deg; ++index)
//        {
//            if(*(adj_start + index) == prev_vertex) {
//                real_t cur_w = 0.0;
//                if (index == 0) cur_w = *acc_weight_start;
//                else cur_w = *(acc_weight_start + index) - *(acc_weight_start + index - 1);
//                real_t tmp_wht = (this->gamma_factor - 1.0) * cur_w;
//                if(tmp_wht > 0.0) {
//                    comm_weight_sum += tmp_wht;
//                    adj_weights.push_back(comm_weight_sum);
//                    comm_neighbors.push_back(*(adj_start + index));
//                }
//            }
//            else if (neighbor_index.find(*(adj_start + index)) != neighbor_index.end())
//            {
//                real_t prev_w = 0.0, cur_w = 0.0;
//                if (index == 0) cur_w = *acc_weight_start;
//                else cur_w = *(acc_weight_start + index) - *(acc_weight_start + index - 1);
//
//                size_t prev_index = neighbor_index[*(adj_start + index)];
//                if (prev_index == 0) prev_w = *prev_acc_weight_start;
//                else {
//                    prev_w = *(prev_acc_weight_start + prev_index) - *(prev_acc_weight_start + prev_index - 1);
//                }
//                comm_weight_sum += this->alpha_factor * prev_w + (this->beta_factor - 1.0) * cur_w;
//                adj_weights.push_back(comm_weight_sum);
//                comm_neighbors.push_back(*(adj_start + index));
//            }
//        }
//        total_weight = comm_weight_sum + (*(acc_weight_start + (deg - 1)));
//    }

    real_t query_pivot_weight(const std::vector<real_t> &adj_weights, const std::vector<vid_t> &comm_neighbors, eid_t pivot) const
    {
        size_t pos = std::upper_bound(comm_neighbors.begin(), comm_neighbors.end(), *(adj_start + pivot)) - comm_neighbors.begin();
        real_t pivot_weight = 0.0;
        if (pos > 0)
        {
            pivot_weight += adj_weights[pos - 1];
        }
        pivot_weight += *(acc_weight_start + pivot);
        return pivot_weight;
    }
};

// template <CtxType ctx_type>
// class node2vec_context : public walk_context<ctx_type>
// {
// public:
//     node2vec_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
//                      vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e) : walk_context<ctx_type>(vertex, num_vertices, start, end, seed)
//     {
//         logstream(LOG_ERROR) << "you should use a specialized node2vec context" << std::endl;
//     }

// };

// template <>
// class node2vec_context<SECONDORDERCTX> : public walk_context<SECONDORDERCTX>
// {
// public:
//     float p, q;
//     node2vec_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
//                      vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, float param_p, float param_q) : walk_context<SECONDORDERCTX>(vertex, num_vertices, start, end, seed, prev, p_adj_s, p_adj_e)
//     {
//         this->p = param_p;
//         this->q = param_q;
//     }

//     void query_neigbors_weight(std::vector<real_t> &adj_weights)
//     {
//         size_t deg = static_cast<size_t>(adj_end - adj_start);
//         adj_weights.resize(deg);
//         std::unordered_set<vid_t> prev_neighbors(prev_adj_start, prev_adj_end);
//         for(size_t index = 0; index < deg; ++index) {
//             if(*(adj_start + index) == prev_vertex) {
//                 adj_weights[index] = 1.0;
//             }else if(prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end()) {
//                 adj_weights[index] = 1.0 / p;
//             }else {
//                 adj_weights[index] = 1.0 / q;
//             }
//         }
//     }

//     void query_comm_neigbors_weight(std::vector<real_t> &adj_weights, std::vector<vid_t> &comm_neighbors, real_t &total_weight)
//     {
//         size_t deg = static_cast<size_t>(adj_end - adj_start);
//         std::unordered_set<vid_t> prev_neighbors(prev_adj_start, prev_adj_end);
//         real_t comm_weight_sum = 0;
//         for(size_t index = 0; index < deg; ++index) {
//             if(*(adj_start + index) == prev_vertex) {
//                 comm_weight_sum += 1.0;
//                 adj_weights.push_back(comm_weight_sum);
//                 comm_neighbors.push_back(*(adj_start + index));
//             }else if(prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end()) {
//                 comm_weight_sum += 1.0 / p;
//                 adj_weights.push_back(comm_weight_sum);
//                 comm_neighbors.push_back(*(adj_start + index));
//             }
//         }
//         total_weight = comm_weight_sum + (deg - comm_neighbors.size()) / q;
//     }

//     real_t query_pivot_weight(const std::vector<real_t> &adj_weights, const std::vector<vid_t> &comm_neighbors, eid_t pivot)
//     {
//         size_t pos = std::upper_bound(comm_neighbors.begin(), comm_neighbors.end(), *(adj_start + pivot)) - comm_neighbors.begin();
//         real_t pivot_weight = 0.0;
//         if(pos > 0) {
//             pivot_weight += adj_weights[pos - 1];
//         }
//         pivot_weight += (pivot + 1 - pos) / q;
//         return pivot_weight;
//     }
// };

// template <>
// class node2vec_context<BIASEDSECONDORDERCTX> : public walk_context<BIASEDSECONDORDERCTX>
// {
// public:
//     float p, q;
//     node2vec_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
//                      vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, real_t *wht_start, real_t *wht_end, float param_p, float param_q) : walk_context<BIASEDSECONDORDERCTX>(vertex, num_vertices, start, end, seed, prev, p_adj_s, p_adj_e, wht_start, wht_end)
//     {
//         this->p = param_p;
//         this->q = param_q;
//     }

//     void query_neigbors_weight(std::vector<real_t> &adj_weights)
//     {
//         size_t deg = static_cast<size_t>(adj_end - adj_start);
//         adj_weights.resize(deg);
//         std::unordered_set<vid_t> prev_neighbors(prev_adj_start, prev_adj_end);
//         for(size_t index = 0; index < deg; ++index) {
//             if(*(adj_start + index) == prev_vertex) {
//                 adj_weights[index] = *(weight_start + index);
//             }else if(prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end()) {
//                 adj_weights[index] = *(weight_start + index) / p;
//             }else {
//                 adj_weights[index] = *(weight_start + index) / q;
//             }
//         }
//     }
// };

// template <>
// class node2vec_context<BIASEDACCSECONDORDERCTX> : public walk_context<BIASEDACCSECONDORDERCTX>
// {
// public:
//     float p, q;
//     node2vec_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
//                      vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, real_t *acc_wht_start, real_t *acc_wht_end,
//                      float param_p, float param_q) : walk_context<BIASEDACCSECONDORDERCTX>(vertex, num_vertices, start, end, seed, prev, p_adj_s, p_adj_e, acc_wht_start, acc_wht_end)
//     {
//         this->p = param_p;
//         this->q = param_q;
//     }

//     node2vec_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
//                      vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, real_t *acc_wht_start, real_t *acc_wht_end,
//                      real_t *pb, vid_t *alias_index, float param_p, float param_q) : walk_context<BIASEDACCSECONDORDERCTX>(vertex, num_vertices, start, end, seed, prev, p_adj_s, p_adj_e, acc_wht_start, acc_wht_end, pb, alias_index)
//     {
//         this->p = param_p;
//         this->q = param_q;
//     }

//     void query_neigbors_weight(std::vector<real_t> &adj_weights)
//     {
//         size_t deg = static_cast<size_t>(adj_end - adj_start);
//         adj_weights.resize(deg);
//         std::unordered_set<vid_t> prev_neighbors(prev_adj_start, prev_adj_end);
//         for(size_t index = 0; index < deg; ++index) {
//             if(*(adj_start + index) == prev_vertex) {
//                 if(index == 0) adj_weights[index] = *acc_weight_start;
//                 else adj_weights[index] = *(acc_weight_start + index) - *(acc_weight_start + index - 1);
//             }else if(prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end()) {
//                 if(index == 0) adj_weights[index] = (*acc_weight_start) / p;
//                 else adj_weights[index] = (*(acc_weight_start + index) - *(acc_weight_start + index - 1)) / p;
//             }else {
//                 if(index == 0) adj_weights[index] = (*acc_weight_start) / q;
//                 else adj_weights[index] = (*(acc_weight_start + index) - *(acc_weight_start + index - 1)) / q;
//             }
//         }
//     }

//     void query_comm_neigbors_weight(std::vector<real_t> &adj_weights, std::vector<vid_t> &comm_neighbors, real_t &total_weight)
//     {
//         size_t deg = static_cast<size_t>(adj_end - adj_start);
//         std::unordered_set<vid_t> prev_neighbors(prev_adj_start, prev_adj_end);
//         real_t comm_weight_sum = 0;
//         for(size_t index = 0; index < deg; ++index) {
//             if(*(adj_start + index) == prev_vertex) {
//                 if(index == 0) comm_weight_sum += (q - 1.0) * (*acc_weight_start);
//                 else comm_weight_sum += (q - 1.0) * (*(acc_weight_start + index) - *(acc_weight_start + index - 1));
//                 adj_weights.push_back(comm_weight_sum);
//                 comm_neighbors.push_back(*(adj_start + index));
//             }else if(prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end()) {
//                 if(index == 0) comm_weight_sum += (q / p - 1.0) * (*acc_weight_start);
//                 else comm_weight_sum += (q / p - 1.0) * (*(acc_weight_start + index) - *(acc_weight_start + index - 1));
//                 adj_weights.push_back(comm_weight_sum);
//                 comm_neighbors.push_back(*(adj_start + index));
//             }
//         }
//         total_weight = comm_weight_sum + (*(acc_weight_start + (deg - 1)));
//     }

//     real_t query_pivot_weight(const std::vector<real_t> &adj_weights, const std::vector<vid_t> &comm_neighbors, eid_t pivot)
//     {
//         size_t pos = std::upper_bound(comm_neighbors.begin(), comm_neighbors.end(), *(adj_start + pivot)) - comm_neighbors.begin();
//         real_t pivot_weight = 0.0;
//         if(pos > 0) {
//             pivot_weight += adj_weights[pos - 1];
//         }
//         pivot_weight += *(acc_weight_start + pivot);
//         return pivot_weight;
//     }
// };


/* ------------------------ autoregressive context start --------------------------------- */

// template <CtxType ctx_type>
// class autoregressive_context : public walk_context<ctx_type>
// {
// public:
//     autoregressive_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
//                            vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e) : walk_context<ctx_type>(vertex, num_vertices, start, end, seed)
//     {
//         logstream(LOG_ERROR) << "you should use a specialized node2vec context" << std::endl;
//     }
// };

// template <>
// class autoregressive_context<SECONDORDERCTX> : public walk_context<SECONDORDERCTX>
// {
// public:
//     real_t alpha;
//     autoregressive_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
//                            vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, float param_alpha) : walk_context<SECONDORDERCTX>(vertex, num_vertices, start, end, seed, prev, p_adj_s, p_adj_e)
//     {
//         this->alpha = param_alpha;
//     }

//     void query_neigbors_weight(std::vector<real_t> &adj_weights)
//     {
//         size_t deg = static_cast<size_t>(adj_end - adj_start);
//         adj_weights.resize(deg);
//         std::unordered_set<vid_t> prev_neighbors(prev_adj_start, prev_adj_end);
//         for (size_t index = 0; index < deg; ++index)
//         {
//             if (prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end())
//             {
//                 adj_weights[index] = (1.0 - alpha);
//             }
//             else
//             {
//                 adj_weights[index] = alpha;
//             }
//         }
//     }

//     void query_comm_neigbors_weight(std::vector<real_t> &adj_weights, std::vector<vid_t> &comm_neighbors, real_t &total_weight)
//     {
//         size_t deg = static_cast<size_t>(adj_end - adj_start);
//         std::unordered_set<vid_t> prev_neighbors(prev_adj_start, prev_adj_end);
//         real_t comm_weight_sum = 0;
//         for (size_t index = 0; index < deg; ++index)
//         {
//             if (prev_neighbors.find(*(adj_start + index)) != prev_neighbors.end())
//             {
//                 comm_weight_sum += 1.0;
//                 adj_weights.push_back(comm_weight_sum);
//                 comm_neighbors.push_back(*(adj_start + index));
//             }
//         }
//         total_weight = comm_weight_sum + (deg - comm_neighbors.size()) * (1.0 - alpha);
//     }

//     real_t query_pivot_weight(const std::vector<real_t> &adj_weights, const std::vector<vid_t> &comm_neighbors, eid_t pivot)
//     {
//         size_t pos = std::upper_bound(comm_neighbors.begin(), comm_neighbors.end(), *(adj_start + pivot)) - comm_neighbors.begin();
//         real_t pivot_weight = 0.0;
//         if (pos > 0)
//         {
//             pivot_weight += adj_weights[pos - 1];
//         }
//         pivot_weight += (pivot + 1 - pos) * (1.0 - alpha);
//         return pivot_weight;
//     }
// };

// template <>
// class autoregressive_context<BIASEDSECONDORDERCTX> : public walk_context<BIASEDSECONDORDERCTX>
// {
// public:
//     real_t alpha;
//     real_t *prev_weight_start, *prev_weight_end;
//     autoregressive_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
//                            vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, real_t *wht_start, real_t *wht_end,
//                            real_t param_alpha, real_t *p_wht_s, real_t *p_wht_e) : walk_context<BIASEDSECONDORDERCTX>(vertex, num_vertices, start, end, seed, prev, p_adj_s, p_adj_e, wht_start, wht_end)
//     {
//         this->alpha = param_alpha;
//         this->prev_weight_start = p_wht_s;
//         this->prev_weight_end   = p_wht_e;
//     }

//     void query_neigbors_weight(std::vector<real_t> &adj_weights)
//     {
//         size_t deg = static_cast<size_t>(adj_end - adj_start);
//         adj_weights.resize(deg);
//         std::unordered_map<vid_t, size_t> neighbor_index;
//         size_t prev_deg = static_cast<size_t>(prev_adj_end - prev_adj_start);
//         for(size_t index = 0; index < prev_deg; ++index) neighbor_index[*(prev_adj_start + index)] = index;

//         for (size_t index = 0; index < deg; ++index)
//         {
//             if (neighbor_index.find(*(adj_start + index)) != neighbor_index.end())
//             {
//                 adj_weights[index] = *(weight_start + index) * (1.0 - alpha) + *(prev_weight_start + neighbor_index[*(adj_start + index)]) * alpha;
//             }
//             else
//             {
//                 adj_weights[index] = *(weight_start + index) * (1.0 - alpha);
//             }
//         }
//     }
// };

// template <>
// class autoregressive_context<BIASEDACCSECONDORDERCTX> : public walk_context<BIASEDACCSECONDORDERCTX>
// {
// public:
//     real_t alpha;
//     real_t *prev_weight_start, *prev_weight_end;
//     autoregressive_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
//                            vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, real_t *acc_wht_start, real_t *acc_wht_end,
//                            real_t param_alpha, real_t *p_wht_s, real_t *p_wht_e) : walk_context<BIASEDACCSECONDORDERCTX>(vertex, num_vertices, start, end, seed, prev, p_adj_s, p_adj_e, acc_wht_start, acc_wht_end)
//     {
//         this->alpha = param_alpha;
//         this->prev_weight_start = p_wht_s;
//         this->prev_weight_end = p_wht_e;
//     }

//     autoregressive_context(vid_t vertex, vid_t num_vertices, vid_t *start, vid_t *end, unsigned *seed,
//                            vid_t prev, vid_t *p_adj_s, vid_t *p_adj_e, real_t *acc_wht_start, real_t *acc_wht_end,
//                            real_t *pb, vid_t *alias_index, real_t param_alpha, real_t *p_wht_s, real_t *p_wht_e) : walk_context<BIASEDACCSECONDORDERCTX>(vertex, num_vertices, start, end, seed, prev, p_adj_s, p_adj_e, acc_wht_start, acc_wht_end, pb, alias_index)
//     {
//         this->alpha = param_alpha;
//         this->prev_weight_start = p_wht_s;
//         this->prev_weight_end = p_wht_e;
//     }

//     void query_neigbors_weight(std::vector<real_t> &adj_weights)
//     {
//         size_t deg = static_cast<size_t>(adj_end - adj_start);
//         adj_weights.resize(deg);
//         std::unordered_map<vid_t, size_t> neighbor_index;
//         size_t prev_deg = static_cast<size_t>(prev_adj_end - prev_adj_start);
//         for(size_t index = 0; index < prev_deg; ++index) neighbor_index[*(prev_adj_start + index)] = index;

//         for (size_t index = 0; index < deg; ++index)
//         {
//             if (neighbor_index.find(*(adj_start + index)) != neighbor_index.end())
//             {
//                 real_t cur_w = 0.0, prev_w = 0.0;
//                 if (index == 0) cur_w = *acc_weight_start;
//                 else cur_w = *(acc_weight_start + index) - *(acc_weight_start + index - 1);

//                 size_t prev_index = neighbor_index[*(adj_start + index)];
//                 if (prev_index == 0) prev_w = *prev_weight_start;
//                 else {
//                     prev_w = *(prev_weight_start + prev_index) - *(prev_weight_start + prev_index - 1);
//                 }
//                 adj_weights[index] = (1.0 - alpha) * cur_w + alpha * prev_w;
//             }
//             else
//             {
//                 if (index == 0)
//                     adj_weights[index] = (*acc_weight_start) * (1.0 - alpha);
//                 else
//                     adj_weights[index] = (*(acc_weight_start + index) - *(acc_weight_start + index - 1)) * (1.0 - alpha);
//             }
//         }
//     }

//     void query_comm_neigbors_weight(std::vector<real_t> &adj_weights, std::vector<vid_t> &comm_neighbors, real_t &total_weight)
//     {
//         size_t deg = static_cast<size_t>(adj_end - adj_start);
//         std::unordered_map<vid_t, size_t> neighbor_index;
//         size_t prev_deg = static_cast<size_t>(prev_adj_end - prev_adj_start);
//         for(size_t index = 0; index < prev_deg; ++index) neighbor_index[*(prev_adj_start + index)] = index;

//         real_t comm_weight_sum = 0;
//         for (size_t index = 0; index < deg; ++index)
//         {
//             if (neighbor_index.find(*(adj_start + index)) != neighbor_index.end())
//             {
//                 real_t prev_w = 0.0;
//                 size_t prev_index = neighbor_index[*(adj_start + index)];
//                 if (prev_index == 0) prev_w = *prev_weight_start;
//                 else {
//                     prev_w = *(prev_weight_start + prev_index) - *(prev_weight_start + prev_index - 1);
//                 }
//                 comm_weight_sum += alpha * prev_w / (1.0 - alpha);
//                 adj_weights.push_back(comm_weight_sum);
//                 comm_neighbors.push_back(*(adj_start + index));
//             }
//         }
//         total_weight = comm_weight_sum + (*(acc_weight_start + (deg - 1)));
//     }

//     real_t query_pivot_weight(const std::vector<real_t> &adj_weights, const std::vector<vid_t> &comm_neighbors, eid_t pivot)
//     {
//         size_t pos = std::upper_bound(comm_neighbors.begin(), comm_neighbors.end(), *(adj_start + pivot)) - comm_neighbors.begin();
//         real_t pivot_weight = 0.0;
//         if (pos > 0)
//         {
//             pivot_weight += adj_weights[pos - 1];
//         }
//         pivot_weight += *(acc_weight_start + pivot);
//         return pivot_weight;
//     }
// };

/* ------------------------ autoregressive context start --------------------------------- */
#endif
