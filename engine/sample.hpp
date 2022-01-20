#ifndef _GRAPH_SAMPLE_H_
#define _GRAPH_SAMPLE_H_

#include <cstdlib>
#include <ctime>
#include <vector>
#include <algorithm>
#include <cassert>
#include <numeric>
#include <iostream>
#include <limits>
#include "api/types.hpp"
#include "metrics/metrics.hpp"
#include "context.hpp"

const size_t INF = std::numeric_limits<size_t>::max();

template <typename iterator_type>
size_t naive_sample_impl(const iterator_type &first, const iterator_type &last) 
{
    size_t n = static_cast<size_t>(last - first);
    return rand() % n;
}

template <typename iterator_type>
size_t its_sample_impl(const iterator_type &first, const iterator_type &last)
{
    size_t n = static_cast<size_t>(last - first);
    std::vector<real_t> prefix_sum(n + 1, 0.0);
    int idx = 1;
    for (iterator_type iter = first; iter != last; ++iter)
    {
        prefix_sum[idx] = prefix_sum[idx - 1] + *iter;
        idx++;
    }

    real_t diff = prefix_sum[n];
    real_t random = (real_t)rand() / (real_t)RAND_MAX;
    real_t rand_val = diff * random;
    size_t pos = std::upper_bound(prefix_sum.begin(), prefix_sum.end(), rand_val) - prefix_sum.begin();
    assert(pos > 0);
#ifdef TEST_SAMPLE
    std::cout << "prefix:";
    for (const auto &s : prefix_sum)
        std::cout << " " << s;
    std::cout << std::endl;
#endif
    return pos - 1;
}

template <typename iterator_type>
size_t alias_sample_impl(const iterator_type &first, const iterator_type &last)
{
    size_t n = static_cast<size_t>(last - first);
    std::vector<real_t> aux_weights(first, last);
    real_t sum = std::accumulate(first, last, 0.0);
    real_t avg = sum / static_cast<real_t>(n);
    std::vector<real_t> prob_table(n), alias_table(n);
    std::vector<size_t> small_table, large_table;
    for (iterator_type iter = first; iter != last; ++iter)
    {
        if (*iter < avg)
            small_table.push_back(iter - first);
        else
            large_table.push_back(iter - first);
    }
    while (!small_table.empty() && !large_table.empty())
    {
        size_t s = small_table.back(), l = large_table.back();
        prob_table[s] = aux_weights[s];
        alias_table[s] = l;
        small_table.pop_back();
        large_table.pop_back();
        real_t r = (aux_weights[s] + aux_weights[l]) - avg;
        aux_weights[l] = r;
        if (r < avg)
            small_table.push_back(l);
        else
            large_table.push_back(l);
    }

    while (!large_table.empty())
    {
        size_t l = large_table.back();
        large_table.pop_back();
        prob_table[l] = avg;
        alias_table[l] = n;
    }

    while (!small_table.empty())
    {
        size_t s = small_table.back();
        small_table.pop_back();
        prob_table[s] = avg;
        alias_table[s] = n;
    }

#ifdef TEST_SAMPLE
    std::cout << "avg: " << avg << std::endl;
    std::cout << "prob table:";
    for (const auto &s : prob_table)
        std::cout << " " << s;
    std::cout << "\nalias table:";
    for (const auto &s : alias_table)
        std::cout << " " << s;
    std::cout << std::endl;
#endif

    real_t rand_val = static_cast<real_t>(rand()) / static_cast<real_t>(RAND_MAX) * avg;
    size_t rand_pos = rand() % n;
    if (rand_val < prob_table[rand_pos])
        return rand_pos;
    else
        return alias_table[rand_pos];
}

template <typename iterator_type>
size_t reject_sample_impl(const iterator_type &first, const iterator_type &last)
{
    size_t n = static_cast<size_t>(last - first);
    real_t pmax = *std::max_element(first, last);
    real_t rand_val = static_cast<real_t>(rand()) / static_cast<real_t>(RAND_MAX) * pmax;
    size_t rand_pos = rand() % n;
    while (rand_val > *(first + rand_pos))
    {
        rand_pos = rand() % n;
        rand_val = static_cast<real_t>(rand()) / static_cast<real_t>(RAND_MAX) * pmax;
    }
    return rand_pos;
}

class sample_t {
public:
    bool use_alias;
    bool use_acc_weight;
    sample_t() : use_alias(false), use_acc_weight(false) {}
    sample_t(bool acc_weight) : use_alias(false), use_acc_weight(acc_weight) {}
    sample_t(bool alias, bool acc_weight) : use_alias(alias), use_acc_weight(acc_weight) {}
};

class sample_policy_t : public sample_t
{
public:
    sample_policy_t() {}
    sample_policy_t(bool acc_weight) : sample_t(acc_weight) {}
    sample_policy_t(bool alias, bool acc_weight) : sample_t(alias, acc_weight) {}

    virtual size_t sample(const std::vector<real_t>& weights) {
        return INF;
    }
    virtual size_t sample(const real_t* first, const real_t* last) {
        return INF;
    }
    virtual size_t sample(const walk_context<UNBAISEDCONTEXT>& ctx) { 
        return INF; 
    }
    virtual size_t sample(const walk_context<BIASEDCONTEXT>& ctx) { 
        return INF; 
    }
    virtual size_t sample(const walk_context<SECONDORDERCTX>& ctx) { 
        return INF; 
    }
    virtual size_t sample(const walk_context<BIASEDSECONDORDERCTX>& ctx) { 
        return INF; 
    }
    virtual size_t sample(const walk_context<BIASEDACCSECONDORDERCTX>& ctx) { 
        return INF; 
    }
    virtual std::string sample_name() const {
        return "base_sample_policy";
    }
};

/**
 * This method generates a uniform [0, 1] random variable r
*/
class naive_sample_t : public sample_policy_t {
public:
    naive_sample_t() {}

    virtual size_t sample(const std::vector<real_t> &weights)
    {
        return naive_sample_impl(weights.begin(), weights.end());
    }

    virtual size_t sample(const real_t *first, const real_t *last)
    {
        return naive_sample_impl(first, last);
    }
    virtual size_t sample(const walk_context<UNBAISEDCONTEXT> &ctx)
    {
        return naive_sample_impl(ctx.adj_start, ctx.adj_end);
    }
    virtual size_t sample(const walk_context<BIASEDCONTEXT> &ctx)
    {
        return naive_sample_impl(ctx.adj_start, ctx.adj_end);
    }
    virtual size_t sample(const walk_context<SECONDORDERCTX> &ctx)
    {
        return naive_sample_impl(ctx.adj_start, ctx.adj_end);
    }
    virtual size_t sample(const walk_context<BIASEDSECONDORDERCTX> &ctx)
    {
        return naive_sample_impl(ctx.adj_start, ctx.adj_end);
    }
    virtual size_t sample(const walk_context<BIASEDACCSECONDORDERCTX> &ctx)
    {
        return naive_sample_impl(ctx.adj_start, ctx.adj_end);
    }
    std::string sample_name() const {
        return "naive_sample";
    }
};

/** This method first computes the cumulative distribution of weights,
 * then generates a random number x between [0, P_sum),
 * finally uses a binary search to find the smallest index i such that x < P_i
*/
class its_sample_t : public sample_policy_t {
public:
    its_sample_t() {}

    virtual size_t sample(const std::vector<real_t> &weights)
    {
        return its_sample_impl(weights.begin(), weights.end());
    }

    virtual size_t sample(const real_t *first, const real_t *last)
    {
        return its_sample_impl(first, last);
    }
    virtual size_t sample(const walk_context<UNBAISEDCONTEXT> &ctx)
    {
        return naive_sample_impl(ctx.adj_start, ctx.adj_end);
    }
    virtual size_t sample(const walk_context<BIASEDCONTEXT> &ctx)
    {
        return its_sample_impl(ctx.weight_start, ctx.weight_end);
    }
    virtual size_t sample(const walk_context<SECONDORDERCTX> &ctx)
    {
        std::vector<real_t> adj_weights;
        ctx.query_neigbors_weight(adj_weights);
        logstream(LOG_DEBUG) << "its_sample : " << ctx.cur_vertex << std::endl;
        return its_sample_impl(adj_weights.begin(), adj_weights.end());
    }
    virtual size_t sample(const walk_context<BIASEDSECONDORDERCTX> &ctx)
    {
        std::vector<real_t> adj_weights;
        ctx.query_neigbors_weight(adj_weights);
        return its_sample_impl(adj_weights.begin(), adj_weights.end());
    }
    virtual size_t sample(const walk_context<BIASEDACCSECONDORDERCTX> &ctx)
    {
        std::vector<real_t> adj_weights;
        ctx.query_neigbors_weight(adj_weights);
        return its_sample_impl(adj_weights.begin(), adj_weights.end());
    }
    std::string sample_name() const {
        return "its_sample";
    }
};

/**
 * This method builds two tables: the probability table H, and the alias table A.
 * The generation phase generates a uniform integer x and retrieves H[x].
 * Next, it generates a uniform real number y in [0, 1), if y < H[x], then pick the first, otherwise select the second.
 *
 * for more details, pelease refer to : https://www.keithschwarz.com/darts-dice-coins/
 */
class alias_sample_t : public sample_policy_t {
public:
    alias_sample_t() {}

    virtual size_t sample(const std::vector<real_t> &weights)
    {
        return alias_sample_impl(weights.begin(), weights.end());
    }

    virtual size_t sample(const real_t *first, const real_t *last)
    {
        return alias_sample_impl(first, last);
    }

    virtual size_t sample(const walk_context<UNBAISEDCONTEXT> &ctx)
    {
        return naive_sample_impl(ctx.adj_start, ctx.adj_end);
    }

    virtual size_t sample(const walk_context<BIASEDCONTEXT> &ctx)
    {
        return alias_sample_impl(ctx.weight_start, ctx.weight_end);
    }

    virtual size_t sample(const walk_context<SECONDORDERCTX> &ctx)
    {
        std::vector<real_t> adj_weights;
        ctx.query_neigbors_weight(adj_weights);
        logstream(LOG_DEBUG) << "alias_sample : " << ctx.cur_vertex << std::endl;
        return alias_sample_impl(adj_weights.begin(), adj_weights.end());
    }

    virtual size_t sample(const walk_context<BIASEDSECONDORDERCTX> &ctx)
    {
        std::vector<real_t> adj_weights;
        ctx.query_neigbors_weight(adj_weights);
        return alias_sample_impl(adj_weights.begin(), adj_weights.end());
    }

    virtual size_t sample(const walk_context<BIASEDACCSECONDORDERCTX> &ctx)
    {
        std::vector<real_t> adj_weights;
        ctx.query_neigbors_weight(adj_weights);
        return alias_sample_impl(adj_weights.begin(), adj_weights.end());
    }

    std::string sample_name() const {
        return "alias_sample";
    }
};

/**
 * This method can be viewed as throwing a dart on a rectangle dartboard
 * util hitting the target area. It follows the following steps:
 * - generate a uniform number x and a uniform real number y in [0, p_max)
 * - if y < p_x then select the target area
 * - else repeat step 1)
*/
class reject_sample_t : public sample_policy_t {
public:
    reject_sample_t() {}

    virtual size_t sample(const std::vector<real_t> &weights)
    {
        return reject_sample_impl(weights.begin(), weights.end());
    }

    virtual size_t sample(const real_t *first, const real_t *last)
    {
        return reject_sample_impl(first, last);
    }

    virtual size_t sample(const walk_context<UNBAISEDCONTEXT> &ctx)
    {
        return naive_sample_impl(ctx.adj_start, ctx.adj_end);
    }

    virtual size_t sample(const walk_context<BIASEDCONTEXT> &ctx)
    {
        return reject_sample_impl(ctx.weight_start, ctx.weight_end);
    }

    virtual size_t sample(const walk_context<SECONDORDERCTX> &ctx)
    {
        std::vector<real_t> adj_weights;
        ctx.query_neigbors_weight(adj_weights);
        logstream(LOG_DEBUG) << "reject_sample : " << ctx.cur_vertex << std::endl;
        return reject_sample_impl(adj_weights.begin(), adj_weights.end());
    }

    virtual size_t sample(const walk_context<BIASEDSECONDORDERCTX> &ctx)
    {
        std::vector<real_t> adj_weights;
        ctx.query_neigbors_weight(adj_weights);
        return reject_sample_impl(adj_weights.begin(), adj_weights.end());
    }

    virtual size_t sample(const walk_context<BIASEDACCSECONDORDERCTX> &ctx)
    {
        std::vector<real_t> adj_weights;
        ctx.query_neigbors_weight(adj_weights);
        return reject_sample_impl(adj_weights.begin(), adj_weights.end());
    }

    std::string sample_name() const {
        return "reject_sample";
    }
};

/**
 * @brief the second-order soop-t sample follows the idea of SOOP paper
 * 
 */
// class second_order_soopt_sample_t : public sample_t {
// public:
//     second_order_soopt_sample_t() : sample_t(true) { }

//     template <typename ContextType>
//     vid_t sample(ContextType &ctx)
//     {
//         eid_t deg = (size_t)(ctx.adj_end - ctx.adj_start);
//         std::vector<real_t> adj_weights;
//         std::vector<vid_t> comm_neighbors;
//         real_t total_weights = 0.0;
//         ctx.query_comm_neigbors_weight(adj_weights, comm_neighbors, total_weights);
//         eid_t low = 0, high = deg - 1;
//         real_t randval = static_cast<real_t>(rand()) / static_cast<real_t>(RAND_MAX) * total_weights;
//         while (low < high) {
//             eid_t mid = low + (high - low + 1) / 2;
//             real_t pivot_weight = ctx.query_pivot_weight(adj_weights, comm_neighbors, mid);
//             if(pivot_weight > randval) high = mid - 1;
//             else low = mid;
//         }
//         return *(ctx.adj_start + low);
//     }

//     std::string sample_name() { return "second_order_soopt_sample_t"; }
// };

class second_order_soopt_sample_t : public sample_policy_t {
public:
    second_order_soopt_sample_t() : sample_policy_t(true) { }

    virtual size_t sample(const std::vector<real_t> &weights)
    {
        return INF;
    }

    virtual size_t sample(const real_t *first, const real_t *last)
    {
        return INF;
    }

    virtual size_t sample(const walk_context<UNBAISEDCONTEXT> &ctx)
    {
        return INF;
    }

    virtual size_t sample(const walk_context<BIASEDCONTEXT> &ctx)
    {
        return INF;
    }

    virtual size_t sample(const walk_context<SECONDORDERCTX> &ctx)
    {
        eid_t deg = (size_t)(ctx.adj_end - ctx.adj_start);
        std::vector<real_t> adj_weights;
        std::vector<vid_t> comm_neighbors;
        real_t total_weights = 0.0;
        ctx.query_comm_neigbors_weight(adj_weights, comm_neighbors, total_weights);
        eid_t low = 0, high = deg - 1;
        real_t randval = static_cast<real_t>(rand()) / static_cast<real_t>(RAND_MAX) * total_weights;
        while (low < high)
        {
            eid_t mid = low + (high - low + 1) / 2;
            real_t pivot_weight = ctx.query_pivot_weight(adj_weights, comm_neighbors, mid);
            if (pivot_weight > randval)
                high = mid - 1;
            else
                low = mid;
        }
        logstream(LOG_DEBUG) << "soopt_sample : " << ctx.cur_vertex << std::endl;
        return *(ctx.adj_start + low);
    }

    virtual size_t sample(const walk_context<BIASEDSECONDORDERCTX> &ctx)
    {
        return INF;
    }

    virtual size_t sample(const walk_context<BIASEDACCSECONDORDERCTX> &ctx)
    {
        eid_t deg = (size_t)(ctx.adj_end - ctx.adj_start);
        std::vector<real_t> adj_weights;
        std::vector<vid_t> comm_neighbors;
        real_t total_weights = 0.0;
        ctx.query_comm_neigbors_weight(adj_weights, comm_neighbors, total_weights);
        eid_t low = 0, high = deg - 1;
        real_t randval = static_cast<real_t>(rand()) / static_cast<real_t>(RAND_MAX) * total_weights;
        while (low < high)
        {
            eid_t mid = low + (high - low + 1) / 2;
            real_t pivot_weight = ctx.query_pivot_weight(adj_weights, comm_neighbors, mid);
            if (pivot_weight > randval)
                high = mid - 1;
            else
                low = mid;
        }
        return *(ctx.adj_start + low);
    }

    virtual std::string sample_name() const
    {
        return "second_order_soopt_sample_t";
    }
};

/**
 * @brief the secon-order optimized alias sample method
 * 
 */
// class second_order_opt_alias_sample_t : public sample_t {
// public:
//     second_order_opt_alias_sample_t() : sample_t(true, true) { }

//     template <typename ContextType>
//     vid_t sample(ContextType &ctx)
//     {
//         eid_t deg = (size_t)(ctx.adj_end - ctx.adj_start);
//         std::vector<real_t> adj_weights;
//         std::vector<vid_t> comm_neighbors;
//         real_t total_weights = 0.0;
//         ctx.query_comm_neigbors_weight(adj_weights, comm_neighbors, total_weights);
//         real_t old_weights = *(ctx.acc_weight_start + (deg - 1));
//         real_t randval = static_cast<real_t>(rand()) / static_cast<real_t>(RAND_MAX) * total_weights;
//         size_t rand_pos = rand_r(ctx.local_seed) % deg;
//         vid_t target_vertex = 0;
//         if(randval < ctx.prob[rand_pos]) target_vertex = *(ctx.adj_start + rand_pos);
//         else if(randval < old_weights) target_vertex = *(ctx.adj_start + ctx.alias[rand_pos]);
//         else {
//             assert(adj_weights.size() > 0);
//             randval = static_cast<real_t>(rand()) / static_cast<real_t>(RAND_MAX) * adj_weights.back();
//             size_t pos = std::upper_bound(adj_weights.begin(), adj_weights.end(), randval) - adj_weights.begin();
//             target_vertex = comm_neighbors[pos];
//         }
//         return target_vertex;
//     }

//     std::string sample_name() { return "second_order_opt_alias_sample_t"; }
// };

class second_order_opt_alias_sample_t : public sample_policy_t {
public:
    second_order_opt_alias_sample_t() : sample_policy_t(true, true) {}

    virtual size_t sample(const std::vector<real_t> &weights)
    {
        return INF;
    }

    virtual size_t sample(const real_t *first, const real_t *last)
    {
        return INF;
    }

    virtual size_t sample(const walk_context<UNBAISEDCONTEXT> &ctx)
    {
        return INF;
    }

    virtual size_t sample(const walk_context<BIASEDCONTEXT> &ctx)
    {
        return INF;
    }

    virtual size_t sample(const walk_context<SECONDORDERCTX> &ctx)
    {
        return INF;
    }

    virtual size_t sample(const walk_context<BIASEDSECONDORDERCTX> &ctx)
    {
        return INF;
    }

    virtual size_t sample(const walk_context<BIASEDACCSECONDORDERCTX> &ctx)
    {
        eid_t deg = (size_t)(ctx.adj_end - ctx.adj_start);
        std::vector<real_t> adj_weights;
        std::vector<vid_t> comm_neighbors;
        real_t total_weights = 0.0;
        ctx.query_comm_neigbors_weight(adj_weights, comm_neighbors, total_weights);
        real_t old_weights = *(ctx.acc_weight_start + (deg - 1));
        real_t randval = static_cast<real_t>(rand()) / static_cast<real_t>(RAND_MAX) * total_weights;
        size_t rand_pos = rand_r(ctx.local_seed) % deg;
        vid_t target_vertex = 0;
        if(randval < ctx.prob[rand_pos]) target_vertex = *(ctx.adj_start + rand_pos);
        else if(randval < old_weights) target_vertex = *(ctx.adj_start + ctx.alias[rand_pos]);
        else {
            assert(adj_weights.size() > 0);
            randval = static_cast<real_t>(rand()) / static_cast<real_t>(RAND_MAX) * adj_weights.back();
            size_t pos = std::upper_bound(adj_weights.begin(), adj_weights.end(), randval) - adj_weights.begin();
            target_vertex = comm_neighbors[pos];
        }
        logstream(LOG_DEBUG) << "opt_alias_sample : " << ctx.cur_vertex << std::endl;
        return target_vertex;
    }

    virtual std::string sample_name() const
    {
        return "second_order_opt_alias_sample_t";
    }
};

// vid_t vertex_sample(const walk_context<UNBAISEDCONTEXT> &ctx, sample_policy_t *sampler) {
//     eid_t deg = (eid_t)(ctx.adj_end - ctx.adj_start);
//     if(deg > 0 && (float)rand_r(ctx.local_seed) / RAND_MAX > ctx.teleport) {
//         vid_t off = (vid_t)rand_r(ctx.local_seed) % deg;
//         return ctx.adj_start[off];
//     } else {
//         return rand_r(ctx.local_seed) % ctx.nvertices;
//     }
// }

// vid_t vertex_sample(const walk_context<BIASEDCONTEXT> &ctx, sample_policy_t *sampler) {
//     eid_t deg = (eid_t)(ctx.adj_end - ctx.adj_start);
//     if(deg > 0 && (float)rand_r(ctx.local_seed) / RAND_MAX > ctx.teleport) {
//         size_t off = sampler->sample(ctx.weight_start, ctx.weight_end);
//         return ctx.adj_start[off];
//     } else {
//         return rand_r(ctx.local_seed) % ctx.nvertices;
//     }
// }

// template<CtxType ctx_type>
// vid_t vertex_sample(node2vec_context<ctx_type> &ctx, sample_policy_t *sampler) {
//     eid_t deg = (eid_t)(ctx.adj_end - ctx.adj_start);
//     if(deg > 0) {
//         std::vector<real_t> adj_weights;
//         ctx.query_neigbors_weight(adj_weights);
//         vid_t off = sampler->sample(adj_weights);
//         return ctx.adj_start[off];
//     } else {
//         return rand_r(ctx.local_seed) % ctx.nvertices;
//     }
// }

// vid_t vertex_sample(node2vec_context<SECONDORDERCTX> &ctx, second_order_soopt_sample_t *sampler) {
//     eid_t deg = (eid_t)(ctx.adj_end - ctx.adj_start);
//     if(deg > 0) {
//         return sampler->sample(ctx);
//     } else {
//         return rand_r(ctx.local_seed) % ctx.nvertices;
//     }
// }

// vid_t vertex_sample(node2vec_context<BIASEDSECONDORDERCTX> &ctx, second_order_soopt_sample_t *sampler) {
//     logstream(LOG_ERROR) << "can't deal with SECONDORDERCTX second-order random walk" << std::endl;
//     return 0;
// }

// vid_t vertex_sample(node2vec_context<BIASEDACCSECONDORDERCTX> &ctx, second_order_soopt_sample_t *sampler) {
//     eid_t deg = (eid_t)(ctx.adj_end - ctx.adj_start);
//     if(deg > 0) {
//         return sampler->sample(ctx);
//     } else {
//         return rand_r(ctx.local_seed) % ctx.nvertices;
//     }
// }

// vid_t vertex_sample(node2vec_context<SECONDORDERCTX> &ctx, second_order_opt_alias_sample_t *sampler)
// {
//     logstream(LOG_ERROR) << "can't deal with SECONDORDERCTX second-order random walk" << std::endl;
//     return 0;
// }

// vid_t vertex_sample(node2vec_context<BIASEDSECONDORDERCTX> &ctx, second_order_opt_alias_sample_t *sampler)
// {
//     logstream(LOG_ERROR) << "can't deal with BIASEDSECONDORDERCTX second-order random walk" << std::endl;
//     return 0;
// }

// vid_t vertex_sample(node2vec_context<BIASEDACCSECONDORDERCTX> &ctx, second_order_opt_alias_sample_t *sampler) {
//     eid_t deg = (eid_t)(ctx.adj_end - ctx.adj_start);
//     if(deg > 0) {
//         return sampler->sample(ctx);
//     } else {
//         return rand_r(ctx.local_seed) % ctx.nvertices;
//     }
// }

// /* ------------------------------ autogressive sample ----------------------------- */
// template<CtxType ctx_type>
// vid_t vertex_sample(autoregressive_context<ctx_type> &ctx, sample_policy_t *sampler) {
//     eid_t deg = (eid_t)(ctx.adj_end - ctx.adj_start);
//     if(deg > 0) {
//         std::vector<real_t> adj_weights;
//         ctx.query_neigbors_weight(adj_weights);
//         vid_t off = sampler->sample(adj_weights);
//         return ctx.adj_start[off];
//     } else {
//         return rand_r(ctx.local_seed) % ctx.nvertices;
//     }
// }

// vid_t vertex_sample(autoregressive_context<SECONDORDERCTX> &ctx, second_order_soopt_sample_t *sampler) {
//     eid_t deg = (eid_t)(ctx.adj_end - ctx.adj_start);
//     if(deg > 0) {
//         return sampler->sample(ctx);
//     } else {
//         return rand_r(ctx.local_seed) % ctx.nvertices;
//     }
// }

// vid_t vertex_sample(autoregressive_context<BIASEDSECONDORDERCTX> &ctx, second_order_soopt_sample_t *sampler) {
//     logstream(LOG_ERROR) << "can't deal with SECONDORDERCTX second-order random walk" << std::endl;
//     return 0;
// }

// vid_t vertex_sample(autoregressive_context<BIASEDACCSECONDORDERCTX> &ctx, second_order_soopt_sample_t *sampler) {
//     eid_t deg = (eid_t)(ctx.adj_end - ctx.adj_start);
//     if(deg > 0) {
//         return sampler->sample(ctx);
//     } else {
//         return rand_r(ctx.local_seed) % ctx.nvertices;
//     }
// }

// vid_t vertex_sample(autoregressive_context<SECONDORDERCTX> &ctx, second_order_opt_alias_sample_t *sampler)
// {
//     logstream(LOG_ERROR) << "can't deal with SECONDORDERCTX second-order random walk" << std::endl;
//     return 0;
// }

// vid_t vertex_sample(autoregressive_context<BIASEDSECONDORDERCTX> &ctx, second_order_opt_alias_sample_t *sampler)
// {
//     logstream(LOG_ERROR) << "can't deal with BIASEDSECONDORDERCTX second-order random walk" << std::endl;
//     return 0;
// }

// vid_t vertex_sample(autoregressive_context<BIASEDACCSECONDORDERCTX> &ctx, second_order_opt_alias_sample_t *sampler) {
//     eid_t deg = (eid_t)(ctx.adj_end - ctx.adj_start);
//     if(deg > 0) {
//         return sampler->sample(ctx);
//     } else {
//         return rand_r(ctx.local_seed) % ctx.nvertices;
//     }
// }

template<CtxType ctx_type>
vid_t vertex_sample(const walk_context<ctx_type> &ctx, sample_policy_t *sampler) {
    eid_t deg = (eid_t)(ctx.adj_end - ctx.adj_start);
    if(deg > 0) {
        return sampler->sample(ctx);
    } else {
        return rand_r(ctx.local_seed) % ctx.nvertices;
    }
}

#endif
