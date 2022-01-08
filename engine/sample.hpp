#ifndef _GRAPH_SAMPLE_H_
#define _GRAPH_SAMPLE_H_

#include <cstdlib>
#include <ctime>
#include <vector>
#include <algorithm>
#include <cassert>
#include <numeric>
#include <iostream>
#include "api/types.hpp"
#include "metrics/metrics.hpp"
#include "context.hpp"

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
    size_t pos = std::lower_bound(prefix_sum.begin(), prefix_sum.end(), rand_val) - prefix_sum.begin();
    assert(pos > 0 && pos <= n);
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
        return 0;
    }
    virtual size_t sample(const real_t* first, const real_t* last) {
        return 0;
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
    naive_sample_t(bool acc_weight) : sample_policy_t(acc_weight) {}
    naive_sample_t(bool alias, bool acc_weight) : sample_policy_t(alias, acc_weight) {}

    virtual size_t sample(const std::vector<real_t> &weights)
    {
        return naive_sample_impl(weights.begin(), weights.end());
    }

    virtual size_t sample(const real_t *first, const real_t *last)
    {
        return naive_sample_impl(first, last);
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
    its_sample_t(bool acc_weight) : sample_policy_t(acc_weight) {}
    its_sample_t(bool alias, bool acc_weight) : sample_policy_t(alias, acc_weight) {}

    virtual size_t sample(const std::vector<real_t> &weights)
    {
        return its_sample_impl(weights.begin(), weights.end());
    }

    virtual size_t sample(const real_t *first, const real_t *last)
    {
        return its_sample_impl(first, last);
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
    alias_sample_t(bool acc_weight) : sample_policy_t(acc_weight) {}
    alias_sample_t(bool alias, bool acc_weight) : sample_policy_t(alias, acc_weight) {}

    virtual size_t sample(const std::vector<real_t> &weights)
    {
        return alias_sample_impl(weights.begin(), weights.end());
    }

    virtual size_t sample(const real_t *first, const real_t *last)
    {
        return alias_sample_impl(first, last);
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
    reject_sample_t(bool acc_weight) : sample_policy_t(acc_weight) {}
    reject_sample_t(bool alias, bool acc_weight) : sample_policy_t(alias, acc_weight) {}

    virtual size_t sample(const std::vector<real_t> &weights)
    {
        return reject_sample_impl(weights.begin(), weights.end());
    }

    virtual size_t sample(const real_t *first, const real_t *last)
    {
        return reject_sample_impl(first, last);
    }

    std::string sample_name() const {
        return "reject_sample";
    }
};

class second_order_sample_t : public sample_t {
public:
    second_order_sample_t() {}
    second_order_sample_t(bool acc_weight) : sample_t(acc_weight) {}
    second_order_sample_t(bool alias, bool acc_weight) : sample_t(alias, acc_weight) {}

    virtual size_t sample(second_order_context *ctx) { return 0; }
    virtual std::string sample_name() const { return "base_second_order_sample"; }
};

class second_order_its_sample_t : public second_order_sample_t {
public:
    second_order_its_sample_t() {}
    second_order_its_sample_t(bool acc_weight) : second_order_sample_t(acc_weight) {}
    second_order_its_sample_t(bool alias, bool acc_weight) : second_order_sample_t(alias, acc_weight) {}

    virtual size_t sample(second_order_context *ctx) {
        std::vector<real_t> adj_weights;
        ctx->query_neighbors_weights(adj_weights);
        its_sample_t sampler;
        return sampler.sample(adj_weights);
    }
    virtual std::string sample_name() const { return "second_order_its_sample"; }
};

class second_order_alias_sample_t : public second_order_sample_t {
public:
    second_order_alias_sample_t() {}
    second_order_alias_sample_t(bool acc_weight) : second_order_sample_t(acc_weight) {}
    second_order_alias_sample_t(bool alias, bool acc_weight) : second_order_sample_t(alias, acc_weight) {}

    virtual size_t sample(second_order_context *ctx) {
        std::vector<real_t> adj_weights;
        ctx->query_neighbors_weights(adj_weights);
        alias_sample_t sampler;
        return sampler.sample(adj_weights);
    }
    virtual std::string sample_name() const { return "second_order_alias_sample"; }
};

class second_order_reject_sample_t : public second_order_sample_t {
public:
    second_order_reject_sample_t() {}
    second_order_reject_sample_t(bool acc_weight) : second_order_sample_t(acc_weight) {}
    second_order_reject_sample_t(bool alias, bool acc_weight) : second_order_sample_t(alias, acc_weight) {}

    virtual size_t sample(second_order_context *ctx) {
        std::vector<real_t> adj_weights;
        ctx->query_neighbors_weights(adj_weights);
        reject_sample_t sampler;
        return sampler.sample(adj_weights);
    }
    virtual std::string sample_name() const { return "second_order_reject_sample"; }
};

class second_order_soopt_sample_t : public second_order_sample_t {
public:
    virtual size_t sample(second_order_context *ctx) {
        return 0;
    }
};

size_t sample(sample_policy_t* sampler, const std::vector<real_t> & weights) {
    return sampler->sample(weights);
}


size_t sample(sample_policy_t *sampler, const real_t* first, const real_t* last) {
    return sampler->sample(first, last);
}

vid_t vertex_sample(graph_context& ctx, sample_policy_t *sampler) {
    eid_t deg = (eid_t)(ctx.adj_end - ctx.adj_start);
    if(deg > 0 && (float)rand_r(ctx.local_seed) / RAND_MAX > ctx.teleport) {
        if(ctx.weight_start == nullptr || ctx.weight_end == nullptr) {
            vid_t off = (vid_t)rand_r(ctx.local_seed) % deg;
            return ctx.adj_start[off];
        }else {
            size_t off = sample(sampler, ctx.weight_start, ctx.weight_end);
            return ctx.adj_start[off];
        }
    }else {
        return rand_r(ctx.local_seed) % ctx.nvertices;
    }
}

vid_t vertex_sample(second_order_context& ctx, second_order_sample_t *sampler) {
    eid_t deg = (eid_t)(ctx.adj_end - ctx.adj_start);
    if(deg > 0) {
        return sampler->sample(&ctx);
    } else {
        return rand_r(ctx.local_seed) % ctx.nvertices;
    }
}

template<typename SampleType>
bool check_use_alias_table(SampleType *sampler) { 
    logstream(LOG_ERROR) << "you are using a unrecognized sample type." << std::endl;
    return false; 
}

template <>
bool check_use_alias_table(sample_policy_t *sampler) { return sampler->use_alias; }

template <>
bool check_use_alias_table(second_order_sample_t *sampler) { return sampler->use_alias; }

template<typename SampleType>
bool check_use_accumulate_weight(SampleType *sampler) { 
    logstream(LOG_ERROR) << "you are using a unrecognized sample type." << std::endl;
    return false; 
}

template <>
bool check_use_accumulate_weight(sample_policy_t *sampler) { return sampler->use_acc_weight; }

template <>
bool check_use_accumulate_weight(second_order_sample_t *sampler) { return sampler->use_acc_weight; }
#endif
