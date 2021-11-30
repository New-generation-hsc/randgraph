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

class sample_policy_t {
protected:
    metrics &_m;
public:
    sample_policy_t(metrics &m) : _m(m) {  }
    virtual size_t sample(const std::vector<real_t>&) {
        return 0;
    }
};

/**
 * This method generates a uniform [0, 1] random variable r
*/
class naive_sample_t : public sample_policy_t {
public:
    naive_sample_t(metrics &m) : sample_policy_t(m) {  }
    size_t sample(const std::vector<real_t>& weights) {
        size_t n = weights.size();
        return rand() % n;
    }
};

/** This method first computes the cumulative distribution of weights,
 * then generates a random number x between [0, P_sum),
 * finally uses a binary search to find the smallest index i such that x < P_i
*/
class its_sample_t : public sample_policy_t {
public:
    its_sample_t(metrics &m) : sample_policy_t(m) {  }
    size_t sample(const std::vector<real_t>& weights) {
        size_t n = weights.size();
        _m.start_time("sample_initialization");
        std::vector<real_t> prefix_sum(n + 1, 0.0);
        for(size_t idx = 1; idx <= n; idx++) {
            prefix_sum[idx] = prefix_sum[idx - 1] + weights[idx - 1];
        }
        _m.stop_time("sample_initialization");

        _m.start_time("sample_generation");
        real_t diff = prefix_sum[n];
        real_t random = (real_t)rand() / (real_t)RAND_MAX;
        real_t rand_val = diff * random;
        size_t pos = std::lower_bound(prefix_sum.begin(), prefix_sum.end(), rand_val) - prefix_sum.begin();
        assert(pos > 0 && pos <= n);
#ifdef TEST_SAMPLE
        std::cout << "prefix:";
        for(const auto & s : prefix_sum) std::cout << " " << s;
        std::cout << std::endl;
#endif
        _m.stop_time("sample_generation");
        return pos - 1;
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
    alias_sample_t(metrics &m) : sample_policy_t(m) {  }
    size_t sample(const std::vector<real_t> &weights){
        size_t n = weights.size();
        _m.start_time("sample_initialization");
        std::vector<real_t> aux_weights(weights.begin(), weights.end());
        real_t sum = std::accumulate(weights.begin(), weights.end(), 0.0);
        real_t avg = sum / static_cast<real_t>(n);
        std::vector<real_t> prob_table(n), alias_table(n);
        std::vector<size_t> small_table, large_table;
        for(size_t idx = 0; idx < n; idx++) {
            if(weights[idx] < avg) small_table.push_back(idx);
            else large_table.push_back(idx);
        }
        while(!small_table.empty() && !large_table.empty()) {
            size_t s = small_table.back(), l = large_table.back();
            prob_table[s] = aux_weights[s];
            alias_table[s] = l;
            small_table.pop_back();
            large_table.pop_back();
            real_t r = (aux_weights[s] + aux_weights[l]) - avg;
            aux_weights[l] = r;
            if(r < avg) small_table.push_back(l);
            else large_table.push_back(l);
        }

        while(!large_table.empty()) {
            size_t l = large_table.back();
            large_table.pop_back();
            prob_table[l] = avg;
            alias_table[l] = n;
        }

        while(!small_table.empty()) {
            size_t s = small_table.back();
            small_table.pop_back();
            prob_table[s] = avg;
            alias_table[s] = n;
        }

        _m.stop_time("sample_initialization");
#ifdef TEST_SAMPLE
        std::cout << "avg: " << avg << std::endl;
        std::cout << "prob table:";
        for (const auto &s : prob_table) std::cout << " " << s;
        std::cout << "\nalias table:";
        for(const auto &s : alias_table) std::cout << " " << s;
        std::cout << std::endl;
#endif

        _m.start_time("sample_generation");
        real_t rand_val = static_cast<real_t>(rand()) / static_cast<real_t>(RAND_MAX) * avg;
        size_t rand_pos = rand() % n;
        _m.stop_time("sample_generation");
        if(rand_val < prob_table[rand_pos]) return rand_pos;
        else return alias_table[rand_pos];
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
    reject_sample_t(metrics &m) : sample_policy_t(m) {  }
    size_t sample(const std::vector<real_t> &weights){
        size_t n = weights.size();
        _m.start_time("sample_initialization");
        real_t pmax = *std::max_element(weights.begin(), weights.end());
        _m.stop_time("sample_initialization");
        _m.start_time("sample_generation");
        real_t rand_val = static_cast<real_t>(rand()) / static_cast<real_t>(RAND_MAX) * pmax;
        real_t rand_pos = rand() % n;
        while(rand_val > weights[rand_pos]) {
            rand_pos = rand() % n;
            rand_val = static_cast<real_t>(rand()) / static_cast<real_t>(RAND_MAX) * pmax;
        }
        _m.stop_time("sample_generation");
        return rand_pos;
    }
};

size_t sample(sample_policy_t* sampler, const std::vector<real_t> & weights) {
    return sampler->sample(weights);
}

#endif
