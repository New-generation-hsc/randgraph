#ifndef _GRAPH_USERPROGRAM_H_
#define _GRAPH_USERPROGRAM_H_

#include "randomwalk.hpp"
#include "node2vec.hpp"
#include "engine/walk.hpp"
#include "engine/cache.hpp"
#include "engine/sample.hpp"

template<typename AppType, typename AppConfig>
class userprogram_t : public AppType {
public:
    userprogram_t(AppConfig& conf) : AppType(conf) { }

    template <typename walk_data_t, WalkType walk_type>
    void prologue(graph_walk<walk_data_t, walk_type> *walk_manager)
    {
    }
    template <typename walk_data_t, WalkType walk_type>
    void update_walk(const walker_t<walk_data_t> &walker, graph_cache *cache, graph_walk<walk_data_t, walk_type> *walk_manager, sample_policy_t *sampler)
    {
    }
};

template<>
class userprogram_t<randomwalk_t, randomwalk_conf_t> : public randomwalk_t {
public:
    userprogram_t(randomwalk_conf_t& conf) : randomwalk_t(conf) { }
    template <typename walk_data_t, WalkType walk_type>
    void prologue(graph_walk<walk_data_t, walk_type> *walk_manager)
    {
        randomwalk_t::prologue<walk_data_t, walk_type>(walk_manager);
    }
    template <typename walk_data_t, WalkType walk_type>
    void update_walk(const walker_t<walk_data_t> &walker, graph_cache *cache, graph_walk<walk_data_t, walk_type> *walk_manager, sample_policy_t *sampler)
    {
        randomwalk_t::update_walk<walk_data_t, walk_type>(walker, cache, walk_manager, sampler);
    }
};

template <>
class userprogram_t<node2vec_t, node2vec_conf_t> : public node2vec_t
{
public:
    userprogram_t(node2vec_conf_t &conf) : node2vec_t(conf) {}
    template <typename walk_data_t, WalkType walk_type>
    void prologue(graph_walk<walk_data_t, walk_type> *walk_manager)
    {
        node2vec_t::prologue<walk_data_t, walk_type>(walk_manager);
    }
    template <typename walk_data_t, WalkType walk_type>
    void update_walk(const walker_t<walk_data_t> &walker, graph_cache *cache, graph_walk<walk_data_t, walk_type> *walk_manager, sample_policy_t *sampler)
    {
        node2vec_t::update_walk<walk_data_t, walk_type>(walker, cache, walk_manager, sampler);
    }
};

#endif