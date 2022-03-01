#ifndef _GRAPH_USERPROGRAM_H_
#define _GRAPH_USERPROGRAM_H_

#include "engine/walk.hpp"
#include "engine/cache.hpp"

template<typename AppType, typename AppConfig>
class userprogram_t : public AppType {
public:
    userprogram_t(AppConfig& conf) : AppType(conf) { }

    template <typename walk_data_t, WalkType walk_type>
    void prologue(graph_walk<walk_data_t, walk_type> *walk_manager)
    {
        AppType::prologue(walk_manager);
    }
    template <typename walk_data_t, WalkType walk_type>
    void update_walk(const walker_t<walk_data_t> &walker, graph_cache *cache, graph_walk<walk_data_t, walk_type> *walk_manager, sample_policy_t *sampler, unsigned int *seed, bool dynamic)
    {
        AppType::update_walk(walker, cache, walk_manager, sampler, seed, dynamic);
    }

    void epilogue() {
        AppType::epilogue();
    }
};

#endif
