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
    template <typename walk_data_t, WalkType walk_type, typename SampleType>
    void update_walk(const walker_t<walk_data_t> &walker, graph_cache *cache, graph_walk<walk_data_t, walk_type> *walk_manager, SampleType *sampler)
    {
        AppType::update_walk(walker, cache, walk_manager, sampler);
    }
};

template<typename AppConf, typename walk_data_t, WalkType walk_type, typename SampleType>
class update_strategy_t {
public:
    static void update_walk(const AppConf & conf, const walker_t<walk_data_t> &walker, graph_cache *cache, graph_walk<walk_data_t, walk_type> *walk_manager, SampleType *sampler) 
    {
        logstream(LOG_ERROR) << "you are using a generic update strategy." << std::endl;
    }
};

#endif