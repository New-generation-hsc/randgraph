#ifndef _GRAPH_ENGINE_H_
#define _GRAPH_ENGINE_H_

#include "cache.hpp"
#include "schedule.hpp"
#include "apps/randomwalk.hpp"

class graph_engine {
public:
    bid_t        exec_block;
    graph_cache  *cache;
    graph_walk   *walk_mangager;
    graph_driver *driver;

    graph_engine(graph_cache& _cache, graph_walk& mangager, graph_driver& _driver) {
        cache         = &_cache;
        walk_mangager = &mangager;
        driver        = &_driver;
    }

    void prologue(randomwalk_t& userprogram) {

    }

    void run(randomwalk_t& userprogram, scheduler& block_scheduler) {

    }

    void epilogue(randomwalk_t& userprogram) { 

    }
};

#endif