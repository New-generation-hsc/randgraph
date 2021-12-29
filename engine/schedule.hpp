#ifndef _GRAPH_SCHEDULE_H_
#define _GRAPH_SCHEDULE_H_

#include <string>
#include <algorithm>
#include <utility>
#include <queue>

#include "cache.hpp"
#include "config.hpp"
#include "driver.hpp"
#include "walk.hpp"
#include "util/util.hpp"
#include "util/io.hpp"
#include "metrics/metrics.hpp"

struct rank_compare {
    bool operator()(const std::pair<bid_t, rank_t>& p1, const std::pair<bid_t, rank_t>& p2) {
        return p1.second < p2.second;
    }
};

struct walk_scheduler_config_t {
    graph_config conf;
    float p;
};

/** graph_scheduler
 *
 * This file contribute to define the interface how to schedule cache blocks
 */

class base_scheduler {
protected:
    int vertdesc, edgedesc, degdesc, whtdesc;  /* the beg_pos, csr, degree file descriptor */
    metrics &_m;
    bool _weighted;

public:
    base_scheduler(graph_config *conf, metrics& m) : _m(m) {
        vertdesc = edgedesc = whtdesc = 0;
        _weighted = false;
        this->setup(conf);
    }

    base_scheduler(metrics& m) : _m(m) {
        vertdesc = edgedesc = whtdesc = 0;
        _weighted = false;
    }

    void setup(graph_config *conf) {
        this->destory();

        std::string beg_pos_name = get_beg_pos_name(conf->base_name, conf->fnum);
        std::string csr_name = get_csr_name(conf->base_name, conf->fnum);
        std::string degree_name = get_degree_name(conf->base_name, conf->fnum);

        vertdesc = open(beg_pos_name.c_str(), O_RDONLY);
        edgedesc = open(csr_name.c_str(), O_RDONLY);
        degdesc = open(degree_name.c_str(), O_RDONLY);
        _weighted = conf->is_weighted;

        if (_weighted)
        {
            std::string weight_name = get_weights_name(conf->base_name, conf->fnum);
            whtdesc = open(weight_name.c_str(), O_RDONLY);
        }
    }

    void destory() {
        if(vertdesc > 0) close(vertdesc);
        if(edgedesc > 0) close(edgedesc);
        if(degdesc > 0) close(degdesc);
        if(_weighted) {
            close(_weighted);
        }
    }

    ~base_scheduler() {
        this->destory();
    }
};

template <typename Config>
class graph_scheduler : public base_scheduler {
private:
    bid_t exec_blk;                   /* the current cache block index used for run */
    bid_t nrblock;                    /* number of cache blocks are used for running */
public:
    graph_scheduler(graph_config *conf, metrics &m) : base_scheduler(conf, m) {
        exec_blk = 0;
        nrblock  = 0;
    }

    graph_scheduler(Config& conf, metrics &m) : base_scheduler(m) {
        exec_blk = 0;
        nrblock = 0;
    }

    /** If the cache block has no walk, then swap out all blocks */
    template <typename walk_data_t, WalkType walk_type>
    bid_t schedule(graph_cache &cache, graph_driver &driver, graph_walk<walk_data_t, walk_type> &walk_manager)
    {
        if(walk_manager.test_finished_cache_walks(&cache)) {
            swap_blocks(cache, driver, walk_manager.global_blocks);
        }
        bid_t ret = exec_blk;
        exec_blk++;
        if(exec_blk >= nrblock) exec_blk = 0;
        return cache.cache_blocks[ret].block->blk;
    }

    void swap_blocks(graph_cache& cache, graph_driver& driver, graph_block* global_blocks) {
        // set the all cached blocks inactive */
        _m.start_time("graph_scheduler_swap_blocks");
        for(bid_t p = 0; p < cache.ncblock; p++) {
            if(cache.cache_blocks[p].block != NULL) {
                cache.cache_blocks[p].block->cache_index = global_blocks->nblocks;
                cache.cache_blocks[p].block->status = INACTIVE;
            }
        }

        bid_t blk = 0;
        std::vector<bid_t> blocks = choose_blocks(cache.ncblock, global_blocks);
        nrblock = blocks.size();
        exec_blk = 0;

        for(const auto & p : blocks) {
            cache.cache_blocks[blk].block  = &global_blocks->blocks[p];
            cache.cache_blocks[blk].block->cache_index = blk;
            cache.cache_blocks[blk].block->status = ACTIVE;
            cache.cache_blocks[blk].beg_pos = (eid_t*)realloc(cache.cache_blocks[blk].beg_pos, (global_blocks->blocks[p].nverts + 1) * sizeof(eid_t));
            cache.cache_blocks[blk].csr     = (vid_t*)realloc(cache.cache_blocks[blk].csr   , global_blocks->blocks[p].nedges * sizeof(vid_t));
            
            driver.load_block_vertex(vertdesc, cache.cache_blocks[blk].beg_pos, global_blocks->blocks[p]);
            driver.load_block_edge(edgedesc,  cache.cache_blocks[blk].csr,    global_blocks->blocks[p]);
            if (_weighted) {
                cache.cache_blocks[blk].weights = (real_t *)realloc(cache.cache_blocks[blk].weights, global_blocks->blocks[p].nedges * sizeof(real_t));
                driver.load_block_weight(whtdesc, cache.cache_blocks[blk].weights, global_blocks->blocks[p]);
            }
            blk++;
        }

        for(; blk < cache.ncblock; blk++) {
            if(cache.cache_blocks[blk].block) {
                cache.cache_blocks[blk].block = NULL;
            }
        }
        _m.stop_time("graph_scheduler_swap_blocks");
    }

    std::vector<bid_t> choose_blocks(bid_t ncblocks, graph_block* global_blocks) {
        std::vector<bid_t> blocks;
        std::priority_queue<std::pair<bid_t, rank_t>, std::vector<std::pair<bid_t, rank_t>>, rank_compare> pq;
        for(bid_t blk = 0; blk < global_blocks->nblocks; blk++) {
            pq.push(std::make_pair(blk, global_blocks->blocks[blk].rank));
        }

        while(!pq.empty() && ncblocks) {
            auto kv = pq.top();
            if(kv.second == 0) break;
            blocks.push_back(kv.first);
            pq.pop();
            ncblocks--;
        }
        std::sort(blocks.begin(), blocks.end());

        return blocks;
    }
};

template <>
graph_scheduler<graph_config>::graph_scheduler(graph_config& conf, metrics &m) : base_scheduler(m) {
    base_scheduler::setup(&conf);
    exec_blk = 0;
    nrblock = 0;
}

/**
 * The following schedule scheme follow the graph walker scheme
 */
template <typename Config>
class walk_schedule_t : public base_scheduler
{
private:
    float prob;
    bid_t exec_blk;
public:
    walk_schedule_t(graph_config* conf, float p, metrics &m) : base_scheduler(conf, m) {
        // nothing need to initialize
        prob = p;
        exec_blk = 0;
    }

    walk_schedule_t(Config& conf, metrics &m) : base_scheduler(m) {
        prob = 0;
        exec_blk = 0;
    }

    template <typename walk_data_t, WalkType walk_type>
    bid_t schedule(graph_cache &cache, graph_driver &driver, graph_walk<walk_data_t, walk_type> &walk_manager)
    {
        bid_t blk = walk_manager.choose_block(prob);
        if(cache.test_block_cached(blk, exec_blk)) {
            return blk;
        }
        _m.start_time("walk_schedule_swap_blocks");
        graph_block *global_blocks = walk_manager.global_blocks;
        exec_blk = swap_block(cache, walk_manager);
        cache.cache_blocks[exec_blk].block = &global_blocks->blocks[blk];
        cache.cache_blocks[exec_blk].block->status = ACTIVE;
        cache.cache_blocks[exec_blk].block->cache_index = exec_blk;

        cache.cache_blocks[exec_blk].beg_pos = (eid_t*)realloc(cache.cache_blocks[exec_blk].beg_pos, (global_blocks->blocks[blk].nverts + 1) * sizeof(eid_t));
        cache.cache_blocks[exec_blk].csr     = (vid_t*)realloc(cache.cache_blocks[exec_blk].csr   , global_blocks->blocks[blk].nedges * sizeof(vid_t));

        driver.load_block_vertex(vertdesc, cache.cache_blocks[exec_blk].beg_pos, global_blocks->blocks[blk]);
        driver.load_block_edge(edgedesc,  cache.cache_blocks[exec_blk].csr,    global_blocks->blocks[blk]);
        if (_weighted) {
            cache.cache_blocks[exec_blk].weights = (real_t *)realloc(cache.cache_blocks[exec_blk].weights, global_blocks->blocks[blk].nedges * sizeof(real_t));
            driver.load_block_weight(whtdesc, cache.cache_blocks[exec_blk].weights, global_blocks->blocks[blk]);
        }
        _m.stop_time("walk_schedule_swap_blocks");
        return blk;
    }

    template <typename walk_data_t, WalkType walk_type>
    bid_t swap_block(graph_cache &cache, graph_walk<walk_data_t, walk_type> &walk_manager)
    {
        wid_t walks_cnt = 0xffffffff;
        bid_t blk = 0;
        int life = -1;
        for(bid_t p = 0; p < cache.ncblock; p++) {
            if(cache.cache_blocks[p].block == NULL) {
                blk = p; break;
            }
            wid_t cnt = walk_manager.nblockwalks(cache.cache_blocks[p].block->blk);
            if(walks_cnt > cnt) {
                walks_cnt = cnt;
                blk = p;
                life = cache.cache_blocks[p].life;
            }else if(walks_cnt == cnt && cache.cache_blocks[p].life > life) {
                blk = p;
                life = cache.cache_blocks[p].life;
            }
            cache.cache_blocks[p].life += 1;
        }
        cache.cache_blocks[blk].life = 0;
        if(cache.cache_blocks[blk].block != NULL) {
            cache.cache_blocks[blk].block->cache_index = walk_manager.global_blocks->nblocks;
        }
        return blk;
    }
};

template<>
walk_schedule_t<walk_scheduler_config_t>::walk_schedule_t(walk_scheduler_config_t& conf, metrics &m) : base_scheduler(m) {
    base_scheduler::setup(&(conf.conf));
    prob = conf.p;
    exec_blk = 0;
}


/**
 * The following scheduler is the second-order scheduler
 * the scheduler selects two block each time and check whether they are already in memory or not
*/
template<typename Config>
class second_order_scheduler_t : public base_scheduler {
private:
    template <typename walk_data_t, WalkType walk_type>
    std::pair<bid_t, bid_t> choose_blocks(graph_cache &cache, graph_walk<walk_data_t, walk_type> &walk_manager) {
        return { 0, 0 };
    }

    std::pair<bid_t, bid_t> choose_blocks(graph_cache &cache, graph_walk<vid_t, SecondOrder> &walk_manager)
    {
        bid_t blk = walk_manager.max_walks_block();
        bid_t nblocks = walk_manager.global_blocks->nblocks;
        return { blk / nblocks, blk % nblocks };
    }

    template<typename walk_data_t, WalkType walk_type>
    bid_t swap_block(graph_cache &cache, graph_walk<walk_data_t, walk_type>& walk_manager) {
        bid_t blk = 0;
        int life = -1;
        for(bid_t p = 0; p < cache.ncblock; ++p) {
            if(cache.cache_blocks[p].block == NULL) {
                blk = p; break;
            }
            if(cache.cache_blocks[p].life > life) {
                blk = p;
                life = cache.cache_blocks[p].life;
            }
        }
        if(cache.cache_blocks[blk].block != NULL) {
            cache.cache_blocks[blk].block->cache_index = walk_manager.global_blocks->nblocks;
        }
        return blk;
    }

    void load_block_info(graph_cache& cache, graph_driver& driver, graph_block* global_blocks, bid_t cache_index, bid_t block_index) {
        cache.cache_blocks[cache_index].block = &global_blocks->blocks[block_index];
        cache.cache_blocks[cache_index].block->status = ACTIVE;
        cache.cache_blocks[cache_index].block->cache_index = cache_index;

        cache.cache_blocks[cache_index].beg_pos = (eid_t *)realloc(cache.cache_blocks[cache_index].beg_pos, (global_blocks->blocks[block_index].nverts + 1) * sizeof(eid_t));
        cache.cache_blocks[cache_index].csr = (vid_t *)realloc(cache.cache_blocks[cache_index].csr, global_blocks->blocks[block_index].nedges * sizeof(vid_t));

        driver.load_block_vertex(vertdesc, cache.cache_blocks[cache_index].beg_pos, global_blocks->blocks[block_index]);
        driver.load_block_edge(edgedesc, cache.cache_blocks[cache_index].csr, global_blocks->blocks[block_index]);
        if (_weighted)
        {
            cache.cache_blocks[cache_index].weights = (real_t *)realloc(cache.cache_blocks[cache_index].weights, global_blocks->blocks[block_index].nedges * sizeof(real_t));
            driver.load_block_weight(whtdesc, cache.cache_blocks[cache_index].weights, global_blocks->blocks[block_index]);
        }
    }

    template<typename walk_data_t, WalkType walk_type>
    bid_t transform(bid_t pblk, bid_t cblk, graph_walk<walk_data_t, walk_type>& walk_manager) {
        return cblk;
    }

    bid_t transform(bid_t pblk, bid_t cblk, graph_walk<vid_t, SecondOrder>& walk_manager) {
        return pblk * walk_manager.global_blocks->nblocks + cblk;
    }
public:
    second_order_scheduler_t(Config& conf, metrics& m) : base_scheduler(m) {

    }

    template <typename walk_data_t, WalkType walk_type>
    bid_t schedule(graph_cache &cache, graph_driver &driver, graph_walk<walk_data_t, walk_type> &walk_manager) {
        std::pair<bid_t, bid_t> select_blocks = choose_blocks(cache, walk_manager);
        bid_t pblk = select_blocks.first, cblk = select_blocks.second;
        
        /* increase the cache block life */
        for(bid_t p = 0; p < cache.ncblock; ++p) cache.cache_blocks[p].life++;
        bid_t p_cache_index = (*(walk_manager.global_blocks))[pblk].cache_index, cache_index = (*(walk_manager.global_blocks))[cblk].cache_index;
        if(p_cache_index != walk_manager.global_blocks->nblocks) {
            cache.cache_blocks[p_cache_index].life = 0;
        }else {
            bid_t new_cache_index = swap_block(cache, walk_manager);
            cache.cache_blocks[new_cache_index].life = 0;
            load_block_info(cache, driver, walk_manager.global_blocks, new_cache_index, pblk);
        }

        if(cache_index != walk_manager.global_blocks->nblocks) {
            cache.cache_blocks[cache_index].life = 0;
        }else {
            bid_t new_cache_index = swap_block(cache, walk_manager);
            cache.cache_blocks[new_cache_index].life = 0;
            load_block_info(cache, driver, walk_manager.global_blocks, new_cache_index, cblk);
        }
        return transform<walk_data_t, walk_type>(pblk, cblk, walk_manager.global_blocks->nblocks);
    }
};

template<>
second_order_scheduler_t<graph_config>::second_order_scheduler_t(graph_config& conf, metrics &m) : base_scheduler(m) {
    setup(&conf);
}

// template<typename Config>
// bid_t second_order_scheduler_t<Config>::choose_blocks()

template<typename BaseType, typename Config>
class scheduler : public BaseType {
public:
    scheduler(Config &conf, metrics &m) : BaseType(conf, m) { }
    template <typename walk_data_t, WalkType walk_type>
    bid_t schedule(graph_cache &cache, graph_driver &driver, graph_walk<walk_data_t, walk_type> &walk_manager) {
        return BaseType::schedule(cache, driver, walk_manager);
    }
};

#endif
