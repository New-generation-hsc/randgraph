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
#include "sample.hpp"

template<typename value_t>
struct rank_compare {
    bool operator()(const std::pair<bid_t, value_t> &p1, const std::pair<bid_t, value_t> &p2)
    {
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
    metrics &_m;

public:
    base_scheduler(metrics& m) : _m(m) {  }

    ~base_scheduler() {}
};

class graph_scheduler : public base_scheduler {
private:
    bid_t exec_blk;                   /* the current cache block index used for run */
    bid_t nrblock;                    /* number of cache blocks are used for running */
public:
    graph_scheduler(metrics &m) : base_scheduler(m) {
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
            driver.load_block_info(cache, global_blocks, blk, p);
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
        std::priority_queue<std::pair<bid_t, rank_t>, std::vector<std::pair<bid_t, rank_t>>, rank_compare<rank_t>> pq;
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


/**
 * @brief The drunkardmob_scheduler following sequential load blocks
 *
 * @tparam Config
 */
class drunkardmob_scheduler : public base_scheduler {
private:
    bid_t exec_blk;                   /* the current cache block index used for run */
public:
    drunkardmob_scheduler(metrics &m) : base_scheduler(m) {
        exec_blk = 0;
    }

    /** If the cache block has no walk, then swap out all blocks */
    template <typename walk_data_t, WalkType walk_type>
    bid_t schedule(graph_cache &cache, graph_driver &driver, graph_walk<walk_data_t, walk_type> &walk_manager)
    {
        for(bid_t blk = 0; blk < cache.ncblock; blk++) cache.cache_blocks[blk].life++;
        bid_t blk = choose_blocks(walk_manager);
        bid_t cache_index = (*(walk_manager.global_blocks))[blk].cache_index;
        if(cache_index != walk_manager.global_blocks->nblocks) {
            cache.cache_blocks[cache_index].life = 0;
        }else {
            bid_t new_cache_index = swap_block(cache, walk_manager.global_blocks);
            cache.cache_blocks[new_cache_index].life = 0;
            driver.load_block_info(cache, walk_manager.global_blocks, new_cache_index, blk);
        }
        return blk;
    }

    bid_t swap_block(graph_cache& cache, graph_block* global_blocks) {
        bid_t blk = 0;
        int life = -1;
        for(bid_t p = 0; p < cache.ncblock; p++) {
            if(cache.cache_blocks[p].block == NULL) {
                blk = p; break;
            }
            if(cache.cache_blocks[p].life > life) {
                blk = p;
                life = cache.cache_blocks[p].life;
            }
        }
        if(cache.cache_blocks[blk].block != NULL) {
            cache.cache_blocks[blk].block->cache_index = global_blocks->nblocks;
        }
        return blk;
    }

    template <typename walk_data_t, WalkType walk_type>
    bid_t choose_blocks(graph_walk<walk_data_t, walk_type> &walk_manager) {
        if(exec_blk == 0) {
            print_walks_distrition(walk_manager);
            exec_blk = walk_manager.global_blocks->nblocks;
        }
        bid_t ret = --exec_blk;
        return ret;
    }

    template <typename walk_data_t, WalkType walk_type>
    void print_walks_distrition(graph_walk<walk_data_t, walk_type> &walk_manager) {
        std::cout << "walks distribution : ";
        for(bid_t blk = 0; blk < walk_manager.global_blocks->nblocks; blk++) {
            std::cout << walk_manager.nblockwalks(blk) << " ";
        }
        std::cout << std::endl;
    }
};


/**
 * The following schedule scheme follow the graph walker scheme
 */
class walk_schedule_t : public base_scheduler
{
private:
    float prob;
    bid_t exec_blk;
public:
    walk_schedule_t(metrics &m) : base_scheduler(m) {
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
        driver.load_block_info(cache, global_blocks, exec_blk, blk);
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

template <typename walk_data_t, WalkType walk_type>
bid_t transform(bid_t pblk, bid_t cblk, graph_walk<walk_data_t, walk_type> &walk_manager)
{
    return cblk;
}

template <>
bid_t transform<vid_t, SecondOrder>(bid_t pblk, bid_t cblk, graph_walk<vid_t, SecondOrder> &walk_manager)
{
    return pblk * walk_manager.global_blocks->nblocks + cblk;
}

/**
 * The naive second-order scheduler which follows the graphwalker major constribution
 * In each schedule, load the block that has the most number of walks and processes them
*/
class navie_graphwalker_scheduler_t : public base_scheduler {
private:
    std::priority_queue<std::pair<bid_t, wid_t>, std::vector<std::pair<bid_t, wid_t>>, rank_compare<wid_t>> block_queue;
    bid_t exec_blk;

    template <typename walk_data_t, WalkType walk_type>
    std::vector<std::pair<bid_t, wid_t>> stream_blocks(graph_walk<walk_data_t, walk_type> &walk_manager) { return {}; }

    std::vector<std::pair<bid_t, wid_t>> stream_blocks(graph_walk<vid_t, SecondOrder> &walk_manager) {
        bid_t nblocks = walk_manager.global_blocks->nblocks;
        std::vector<std::pair<bid_t, wid_t>> block_info;
        for(bid_t blk = 0; blk < nblocks; ++blk) {
            bid_t exact_blk = blk * nblocks + exec_blk;
            wid_t nwalks = walk_manager.nblockwalks(exact_blk);
            if(nwalks > 0) block_info.push_back({blk, nwalks});
        }
        return block_info;
    }

    template <typename walk_data_t, WalkType walk_type>
    bid_t choose_block(graph_walk<walk_data_t, walk_type>& walk_manager) { return 0; }

    bid_t choose_block(graph_walk<vid_t, SecondOrder> &walk_manager) {
        bid_t target_block = 0, nblocks = walk_manager.global_blocks->nblocks;
        wid_t max_walks = 0;
        for(bid_t cblk = 0; cblk < nblocks; ++cblk) {
            wid_t nwalks = 0;
            for(bid_t pblk = 0; pblk < nblocks; ++pblk) nwalks += walk_manager.nblockwalks(pblk * nblocks + cblk);
            if(nwalks > max_walks) {
                max_walks = nwalks;
                target_block = cblk;
            }
        }
        return target_block;
    }

    template<typename walk_data_t, WalkType walk_type>
    bid_t swap_block(graph_cache &cache, graph_walk<walk_data_t, walk_type>& walk_manager, bid_t exclude_block) {
        bid_t blk = 0;
        int life = -1;
        for(bid_t p = 0; p < cache.ncblock; ++p) {
            if(cache.cache_blocks[p].block == NULL) {
                blk = p; break;
            }
            if(cache.cache_blocks[p].life > life && cache.cache_blocks[p].block->blk != exclude_block) {
                blk = p;
                life = cache.cache_blocks[p].life;
            }
        }
        if(cache.cache_blocks[blk].block != NULL) {
            cache.cache_blocks[blk].block->cache_index = walk_manager.global_blocks->nblocks;
        }
        return blk;
    }

    template <typename walk_data_t, WalkType walk_type>
    void swapin_block(graph_cache &cache, graph_driver &driver, graph_walk<walk_data_t, walk_type> &walk_manager, bid_t select_block, bid_t exclude_blk)
    {
        bid_t cache_index = (*(walk_manager.global_blocks))[select_block].cache_index;
        if(cache_index == walk_manager.global_blocks->nblocks) {
            cache_index = swap_block(cache, walk_manager, exclude_blk);
            driver.load_block_info(cache, walk_manager.global_blocks, cache_index, select_block);
        }
        cache.cache_blocks[cache_index].life = 0;
    }

public:
    navie_graphwalker_scheduler_t(metrics& m) : base_scheduler(m) { }

    template <typename walk_data_t, WalkType walk_type>
    bid_t schedule(graph_cache &cache, graph_driver &driver, graph_walk<walk_data_t, walk_type> &walk_manager) {
        if(block_queue.empty()) {
            exec_blk = choose_block(walk_manager);
            for (bid_t p = 0; p < cache.ncblock; ++p) cache.cache_blocks[p].life++;
            swapin_block(cache, driver, walk_manager, exec_blk, walk_manager.global_blocks->nblocks);
            auto block_info = stream_blocks(walk_manager);
            for(auto & info : block_info) {
                block_queue.push(info);
            }
        }
        bid_t blk = block_queue.top().first;
        block_queue.pop();
        swapin_block(cache, driver, walk_manager, blk, exec_blk);
        return blk * walk_manager.global_blocks->nblocks + exec_blk;
    }
};


/**
 * The following scheduler is the second-order scheduler
 * the scheduler selects two block each time and check whether they are already in memory or not
*/
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
    bid_t swap_block(graph_cache &cache, graph_walk<walk_data_t, walk_type>& walk_manager, bid_t exclude_cache_index) {
        bid_t blk = 0;
        int life = -1;
        wid_t active_walks_cnt = 0xffffffff;
        for(bid_t p = 0; p < cache.ncblock; ++p) {
            if(p == exclude_cache_index) continue;
            if(cache.cache_blocks[p].block == NULL) {
                blk = p; break;
            }
            if(walk_manager.block_active_walks(cache.cache_blocks[p].block->blk) < active_walks_cnt) {
                blk = p;
                life = cache.cache_blocks[p].life;
                active_walks_cnt = walk_manager.block_active_walks(cache.cache_blocks[p].block->blk);
            } else if(walk_manager.block_active_walks(cache.cache_blocks[p].block->blk) == active_walks_cnt && cache.cache_blocks[p].life > life) {
                blk = p;
                life = cache.cache_blocks[p].life;
            }
        }
        if(cache.cache_blocks[blk].block != NULL) {
            cache.cache_blocks[blk].block->cache_index = walk_manager.global_blocks->nblocks;
        }
        return blk;
    }

public:
    second_order_scheduler_t(metrics& m) : base_scheduler(m) { }

    template <typename walk_data_t, WalkType walk_type>
    bid_t schedule(graph_cache &cache, graph_driver &driver, graph_walk<walk_data_t, walk_type> &walk_manager) {
        std::pair<bid_t, bid_t> select_blocks = choose_blocks(cache, walk_manager);
        bid_t pblk = select_blocks.first, cblk = select_blocks.second;

#ifdef TESTDEBUG
        logstream(LOG_DEBUG) << "second-order schedule blocks [" << pblk << ", " << cblk << "]" << std::endl;
#endif

        _m.start_time("second_order_scheduler_swap_blocks");
        /* increase the cache block life */
        for(bid_t p = 0; p < cache.ncblock; ++p) cache.cache_blocks[p].life++;
        bid_t p_cache_index = (*(walk_manager.global_blocks))[pblk].cache_index;
        if(p_cache_index != walk_manager.global_blocks->nblocks) {
            cache.cache_blocks[p_cache_index].life = 0;
        }else {
            p_cache_index = swap_block(cache, walk_manager, walk_manager.global_blocks->nblocks);
            cache.cache_blocks[p_cache_index].life = 0;
            driver.load_block_info(cache, walk_manager.global_blocks, p_cache_index, pblk);
        }

        bid_t cache_index = (*(walk_manager.global_blocks))[cblk].cache_index;
        if(cache_index != walk_manager.global_blocks->nblocks) {
            cache.cache_blocks[cache_index].life = 0;
        }else {
            cache_index = swap_block(cache, walk_manager, p_cache_index);
            cache.cache_blocks[cache_index].life = 0;
            driver.load_block_info(cache, walk_manager.global_blocks, cache_index, cblk);
        }
        _m.stop_time("second_order_scheduler_swap_blocks");
        return transform<walk_data_t, walk_type>(pblk, cblk, walk_manager);
    }
};


template<typename BaseType>
class scheduler : public BaseType {
public:
    scheduler(metrics &m) : BaseType(m) { }

    template <typename walk_data_t, WalkType walk_type>
    bid_t schedule(graph_cache &cache, graph_driver &driver, graph_walk<walk_data_t, walk_type> &walk_manager) {
        return BaseType::schedule(cache, driver, walk_manager);
    }
};

#endif
