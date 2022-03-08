#ifndef _GRAPH_SCHEDULE_H_
#define _GRAPH_SCHEDULE_H_

#include <limits>
#include <string>
#include <algorithm>
#include <utility>
#include <queue>
#include <random>
#include <numeric>

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
    bid_t first_block, second_block;

    template <typename walk_data_t, WalkType walk_type>
    void choose_blocks(graph_cache &cache, graph_walk<walk_data_t, walk_type> &walk_manager) {  }

    wid_t choose_blocks(graph_cache &cache, graph_walk<vid_t, SecondOrder> &walk_manager)
    {
        bid_t nblocks = walk_manager.global_blocks->nblocks;
        wid_t nwalks = 0;
        for(bid_t blk = 0; blk < nblocks; blk++) {
            for(bid_t sblk = blk; sblk < nblocks; sblk++) {
                wid_t walk_cnt = 0;
                for(bid_t index = 0; index < cache.ncblock; index++) {
                    if(cache.cache_blocks[index].block != NULL) {
                        bid_t cblk = cache.cache_blocks[index].block->blk;
                        if(cblk == blk || cblk == sblk) continue;
                        walk_cnt += walk_manager.nblockwalks(cblk * nblocks + blk);
                        walk_cnt += walk_manager.nblockwalks(cblk * nblocks + sblk);
                    }
                }

                walk_cnt += walk_manager.nblockwalks(blk * nblocks + sblk) + walk_manager.nblockwalks(blk * nblocks + blk);
                if(sblk != blk) {
                    walk_cnt += walk_manager.nblockwalks(sblk * nblocks + blk) + walk_manager.nblockwalks(sblk * nblocks + sblk);
                }

                if(walk_cnt > nwalks) {
                    nwalks = walk_cnt;
                    first_block = blk, second_block = sblk;
                }
            }
        }
        return nwalks;
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
    second_order_scheduler_t(metrics& m) : base_scheduler(m) {
        first_block = std::numeric_limits<bid_t>::max();
        second_block = std::numeric_limits<bid_t>::max();
    }

    template <typename walk_data_t, WalkType walk_type>
    bid_t schedule(graph_cache &cache, graph_driver &driver, graph_walk<walk_data_t, walk_type> &walk_manager) {
        if(second_block != std::numeric_limits<bid_t>::max()) {
            bid_t ret = second_block;
            second_block = std::numeric_limits<bid_t>::max();
            return ret;
        }

        choose_blocks(cache, walk_manager);

#ifdef TESTDEBUG
        logstream(LOG_DEBUG) << "second-order schedule blocks [" << first_block << ", " << second_block << "]" << std::endl;
#endif

        _m.start_time("second_order_scheduler_swap_blocks");
        /* increase the cache block life */
        for(bid_t p = 0; p < cache.ncblock; ++p) cache.cache_blocks[p].life++;
        bid_t p_cache_index = (*(walk_manager.global_blocks))[first_block].cache_index;
        if(p_cache_index != walk_manager.global_blocks->nblocks) {
            cache.cache_blocks[p_cache_index].life = 0;
        }else {
            p_cache_index = swap_block(cache, walk_manager, walk_manager.global_blocks->nblocks);
            cache.cache_blocks[p_cache_index].life = 0;
            driver.load_block_info(cache, walk_manager.global_blocks, p_cache_index, first_block);
        }

        bid_t cache_index = (*(walk_manager.global_blocks))[second_block].cache_index;
        if(cache_index != walk_manager.global_blocks->nblocks) {
            cache.cache_blocks[cache_index].life = 0;
        }else {
            cache_index = swap_block(cache, walk_manager, p_cache_index);
            cache.cache_blocks[cache_index].life = 0;
            driver.load_block_info(cache, walk_manager.global_blocks, cache_index, second_block);
        }
        _m.stop_time("second_order_scheduler_swap_blocks");
        bid_t ret = first_block;
        first_block = std::numeric_limits<bid_t>::max();
        return ret;
    }
};


/**
 * The following scheduler is the surfer scheduler
 * the scheduler selects one block at first, if the total walks is less than 1000, then choose two blocks
*/
class surfer_scheduler_t: public base_scheduler {
private:
    bid_t first_block, second_block;

    template <typename walk_data_t, WalkType walk_type>
    void choose_blocks(graph_cache &cache, graph_walk<walk_data_t, walk_type> &walk_manager) {  }

    wid_t choose_single_blocks(graph_cache &cache, graph_walk<vid_t, SecondOrder> &walk_manager)
    {
        bid_t nblocks = walk_manager.global_blocks->nblocks;
        wid_t nwalks = 0;
        for(bid_t blk = 0; blk < nblocks; blk++) {
            wid_t walk_cnt = 0;
            for(bid_t index = 0; index < cache.ncblock; index++) {
                if(cache.cache_blocks[index].block != NULL) {
                    bid_t cblk = cache.cache_blocks[index].block->blk;
                    if(cblk == blk) continue;
                    walk_cnt += walk_manager.nblockwalks(cblk * nblocks + blk);
                }
            }

            walk_cnt += walk_manager.nblockwalks(blk * nblocks + blk);
            if(walk_cnt > nwalks) {
                nwalks = walk_cnt;
                first_block = blk;
            }
        }
        second_block = std::numeric_limits<bid_t>::max();
        logstream(LOG_DEBUG) << "choose single block, block = " << first_block << ", nwalks = " << nwalks << std::endl;
        return nwalks;
    }

    wid_t choose_double_blocks(graph_cache &cache, graph_walk<vid_t, SecondOrder> &walk_manager)
    {
        bid_t nblocks = walk_manager.global_blocks->nblocks;
        wid_t nwalks = 0;
        for(bid_t blk = 0; blk < nblocks; blk++) {
            for(bid_t sblk = blk; sblk < nblocks; sblk++) {
                wid_t walk_cnt = 0;
                for(bid_t index = 0; index < cache.ncblock; index++) {
                    if(cache.cache_blocks[index].block != NULL) {
                        bid_t cblk = cache.cache_blocks[index].block->blk;
                        if(cblk != blk) walk_cnt += walk_manager.nblockwalks(cblk * nblocks + blk);
                        if(cblk != sblk && blk != sblk) walk_cnt += walk_manager.nblockwalks(cblk * nblocks + sblk);
                        // if(cblk == blk || cblk == sblk) continue;
                        // walk_cnt += walk_manager.nblockwalks(cblk * nblocks + blk);
                        // walk_cnt += walk_manager.nblockwalks(cblk * nblocks + sblk);
                    }
                }

                walk_cnt += walk_manager.nblockwalks(blk * nblocks + sblk) + walk_manager.nblockwalks(blk * nblocks + blk);
                if(sblk != blk) {
                    walk_cnt += walk_manager.nblockwalks(sblk * nblocks + blk) + walk_manager.nblockwalks(sblk * nblocks + sblk);
                }

                if(walk_cnt > nwalks) {
                    nwalks = walk_cnt;
                    first_block = blk, second_block = sblk;
                }
            }
        }
        if(first_block == second_block) second_block = std::numeric_limits<bid_t>::max();
        logstream(LOG_DEBUG) << "choose double block, block = " << first_block << " -> "<< second_block << ", nwalks = " << nwalks << std::endl;
        return nwalks;
    }

    void choose_blocks(graph_cache &cache, graph_walk<vid_t, SecondOrder> &walk_manager)
    {
        wid_t total_walks = walk_manager.nwalks();
        if(total_walks < 1000) {
            choose_double_blocks(cache, walk_manager);
        } else {
            wid_t nwalks = choose_single_blocks(cache, walk_manager);
            if(nwalks < 100) {
                choose_double_blocks(cache, walk_manager);
            }
        }
    }

    template<typename walk_data_t, WalkType walk_type>
    bid_t swap_block(graph_cache &cache, graph_walk<walk_data_t, walk_type>& walk_manager, bid_t exec_block, bid_t exclude_cache_index) {
        bid_t blk = 0;
        int life = -1;
        wid_t active_walks_cnt = 0xffffffff;
        bid_t nblocks = walk_manager.global_blocks->nblocks;
        for(bid_t p = 0; p < cache.ncblock; ++p) {
            if(p == exclude_cache_index) continue;
            if(cache.cache_blocks[p].block == NULL) {
                blk = p; break;
            }
            // if(walk_manager.block_active_walks(cache.cache_blocks[p].block->blk) < active_walks_cnt) {
            //     blk = p;
            //     life = cache.cache_blocks[p].life;
            //     active_walks_cnt = walk_manager.block_active_walks(cache.cache_blocks[p].block->blk);
            // } else if(walk_manager.block_active_walks(cache.cache_blocks[p].block->blk) == active_walks_cnt && cache.cache_blocks[p].life > life) {
            //     blk = p;
            //     life = cache.cache_blocks[p].life;
            // }
            bid_t cblk = cache.cache_blocks[p].block->blk;
            if(walk_manager.nblockwalks(cblk * nblocks + exec_block) < active_walks_cnt) {
                blk = p;
                life = cache.cache_blocks[p].life;
                active_walks_cnt = walk_manager.nblockwalks(cblk * nblocks + exec_block);
            }else if(walk_manager.nblockwalks(cblk * nblocks + exec_block) == active_walks_cnt && cache.cache_blocks[p].life > life) {
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
    surfer_scheduler_t(metrics& m) : base_scheduler(m) {
        first_block = std::numeric_limits<bid_t>::max();
        second_block = std::numeric_limits<bid_t>::max();
    }

    template <typename walk_data_t, WalkType walk_type>
    bid_t schedule(graph_cache &cache, graph_driver &driver, graph_walk<walk_data_t, walk_type> &walk_manager) {
        if(second_block != std::numeric_limits<bid_t>::max()) {
            bid_t ret = second_block;
            second_block = std::numeric_limits<bid_t>::max();
            return ret;
        }

        choose_blocks(cache, walk_manager);

#ifdef TESTDEBUG
        logstream(LOG_DEBUG) << "second-order schedule blocks [" << first_block << ", " << second_block << "]" << std::endl;
#endif

        _m.start_time("second_order_scheduler_swap_blocks");
        /* increase the cache block life */
        for(bid_t p = 0; p < cache.ncblock; ++p) cache.cache_blocks[p].life++;
        bid_t p_cache_index = (*(walk_manager.global_blocks))[first_block].cache_index;
        if(p_cache_index != walk_manager.global_blocks->nblocks) {
            cache.cache_blocks[p_cache_index].life = 0;
        }else {
            p_cache_index = swap_block(cache, walk_manager, first_block, walk_manager.global_blocks->nblocks);
            cache.cache_blocks[p_cache_index].life = 0;
            driver.load_block_info(cache, walk_manager.global_blocks, p_cache_index, first_block);
        }

        if(second_block != std::numeric_limits<bid_t>::max()) {
            bid_t cache_index = (*(walk_manager.global_blocks))[second_block].cache_index;
            if(cache_index != walk_manager.global_blocks->nblocks) {
                cache.cache_blocks[cache_index].life = 0;
            }else {
                cache_index = swap_block(cache, walk_manager, second_block, p_cache_index);
                cache.cache_blocks[cache_index].life = 0;
                driver.load_block_info(cache, walk_manager.global_blocks, cache_index, second_block);
            }
        }

        _m.stop_time("second_order_scheduler_swap_blocks");
        bid_t ret = first_block;
        first_block = std::numeric_limits<bid_t>::max();
        return ret;
    }
};

/**
 * The following scheduler is the surfer scheduler
 * the scheduler selects one block at first, if the total walks is less than 1000, then choose two blocks
 */
class simulated_annealing_scheduler_t : public base_scheduler
{
private:
    std::vector<bid_t> bucket_sequences;
    std::vector<bid_t> buckets;
    size_t index;


    template <typename walk_data_t, WalkType walk_type>
    void choose_blocks(graph_cache &cache, graph_walk<walk_data_t, walk_type> &walk_manager) {}

    void choose_blocks(graph_cache &cache, graph_driver &driver, graph_walk<vid_t, SecondOrder> &walk_manager)
    {
        std::unordered_set<bid_t> cache_blocks;
        for(bid_t blk = 0; blk < cache.ncblock; blk++) {
            if(cache.cache_blocks[blk].block != NULL) cache_blocks.insert(cache.cache_blocks[blk].block->blk);
        }

        bid_t nblocks = walk_manager.nblocks;
        std::vector<wid_t> block_walks(nblocks * nblocks);
        for(bid_t blk = 0; blk < nblocks * nblocks; blk++) {
            block_walks[blk] = walk_manager.nblockwalks(blk);
        }

        std::vector<wid_t> partition_walks(nblocks, 0);
        for(bid_t p_blk = 0; p_blk < nblocks; p_blk++) {
            for(bid_t c_blk = 0; c_blk < nblocks; c_blk++) {
                partition_walks[p_blk] += block_walks[p_blk * nblocks + c_blk];
                if(p_blk != c_blk) partition_walks[p_blk] += block_walks[c_blk * nblocks + p_blk];
            }
        }

        auto cmp = [&partition_walks, &walk_manager](bid_t u, bid_t v)
        {
            return (*walk_manager.global_blocks)[u].exp_walk_len * partition_walks[u] > (*walk_manager.global_blocks)[v].exp_walk_len * partition_walks[v];
        };

        std::vector<bid_t> block_indexs(nblocks, 0);
        std::iota(block_indexs.begin(), block_indexs.end(), 0);
        std::sort(block_indexs.begin(), block_indexs.end(), cmp);

        wid_t most_nwalks = 0;
        bid_t best_index = 0;
        for(bid_t p_index = cache.ncblock - 1; p_index < nblocks; p_index++) {
            wid_t nwalks = 0;
            for(bid_t c_index = 0; c_index < cache.ncblock - 1; c_index++) {
                nwalks += block_walks[block_indexs[p_index] * nblocks + block_indexs[c_index]] + block_walks[block_indexs[c_index] * nblocks + block_indexs[p_index]];
            }
            if(nwalks > most_nwalks) {
                best_index = p_index;
                most_nwalks = nwalks;
            }
        }
        std::swap(block_indexs[cache.ncblock - 1], block_indexs[best_index]);
        std::vector<bid_t> candidate_blocks(cache.ncblock);
        for (bid_t blk = 0; blk < cache.ncblock; blk++) candidate_blocks[blk] = block_indexs[blk];

        // auto cal_nwalks = [&block_walks, nblocks](const std::vector<bid_t>& blocks) {
        //     wid_t nwalks = 0;
        //     for(auto p_blk : blocks) {
        //         for(auto c_blk : blocks) {
        //             nwalks += block_walks[p_blk * nblocks + c_blk];
        //         }
        //     }
        //     return nwalks;
        // };

        auto cal_score = [&block_walks, &walk_manager, nblocks](const std::vector<bid_t>& blocks) {
            wid_t score = 0;
            for(auto p_blk : blocks) {
                for(auto c_blk : blocks) {
                    score += (*walk_manager.global_blocks)[c_blk].exp_walk_len * block_walks[p_blk * nblocks + c_blk];
                }
            }
            return score;
        };
        
        // real_t T = 100.0, alpha = 0.2;
        size_t max_iter = 30, iter = 0;
        size_t can_comm = 0;
        // wid_t can_nwalks = cal_nwalks(candidate_blocks);
        for(auto blk : candidate_blocks) if(cache_blocks.find(blk) != cache_blocks.end()) can_comm++;
        real_t y_can = cal_score(candidate_blocks) / (cache.ncblock - can_comm);
        
        // real_t y_can = (real_t)can_nwalks / (cache.ncblock - can_comm);
        // std::cout << "candidate walks : " << can_nwalks << ", y_can : " << y_can << std::endl;

        // std::cout << "block index : ";
        // for(auto blk : block_indexs) std::cout << blk << " ";
        // std::cout << std::endl;

        while(iter < max_iter) {
            std::vector<bid_t> tmp_blocks = candidate_blocks;
            size_t pos = rand() % (nblocks - cache.ncblock) + cache.ncblock, tmp_pos = rand() % cache.ncblock;
            std::swap(tmp_blocks[tmp_pos], block_indexs[pos]);
            // wid_t tmp_nwalks = cal_nwalks(tmp_blocks);
            size_t tmp_comm = 0;
            for(auto blk : tmp_blocks) if(cache_blocks.find(blk) != cache_blocks.end()) tmp_comm++;
            // real_t y_tmp = (real_t)tmp_nwalks / (cache.ncblock - tmp_comm);
            real_t y_tmp = cal_score(tmp_blocks) / (cache.ncblock - tmp_comm);

            if(y_tmp > y_can) {
                // std::cout << "candidate blocks : ";
                // for(auto blk : candidate_blocks) std::cout << blk << " ";
                // std::cout << std::endl;
                // std::cout << "tmp blocks : ";
                // for(auto blk : tmp_blocks) std::cout << blk << " ";
                // std::cout << std::endl;
                candidate_blocks = tmp_blocks;
                y_can = y_tmp;
                // std::cout << "tmp walks : " << tmp_nwalks << ", tmp_can : " << y_tmp  << ", pos" << pos << std::endl;
            } else {
                std::swap(tmp_blocks[tmp_pos], block_indexs[pos]);
            }
            iter++;
        }

        buckets = candidate_blocks;
        std::unordered_set<bid_t> bucket_uncached, bucket_cached;
        
        for(bid_t blk = 0; blk < buckets.size(); blk++) {
            if(cache_blocks.find(buckets[blk]) != cache_blocks.end()) {
                bucket_cached.insert(buckets[blk]);
            }else{
                bucket_uncached.insert(buckets[blk]);
            }
        }

        size_t pos = 0;
        for(auto blk : bucket_cached) {
            bid_t cache_index = (*(walk_manager.global_blocks))[blk].cache_index;
            swap(cache.cache_blocks[pos], cache.cache_blocks[cache_index]);
            cache.cache_blocks[pos].block->cache_index = pos;
            cache.cache_blocks[cache_index].block->cache_index = cache_index;
            std::cout << "swap block info, blk = " << blk << ", from " << cache_index << " to " << pos << std::endl;
            pos++;
        }
        
        for(auto blk : bucket_uncached) {
            if(cache.cache_blocks[pos].block != NULL) {
                cache.cache_blocks[pos].block->cache_index = nblocks;
            }
            std::cout << "load block info, blk = " << blk << " -> cache_index = " << pos << std::endl;
            driver.load_block_info(cache, walk_manager.global_blocks, pos, blk);
            pos++;
        }

        std::cout << "bucket sequence : ";
        for(auto p_blk : buckets) {
            for(auto c_blk : buckets) {
                if(block_walks[p_blk * nblocks + c_blk] > 0) {
                    std::cout << p_blk << " -> " << c_blk << ", ";
                    bucket_sequences.push_back(p_blk * nblocks + c_blk);
                }
            }
        }
        std::cout << std::endl;
    }

public:
    simulated_annealing_scheduler_t(metrics &m) : base_scheduler(m)
    {
        index = 0;
    }

    template <typename walk_data_t, WalkType walk_type>
    bid_t schedule(graph_cache &cache, graph_driver &driver, graph_walk<walk_data_t, walk_type> &walk_manager)
    {
        _m.start_time("simulated_annealing_scheduler_swap_blocks");
        if(index == bucket_sequences.size()) {
            bucket_sequences.clear();
            buckets.clear();
            choose_blocks(cache, driver, walk_manager);
            for(auto blk : buckets) std::cout << blk << " ";
            std::cout << std::endl;
            index = 0;
        }
        _m.stop_time("simulated_annealing_scheduler_swap_blocks");
        return bucket_sequences[index++];
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
