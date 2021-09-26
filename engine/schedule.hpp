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

/** graph_scheduler
 * 
 * This file contribute to define the interface how to schedule cache blocks
 */

class scheduler {
protected:
    int vertdesc, edgedesc, degdesc;  /* the beg_pos, csr, degree file descriptor */
    metrics &_m;

public:
    scheduler(graph_config *conf, metrics& m) : _m(m) {
        std::string beg_pos_name    = get_beg_pos_name(conf->base_name, conf->fnum);
        std::string csr_name        = get_csr_name(conf->base_name, conf->fnum);
        std::string degree_name     = get_degree_name(conf->base_name, conf->fnum);

        vertdesc = open(beg_pos_name.c_str(), O_RDONLY);
        edgedesc = open(csr_name.c_str(), O_RDONLY);
        degdesc  = open(degree_name.c_str(), O_RDONLY);
    }
    ~scheduler() { 
        close(vertdesc);
        close(edgedesc);
        close(degdesc);
    }
    virtual bid_t schedule(graph_cache& cache, graph_driver& driver, graph_walk &walk_manager) = 0;

};

class graph_scheduler : public scheduler {
private:
    bid_t exec_blk;                   /* the current cache block index used for run */
    bid_t nrblock;                    /* number of cache blocks are used for running */
public:
    graph_scheduler(graph_config *conf, metrics &m) : scheduler(conf, m) {
        exec_blk = 0;
        nrblock  = 0;
    }

    /** If the cache block has no walk, then swap out all blocks */
    bid_t schedule(graph_cache& cache, graph_driver& driver, graph_walk &walk_manager) { 
        if(walk_manager.test_finished_cache_walks(&cache)) {
            swap_blocks(cache, driver, walk_manager.global_blocks);
        }
        bid_t ret = exec_blk;
        exec_blk++;
        if(exec_blk >= nrblock) exec_blk = 0;
        return ret;
    }

    void swap_blocks(graph_cache& cache, graph_driver& driver, graph_block* global_blocks) { 
        // set the all cached blocks inactive */
        _m.start_time("graph_scheduler_swap_blocks");
        for(bid_t p = 0; p < cache.ncblock; p++) {
            if(cache.cache_blocks[p].block != NULL) {
                cache.cache_blocks[p].block->status = INACTIVE;
            }
        }

        bid_t blk = 0;
        std::vector<bid_t> blocks = choose_blocks(cache.ncblock, global_blocks);
        // for(; blk < blocks.size(); blk++) {
        //     bid_t p = blocks[blk];
        //     cache.cache_blocks[blk].block  = &global_blocks->blocks[p];
        //     cache.cache_blocks[blk].block->status = ACTIVE;
        //     cache.cache_blocks[blk].beg_pos = (eid_t*)realloc(cache.cache_blocks[blk].beg_pos, (global_blocks->blocks[p].nverts + 1) * sizeof(eid_t));
        //     cache.cache_blocks[blk].csr     = (vid_t*)realloc(cache.cache_blocks[blk].csr   , global_blocks->blocks[p].nedges * sizeof(vid_t));

        //     driver.load_block_vertex(vertdesc, cache.cache_blocks[blk].beg_pos, global_blocks->blocks[p]);
        //     driver.load_block_edge(edgedesc,  cache.cache_blocks[blk].csr,    global_blocks->blocks[p]);
        // }

        // nrblock = blocks.size();
        // exec_blk = 0;

        // for(; blk < cache.ncblock; blk++) {
        //     if(cache.cache_blocks[blk].block) {
        //         cache.cache_blocks[blk].block = NULL;
        //     }
        // }

        std::vector<bid_t> noncached_blocks;
        for(const auto & p : blocks) {
            bid_t cache_index = 0;
            if(cache.test_block_cached(p, cache_index)) {
                std::swap(cache[cache_index], cache[blk]);
                blk++;
            }else noncached_blocks.push_back(p);
        }

        for(const auto & p : noncached_blocks) {
            cache.cache_blocks[blk].block  = &global_blocks->blocks[p];
            cache.cache_blocks[blk].block->status = ACTIVE;
            cache.cache_blocks[blk].beg_pos = (eid_t*)realloc(cache.cache_blocks[blk].beg_pos, (global_blocks->blocks[p].nverts + 1) * sizeof(eid_t));
            cache.cache_blocks[blk].csr     = (vid_t*)realloc(cache.cache_blocks[blk].csr   , global_blocks->blocks[p].nedges * sizeof(vid_t));

            driver.load_block_vertex(vertdesc, cache.cache_blocks[blk].beg_pos, global_blocks->blocks[p]);
            driver.load_block_edge(edgedesc,  cache.cache_blocks[blk].csr,    global_blocks->blocks[p]);
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


/**
 * The following schedule scheme follow the graph walker scheme
 */
class walk_schedule_t : public scheduler {
private:
    float prob;
    bid_t exec_blk;
public:
    walk_schedule_t(graph_config* conf, float p, metrics &m) : scheduler(conf, m) {
        // nothing need to initialize
        prob = p;
        exec_blk = 0;
    }

    bid_t schedule(graph_cache& cache, graph_driver& driver, graph_walk &walk_manager) {
        bid_t blk = walk_manager.choose_block(prob);
        if(cache.test_block_cached(blk, exec_blk)) {
            return exec_blk;
        }
        _m.start_time("walk_schedule_swap_blocks");
        graph_block *global_blocks = walk_manager.global_blocks;
        exec_blk = swap_block(cache, walk_manager);
        cache.cache_blocks[exec_blk].block = &global_blocks->blocks[blk];
        cache.cache_blocks[exec_blk].block->status = ACTIVE;

        cache.cache_blocks[exec_blk].beg_pos = (eid_t*)realloc(cache.cache_blocks[exec_blk].beg_pos, (global_blocks->blocks[blk].nverts + 1) * sizeof(eid_t));
        cache.cache_blocks[exec_blk].csr     = (vid_t*)realloc(cache.cache_blocks[exec_blk].csr   , global_blocks->blocks[blk].nedges * sizeof(vid_t));

        driver.load_block_vertex(vertdesc, cache.cache_blocks[exec_blk].beg_pos, global_blocks->blocks[blk]);
        driver.load_block_edge(edgedesc,  cache.cache_blocks[exec_blk].csr,    global_blocks->blocks[blk]);
        _m.stop_time("walk_schedule_swap_blocks");
        return exec_blk;
    }

    bid_t swap_block(graph_cache& cache, graph_walk &walk_mangager) {
        wid_t walks_cnt = 0xffffffff;
        bid_t blk = 0;
        int life = -1;
        for(bid_t p = 0; p < cache.ncblock; p++) {
            if(cache.cache_blocks[p].block == NULL) {
                blk = p; break;
            }
            wid_t cnt = walk_mangager.nblockwalks(cache.cache_blocks[p].block->blk);
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
        return blk;
    }
};

#endif