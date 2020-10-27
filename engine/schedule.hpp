#ifndef _GRAPH_SCHEDULE_H_
#define _GRAPH_SCHEDULE_H_

#include <string>
#include <algorithm>
#include <utility>
#include <queue>

#include "cache.hpp"
#include "config.hpp"
#include "driver.hpp"
#include "util/util.hpp"
#include "util/io.hpp"

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
    graph_block *global_blocks;

public:
    virtual ~scheduler() { }
    virtual void schedule(graph_cache& cache, graph_driver& driver) = 0;

};

class graph_scheduler : public scheduler {
private:
    int vertdesc, edgedesc, degdesc;  /* the beg_pos, csr, degree file descriptor */

public:
    graph_scheduler(graph_config *conf, graph_block& blocks) {
        
        std::string beg_pos_name    = get_beg_pos_name(conf->base_name, conf->fnum);
        std::string csr_name        = get_csr_name(conf->base_name, conf->fnum);
        std::string degree_name     = get_degree_name(conf->base_name, conf->fnum);

        vertdesc = open(beg_pos_name.c_str(), O_RDONLY);
        edgedesc = open(csr_name.c_str(), O_RDONLY);
        degdesc  = open(degree_name.c_str(), O_RDONLY);

        global_blocks = &blocks;
    }

    /** If the cache block has no walk, then swap out all blocks */
    void schedule(graph_cache& cache, graph_driver& driver) { 
        bid_t blk = 0;
        std::vector<bid_t> blocks = choose_blocks(cache.ncblock);
        for(; blk < blocks.size(); blk++) {
            bid_t p = blocks[blk];
            global_blocks->blocks[p].status = ACTIVE;
            cache.cache_blocks[blk].block  = &global_blocks->blocks[p];
            cache.cache_blocks[blk].beg_pos = (eid_t*)realloc(cache.cache_blocks[blk].beg_pos, (global_blocks->blocks[p].nverts + 1) * sizeof(eid_t));
            cache.cache_blocks[blk].csr     = (vid_t*)realloc(cache.cache_blocks[blk].csr   , global_blocks->blocks[p].nedges * sizeof(vid_t));

            driver.load_block_vertex(vertdesc, cache.cache_blocks[blk].beg_pos, global_blocks->blocks[p]);
            driver.load_block_edge(edgedesc,  cache.cache_blocks[blk].csr,    global_blocks->blocks[p]);
        }

        cache.nrblock = blocks.size();

        for(; blk < cache.ncblock; blk++) {
            if(cache.cache_blocks[blk].block) {
                cache.cache_blocks[blk].block->status = INACTIVE;
            }
        }
    }

    std::vector<bid_t> choose_blocks(bid_t ncblocks) { 
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

#endif