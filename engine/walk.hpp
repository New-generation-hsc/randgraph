#ifndef _GRAPH_WALK_H_
#define _GRAPH_WALK_H_

#include <algorithm>
#include "api/types.hpp"
#include "api/graph_buffer.hpp"
#include "cache.hpp"

struct walk_t { 
    hid_t hop   : 16;
    vid_t pos    : 24;   /* current walk current pos vertex */
    vid_t source : 24;   /* walk source vertex */
};

class graph_walk {
public:
    vid_t nvertices;
    eid_t nedges;
    tid_t nthreads;     /* number of threads */
    graph_block *global_blocks;

    std::vector<wid_t>     block_nmwalk;  /* record each block number of walks in memroy */
    std::vector<wid_t>     block_ndwalk;  /* record each block number of walks in disk */
    std::vector<int>       block_desc;     /* the descriptor of each block walk file */
    graph_buffer<walk_t> **block_walks;   /* the walk resident in memory */
    graph_buffer<walk_t>   walks;         /* the walks in cuurent block */

    graph_driver *global_driver;

    graph_walk(graph_config& conf, graph_block & blocks, graph_driver &driver) {
        nvertices = conf.nvertices;
        nedges    = conf.nedges;
        nthreads = conf.nthreads;
        global_blocks = &blocks;

        block_nmwalk.resize(global_blocks->nblocks);
        std::fill(block_nmwalk.begin(), block_nmwalk.end(), 0);
        block_ndwalk.resize(global_blocks->nblocks);
        std::fill(block_ndwalk.begin(), block_ndwalk.end(), 0);

        block_desc.resize(global_blocks->nblocks);
        for(bid_t blk = 0; blk < global_blocks->nblocks; blk++) { 
            std::string walk_name = get_walk_name(conf.base_name, blk);
            block_desc[blk] = open(walk_name.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
        }
        
        block_walks = (graph_buffer<walk_t> **)malloc(nthreads * sizeof(graph_buffer<wid_t> *));
        for(tid_t tid = 0; tid < nthreads; tid++) {
            block_walks[tid] = (graph_buffer<walk_t> *)malloc(global_blocks->nblocks * sizeof(graph_buffer<walk_t>));
            for(bid_t blk = 0; blk < global_blocks->nblocks; blk++) {
                block_walks[tid][blk].alloc(MAX_TWALKS);
            }
        }

        global_driver = &driver;
    }

    ~graph_walk() {
        for(bid_t blk = 0; blk < global_blocks->nblocks; blk++) {
            close(block_desc[blk]);
        }

        for(tid_t tid = 0; tid < nthreads; tid++) {
            for(bid_t blk = 0; blk < global_blocks->nblocks; blk++) {
                block_walks[tid][blk].destroy();
            }
           free(block_walks[tid]);
        }
        free(block_walks);
    }

    void move_walk(walk_t oldwalk, bid_t blk, tid_t t, vid_t dst, hid_t hop) {
        block_nmwalk[blk] += 1;
        walk_t newwalk = walk_recode(oldwalk, hop, dst);
        block_walks[t][blk].push_back(newwalk);
        global_blocks->update_rank(dst);
        if(block_walks[t][blk].full()) {
            persistent_walks(t, blk);
        }
    }

    void persistent_walks(tid_t t, bid_t blk) {
        global_driver->dump_walk(block_desc[blk], block_walks[t][blk]);
        block_ndwalk[blk] += block_walks[t][blk].size();
        block_nmwalk[blk] -= block_walks[t][blk].size();
        block_walks[t][blk].clear();
    }

    wid_t nwalks() {
        wid_t walksum = 0;
        for(bid_t blk = 0; blk < global_blocks->nblocks; blk++) {
            walksum += block_nmwalk[blk] + block_ndwalk[blk];
        }
        return walksum;
    }

    wid_t ncwalks(graph_cache *cache) {
        wid_t walk_sum = 0;
        for(bid_t p = 0; p < cache->nrblock; p++) {
            bid_t blk = cache->cache_blocks[p].block->blk;
            walk_sum += this->block_nmwalk[blk] + this->block_ndwalk[blk];
        }
        return walk_sum;
    }

    wid_t nblockwalks(bid_t blk) {
        return block_nmwalk[blk] + block_ndwalk[blk];
    }

    void load_walks(bid_t exec_block) {
        wid_t walk_count = block_nmwalk[exec_block] + block_ndwalk[exec_block];
        walks.alloc(walk_count);
        global_driver->load_walk(block_desc[exec_block], walk_count, walks);
    }

    void dump_walks(bid_t exec_block) {
        global_driver->dump_walk(block_desc[exec_block], walks);
        walks.destroy();
    }

    void cleanup(bid_t exec_block) {
        block_ndwalk[exec_block] = 0;
        block_nmwalk[exec_block] = 0;
        ftruncate(block_desc[exec_block], 0);
        global_blocks->reset_rank(exec_block);
    }

    bool test_finished_walks() {
        return this->nwalks() == 0;
    }

    bool test_finished_cache_walks(graph_cache *cache) {
        return this->ncwalks(cache) == 0;
    }
};

walk_t walk_encode(hid_t hop, vid_t curr, vid_t source) {
    walk_t walk;
    walk.hop   = hop;
    walk.pos    = curr & 0xffffff;
    walk.source = source & 0xffffff;
    return walk;
}

walk_t walk_recode(walk_t walk, hid_t hop, vid_t curr) {
    walk.hop = hop;
    walk.pos  = curr & 0xffffff;
    return walk;
}

#endif