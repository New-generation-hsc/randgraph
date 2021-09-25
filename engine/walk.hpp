#ifndef _GRAPH_WALK_H_
#define _GRAPH_WALK_H_

#include <algorithm>
#include "api/types.hpp"
#include "api/graph_buffer.hpp"
#include "cache.hpp"

class graph_walk {
public:
    vid_t nvertices;
    eid_t nedges;
    tid_t nthreads;     /* number of threads */
    graph_block *global_blocks;

    std::vector<hid_t> maxhops;   /* record the block has at least `maxhops` to finished */
    std::vector<std::vector<wid_t>>     block_nmwalk;  /* record each block number of walks in memroy */
    std::vector<std::vector<wid_t>>     block_ndwalk;  /* record each block number of walks in disk */
    std::vector<int>       block_desc;     /* the descriptor of each block walk file */
    graph_buffer<walk_t> **block_walks;   /* the walk resident in memory */
    graph_buffer<walk_t>   walks;         /* the walks in cuurent block */

    graph_driver *global_driver;
    std::string base_name;                /* the dataset base name */

    graph_walk(graph_config& conf, graph_block & blocks, graph_driver &driver) {
        nvertices = conf.nvertices;
        nedges    = conf.nedges;
        nthreads = conf.nthreads;
        global_blocks = &blocks;
        base_name = conf.base_name;

        maxhops.resize(global_blocks->nblocks, 0);

        block_nmwalk.resize(global_blocks->nblocks);
        for(bid_t blk = 0; blk < global_blocks->nblocks; blk++) {
            block_nmwalk[blk].resize(nthreads);
            std::fill(block_nmwalk[blk].begin(), block_nmwalk[blk].end(), 0);
        }

        block_ndwalk.resize(global_blocks->nblocks);
        for(bid_t blk = 0; blk < global_blocks->nblocks; blk++) {
            block_ndwalk[blk].resize(nthreads);
            std::fill(block_ndwalk[blk].begin(), block_ndwalk[blk].end(), 0);
        }

        block_desc.resize(global_blocks->nblocks);
        for(bid_t blk = 0; blk < global_blocks->nblocks; blk++) { 
            std::string walk_name = get_walk_name(conf.base_name, blk);
            block_desc[blk] = open(walk_name.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_APPEND, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
        }
        
        block_walks = (graph_buffer<walk_t> **)malloc(global_blocks->nblocks * sizeof(graph_buffer<wid_t> *));
        for(bid_t blk = 0; blk < global_blocks->nblocks; blk++) {
            block_walks[blk] = (graph_buffer<walk_t> *)malloc(nthreads * sizeof(graph_buffer<walk_t>));
            for(tid_t tid = 0; tid < nthreads; tid++) {
                block_walks[blk][tid].alloc(MAX_TWALKS);
            }
        }

        global_driver = &driver;
    }

    ~graph_walk() {
        for(bid_t blk = 0; blk < global_blocks->nblocks; blk++) {
            close(block_desc[blk]);
            std::string walk_name = get_walk_name(base_name, blk);
            unlink(walk_name.c_str());
        }

        for(bid_t blk = 0; blk < global_blocks->nblocks; blk++) {
            for(tid_t tid = 0; tid < nthreads; tid++) {
                block_walks[blk][tid].destroy();
            }
           free(block_walks[blk]);
        }
        free(block_walks);
    }

    void move_walk(walk_t oldwalk, bid_t blk, tid_t t, vid_t dst, hid_t hop) {
        if (block_walks[blk][t].full()){
            persistent_walks(t, blk);
        }
        block_nmwalk[blk][t] += 1;
        walk_t newwalk = WALKER_MAKEUP(WALKER_SOURCE(oldwalk), dst, hop);
        block_walks[blk][t].push_back(newwalk);
        global_blocks->update_rank(dst);
    }

    void persistent_walks(tid_t t, bid_t blk) {
        block_ndwalk[blk][t] += block_walks[blk][t].size();
        block_nmwalk[blk][t] -= block_walks[blk][t].size();
        global_driver->dump_walk(block_desc[blk], block_walks[blk][t]);
        block_walks[blk][t].clear();
    }

    wid_t nwalks() {
        wid_t walksum = 0;
        for(bid_t blk = 0; blk < global_blocks->nblocks; blk++) {
            walksum += this->nblockwalks(blk);
        }
        return walksum;
    }

    wid_t ncwalks(graph_cache *cache) {
        wid_t walk_sum = 0;
        for(bid_t p = 0; p < cache->ncblock; p++) {
            if(cache->cache_blocks[p].block != NULL && cache->cache_blocks[p].block->status != INACTIVE) {
                bid_t blk = cache->cache_blocks[p].block->blk;
                walk_sum += this->nblockwalks(blk);
            }
        }
        return walk_sum;
    }

    wid_t nblockwalks(bid_t blk) {
        wid_t walksum = 0;
        for(tid_t t = 0; t < nthreads; t++) {
            walksum += block_nmwalk[blk][t] + block_ndwalk[blk][t];
        }
        return walksum;
    }

    wid_t nmwalks(bid_t exec_block) {
        wid_t walksum = 0;
        for(tid_t t = 0; t < nthreads; t++) {
            walksum += block_nmwalk[exec_block][t];
        }
        return walksum;
    }

    wid_t ndwalks(bid_t exec_block) { 
        wid_t walksum = 0;
        for(tid_t t = 0; t < nthreads; t++) {
            walksum += block_ndwalk[exec_block][t];
        }
        return walksum;
    }

    void load_walks(bid_t exec_block) {
        wid_t mwalk_count = this->nmwalks(exec_block), dwalk_count = this->ndwalks(exec_block);
        walks.alloc(mwalk_count + dwalk_count);
        global_driver->load_walk(block_desc[exec_block], dwalk_count, walks);
        
        /** load the in-memory */
        for(tid_t t = 0; t < nthreads; t++) {
            if(block_walks[exec_block][t].empty()) continue;
            for(wid_t w = 0; w < block_walks[exec_block][t].size(); w++) {
                walks.push_back(block_walks[exec_block][t][w]);
            }
        }
        assert(walks.size() == mwalk_count + dwalk_count);
    }

    void dump_walks(bid_t exec_block) {
        walks.destroy();
        std::fill(block_ndwalk[exec_block].begin(), block_ndwalk[exec_block].end(), 0);
        std::fill(block_nmwalk[exec_block].begin(), block_nmwalk[exec_block].end(), 0);
        ftruncate(block_desc[exec_block], 0);
        global_blocks->reset_rank(exec_block);
        maxhops[exec_block] = 0;

        /* clear the in-memory walks */
        for(tid_t t = 0; t < nthreads; t++) {
            block_walks[exec_block][t].clear();
        }
    }

    bool test_finished_walks() {
        return this->nwalks() == 0;
    }

    bool test_finished_cache_walks(graph_cache *cache) {
        return this->ncwalks(cache) == 0;
    }

    bid_t max_walks_block() {
        wid_t max_walks = 0;
        bid_t blk = 0;
        for(bid_t p = 0; p < global_blocks->nblocks; p++) {
            wid_t walk_cnt = this->nblockwalks(p);
            if(max_walks < walk_cnt) {
                max_walks = walk_cnt;
                blk = p;
            }
        }
        return blk;
    }

    void set_max_hop(bid_t blk, hid_t hop) {
        #pragma omp critical
        {
            if(maxhops[blk] < hop) maxhops[blk] = hop;
        }
    }

    bid_t max_hops_block() { 
        hid_t walk_hop = 0;
        bid_t blk = 0;
        for(bid_t p = 0; p < global_blocks->nblocks; p++) {
            if(maxhops[p] > walk_hop) {
                walk_hop = maxhops[p];
                blk = p;
            }
        }
        if(this->nblockwalks(blk) == 0) return max_walks_block();
        return blk;
    }

    bid_t choose_block(float prob) {
        float cc = (float)rand() / RAND_MAX;
        if(cc < prob) return max_hops_block();
        else return max_walks_block();
    }
};

#endif