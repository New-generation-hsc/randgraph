#ifndef _GRAPH_WALK_H_
#define _GRAPH_WALK_H_

#include <algorithm>
#include "api/types.hpp"
#include "api/graph_buffer.hpp"
#include "cache.hpp"

class block_desc_manager_t {
private:
    int desc;
public:
    block_desc_manager_t(const std::string&& file_name) {
        desc = open(file_name.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_APPEND, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
    }
    ~block_desc_manager_t() {
        if(desc > 0) close(desc);
    }
    int get_desc() const { return desc; }
};

template<WalkType walk_type>
inline bid_t total_blocks(bid_t nblocks) {
    return nblocks;
}

template<>
inline bid_t total_blocks<SecondOrder>(bid_t nblocks) {
    return nblocks * nblocks;
}

template<typename walk_data_t, WalkType walk_type> 
class graph_walk {
public:
    std::string base_name;  /* the dataset base name, indicate the walks store path */
    vid_t nvertices;
    bid_t nblocks;
    tid_t nthreads;
    graph_driver *global_driver;
    std::vector<hid_t> maxhops;                         /* record the block has at least `maxhops` to finished */
    graph_buffer<walker_t<walk_data_t>> walks;          /* the walks in current block */
    graph_buffer<walker_t<walk_data_t>> **block_walks;  /* the walk resident in memroy */
    std::vector<std::vector<wid_t>> block_nmwalk;       /* record each block number of walks in memroy */
    std::vector<std::vector<wid_t>> block_ndwalk;       /* record each block number of walks in disk */
    graph_block *global_blocks;
    graph_walk(const std::string& name, vid_t nverts, tid_t threads, graph_driver& driver, graph_block &blocks) {
        base_name = name;
        nvertices = nverts;
        nthreads = threads;
        global_driver = &driver;
        global_blocks = &blocks;
        nblocks = global_blocks->nblocks;

        bid_t totblocks = total_blocks<walk_type>(nblocks);
        maxhops.resize(totblocks, 0);

        block_nmwalk.resize(totblocks);
        for (bid_t blk = 0; blk < totblocks; blk++)
        {
            block_nmwalk[blk].resize(nthreads);
            std::fill(block_nmwalk[blk].begin(), block_nmwalk[blk].end(), 0);
        }

        block_ndwalk.resize(totblocks);
        for (bid_t blk = 0; blk < totblocks; blk++)
        {
            block_ndwalk[blk].resize(nthreads);
            std::fill(block_ndwalk[blk].begin(), block_ndwalk[blk].end(), 0);
        }

        block_walks = (graph_buffer<walker_t<walk_data_t>> **)malloc(totblocks * sizeof(graph_buffer<walker_t<walk_data_t>> *));
        for (bid_t blk = 0; blk < totblocks; blk++)
        {
            block_walks[blk] = (graph_buffer<walker_t<walk_data_t>> *)malloc(nthreads * sizeof(graph_buffer<walker_t<walk_data_t>>));
            for (tid_t tid = 0; tid < nthreads; tid++)
            {
                block_walks[blk][tid].alloc(MAX_TWALKS);
            }
        }
    }

    ~graph_walk()
    {
        for (bid_t blk = 0; blk < total_blocks<walk_type>(this->nblocks); blk++)
        {
            for (tid_t tid = 0; tid < nthreads; tid++)
            {
                block_walks[blk][tid].destroy();
            }
            free(block_walks[blk]);
        }
        free(block_walks);

        for (bid_t blk = 0; blk < total_blocks<walk_type>(this->nblocks); blk++)
        {
            std::string walk_name = get_walk_name(base_name, blk);
            if(test_exists(walk_name)) unlink(walk_name.c_str());
        }
    }

    void move_walk(const walker_t<walk_data_t> &walker)
    {
        tid_t t = static_cast<vid_t>(omp_get_thread_num());
        bid_t blk = walk_data_block<walk_data_t, walk_type>::get(walker, global_blocks);
        if(block_walks[blk][t].full()) {
            persistent_walks(blk, t);
        }

        block_nmwalk[blk][t] += 1;
        block_walks[blk][t].push_back(walker);
        global_blocks->update_rank(WALKER_POS(walker));
    }

    void persistent_walks(bid_t blk, tid_t t)
    {
        block_ndwalk[blk][t] += block_walks[blk][t].size();
        block_nmwalk[blk][t] -= block_walks[blk][t].size();
        block_desc_manager_t block_desc(get_walk_name(base_name, blk));
        global_driver->dump_walk(block_desc.get_desc(), block_walks[blk][t]);
        block_walks[blk][t].clear();
    }

    wid_t nblockwalks(bid_t blk)
    {
        wid_t walksum = 0;
        for (tid_t t = 0; t < nthreads; t++)
        {
            walksum += block_nmwalk[blk][t] + block_ndwalk[blk][t];
        }
        return walksum;
    }

    wid_t nmwalks(bid_t exec_block)
    {
        wid_t walksum = 0;
        for (tid_t t = 0; t < nthreads; t++)
        {
            walksum += block_nmwalk[exec_block][t];
        }
        return walksum;
    }

    wid_t ndwalks(bid_t exec_block)
    {
        wid_t walksum = 0;
        for (tid_t t = 0; t < nthreads; t++)
        {
            walksum += block_ndwalk[exec_block][t];
        }
        return walksum;
    }

    wid_t ncwalks(graph_cache *cache)
    {
        wid_t walk_sum = 0;
        for (bid_t p = 0; p < cache->ncblock; p++)
        {
            if (cache->cache_blocks[p].block != NULL && cache->cache_blocks[p].block->status != INACTIVE)
            {
                bid_t blk = cache->cache_blocks[p].block->blk;
                walk_sum += this->nblockwalks(blk);
            }
        }
        return walk_sum;
    }

    wid_t nwalks()
    {
        wid_t walksum = 0;
        for (bid_t blk = 0; blk < total_blocks<walk_type>(nblocks); blk++)
        {
            walksum += this->nblockwalks(blk);
        }
        return walksum;
    }

    void load_walks(bid_t exec_block)
    {
        wid_t mwalk_count = this->nmwalks(exec_block), dwalk_count = this->ndwalks(exec_block);
        walks.alloc(mwalk_count + dwalk_count);
        block_desc_manager_t block_desc(get_walk_name(base_name, exec_block));
        global_driver->load_walk(block_desc.get_desc(), dwalk_count, walks);

        /** load the in-memory */
        for (tid_t t = 0; t < nthreads; t++)
        {
            if (block_walks[exec_block][t].empty())
                continue;
            for (wid_t w = 0; w < block_walks[exec_block][t].size(); w++)
            {
                walks.push_back(block_walks[exec_block][t][w]);
            }
        }
        assert(walks.size() == mwalk_count + dwalk_count);
    }

    void dump_walks(bid_t exec_block)
    {
        walks.destroy();
        std::fill(block_ndwalk[exec_block].begin(), block_ndwalk[exec_block].end(), 0);
        std::fill(block_nmwalk[exec_block].begin(), block_nmwalk[exec_block].end(), 0);
        block_desc_manager_t block_desc(get_walk_name(base_name, exec_block));
        ftruncate(block_desc.get_desc(), 0);
        global_blocks->reset_rank(exec_block % nblocks);
        maxhops[exec_block] = 0;

        /* clear the in-memory walks */
        for (tid_t t = 0; t < nthreads; t++)
        {
            block_walks[exec_block][t].clear();
        }
    }

    bool test_finished_walks()
    {
        return this->nwalks() == 0;
    }

    bool test_finished_cache_walks(graph_cache *cache)
    {
        return this->ncwalks(cache) == 0;
    }

    bid_t max_walks_block()
    {
        wid_t max_walks = 0;
        bid_t blk = 0;
        for (bid_t p = 0; p < total_blocks<walk_type>(this->nblocks); p++)
        {
            wid_t walk_cnt = this->nblockwalks(p);
            if (max_walks < walk_cnt)
            {
                max_walks = walk_cnt;
                blk = p;
            }
        }
        return blk;
    }

    void set_max_hop(const walker_t<walk_data_t>& walker) {
        bid_t blk = walk_data_block<walk_data_t, walk_type>::get(walker, global_blocks);
        hid_t hop = WALKER_HOP(walker);
        #pragma omp critical
        {
            if(maxhops[blk] < hop) maxhops[blk] = hop;
        }
    }

    void set_max_hop(bid_t blk, hid_t hop)
    {
        #pragma omp critical
        {
            if (maxhops[blk] < hop) maxhops[blk] = hop;
        }
    }

    bid_t max_hops_block()
    {
        hid_t walk_hop = 0;
        bid_t blk = 0;
        for (bid_t p = 0; p < total_blocks<walk_type>(this->nblocks); p++)
        {
            if (maxhops[p] > walk_hop)
            {
                walk_hop = maxhops[p];
                blk = p;
            }
        }
        if (this->nblockwalks(blk) == 0)
            return max_walks_block();
        return blk;
    }

    bid_t choose_block(float prob)
    {
        float cc = (float)rand() / RAND_MAX;
        if (cc < prob)
            return max_hops_block();
        else
            return max_walks_block();
    }

    wid_t block_active_walks(bid_t blk) { return 0; }
};

template<>
wid_t graph_walk<empty_data_t, FirstOrder>::block_active_walks(bid_t blk) {
    return this->nblockwalks(blk);
}

template<>
wid_t graph_walk<vid_t, SecondOrder>::block_active_walks(bid_t blk) {
    wid_t walks_cnt = 0;
    for(bid_t p = 0; p < nblocks; p++) {
        walks_cnt += this->nblockwalks(p * nblocks + blk);
        walks_cnt += this->nblockwalks(blk * nblocks + p);
    }
    walks_cnt -= this->nblockwalks(blk * nblocks + blk);
    return walks_cnt;
}

#endif
