#ifndef _GRAPH_WALK_H_
#define _GRAPH_WALK_H_

#include <algorithm>
#include "api/types.hpp"
#include "api/graph_buffer.hpp"
#include "util/hash.hpp"
#include "cache.hpp"

class block_desc_manager_t {
private:
    int desc;
public:
    block_desc_manager_t(const std::string&& file_name) {
        desc = open(file_name.c_str(), O_RDWR | O_CREAT | O_APPEND, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
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

    bool load_bf;

    // BloomFilter *bf;
    graph_walk(graph_config& conf, graph_driver& driver, graph_block &blocks) {
        base_name = conf.base_name;
        nvertices = conf.nvertices;
        nthreads = conf.nthreads;
        global_driver = &driver;
        global_blocks = &blocks;
        nblocks = global_blocks->nblocks;

        bid_t totblocks = total_blocks<walk_type>(nblocks);
        maxhops.resize(totblocks, 0);

        walks.alloc(nthreads * MAX_TWALKS);

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

        for (bid_t blk = 0; blk < total_blocks<walk_type>(this->nblocks); blk++)
        {
            std::string walk_name = get_walk_name(base_name, blk);
            if(test_exists(walk_name)) unlink(walk_name.c_str());
        }

        // bf = nullptr;
        // if(conf.filter && conf.reordered) {
        //     std::string filter_name = get_bloomfilter_name(base_name, 0);
        //     bf = new BloomFilter();
        //     bf->load_bloom_filter(filter_name);
        // }
        load_bf = conf.filter;
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
        walks.destroy();

        // if(bf) delete bf;
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
        appendfile(get_walk_name(base_name, blk), block_walks[blk][t].buffer_begin(), block_walks[blk][t].size());
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

    size_t load_memory_walks(bid_t exec_block) {
        wid_t mwalk_count = this->nmwalks(exec_block);
        walks.clear();

        /* load in memory walks */
        for (tid_t t = 0; t < nthreads; t++)
        {
            if (block_walks[exec_block][t].empty())
                continue;
            for (wid_t w = 0; w < block_walks[exec_block][t].size(); w++)
            {
                walks.push_back(block_walks[exec_block][t][w]);
            }
        }

        /* clear memory walks */
        std::fill(block_nmwalk[exec_block].begin(), block_nmwalk[exec_block].end(), 0);
        for (tid_t t = 0; t < nthreads; t++)
        {
            block_walks[exec_block][t].clear();
        }

        assert(mwalk_count == walks.size());
        return walks.size();
    }

    size_t load_disk_walks(bid_t exec_block, wid_t walk_cnt, wid_t loaded_walks) {
        walks.clear();
        block_desc_manager_t block_desc(get_walk_name(base_name, exec_block));
        global_driver->load_walk(block_desc.get_desc(), walk_cnt, loaded_walks, walks);
        return walks.size();
    }

    void dump_walks(bid_t exec_block)
    {
        std::fill(block_ndwalk[exec_block].begin(), block_ndwalk[exec_block].end(), 0);
        block_desc_manager_t block_desc(get_walk_name(base_name, exec_block));
        ftruncate(block_desc.get_desc(), 0);

        global_blocks->reset_rank(exec_block % nblocks);
        maxhops[exec_block] = 0;
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

struct block_walk_state_t {
    wid_t num_mem_walks, num_disk_walks, disk_load_walks;
};

template<WalkType walk_type>
struct block_walks_impl_t {
    bool has_finished() const { return false; }
    template<typename walk_data_t>
    size_t query_block_state(graph_walk<walk_data_t, walk_type> &walk_manager, graph_cache &cache, bid_t exec_block) {
        return 0;
    }
    template<typename walk_data_t>
    size_t load_walks(graph_walk<walk_data_t, walk_type> &walk_manager, bid_t exec_block) {
        return 0;
    }
};

template<>
struct block_walks_impl_t<FirstOrder> {
    block_walk_state_t state;

    block_walks_impl_t() {  }
    bool has_finished() const {
        return state.num_mem_walks == 0 && state.num_disk_walks == 0;
    }

    template<typename walk_data_t>
    size_t query_block_state(graph_walk<walk_data_t, FirstOrder> &walk_manager, graph_cache &cache, bid_t exec_block) {
        state.num_mem_walks = walk_manager.nmwalks(exec_block);
        state.num_disk_walks = walk_manager.ndwalks(exec_block);
        state.disk_load_walks = 0;
        return state.num_mem_walks + state.num_disk_walks;
    }

    template<typename walk_data_t>
    size_t load_walks(graph_walk<walk_data_t, FirstOrder> &walk_manager, bid_t exec_block) {
        size_t nwalks = 0;
        if(state.num_mem_walks > 0) {
            nwalks = walk_manager.load_memory_walks(exec_block);
            state.num_mem_walks = 0;
        } else if(state.num_disk_walks > 0) {
            wid_t interval_max_walks = 32 * MAX_TWALKS;
            wid_t interval_walks = std::min(state.num_disk_walks, interval_max_walks);
            state.num_disk_walks -= interval_walks;
            nwalks = walk_manager.load_disk_walks(exec_block, interval_walks, state.disk_load_walks);
            state.disk_load_walks += interval_walks;
        }

        if(state.num_disk_walks == 0) {
            walk_manager.dump_walks(exec_block);
        }
        return nwalks;
    }
};

template<>
struct block_walks_impl_t<SecondOrder> {
    std::vector<bid_t> active_cache_blocks;
    block_walk_state_t state;
    size_t idx;
    bool queried;

    block_walks_impl_t() {
        state.num_mem_walks = state.num_disk_walks = state.disk_load_walks = 0;
        idx = 0;
        queried = false;
    }

    bool has_finished() const {
        return idx >= active_cache_blocks.size();
    }

    template<typename walk_data_t>
    size_t query_block_state(graph_walk<walk_data_t, SecondOrder> &walk_manager, graph_cache &cache, bid_t exec_block) {
        // the total_walks is just an approximate value
        bid_t nblocks = walk_manager.global_blocks->nblocks;
        size_t total_walks = 0;
        for(bid_t index = 0; index < cache.ncblock; index++) {
            if(cache.cache_blocks[index].block != NULL) {
               bid_t blk = cache.cache_blocks[index].block->blk;
               wid_t walk_cnt = walk_manager.nblockwalks(blk * nblocks + exec_block);
               if(walk_cnt > 0) {
                   active_cache_blocks.push_back(blk * nblocks + exec_block);
                   total_walks += walk_cnt;
               }

               if(blk != exec_block && (walk_cnt = walk_manager.nblockwalks(exec_block * nblocks + blk)) > 0) {
                    active_cache_blocks.push_back(exec_block * nblocks + blk);
                    total_walks += walk_cnt;
               }
            }
        }
        return total_walks;
    }

    template<typename walk_data_t>
    void query_interval_block_state(graph_walk<walk_data_t, SecondOrder> &walk_manager, bid_t select_block) {
        state.num_mem_walks = walk_manager.nmwalks(select_block);
        state.num_disk_walks = walk_manager.ndwalks(select_block);
        state.disk_load_walks = 0;
        queried = true;
    }

    template<typename walk_data_t>
    size_t load_walks(graph_walk<walk_data_t, SecondOrder> &walk_manager, bid_t exec_block) {
        size_t nwalks = 0;
        bid_t select_block = active_cache_blocks[idx];
        if(!queried) {
            query_interval_block_state(walk_manager, select_block);
        }

        if(state.num_mem_walks > 0) {
            nwalks = walk_manager.load_memory_walks(select_block);
            state.num_mem_walks = 0;
            logstream(LOG_DEBUG) << "load memory walks from " << select_block / walk_manager.nblocks << " to " << select_block % walk_manager.nblocks << ", walks = " << nwalks << std::endl;
        } else if(state.num_disk_walks > 0) {
            wid_t interval_max_walks = 32 * MAX_TWALKS;
            wid_t interval_walks = std::min(state.num_disk_walks, interval_max_walks);
            state.num_disk_walks -= interval_walks;
            nwalks = walk_manager.load_disk_walks(select_block, interval_walks, state.disk_load_walks);
            state.disk_load_walks += interval_walks;
            logstream(LOG_DEBUG) << "load disk walks from " << select_block / walk_manager.nblocks << " to " << select_block % walk_manager.nblocks << ", walks = " << nwalks << std::endl;
        }

        if(state.num_disk_walks == 0) {
            walk_manager.dump_walks(select_block);
            idx++;
            queried = false;
        }
        return nwalks;
    }
};

#endif
