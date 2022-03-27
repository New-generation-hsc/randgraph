#ifndef _GRAPH_CACHE_H_
#define _GRAPH_CACHE_H_

#include <vector>
#include <cstdlib>
#include <cassert>
#include <mutex>
#include <memory>

#include "api/constants.hpp"
#include "api/types.hpp"
#include "util/util.hpp"
#include "util/hash.hpp"
#include "util/io.hpp"
#include "config.hpp"

/**
 * This file contribute to define graph block cache structure and some operations
 * on the cache, such as swapin and swapout, schedule blocks.
 */

/** block has four state
 *
 * `USING`      : the block is running
 * `USED`       : the block is finished runing, but still in memory
 * `ACTIVE`     : the block is in memroy, but not use
 * `INACTIVE`   : the block is in disk
 */

enum block_state {
    USING = 1, USED, ACTIVE, INACTIVE
};

class block_t {
public:
    bid_t blk, cache_index;             /* the block number, and the memory index */
    vid_t start_vert, nverts;           /* block start vertex and the number of vertex in block */
    eid_t start_edge, nedges;           /* block start edge and the number of edges in this block */

    block_state status;                 /* indicate the state of this block */
    rank_t rank;                        /* record the block rank */
    std::shared_ptr<std::mutex> mtx;    /* mutex for safe update the rank */

    real_t exp_walk_len;                /* expected walk length */

#ifdef PROF_METRIC
    size_t loaded_count;
#endif

    block_t() {
        blk = cache_index = 0;
        start_vert = nverts = 0;
        start_edge = nedges = 0;
        status  = INACTIVE;
        mtx = std::make_shared<std::mutex>();

#ifdef PROF_METRIC
        loaded_count = 0;
#endif
    }

    block_t& operator=(const block_t& other) {
        if(this != &other) {
            this->blk        = other.blk;
            this->start_vert = other.start_vert;
            this->nverts     = other.nverts;
            this->start_edge = other.start_edge;
            this->nedges     = other.nedges;
            this->status     = other.status;
            this->rank       = other.rank;
        }
        return *this;
    }

#ifdef PROF_METRIC
    void update_loaded_count() { loaded_count += 1; };
#endif
};

class cache_block {
public:
    block_t *block;

    eid_t *beg_pos;
    vid_t *degree;
    vid_t *csr;
    real_t *weights;

    /* the following two field just for alias table sample method */
    real_t *prob;
    vid_t  *alias;
    real_t *acc_weights;

    bool loaded_alias;
    BloomFilter *bf;

    /**
     * record each block life, when swap out, the largest life block will be evicted
     */
    int life;

    cache_block() {
        block   = NULL;
        beg_pos = NULL;
        degree  = NULL;
        csr     = NULL;
        weights = NULL;
        prob    = NULL;
        alias   = NULL;
        acc_weights = NULL;
        bf = nullptr;
        life = 0;
        loaded_alias = false;
    }

    ~cache_block() {
        if(beg_pos) free(beg_pos);
        if(degree)  free(degree);
        if(csr)     free(csr);
        if(weights) free(weights);
        if(prob)    free(prob);
        if(alias)   free(alias);
        if(acc_weights) free(acc_weights);
        if(bf) delete bf;
    }

    void make_filter(bool filter) {
        if(filter) bf = new BloomFilter;
    }
};

void swap(cache_block& cb1, cache_block& cb2) {
    block_t *tblock = cb2.block;
    eid_t *tbeg_pos = cb2.beg_pos;
    vid_t *tdegree  = cb2.degree;
    vid_t *tcsr     = cb2.csr;
    real_t *tw      = cb2.weights;
    real_t *tp      = cb2.prob;
    vid_t *ta       = cb2.alias;
    real_t *tacc    = cb2.acc_weights;
    BloomFilter *tbf = cb2.bf;
    bool tl         = cb2.loaded_alias;
    int tlife       = cb2.life;

    cb2.block = cb1.block;
    cb2.beg_pos = cb1.beg_pos;
    cb2.degree = cb1.degree;
    cb2.csr = cb1.csr;
    cb2.weights = cb1.weights;
    cb2.prob    = cb1.prob;
    cb2.alias   = cb1.alias;
    cb2.acc_weights = cb1.acc_weights;
    cb2.bf      = cb1.bf;
    cb2.loaded_alias = cb1.loaded_alias;
    cb2.life    = cb1.life;

    cb1.block = tblock;
    cb1.beg_pos = tbeg_pos;
    cb1.degree = tdegree;
    cb1.csr = tcsr;
    cb1.weights = tw;
    cb1.prob = tp;
    cb1.alias = ta;
    cb1.acc_weights = tacc;
    cb1.bf = tbf;
    cb1.loaded_alias = tl;
    cb1.life = tlife;
}

class graph_block {
public:
    bid_t nblocks;
    std::vector<block_t> blocks;

    graph_block(graph_config* conf) {
        std::string vert_block_name = get_vert_blocks_name(conf->base_name, conf->blocksize, conf->reordered);
        std::string edge_block_name = get_edge_blocks_name(conf->base_name, conf->blocksize, conf->reordered);
        std::string walk_len_name   = get_expected_walk_length_name(conf->base_name, conf->fnum);

        std::vector<vid_t> vblocks = load_graph_blocks<vid_t>(vert_block_name);
        std::vector<eid_t> eblocks = load_graph_blocks<eid_t>(edge_block_name);
        std::vector<real_t> wblocks = load_graph_blocks<real_t>(walk_len_name);

        nblocks = vblocks.size() - 1;
        blocks.resize(nblocks);

        for(bid_t blk = 0; blk < nblocks; blk++) {
            blocks[blk].blk = blk;
            blocks[blk].cache_index = nblocks;
            blocks[blk].start_vert = vblocks[blk];
            blocks[blk].nverts     = vblocks[blk+1] - vblocks[blk];
            blocks[blk].start_edge = eblocks[blk];
            blocks[blk].nedges     = eblocks[blk+1] - eblocks[blk];
            blocks[blk].status     = INACTIVE;
            blocks[blk].rank       = 0;
            blocks[blk].exp_walk_len = wblocks[blk];

            logstream(LOG_INFO) << "blk [ " << blk << " ] : vert = [ " << blocks[blk].start_vert << ", " << blocks[blk].start_vert + blocks[blk].nverts << " ], csr = [ ";
            logstream(LOG_INFO) << blocks[blk].start_edge << ", " << blocks[blk].start_edge + blocks[blk].nedges << " ]" << std::endl;
        }
    }

    block_t& operator[](bid_t blk) {
        assert(blk < nblocks);
        return blocks[blk];
    }

    void reset_rank(bid_t blk) {
        assert(blk < nblocks);
        blocks[blk].rank = 0;
    }

    void update_rank(vid_t dst) {
        bid_t blk = get_block(dst);
        std::lock_guard<std::mutex> lock(*blocks[blk].mtx);
        blocks[blk].rank += 1;
    }

    bid_t get_block(vid_t v) {
        bid_t blk = 0;
        for(; blk < nblocks; blk++) {
            if(v < blocks[blk].start_vert + blocks[blk].nverts) return blk;
        }
        return nblocks;
    }

#ifdef PROF_METRIC
    void report() {
        for(bid_t blk = 0; blk < nblocks; blk++) {
            logstream(LOG_INFO) << "blk [ " << blk << " ] : loaded count = [ " << blocks[blk].loaded_count << " ]" << std::endl;
        }
    }
#endif
};

class graph_cache {
public:
    bid_t ncblock;                  /* number of cache blocks */
    std::vector<cache_block> cache_blocks; /* the cached blocks */
    std::vector<bid_t> walk_blocks;

    graph_cache(bid_t nblocks, graph_config *conf) {
        setup(nblocks, conf->cache_size, conf->blocksize);
        for(auto & cblk : cache_blocks) cblk.make_filter(conf->filter);
    }

    cache_block& operator[](size_t index) {
        assert(index < ncblock);
        return cache_blocks[index];
    }

    cache_block operator[](size_t index) const {
        assert(index < ncblock);
        return cache_blocks[index];
    }

    void setup(bid_t nblocks, size_t cache_size, size_t blocksize = BLOCK_SIZE) {
        ncblock = min_value(nblocks, cache_size / blocksize);
        assert(ncblock > 0);
        cache_blocks.resize(ncblock);
    }

    bool test_block_cached(bid_t blk, bid_t &exec_blk) {
        for(bid_t p = 0; p < ncblock; p++) {
            if(cache_blocks[p].block != NULL && cache_blocks[p].block->blk == blk) {
                exec_blk = p;
                return true;
            }
        }
        return false;
    }
};

template <typename walk_data_t, WalkType walk_type>
struct walk_data_block {
    static bid_t get(const walker_t<walk_data_t> &walker, graph_block *global_blocks) {
        return 0;
    }
};

template <typename walk_data_t>
struct walk_data_block<walk_data_t, FirstOrder> {
    static bid_t get(const walker_t<walk_data_t> &walker, graph_block *global_blocks) {
        return global_blocks->get_block(WALKER_POS(walker));
    }
};

template <typename walk_data_t>
struct walk_data_block<walk_data_t, SecondOrder> {
    static bid_t get(const walker_t<walk_data_t> &walker, graph_block *global_blocks) {
        bid_t pblk = global_blocks->get_block(get_vertex_from_walk(walker.data));
        bid_t cblk = global_blocks->get_block(WALKER_POS(walker));
        return pblk * global_blocks->nblocks + cblk;
    }
};

#endif
