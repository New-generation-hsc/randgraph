#ifndef _GRAPH_PRECOMPUTE_H_
#define _GRAPH_PRECOMPUTE_H_

#include <string>
#include <vector>
#include <numeric>
#include <queue>
#include <omp.h>
#include <algorithm>
#include "api/types.hpp"
#include "util/util.hpp"
#include "util/io.hpp"

/* the block structure used for precompute */
struct pre_block_t {
    vid_t start_vert, nverts;
    eid_t start_edge, nedges;
    eid_t *beg_pos;
    vid_t *csr;
    real_t *weights;

    pre_block_t() {
        beg_pos = NULL;
        csr = NULL;
        weights = NULL;
    }
    ~pre_block_t() {
        if(beg_pos) free(beg_pos);
        if(csr) free(csr);
        if(weights) free(weights);
    }
};

/**
 * This structure stores the block alias table information
 * `prob`  : the next hop probability for each vertex in the block
 * `alias` : the alias index if the selected index is not accepted.
*/
struct pre_alias_table {
    real_t *prob;
    vid_t  *alias;
    pre_alias_table() {
        prob = NULL;
        alias = NULL;
    }
    ~pre_alias_table() {
        if(prob) free(prob);
        if(alias) free(alias);
    }
};

void construct_alias_table(const pre_block_t& block, pre_alias_table& table) {
    omp_set_num_threads(omp_get_max_threads());

#pragma omp parallel for schedule(static)
    for (vid_t vertex = 0; vertex < block.nverts; ++vertex)
    {
        eid_t adj_head = block.beg_pos[vertex] - block.start_edge, adj_tail = block.beg_pos[vertex + 1] - block.start_edge;
        eid_t deg = adj_tail - adj_head;
        real_t sum = std::accumulate(block.weights + adj_head, block.weights + adj_tail, 0.0);
        std::queue<vid_t> small, large;
        for(eid_t off = adj_head; off < adj_tail; ++off) {
            table.prob[off] = block.weights[off] * deg;
            if (table.prob[off] < sum) small.push(off - adj_head);
            else large.push(off - adj_head);
        }
        
        real_t *adj_prob  = table.prob + adj_head;
        vid_t  *adj_alias = table.alias + adj_head;
        while(!small.empty() && !large.empty()) {
            vid_t s = small.front(), l = large.front();
            small.pop(); large.pop();
            adj_alias[s] = l;
            adj_prob[l] -= (sum - adj_prob[s]);
            
            if(adj_prob[l] < sum) small.push(l);
            else large.push(l);
        }

        while(!large.empty()) {
            auto l = large.front();
            large.pop();
            adj_prob[l] = sum;
            adj_alias[l] = deg;
        }

        while(!small.empty()) {
            auto s = small.front();
            small.pop();
            adj_prob[s] = sum;
            adj_alias[s] = deg;
        }
    }
}

void construct_accumulate(const pre_block_t& block, real_t* acw) {
    omp_set_num_threads(omp_get_max_threads());

#pragma omp parallel for schedule(static)
    for(vid_t vertex = 0; vertex < block.nverts; ++vertex) {
        real_t s = 0.0;
        eid_t adj_head = block.beg_pos[vertex] - block.start_edge, adj_tail = block.beg_pos[vertex + 1] - block.start_edge;
        for(eid_t off = adj_head; off < adj_tail; ++off) {
            s += block.weights[off];
            acw[off] = s;
        }
    }
}

/**
 * This method does following two things:
 * 1. accumulate the edge weight for each vertex and store in secondary storage e.g. disk
 * 2. construct the first-order alias table for each vertex and store in secondary storage
*/
void second_order_precompute(const std::string& filename, int fnum, size_t blocksize) {
    std::string vert_block_name = get_vert_blocks_name(filename, blocksize);
    std::string edge_block_name = get_edge_blocks_name(filename, blocksize);
    std::string weight_name = get_weights_name(filename, fnum);
    std::string beg_pos_name = get_beg_pos_name(filename, fnum);
    std::string csr_name = get_csr_name(filename, fnum);
    std::string prob_name = get_prob_name(filename, fnum);
    std::string alias_name = get_alias_name(filename, fnum);
    std::string acc_name = get_accumulate_name(filename, fnum);

    std::vector<vid_t> vblocks = load_graph_blocks<vid_t>(vert_block_name);
    std::vector<eid_t> eblocks = load_graph_blocks<eid_t>(edge_block_name);

    int vertdesc = open(beg_pos_name.c_str(), O_RDONLY);
    int edgedesc = open(csr_name.c_str(), O_RDONLY);
    int whtdesc  = open(weight_name.c_str(), O_RDONLY);
    assert(vertdesc > 0 && edgedesc > 0);

    bid_t nblocks = vblocks.size() - 1;
    logstream(LOG_INFO) << "load vblocks and eblocks successfully, block count : " << nblocks << std::endl;
    pre_block_t block;
    pre_alias_table table;
    real_t *acw = NULL;
    logstream(LOG_INFO) << "start to compute the alias table and accumulate array, nblocks = " << nblocks << std::endl;
    for(bid_t blk = 0; blk < nblocks; blk++) {
        block.nverts = vblocks[blk + 1] - vblocks[blk];
        block.nedges = eblocks[blk + 1] - eblocks[blk];
        block.start_vert = vblocks[blk];
        block.start_edge = eblocks[blk];

        block.beg_pos = (eid_t*)realloc(block.beg_pos, (block.nverts + 1) * sizeof(eid_t));
        block.csr     = (vid_t*)realloc(block.csr, block.nedges * sizeof(vid_t));
        block.weights = (real_t*)realloc(block.weights, block.nedges * sizeof(real_t));
        table.prob    = (real_t*)realloc(table.prob, block.nedges * sizeof(real_t));
        table.alias   = (vid_t*)realloc(table.alias, block.nedges * sizeof(vid_t));
        acw           = (real_t*)realloc(acw, block.nedges * sizeof(real_t));

        pread(vertdesc, block.beg_pos, (block.nverts + 1) * sizeof(eid_t), block.start_vert * sizeof(eid_t));
        pread(edgedesc, block.csr, block.nedges * sizeof(vid_t), block.start_edge * sizeof(vid_t));
        pread(whtdesc, block.weights, block.nedges * sizeof(real_t), block.start_edge * sizeof(real_t));

        logstream(LOG_INFO) << "start computing the alias table and accumulating arry for block = " << blk << std::endl;

        construct_alias_table(block, table);
        appendfile(prob_name, table.prob, block.nedges);
        appendfile(alias_name, table.alias, block.nedges);
        logstream(LOG_INFO) << "finish computing the alias table for block = " << blk << std::endl;

        construct_accumulate(block, acw);
        appendfile(acc_name, acw, block.nedges);
        logstream(LOG_INFO) << "finish computing the accumulating array for block = " << blk << std::endl;
    }

    if(acw) free(acw);
    close(vertdesc);
    close(edgedesc);
}

static void sort_block_vertex_neighbors(const pre_block_t& block, vid_t *new_csr, real_t *new_weights)
{
    vid_t *csr_start = block.csr;
    auto cmp = [&csr_start](eid_t u, eid_t v) { return csr_start[u] < csr_start[v]; };
    omp_set_num_threads(omp_get_max_threads());

#pragma omp parallel for schedule(static)
    for(vid_t vertex = 0; vertex < block.nverts; ++vertex) {
        eid_t adj_head = block.beg_pos[vertex] - block.start_edge, adj_tail = block.beg_pos[vertex + 1] - block.start_edge;
        std::vector<eid_t> index(adj_tail - adj_head);
        std::iota(index.begin(), index.end(), adj_head);
        std::sort(index.begin(), index.end(), cmp);
        for(eid_t off = adj_head; off < adj_tail; ++off) {
            new_csr[off] = block.csr[index[off - adj_head]];
            if(new_weights != nullptr) {
                new_weights[off] = block.weights[index[off - adj_head]];
            }
        }
    }
}

void sort_vertex_neighbors(const std::string &filename, int fnum, size_t blocksize, bool weighted)
{
    std::string vert_block_name = get_vert_blocks_name(filename, blocksize);
    std::string edge_block_name = get_edge_blocks_name(filename, blocksize);
    std::string weight_name = get_weights_name(filename, fnum);
    std::string beg_pos_name = get_beg_pos_name(filename, fnum);
    std::string csr_name = get_csr_name(filename, fnum);

    std::vector<vid_t> vblocks = load_graph_blocks<vid_t>(vert_block_name);
    std::vector<eid_t> eblocks = load_graph_blocks<eid_t>(edge_block_name);

    int vertdesc = open(beg_pos_name.c_str(), O_RDONLY);
    int edgedesc = open(csr_name.c_str(), O_RDWR);
    int whtdesc = 0;
    if(weighted) {
        whtdesc = open(weight_name.c_str(), O_RDWR);
    }
    assert(vertdesc > 0 && edgedesc > 0);
    bid_t nblocks = vblocks.size() - 1;
    logstream(LOG_INFO) << "load vblocks and eblocks successfully, block count : " << nblocks << std::endl;
    pre_block_t block;
    logstream(LOG_INFO) << "start to sort the vertex neighbors, nblocks = " << nblocks << std::endl;
    
    vid_t *new_csr = nullptr;
    real_t *new_weights = nullptr;
    for (bid_t blk = 0; blk < nblocks; blk++)
    {
        block.nverts = vblocks[blk + 1] - vblocks[blk];
        block.nedges = eblocks[blk + 1] - eblocks[blk];
        block.start_vert = vblocks[blk];
        block.start_edge = eblocks[blk];

        block.beg_pos = (eid_t *)realloc(block.beg_pos, (block.nverts + 1) * sizeof(eid_t));
        block.csr = (vid_t *)realloc(block.csr, block.nedges * sizeof(vid_t));
        if(weighted) {
            block.weights = (real_t *)realloc(block.weights, block.nedges * sizeof(real_t));
        }

        pread(vertdesc, block.beg_pos, (block.nverts + 1) * sizeof(eid_t), block.start_vert * sizeof(eid_t));
        pread(edgedesc, block.csr, block.nedges * sizeof(vid_t), block.start_edge * sizeof(vid_t));
        if(weighted) {
            pread(whtdesc, block.weights, block.nedges * sizeof(real_t), block.start_edge * sizeof(real_t));
        }

        new_csr = (vid_t *)realloc(new_csr, block.nedges * sizeof(vid_t));
        if(weighted) {
            new_weights = (real_t *)realloc(new_weights, block.nedges * sizeof(real_t));
        }

        sort_block_vertex_neighbors(block, new_csr, new_weights);
        pwrite(edgedesc, new_csr, block.nedges * sizeof(vid_t), block.start_edge * sizeof(vid_t));
        if(weighted) {
            pwrite(whtdesc, new_weights, block.nedges * sizeof(real_t), block.start_edge * sizeof(real_t));
        }
        logstream(LOG_INFO) << "finish sort vertex neighbors for block = " << blk << std::endl;
    }

    if(new_csr) free(new_csr);
    if(new_weights) free(new_weights);
}

#endif