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
#include "util/hash.hpp"

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

        load_block_range(vertdesc, block.beg_pos, (block.nverts + 1), block.start_vert * sizeof(eid_t));
        load_block_range(edgedesc, block.csr, block.nedges, block.start_edge * sizeof(vid_t));
        load_block_range(whtdesc, block.weights, block.nedges, block.start_edge * sizeof(real_t));

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

        load_block_range(vertdesc, block.beg_pos, (block.nverts + 1), block.start_vert * sizeof(eid_t));
        load_block_range(edgedesc, block.csr, block.nedges, block.start_edge * sizeof(vid_t));
        if(weighted) {
            load_block_range(whtdesc, block.weights, block.nedges, block.start_edge * sizeof(real_t));
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

struct cmp {
    bool operator()(const std::pair<vid_t, vid_t>& p1, const std::pair<vid_t, vid_t>& p2) {
        return p1.first > p2.first;
    }
};

void max_degree(const std::string& filename, int fnum, size_t blocksize, size_t top) {
    std::string vert_block_name = get_vert_blocks_name(filename, blocksize);
    std::string beg_pos_name = get_beg_pos_name(filename, fnum);
    std::vector<vid_t> vblocks = load_graph_blocks<vid_t>(vert_block_name);
    int vertdesc = open(beg_pos_name.c_str(), O_RDONLY);

    bid_t nblocks = vblocks.size() - 1;
    logstream(LOG_INFO) << "load vblocks and eblocks successfully, block count : " << nblocks << std::endl;
    pre_block_t block;
    using pair_t = std::pair<vid_t, vid_t>;
    std::priority_queue<pair_t, std::vector<pair_t>, cmp> q;
    for(bid_t blk = 0; blk < nblocks; blk++) {
        block.nverts = vblocks[blk + 1] - vblocks[blk];
        block.start_vert = vblocks[blk];
        block.beg_pos = (eid_t*)realloc(block.beg_pos, (block.nverts + 1) * sizeof(eid_t));
        load_block_range(vertdesc, block.beg_pos, (block.nverts + 1), block.start_vert * sizeof(eid_t));
        for(vid_t vertex = 0; vertex < block.nverts; vertex++) {
            eid_t deg = block.beg_pos[vertex+1] - block.beg_pos[vertex];
            q.push({deg, vertex});
            if(q.size() > top) q.pop();
        }

        logstream(LOG_INFO) << "finish computing the max degree array for block = " << blk << std::endl;
    }

    close(vertdesc);
    while(!q.empty()) {
        auto p = q.top();
        std::cout << "vertex : " << p.second << ", deg : " << p.first << std::endl;
        q.pop();
    }
}


void max_link_block(const std::string& filename, int fnum, size_t blocksize) {
    std::string vert_block_name = get_vert_blocks_name(filename, blocksize);
    std::string edge_block_name = get_edge_blocks_name(filename, blocksize);
    std::string beg_pos_name = get_beg_pos_name(filename, fnum);
    std::string csr_name = get_csr_name(filename, fnum);

    std::vector<vid_t> vblocks = load_graph_blocks<vid_t>(vert_block_name);
    std::vector<eid_t> eblocks = load_graph_blocks<eid_t>(edge_block_name);

    int vertdesc = open(beg_pos_name.c_str(), O_RDONLY);
    int edgedesc = open(csr_name.c_str(), O_RDONLY);
    assert(vertdesc > 0 && edgedesc > 0);

    bid_t nblocks = vblocks.size() - 1;
    logstream(LOG_INFO) << "load vblocks and eblocks successfully, block count : " << nblocks << std::endl;
    pre_block_t block;
    logstream(LOG_INFO) << "start to compute the alias table and accumulate array, nblocks = " << nblocks << std::endl;
    int link_block = 0;
    vid_t hub_vertex;
    for(bid_t blk = 0; blk < nblocks; blk++) {
        block.nverts = vblocks[blk + 1] - vblocks[blk];
        block.nedges = eblocks[blk + 1] - eblocks[blk];
        block.start_vert = vblocks[blk];
        block.start_edge = eblocks[blk];

        block.beg_pos = (eid_t*)realloc(block.beg_pos, (block.nverts + 1) * sizeof(eid_t));
        block.csr     = (vid_t*)realloc(block.csr, block.nedges * sizeof(vid_t));

        load_block_range(vertdesc, block.beg_pos, (block.nverts + 1), block.start_vert * sizeof(eid_t));
        load_block_range(edgedesc, block.csr, block.nedges, block.start_edge * sizeof(vid_t));

        for(vid_t vertex = 0; vertex < block.nverts; vertex++) {
            int link = 0;
            eid_t vertex_deg = block.beg_pos[vertex+1] - block.beg_pos[vertex];
            for(eid_t off = 0; off < vertex_deg; off++) {
                if(block.csr[off] < block.start_vert || block.csr[off] >= block.start_vert + block.nverts) link++;
            }
            if(link > link_block) {
                link_block = link;
                hub_vertex = block.start_vert + vertex;
            }
        }

        logstream(LOG_INFO) << "finish computing the accumulating array for block = " << blk << std::endl;
    }

    std::cout << "hub vertex = " << hub_vertex << ", link block = " << link_block << std::endl;
    close(vertdesc);
    close(edgedesc);
}

void calc_vertex_neighbor_dist(const std::string& filename, int fnum, size_t blocksize, vid_t pivot_vertex) {
    std::string vert_block_name = get_vert_blocks_name(filename, blocksize);
    std::string edge_block_name = get_edge_blocks_name(filename, blocksize);
    std::string beg_pos_name = get_beg_pos_name(filename, fnum);
    std::string csr_name = get_csr_name(filename, fnum);

    std::vector<vid_t> vblocks = load_graph_blocks<vid_t>(vert_block_name);
    std::vector<eid_t> eblocks = load_graph_blocks<eid_t>(edge_block_name);

    int vertdesc = open(beg_pos_name.c_str(), O_RDONLY);
    int edgedesc = open(csr_name.c_str(), O_RDONLY);
    assert(vertdesc > 0 && edgedesc > 0);
    bid_t blk = get_block(vblocks, pivot_vertex);

    bid_t nblocks = vblocks.size() - 1;
    logstream(LOG_INFO) << "load vblocks and eblocks successfully, block count : " << nblocks << ", vertex block = " << blk  << std::endl;
    pre_block_t block;
    block.nverts = vblocks[blk + 1] - vblocks[blk];
    block.nedges = eblocks[blk + 1] - eblocks[blk];
    block.start_vert = vblocks[blk];
    block.start_edge = eblocks[blk];

    block.beg_pos = (eid_t*)realloc(block.beg_pos, (block.nverts + 1) * sizeof(eid_t));
    block.csr     = (vid_t*)realloc(block.csr, block.nedges * sizeof(vid_t));

    load_block_range(vertdesc, block.beg_pos, (block.nverts + 1), block.start_vert * sizeof(eid_t));
    load_block_range(edgedesc, block.csr, block.nedges, block.start_edge * sizeof(vid_t));

    vid_t vertex_off = pivot_vertex - block.start_vert;
    std::vector<int> block_cnts(nblocks, 0);
    logstream(LOG_DEBUG) << "vertex off : " << vertex_off << ", off = " << block.beg_pos[vertex_off] - block.start_edge << ", off end = " << block.beg_pos[vertex_off + 1] - block.start_edge << std::endl;
    for(eid_t off = block.beg_pos[vertex_off] - block.start_edge; off < block.beg_pos[vertex_off + 1] - block.start_edge; off++) {
        bid_t adj_blk = get_block(vblocks, block.csr[off]);
        block_cnts[adj_blk]++;
    }

    int deg_cnts = 0;
    std::cout << "neighbors block dist : ";
    for(const auto & cnt : block_cnts) {
        std::cout << cnt << " ";
        deg_cnts += cnt;
    }
    std::cout << std::endl;
    std::cout << "vertex degree : " << deg_cnts << std::endl;

    close(vertdesc);
    close(edgedesc);
}

/**
 * This method does following things:
 * using bloom filter to store the top100 high-degree vertex neighbors
*/
void make_top100_bloom_filter(const std::string& filename, int fnum, size_t blocksize) {
    std::string vert_block_name = get_vert_blocks_name(filename, blocksize, true);
    std::string edge_block_name = get_edge_blocks_name(filename, blocksize, true);
    std::string beg_pos_name = get_beg_pos_name(filename, fnum, true);
    std::string csr_name = get_csr_name(filename, fnum, true);

    std::vector<vid_t> vblocks = load_graph_blocks<vid_t>(vert_block_name);
    std::vector<eid_t> eblocks = load_graph_blocks<eid_t>(edge_block_name);

    int vertdesc = open(beg_pos_name.c_str(), O_RDONLY);
    int edgedesc = open(csr_name.c_str(), O_RDONLY);
    assert(vertdesc > 0 && edgedesc > 0);

    bid_t nblocks = vblocks.size() - 1;
    logstream(LOG_INFO) << "load vblocks and eblocks successfully, block count : " << nblocks << std::endl;
    pre_block_t block;

    bid_t num_blocks = 0;
    while(num_blocks < nblocks) {
        if(vblocks[num_blocks] >= 100) break;
        num_blocks++;
    }

    eid_t num_edges = eblocks[num_blocks];
    BloomFilter bf;
    bf.create(num_edges);
    bf.set_num_blocks(num_blocks);

    for(bid_t blk = 0; blk < num_blocks; blk++) {
        block.nverts = vblocks[blk + 1] - vblocks[blk];
        block.nedges = eblocks[blk + 1] - eblocks[blk];
        block.start_vert = vblocks[blk];
        block.start_edge = eblocks[blk];

        block.beg_pos = (eid_t*)realloc(block.beg_pos, (block.nverts + 1) * sizeof(eid_t));
        block.csr     = (vid_t*)realloc(block.csr, block.nedges * sizeof(vid_t));

        load_block_range(vertdesc, block.beg_pos, (block.nverts + 1), block.start_vert * sizeof(eid_t));
        load_block_range(edgedesc, block.csr, block.nedges, block.start_edge * sizeof(vid_t));

        for(vid_t v = 0; v < block.nverts; v++) {
            for(eid_t off = block.beg_pos[v] - block.start_edge; off < block.beg_pos[v+1] - block.start_edge; off++) {
                bf.insert(block.start_vert + v, block.csr[off]);
            }
        }
        logstream(LOG_INFO) << "finish making bloom filter for block = " << blk << std::endl;
    }
    logstream(LOG_DEBUG) << "make bloomfilter for num_blocks = " << num_blocks << ", num_vertex = " << vblocks[num_blocks] << ", num_edges = " << num_edges << std::endl;

    // dump the bloomfilter
    std::string output = get_bloomfilter_name(filename, fnum);
    bf.dump_bloom_filter(output);
    logstream(LOG_DEBUG) << "successfully dump bloom filter." << std::endl;
}
#endif
