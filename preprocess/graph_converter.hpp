#ifndef _GRAPH_CONVERTER_H_
#define _GRAPH_CONVERTER_H_

#include <string>
#include <vector>
#include <fstream>
#include "api/graph_buffer.hpp"
#include "api/constants.hpp"
#include "api/types.hpp"
#include "logger/logger.hpp"
#include "util/util.hpp"
#include "util/io.hpp"
#include "precompute.hpp"
#include "split.hpp"


/** This file defines the data structure that contribute to convert the text format graph to some specific format */

class base_converter {
public:
    base_converter() = default;
    ~base_converter() { }
    virtual void initialize() { };
    virtual void convert(vid_t from, vid_t to) { };
    virtual void finalize() { };
};


/**
 * Convert the text format graph dataset into the csr format. the dataset may be very large, so we may split the csr data
 * into multiple files.
 * `fnum` records the number of files used to store the csr data.
 * `beg_pos` : records the vertex points the position of csr array
 * `csr` : records the edge destination
 * `deg` : records the degree of each vertex
 *
 * `adj` : records the current vertex neighbors
 * `curr_vert` : the current vertex id
 * `max_vert` : records the max vertex id
 * `rd_edges` : the number of edges that has been read, use to split into multiply files.
 */
class graph_converter : public base_converter {
private:
    int fnum;
    bool _weighted;
    bool _sorted;
    graph_buffer<eid_t> beg_pos;
    graph_buffer<vid_t> csr;
    graph_buffer<vid_t> deg;
    graph_buffer<real_t> weights;

    std::vector<vid_t> adj;
    std::vector<real_t> adj_weights;
    vid_t curr_vert;
    vid_t max_vert;

    /* for buffer */
    vid_t buf_vstart; /* current buffer start vertex */
    eid_t buf_estart; /* current buffer start edge */
    eid_t rd_edges;
    eid_t csr_pos;

    std::string output_filename;

    void setup_output(const std::string& input) {
        std::string folder = randgraph_output_folder(get_path_name(input), BLOCK_SIZE);
        if(!test_folder_exists(folder)) randgraph_mkdir(folder.c_str());
        output_filename = randgraph_output_filename(get_path_name(input), get_file_name(input), BLOCK_SIZE);
    }

    void setup_output(const std::string& path, const std::string& dataset) {
        std::string folder = randgraph_output_folder(path, BLOCK_SIZE);
        if(!test_folder_exists(folder)) randgraph_mkdir(folder.c_str());
        output_filename = randgraph_output_filename(path, dataset, BLOCK_SIZE);
    }

    void flush_beg_pos() {
        std::string name = get_beg_pos_name(output_filename, fnum);
        appendfile(name, beg_pos.buffer_begin(), beg_pos.size());
        beg_pos.clear();
    }

    void flush_degree() {
        std::string name = get_degree_name(output_filename, fnum);
        appendfile(name, deg.buffer_begin(), deg.size());
        buf_vstart += deg.size();
        deg.clear();
    }

    void flush_csr() {
        eid_t max_nedges = (eid_t)FILE_SIZE / sizeof(vid_t);
        if(rd_edges + csr.size() > max_nedges) {
            fnum += 1;
            rd_edges = 0;
        }
        std::string name = get_csr_name(output_filename, fnum);
        appendfile(name, csr.buffer_begin(), csr.size());
        rd_edges += csr.size();
        buf_estart += csr.size();
        csr.clear();
    }

    void flush_weights() {
        std::string name = get_weights_name(output_filename, fnum);
        appendfile(name, weights.buffer_begin(), weights.size());
        weights.clear();
    }

    void sync_buffer() {
        for(auto & dst : adj) csr.push_back(dst);
        if(_weighted) {
            for(const auto & w : adj_weights) weights.push_back(w);
        }
        csr_pos += adj.size();
        beg_pos.push_back(csr_pos);
        deg.push_back(adj.size());
    }

    void sync_zeros(vid_t zeronodes) {
        while(zeronodes--){
            if(beg_pos.full()) flush_buffer();
            beg_pos.push_back(csr_pos);
            deg.push_back(0);
        }
    }
public:
    graph_converter() = delete;
    graph_converter(const std::string& path, bool weighted = false, bool sorted = false) {
        fnum = 0;
        beg_pos.alloc(VERT_SIZE);
        csr.alloc(EDGE_SIZE);
        deg.alloc(VERT_SIZE);
        curr_vert = max_vert = buf_vstart = buf_estart = rd_edges = csr_pos = 0;
        setup_output(path);
        _weighted = weighted;
        _sorted = sorted;
        if(_weighted) {
            weights.alloc(EDGE_SIZE);
        }
    }
    graph_converter(const std::string& folder, const std::string& dataset, bool weighted = false, bool sorted = false) {
        fnum = 0;
        beg_pos.alloc(VERT_SIZE);
        csr.alloc(EDGE_SIZE);
        deg.alloc(VERT_SIZE);
        curr_vert = max_vert = buf_vstart = buf_estart = rd_edges = csr_pos = 0;
        setup_output(folder, dataset);
        _weighted = weighted;
        _sorted = sorted;
        if(_weighted) {
            weights.alloc(EDGE_SIZE);
        }
    }
    graph_converter(const std::string& path, size_t vert_size, size_t edge_size, bool weighted = false, bool sorted = false) {
        fnum = 0;
        beg_pos.alloc(vert_size);
        csr.alloc(edge_size);
        deg.alloc(vert_size);
        curr_vert = max_vert = buf_vstart = buf_estart = rd_edges = csr_pos = 0;
        setup_output(path);
        _weighted = weighted;
        _sorted = sorted;
        if(_weighted) {
            weights.alloc(EDGE_SIZE);
        }
    }
    ~graph_converter() {
        beg_pos.destroy();
        csr.destroy();
        deg.destroy();
    }

    void initialize() {
        beg_pos.clear();
        csr.clear();
        deg.clear();
        curr_vert = max_vert = buf_vstart = buf_estart = rd_edges = csr_pos = 0;
        beg_pos.push_back(0);
    }

    void convert(vid_t from, vid_t to, real_t *weight) {
        max_vert = max_value(max_vert, from);
        max_vert = max_value(max_vert, to);

        if(from == curr_vert) {
            adj.push_back(to);
            if(_weighted) adj_weights.push_back(*weight);
        }
        else {
            if(csr.test_overflow(adj.size()) || beg_pos.full() ) {
                flush_buffer();
            }
            if(adj.size() > EDGE_SIZE) {
                logstream(LOG_ERROR) << "Too small memory capacity with EDGE_SIZE = " << EDGE_SIZE << " to support larger out degree = " << adj.size() << std::endl;
                assert(false);
            }
            sync_buffer();
            if(from - curr_vert > 1) sync_zeros(from - curr_vert - 1);
            curr_vert = from;
            adj.clear();
            adj.push_back(to);
            if(_weighted) {
                adj_weights.clear();
                adj_weights.push_back(*weight);
            }
        }
    }

    void flush_buffer() {
        logstream(LOG_INFO) << "Buffer : [ " << buf_vstart << ", " <<  buf_vstart + deg.size() << " ), csr position : [ " << buf_estart << ", " << csr_pos << " )" << std::endl;
        if(!csr.empty()) flush_csr();
        if(!beg_pos.empty()) flush_beg_pos();
        if(!deg.empty()) flush_degree();
        if(_weighted && !weights.empty()) flush_weights();
    }

    void finalize() {
        sync_buffer();
        if(max_vert > curr_vert) sync_zeros(max_vert - curr_vert);
        flush_buffer();

        logstream(LOG_INFO) << "nvertices = " << max_vert + 1 << ", nedges = " << csr_pos << ", files : " << fnum + 1 << std::endl;
    }

    int get_fnum() { return this->fnum + 1; }
    std::string get_output_filename() const { return output_filename; }
    bool is_weighted() const { return _weighted; }

    bool need_sorted() const { return _sorted; }
};

void convert(std::string filename, graph_converter &converter, size_t blocksize = BLOCK_SIZE) {

    FILE *fp = fopen(filename.c_str(), "r");
    assert(fp != NULL);

    converter.initialize();
    char line[1024];
    while(fgets(line, 1024, fp) != NULL) {
        if(line[0] == '#') continue;
        if(line[0] == '%') continue;

        char *t1, *t2, *t3;
        t1 = strtok(line, "\t, ");
        t2 = strtok(NULL, "\t, ");
        t3 = strtok(NULL, "\t, ");
        if(t1 == NULL || t2 == NULL) {
            logstream(LOG_ERROR) << "Input file is not the right format. Expected <from> <to>" << std::endl;
            assert(false);
        }
        vid_t from = atoi(t1);
        vid_t to = atoi(t2);
        if(from == to) continue;
        if(converter.is_weighted()) {
            assert(t3 != NULL);
            real_t w = static_cast<real_t>(atof(t3));
            converter.convert(from, to, &w);
        }else {
            converter.convert(from, to, NULL);
        }
    }
    converter.finalize();

    /* split the data into multiple blocks */
    split_blocks(converter.get_output_filename(), 0, blocksize, false);

    if(converter.need_sorted()) {
        sort_vertex_neighbors(converter.get_output_filename(), 0, blocksize, converter.is_weighted());
    }

    /* if the graph is weighted, then preprocess the alias table. */
    if(converter.is_weighted()) {
        second_order_precompute(converter.get_output_filename(), 0, blocksize);
    }

    /* make bloom filter for each block */
    make_graph_bloom_filter(converter.get_output_filename(), 0, blocksize, false);
}

/** compute the given graph each vertex point to the same block ratio */
void compute_graph_degree_ratio(const std::string& filename, int fnum, size_t blocksize = BLOCK_SIZE) {
    std::string vert_block_name = get_vert_blocks_name(filename, blocksize);
    std::string edge_block_name = get_edge_blocks_name(filename, blocksize);
    std::string degree_name     = get_degree_name(filename, fnum);
    std::string csr_name        = get_csr_name(filename, fnum);
    std::string output          = get_ratio_name(filename, fnum);

    std::vector<vid_t> vblocks = load_graph_blocks<vid_t>(vert_block_name);
    std::vector<eid_t> eblocks = load_graph_blocks<eid_t>(edge_block_name);

    int vertdesc = open(degree_name.c_str(), O_RDONLY);
    int edgedesc = open(csr_name.c_str(), O_RDONLY);
    assert(vertdesc > 0 && edgedesc > 0);

    bid_t nblocks = vblocks.size() - 1;
    logstream(LOG_INFO) << "load vblocks and eblocks successfully, block count : " << nblocks << std::endl;
    vid_t *degree = NULL, *csr = NULL;
    float *ratio = NULL;
    for(bid_t blk = 0; blk < nblocks; blk++) {
        vid_t nverts = vblocks[blk+1] - vblocks[blk];
        eid_t nedges = eblocks[blk+1] - eblocks[blk];

        degree = (vid_t*)realloc(degree, nverts * sizeof(vid_t));
        ratio  = (float *)realloc(ratio, nverts * sizeof(float));
        for(vid_t v = 0; v < nverts; v++) ratio[v] = 0.0;
        csr    = (vid_t*)realloc(csr, nedges * sizeof(vid_t));
        assert(degree != NULL && ratio != NULL && csr != NULL);
        logstream(LOG_INFO) << "start load block " << blk << ", vert range : [ " << vblocks[blk] << ", " << vblocks[blk+1] << " ), edge range : [ " << eblocks[blk] << ", " << eblocks[blk+1] << " )" << std::endl;
        load_block_range(vertdesc, degree, nverts, vblocks[blk] * sizeof(vid_t));
        load_block_range(edgedesc, csr,    nedges, eblocks[blk] * sizeof(vid_t));

        eid_t edge_pos = 0;
        for(vid_t v = 0; v < nverts; v++) {
            if(degree[v] == 0) continue;
            float sum = 0.0;
            vid_t deg = degree[v];
            assert(edge_pos + deg <= nedges);
            for(eid_t e = edge_pos; e < edge_pos + deg; e++) {
                bid_t dst = get_block(vblocks, csr[e]);
                if(dst == blk) sum += 1.0 / (float)deg;
            }
            ratio[v] = sum;
            edge_pos += deg;
        }
        assert(edge_pos == nedges);
        appendfile(output,ratio, nverts);
    }

    /** free the allocate memory */
    if(degree) free(degree);
    if(csr)    free(csr);
    if(ratio)  free(ratio);
}

#endif
