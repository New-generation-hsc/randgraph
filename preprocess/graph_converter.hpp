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

/** This file defines the data structure that contribute to convert the text format graph to some specific format */

class base_converter {
protected:
    std::string output_filename;
public:
    base_converter() { }
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
    graph_buffer<eid_t> beg_pos;
    graph_buffer<vid_t> csr;
    graph_buffer<vid_t> deg;

    std::vector<vid_t> adj;
    vid_t curr_vert;
    vid_t max_vert;

    /* for buffer */
    vid_t buf_vstart; /* current buffer start vertex */
    eid_t buf_estart; /* current buffer start edge */
    eid_t rd_edges;
    eid_t csr_pos;

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

    void sync_buffer() {
        for(auto & dst : adj) csr.push_back(dst);
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
    graph_converter(std::string output) { 
        output_filename = output;
        fnum = 0;
        beg_pos.alloc(VERT_SIZE);
        csr.alloc(EDGE_SIZE);
        deg.alloc(VERT_SIZE);
        curr_vert = max_vert = buf_vstart = buf_estart = rd_edges = csr_pos = 0;
    }
    graph_converter(std::string output, size_t vert_size, size_t edge_size) {
        output_filename = output;
        fnum = 0;
        beg_pos.alloc(vert_size);
        csr.alloc(edge_size);
        deg.alloc(vert_size);
        curr_vert = max_vert = buf_vstart = buf_estart = rd_edges = csr_pos = 0;
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

    void convert(vid_t from, vid_t to) {
        max_vert = max_value(max_vert, from);
        max_vert = max_value(max_vert, to);

        if(from == curr_vert) adj.push_back(to);
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
        }
    }

    void flush_buffer() {
        logstream(LOG_INFO) << "Buffer : [ " << buf_vstart << ", " <<  buf_vstart + deg.size() << " ), csr position : [ " << buf_estart << ", " << csr_pos << " )" << std::endl;
        if(!csr.empty()) flush_csr();
        if(!beg_pos.empty()) flush_beg_pos();
        if(!deg.empty()) flush_degree();
    }

    void finalize() {
        sync_buffer();
        if(max_vert > curr_vert) sync_zeros(max_vert - curr_vert);
        flush_buffer();

        logstream(LOG_INFO) << "nvertices = " << max_vert + 1 << ", nedges = " << csr_pos << ", files : " << fnum + 1 << std::endl;
    }

    int get_fnum() { return this->fnum + 1; }
};

void convert(std::string filename, graph_converter &converter) {
    
    FILE *fp = fopen(filename.c_str(), "r");
    assert(fp != NULL);
    
    converter.initialize();
    char line[1024];
    while(fgets(line, 1024, fp) != NULL) {
        if(line[0] == '#') continue;
        if(line[0] == '%') continue;

        char *t1, *t2;
        t1 = strtok(line, "\t, ");
        t2 = strtok(NULL, "\t, ");
        if(t1 == NULL || t2 == NULL) {
            logstream(LOG_ERROR) << "Input file is not the right format. Expected <from> <to>" << std::endl;
            assert(false);
        }
        vid_t from = atoi(t1);
        vid_t to = atoi(t2);
        if(from == to) continue;
        converter.convert(from, to);
    }
    converter.finalize();
}

/** =========================================================================== */
/*  This code has some bugs, I will fix them in a few days                      */
/** =========================================================================== */
/** split the beg_pos into multiple blocks, each block max size is BLOCKSIZE */
size_t split_blocks(std::string filename, int fnum, size_t block_size = BLOCK_SIZE) {
    eid_t max_nedges = (eid_t)block_size / sizeof(vid_t);
    logstream(LOG_INFO) << "start split blocks, blocksize = " << block_size / (1024 * 1024) << "MB, max_nedges = " << max_nedges << std::endl;

    vid_t cur_pos  = 0;
    eid_t rd_edges = 0;  /* read edges */
    vid_t rd_verts = 0;  /* read vertices */
    std::vector<vid_t> vblocks;  /* vertex blocks */
    std::vector<eid_t> eblocks;  /* edge   blocks */
    vblocks.push_back(cur_pos);
    eblocks.push_back(rd_edges);

    std::string name = concatnate_name(filename, fnum) + ".beg";
    int fd = open(name.c_str(), O_RDONLY);
    assert(fd >= 0);
    vid_t nvertices = lseek(fd, 0, SEEK_END) / sizeof(eid_t);
    eid_t *beg_pos = (eid_t*)malloc(VERT_SIZE * sizeof(eid_t));
    assert(beg_pos != NULL);

    vid_t rv;
    while(rd_verts < nvertices) {
        rv = min_value(nvertices - rd_verts, VERT_SIZE);
        pread(fd, beg_pos, (size_t)rv * sizeof(eid_t), (off_t)rd_verts * sizeof(eid_t));
        for(vid_t v = 0; v < rv; v++) {
            if(beg_pos[v] - rd_edges > max_nedges) {
                logstream(LOG_INFO) << "Block " << vblocks.size() - 1 << " : [ " << cur_pos << ", " << rd_verts + v - 1 << " ), csr position : [ " << rd_edges << ", " << beg_pos[v-1] << " )" << std::endl;
                cur_pos = rd_verts + v - 1;
                vblocks.push_back(cur_pos);
                rd_edges = beg_pos[v-1];
                eblocks.push_back(rd_edges);
            }
        }
        rd_verts += rv;
    }
    logstream(LOG_INFO) << "Block " << vblocks.size() - 1 << " : [ " << cur_pos << ", " << nvertices << " ), csr position : [ " << rd_edges << ", " << beg_pos[rv-1] << " )" << std::endl;
    logstream(LOG_INFO) << "Total blocks num : " << vblocks.size() << std::endl;
    close(fd);
    vblocks.push_back(nvertices-1);
    rd_edges = beg_pos[rv-1];
    eblocks.push_back(rd_edges);

    /** write the vertex split points into vertex block file */
    std::string vblockfile = get_vert_blocks_name(filename, block_size);
    auto vblf = std::fstream(vblockfile.c_str(), std::ios::out | std::ios::binary);
    vblf.write((char*)&vblocks[0], vblocks.size() * sizeof(vid_t));
    vblf.close();

    /** write the edge split points into edge block file */
    std::string eblockfile = get_edge_blocks_name(filename, block_size);
    auto eblf = std::fstream(eblockfile.c_str(), std::ios::out | std::ios::binary);
    eblf.write((char*)&eblocks[0], eblocks.size() * sizeof(eid_t));
    eblf.close();

    return vblocks.size() - 1;
}

#endif