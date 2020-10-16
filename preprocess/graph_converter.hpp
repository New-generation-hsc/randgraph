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
        std::string name = concatnate_name(output_filename, fnum) + ".beg";
        appendfile(name, beg_pos.buffer_begin(), beg_pos.size());
        buf_vstart += deg.size();
        beg_pos.clear();
    }

    void flush_degree() { 
        std::string name = concatnate_name(output_filename, fnum) + ".deg";
        appendfile(name, deg.buffer_begin(), deg.size());
        deg.clear();
    }

    void flush_csr() {
        size_t max_nedges = FILE_SIZE / sizeof(eid_t);
        if(rd_edges + csr.size() > max_nedges) {
            fnum += 1;
            rd_edges = 0;
        }
        std::string name = concatnate_name(output_filename, fnum) + ".csr";
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
        logstream(LOG_INFO) << "Buffer : [ " << buf_vstart << ", " <<  buf_vstart + beg_pos.size() << " ), csr position : [ " << buf_estart << ", " << csr_pos << ")" << std::endl;
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

size_t convert(std::string filename, graph_converter &converter) {
    
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

#endif