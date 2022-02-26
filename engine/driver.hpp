#ifndef _GRAPH_DRIVER_H_
#define _GRAPH_DRIVER_H_

#include "cache.hpp"
#include "util/io.hpp"
#include "api/graph_buffer.hpp"
#include "api/types.hpp"
#include "metrics/metrics.hpp"

/** graph_driver
 * This file contribute to define the operations of how to read from disk
 * or how to write graph data into disk
 */

class graph_driver {
private:
    int vertdesc, edgedesc, degdesc, whtdesc;  /* the beg_pos, csr, degree file descriptor */
    int prob_desc, alias_desc, acc_wht_desc; /* the alias table descriptor and accumulate weight descriptor */
    metrics &_m;
    bool _weighted;
public:
    graph_driver(graph_config *conf, metrics &m) : _m(m)
    {
        vertdesc = edgedesc = whtdesc = 0;
        prob_desc = alias_desc = acc_wht_desc = 0;
        _weighted = false;
        this->setup(conf);
    }

    graph_driver(metrics &m) : _m(m) {
        vertdesc = edgedesc = whtdesc = 0;
        prob_desc = alias_desc = acc_wht_desc = 0;
        _weighted = false;
    }

    void setup(graph_config *conf) {
        this->destory();

        std::string beg_pos_name = get_beg_pos_name(conf->base_name, conf->fnum, conf->reordered);
        std::string csr_name = get_csr_name(conf->base_name, conf->fnum, conf->reordered);
        logstream(LOG_DEBUG) << "load beg_pos_name : " << beg_pos_name << ", csr_name : " << csr_name << std::endl;

        vertdesc = open(beg_pos_name.c_str(), O_RDONLY);
        edgedesc = open(csr_name.c_str(), O_RDONLY);
        _weighted = conf->is_weighted;

        if (_weighted)
        {
            std::string prob_name = get_prob_name(conf->base_name, conf->fnum);
            if(test_exists(prob_name)) prob_desc = open(prob_name.c_str(), O_RDONLY);
            std::string alias_name = get_alias_name(conf->base_name, conf->fnum);
            if(test_exists(alias_name)) alias_desc = open(alias_name.c_str(), O_RDONLY);

            std::string acc_weight_name = get_accumulate_name(conf->base_name, conf->fnum);
            if(test_exists(acc_weight_name)) acc_wht_desc = open(acc_weight_name.c_str(), O_RDONLY);
            std::string weight_name = get_weights_name(conf->base_name, conf->fnum);
            if(test_exists(weight_name)) whtdesc = open(weight_name.c_str(), O_RDONLY);
        }
    }

    void load_block_info(graph_cache &cache, graph_block *global_blocks, bid_t cache_index, bid_t block_index)
    {
        _m.start_time("load_block_info");
        cache.cache_blocks[cache_index].block = &global_blocks->blocks[block_index];
        cache.cache_blocks[cache_index].block->status = ACTIVE;
        cache.cache_blocks[cache_index].block->cache_index = cache_index;

        cache.cache_blocks[cache_index].beg_pos = (eid_t *)realloc(cache.cache_blocks[cache_index].beg_pos, (global_blocks->blocks[block_index].nverts + 1) * sizeof(eid_t));
        cache.cache_blocks[cache_index].csr = (vid_t *)realloc(cache.cache_blocks[cache_index].csr, global_blocks->blocks[block_index].nedges * sizeof(vid_t));

        load_block_vertex(vertdesc, cache.cache_blocks[cache_index].beg_pos, global_blocks->blocks[block_index]);
        load_block_edge(edgedesc, cache.cache_blocks[cache_index].csr, global_blocks->blocks[block_index]);

        if(_weighted) {
            cache.cache_blocks[cache_index].acc_weights = (real_t *)realloc(cache.cache_blocks[cache_index].acc_weights, global_blocks->blocks[block_index].nedges * sizeof(real_t));
            load_block_weight(acc_wht_desc, cache.cache_blocks[cache_index].acc_weights, global_blocks->blocks[block_index]);
        }
        cache.cache_blocks[cache_index].loaded_alias = false;
        _m.stop_time("load_block_info");
    }

    void load_extra_meta(graph_cache &cache, graph_block *global_blocks, bid_t cache_index, bid_t block_index, bool use_alias) {
        _m.start_time("load_extra_meta");
        if (_weighted)
        {
            if(use_alias && !cache.cache_blocks[cache_index].loaded_alias) {
                logstream(LOG_DEBUG) << "load block info alias table for block : " << block_index << std::endl;
                cache.cache_blocks[cache_index].prob = (real_t *)realloc(cache.cache_blocks[cache_index].prob, global_blocks->blocks[block_index].nedges * sizeof(real_t));
                load_block_prob(prob_desc, cache.cache_blocks[cache_index].prob, global_blocks->blocks[block_index]);
                cache.cache_blocks[cache_index].alias = (vid_t*)realloc(cache.cache_blocks[cache_index].alias, global_blocks->blocks[block_index].nedges * sizeof(vid_t));
                load_block_alias(alias_desc, cache.cache_blocks[cache_index].alias, global_blocks->blocks[block_index]);
                cache.cache_blocks[cache_index].loaded_alias = true;
            }
        }
        _m.stop_time("load_extra_meta");
    }

    void destory() {
        if(vertdesc > 0) close(vertdesc);
        if(edgedesc > 0) close(edgedesc);
        if(_weighted) {
            if(prob_desc > 0) close(prob_desc);
            if(alias_desc > 0) close(alias_desc);
            if(acc_wht_desc > 0) close(acc_wht_desc);
            if(whtdesc > 0) close(whtdesc);
        }
    }

    void load_block_vertex(int fd, eid_t *buf, const block_t &block) {
        load_block_range(fd, buf, block.nverts + 1, block.start_vert * sizeof(eid_t));
    }

    void load_block_degree(int fd, vid_t *buf, const block_t &block) {
        load_block_range(fd, buf, block.nverts, block.start_vert * sizeof(vid_t));
    }

    void load_block_edge(int fd, vid_t *buf, const block_t &block) {
        load_block_range(fd, buf, block.nedges, block.start_edge * sizeof(vid_t));
    }

    void load_block_weight(int fd, real_t* buf, const block_t& block) {
        load_block_range(fd, buf, block.nedges, block.start_edge * sizeof(real_t));
    }

    void load_block_prob(int fd, real_t* buf, const block_t& block) {
        load_block_range(fd, buf, block.nedges, block.start_edge * sizeof(real_t));
    }

    void load_block_alias(int fd, vid_t* buf, const block_t& block) {
        load_block_range(fd, buf, block.nedges, block.start_edge * sizeof(vid_t));
    }

    template<typename walk_data_t>
    void load_walk(int fd, size_t cnt, size_t loaded_cnt, graph_buffer<walk_data_t> &walks) {
        off_t off = loaded_cnt * sizeof(walk_data_t);
        load_block_range(fd, walks.buffer_begin(), cnt, off);
        walks.set_size(cnt);
    }

    template<typename walk_data_t>
    void dump_walk(int fd, graph_buffer<walk_data_t> &walks) {
        dump_block_range(fd, walks.buffer_begin(), walks.size(), 0);
    }
};

#endif
