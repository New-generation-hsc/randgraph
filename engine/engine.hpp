#ifndef _GRAPH_ENGINE_H_
#define _GRAPH_ENGINE_H_

#include "cache.hpp"
#include "schedule.hpp"
#include "apps/randomwalk.hpp"
#include "util/timer.hpp"
#include "metrics/metrics.hpp"

class graph_engine {
public:
    bid_t        exec_block;
    graph_cache  *cache;
    graph_walk   *walk_mangager;
    graph_driver *driver;
    graph_config *conf;
    graph_timer      timer;

    // statistic metric
    metrics &_m;

    graph_engine(graph_cache& _cache, graph_walk& mangager, graph_driver& _driver, graph_config& _conf, metrics &m) : _m(m){
        cache         = &_cache;
        walk_mangager = &mangager;
        driver        = &_driver;
        conf          = &_conf;
    }

    void prologue(randomwalk_t& userprogram) {
        logstream(LOG_INFO) << "  =================  STARTED  ======================  " << std::endl;
        logstream(LOG_INFO) << "Random walks, random generate " << userprogram.get_numsources() << " walks on whole graph, exec_threads = " << conf->nthreads << std::endl;
        logstream(LOG_INFO) << "vertices : " << conf->nvertices << ", edges : " << conf->nedges << std::endl;
        srand(time(0));
        tid_t exec_threads = conf->nthreads;
        omp_set_num_threads(exec_threads);

        // for parallel generate random sources
        {
            #pragma omp parallel for schedule(static)
            for(wid_t idx = 0; idx < userprogram.get_numsources(); idx++) {
                vid_t s = rand() % walk_mangager->nvertices;
                bid_t blk = walk_mangager->global_blocks->get_block(s);
                walk_t walk = WALKER_MAKEUP(s, s, userprogram.get_hops());
                walk_mangager->move_walk(walk, blk, omp_get_thread_num(), s, userprogram.get_hops());
            }
        }

        for(bid_t blk = 0; blk < walk_mangager->global_blocks->nblocks; blk++) {
            walk_mangager->set_max_hop(blk, userprogram.get_hops());
        }

        const auto& blocks = walk_mangager->global_blocks->blocks;
        for(bid_t blk = 0; blk < walk_mangager->global_blocks->nblocks; blk++) {
            logstream(LOG_INFO) << "blk [ " << blk << " ] : vert = [ " << blocks[blk].start_vert << ", " << blocks[blk].start_vert + blocks[blk].nverts << " ], csr = [ ";
            logstream(LOG_INFO) << blocks[blk].start_edge << ", " << blocks[blk].start_edge + blocks[blk].nedges << " ], walks num = ";
            logstream(LOG_INFO) << walk_mangager->nblockwalks(blk) << ", vertices num = " << blocks[blk].nverts << std::endl;
        }
    }

    void run(randomwalk_t& userprogram, scheduler& block_scheduler) {
        logstream(LOG_DEBUG) << "graph blocks : " << walk_mangager->global_blocks->nblocks << ", memory blocks : " << cache->ncblock << std::endl;
        logstream(LOG_INFO) << "Random walks start executing, please wait for a minute." << std::endl;
        timer.start_time();
        int run_count = 0;
        while(!walk_mangager->test_finished_walks()) {
            bid_t exec_idx = block_scheduler.schedule(*cache, *driver, *walk_mangager);
            exec_block = cache->cache_blocks[exec_idx].block->blk;
#ifdef FASTSKIP
            if(!walk_mangager->ismodify[exec_block]) continue; // fast skip
#endif
            cache_block *run_block  = &cache->cache_blocks[exec_idx];
            run_block->block->status = USING;

            /* load `exec_block` walks into memory */
            wid_t nwalks = walk_mangager->nblockwalks(exec_block);
            walk_mangager->load_walks(exec_block);

            vid_t nverts = run_block->block->nverts;
            eid_t nedges = run_block->block->nedges;
            if(run_count % 100 == 0)
            {
                logstream(LOG_DEBUG) << timer.runtime() << "s : run count : " << run_count << std::endl;
                logstream(LOG_DEBUG) << "nverts = " << nverts << ", nedges = " << nedges << std::endl;
                logstream(LOG_INFO) << "exec_block : " << exec_block << ", walk num : " << nwalks << ", walksum : " << walk_mangager->nwalks() << std::endl;
            }
            exec_block_walk(userprogram, nwalks, run_block);
            walk_mangager->dump_walks(exec_block);
            run_block->block->status = USED;
            run_count++;
        }
        logstream(LOG_DEBUG) << timer.runtime() << "s, total run count : " << run_count << std::endl;
    }

    void epilogue(randomwalk_t& userprogram) {
        logstream(LOG_INFO) << "  ================= FINISHED ======================  " << std::endl;
    }

    void exec_block_walk(randomwalk_t &userprogram, wid_t nwalks, cache_block *run_block) {
        if(nwalks < 100) omp_set_num_threads(1);
        else omp_set_num_threads(conf->nthreads);

        {
            #pragma omp parallel for schedule(static)
            for(wid_t idx = 0; idx < nwalks; idx++) {
                userprogram.update_walk(walk_mangager->walks[idx], run_block, walk_mangager);
            }
        }
    }
};

#endif
