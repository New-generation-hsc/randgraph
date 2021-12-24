#ifndef _GRAPH_ENGINE_H_
#define _GRAPH_ENGINE_H_

#include "cache.hpp"
#include "schedule.hpp"
#include "apps/randomwalk.hpp"
#include "util/timer.hpp"
#include "metrics/metrics.hpp"
#include "sample.hpp"

template<typename walk_data_t, WalkType walk_type>
class graph_engine {
public:
    bid_t                               exec_block;
    graph_cache                         *cache;
    graph_walk<walk_data_t, walk_type>  *walk_manager;
    graph_driver                        *driver;
    graph_config                        *conf;
    graph_timer                         timer;

    // statistic metric
    metrics &_m;

    graph_engine(graph_cache& _cache, graph_walk<walk_data_t, walk_type>& manager, graph_driver& _driver, graph_config& _conf, metrics &m) : _m(m){
        cache         = &_cache;
        walk_manager = &manager;
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

        userprogram.prologue<walk_data_t, walk_type>(walk_manager);
    }

    template<typename BaseType, typename Config>
    void run(randomwalk_t& userprogram, scheduler<BaseType, Config>* block_scheduler, sample_policy_t* sampler) {
        logstream(LOG_DEBUG) << "graph blocks : " << walk_manager->global_blocks->nblocks << ", memory blocks : " << cache->ncblock << std::endl;
        logstream(LOG_INFO) << "Random walks start executing, please wait for a minute." << std::endl;
        timer.start_time();
        int run_count = 0;
        while(!walk_manager->test_finished_walks()) {
            bid_t exec_idx = block_scheduler->schedule(*cache, *driver, *walk_manager);
            exec_block = cache->cache_blocks[exec_idx].block->blk;

            cache_block *run_block  = &cache->cache_blocks[exec_idx];
            run_block->block->status = USING;

            /* load `exec_block` walks into memory */
            wid_t nwalks = walk_manager->nblockwalks(exec_block);
            walk_manager->load_walks(exec_block);

            vid_t nverts = run_block->block->nverts;
            eid_t nedges = run_block->block->nedges;
            if(run_count % 100 == 0)
            {
                logstream(LOG_DEBUG) << timer.runtime() << "s : run count : " << run_count << std::endl;
                logstream(LOG_DEBUG) << "nverts = " << nverts << ", nedges = " << nedges << std::endl;
                logstream(LOG_INFO) << "exec_block : " << exec_block << ", walk num : " << nwalks << ", walksum : " << walk_manager->nwalks() << std::endl;
            }
            exec_block_walk(userprogram, nwalks, sampler);
            walk_manager->dump_walks(exec_block);
            run_block->block->status = USED;
            run_count++;
        }
        logstream(LOG_DEBUG) << timer.runtime() << "s, total run count : " << run_count << std::endl;
    }

    void epilogue(randomwalk_t& userprogram) {
        logstream(LOG_INFO) << "  ================= FINISHED ======================  " << std::endl;
    }

    void exec_block_walk(randomwalk_t &userprogram, wid_t nwalks, sample_policy_t* sampler) {
        if(nwalks < 100) omp_set_num_threads(1);
        else omp_set_num_threads(conf->nthreads);

        _m.start_time("exec_block_walk");
        {
            #pragma omp parallel for schedule(static)
            for(wid_t idx = 0; idx < nwalks; idx++) {
                userprogram.update_walk(walk_manager->walks[idx], cache, walk_manager, sampler);
            }
        }
        _m.stop_time("exec_block_walk");
    }
};

#endif
