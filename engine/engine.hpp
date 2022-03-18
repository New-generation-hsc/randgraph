#ifndef _GRAPH_ENGINE_H_
#define _GRAPH_ENGINE_H_

#include <functional>
#include "cache.hpp"
#include "schedule.hpp"
#include "apps/userprogram.hpp"
#include "util/timer.hpp"
#include "metrics/metrics.hpp"
#include "sample.hpp"

template<typename walk_data_t, WalkType walk_type>
class graph_engine {
public:
    graph_cache                         *cache;
    graph_walk<walk_data_t, walk_type>  *walk_manager;
    graph_driver                        *driver;
    graph_config                        *conf;
    graph_timer                         gtimer;
    std::vector<unsigned int>           seeds;

    // statistic metric
    metrics &_m;

    graph_engine(graph_cache& _cache, graph_walk<walk_data_t, walk_type>& manager, graph_driver& _driver, graph_config& _conf, metrics &m) : _m(m){
        cache         = &_cache;
        walk_manager = &manager;
        driver        = &_driver;
        conf          = &_conf;
        seeds = std::vector<unsigned int>(conf->nthreads);
        for(tid_t tid = 0; tid < conf->nthreads; tid++) {
            seeds[tid] = time(NULL) + tid;
        }
    }

    template <typename AppType, typename AppConfig>
    void prologue(userprogram_t<AppType, AppConfig> &userprogram, std::function<void(graph_walk<walk_data_t, walk_type> *)> init_func = nullptr)
    {
        logstream(LOG_INFO) << "  =================  STARTED  ======================  " << std::endl;
        logstream(LOG_INFO) << "Random walks, random generate " << userprogram.get_numsources() << " walks on whole graph, exec_threads = " << conf->nthreads << std::endl;
        logstream(LOG_INFO) << "vertices : " << conf->nvertices << ", edges : " << conf->nedges << std::endl;
        srand(time(0));
        tid_t exec_threads = conf->nthreads;
        omp_set_num_threads(exec_threads);

        _m.start_time("run_app");
        userprogram.prologue(walk_manager, init_func);
    }

    template <typename BaseType, typename AppType, typename AppConfig>
    void run(userprogram_t<AppType, AppConfig> &userprogram, scheduler<BaseType> *block_scheduler, sample_policy_t *sampler)
    {
        logstream(LOG_DEBUG) << "graph blocks : " << walk_manager->global_blocks->nblocks << ", memory blocks : " << cache->ncblock << std::endl;
        logstream(LOG_INFO) << "Random walks start executing, please wait for a minute." << std::endl;
        gtimer.start_time();
        int run_count = 0;
        bid_t nblocks = walk_manager->global_blocks->nblocks, cur_block = nblocks;
        wid_t nwalks = 0, interval_max_walks = conf->nthreads * MAX_TWALKS;
        while(!walk_manager->test_finished_walks()) {
            // wid_t total_walks = walk_manager->nwalks();
            // if(total_walks <= 1000000) {
            //     driver->set_filter(false);
            //     walk_manager->load_bf = false;
            // }
            bid_t select_block = block_scheduler->schedule(*cache, *driver, *walk_manager);
            if (select_block % nblocks != cur_block)
            {
                cur_block = select_block % nblocks;
                wid_t total_walks = walk_manager->nwalks();

                bid_t cache_index = (*(walk_manager->global_blocks))[cur_block].cache_index;
                cache_block *run_block = &cache->cache_blocks[cache_index];

                logstream(LOG_DEBUG) << "run time : " << gtimer.runtime() << ", run block =  " << cur_block << std::endl;
                logstream(LOG_DEBUG) << "nverts = " << run_block->block->nverts << ", nedges = " << run_block->block->nedges << ", sampler : " << sampler->sample_name() << std::endl;
                logstream(LOG_DEBUG) << "run_count = " << run_count << ", total walks = " << total_walks << std::endl;

                driver->load_extra_meta(*cache, walk_manager->global_blocks, cache_index, cur_block, sampler->use_alias);
            }

            wid_t num_disk_walks = walk_manager->ndwalks(select_block), disk_load_walks = 0;
            nwalks = walk_manager->load_memory_walks(select_block);
            logstream(LOG_DEBUG) << "load memory walks from " << select_block / nblocks << " to " << cur_block << ", walks = " << nwalks << std::endl;
            update_walk(userprogram, nwalks, sampler);
            while (num_disk_walks > 0)
            {
                wid_t interval_walks = std::min(num_disk_walks, interval_max_walks);
                num_disk_walks -= interval_walks;
                nwalks = walk_manager->load_disk_walks(select_block, interval_walks, disk_load_walks);
                disk_load_walks += interval_walks;
                logstream(LOG_DEBUG) << "load disk walks from " << select_block / nblocks << " to " << cur_block << ", walks = " << nwalks << std::endl;
                update_walk(userprogram, nwalks, sampler);
            }
            walk_manager->dump_walks(select_block);
            run_count++;
        }
        logstream(LOG_DEBUG) << gtimer.runtime() << "s, total run count : " << run_count << std::endl;
    }

    template <typename AppType, typename AppConfig>
    void epilogue(userprogram_t<AppType, AppConfig> &userprogram)
    {
        userprogram.epilogue();
        _m.stop_time("run_app");
        logstream(LOG_INFO) << "  ================= FINISHED ======================  " << std::endl;
    }

    template <typename AppType, typename AppConfig>
    void update_walk(userprogram_t<AppType, AppConfig> &userprogram, wid_t nwalks, sample_policy_t *sampler)
    {
        if(nwalks < 100) omp_set_num_threads(1);
        else omp_set_num_threads(conf->nthreads);

        _m.start_time("exec_block_walk");
        {
            #pragma omp parallel for schedule(static)
            for(wid_t idx = 0; idx < nwalks; idx++) {
                userprogram.update_walk(walk_manager->walks[idx], cache, walk_manager, sampler, &seeds[omp_get_thread_num()], conf->dynamic);
            }
        }
        _m.stop_time("exec_block_walk");
    }

    // template <typename AppType, typename AppConfig>
    // void exec_block(userprogram_t<AppType, AppConfig> &userprogram, bid_t exec_block, sample_context_t *sampler_context, int run_count, wid_t total_walks) {
    //     block_walks_impl_t<walk_type> block_state;
    //     wid_t approximate_walks = block_state.query_block_state(*walk_manager, *cache, exec_block);
    //     bid_t cache_index = (*(walk_manager->global_blocks))[exec_block].cache_index;
    //     cache_block *run_block  = &cache->cache_blocks[cache_index];
    //     sample_policy_t *sampler = sampler_context->sample_switch(run_block, approximate_walks, total_walks);

    //     logstream(LOG_DEBUG) << "run time : " << gtimer.runtime() << ", run block =  " << exec_block << ", approximate walks = " << approximate_walks  << std::endl;
    //     logstream(LOG_DEBUG) << "nverts = " << run_block->block->nverts << ", nedges = " << run_block->block->nedges << ", walk density = " << (real_t)approximate_walks / run_block->block->nverts << ", sampler : " << sampler->sample_name() << std::endl;
    //     logstream(LOG_DEBUG) << "run_count = " << run_count << ", total walks = " << total_walks << std::endl;

    //     driver->load_extra_meta(*cache, walk_manager->global_blocks, cache_index, exec_block, sampler->use_alias);
    //     while(!block_state.has_finished()) {
    //         wid_t nwalks = block_state.load_walks(*walk_manager, exec_block);
    //         update_walk(userprogram, nwalks, sampler);
    //     }
    // }
};

#endif
