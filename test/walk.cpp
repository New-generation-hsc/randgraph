#include <omp.h>
#include "api/constants.hpp"
#include "engine/config.hpp"
#include "engine/cache.hpp"
#include "engine/schedule.hpp"
#include "engine/walk.hpp"
#include "engine/engine.hpp"
#include "engine/sample.hpp"
#include "logger/logger.hpp"
#include "util/io.hpp"
#include "util/util.hpp"
#include "apps/randomwalk.hpp"
#include "metrics/metrics.hpp"
#include "metrics/reporter.hpp"

int main(int argc, const char* argv[]) {
    assert(argc >= 2);
    set_argc(argc, argv);
    logstream(LOG_INFO) << "app : " << argv[0] << ", dataset : " << argv[1] << std::endl;
    std::string input = remove_extension(argv[1]);
    std::string base_name = randgraph_output_filename(get_path_name(input), get_file_name(input), BLOCK_SIZE);

    /* graph meta info */
    vid_t nvertices;
    eid_t nedges;
    bool weighted = get_option_bool("weighted");
    load_graph_meta(base_name, &nvertices, &nedges, weighted);

    graph_config conf = {
        base_name,
        0,
        BLOCK_SIZE,
        (tid_t)omp_get_max_threads(),
        nvertices,
        nedges,
        weighted
    };

    graph_block blocks(&conf);
    graph_driver driver;
    metrics m("randomwalk");
    bool is_walk_schedule = get_option_bool("-w");
    graph_scheduler block_scheduler(&conf, m);
    walk_schedule_t walk_scheduler(&conf, 0.2, m);

    graph_walk walk_mangager(conf, blocks, driver);
    bid_t nmblocks = get_option_int("nmblocks", blocks.nblocks);
    int walks = get_option_int("walks", 100000);
    int steps = get_option_int("length", 25);
    graph_cache cache(min_value(nmblocks, blocks.nblocks), conf.blocksize);

    randomwalk_t userprogram(walks, steps, 0.15, m);
    graph_engine engine(cache, walk_mangager, driver, conf, m);

    naive_sample_t  naive_sampler(m);
    its_sample_t    its_sampler(m);
    alias_sample_t  alias_sampler(m);
    reject_sample_t reject_sampler(m);

    scheduler *scheduler = nullptr;
    sample_policy_t *sampler = nullptr;


    engine.prologue(userprogram);
    if(is_walk_schedule) scheduler = &walk_scheduler;
    else scheduler = &block_scheduler;

    std::string type = get_option_string("sample", "naive");
    if(type == "its") sampler = &its_sampler;
    else if(type == "alias") sampler = &alias_sampler;
    else if(type == "reject") sampler = &reject_sampler;
    else sampler = &naive_sampler;

    logstream(LOG_INFO) << "sample policy : " << sampler->sample_name() << std::endl;
    engine.run(userprogram, scheduler, sampler);
    engine.epilogue(userprogram);

    metrics_report(m);

    return 0;
}
