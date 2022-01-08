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
#include "apps/userprogram.hpp"
#include "metrics/metrics.hpp"
#include "metrics/reporter.hpp"

int main(int argc, const char *argv[])
{
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
    metrics m("node2vec");

    graph_walk<vid_t, SecondOrder> walk_mangager(conf.base_name, conf.nvertices, conf.nthreads, driver, blocks);
    bid_t nmblocks = get_option_int("nmblocks", blocks.nblocks);
    wid_t walks = (wid_t)get_option_int("walks", 100000);
    hid_t steps = (hid_t)get_option_int("length", 25);
    graph_cache cache(min_value(nmblocks, blocks.nblocks), conf.blocksize);

    node2vec_conf_t app_conf = { walks, steps, 0.5, 2, weighted };
    userprogram_t<node2vec_t, node2vec_conf_t> userprogram(app_conf);
    graph_engine<vid_t, SecondOrder> engine(cache, walk_mangager, driver, conf, m);

    second_order_its_sample_t its_sampler;
    second_order_alias_sample_t alias_sampler;
    second_order_reject_sample_t reject_sampler;

    // scheduler *scheduler = nullptr;
    second_order_sample_t *sampler = nullptr;
    std::string type = get_option_string("sample", "its");
    if (type == "its")
        sampler = &its_sampler;
    else if (type == "alias")
        sampler = &alias_sampler;
    else if (type == "reject")
        sampler = &reject_sampler;
    else
        sampler = &its_sampler;

    logstream(LOG_INFO) << "sample policy : " << sampler->sample_name() << std::endl;

    // scheduler<second_order_scheduler_t<graph_config>, graph_config> walk_scheduler(conf, m);
    scheduler<navie_graphwalker_scheduler_t<graph_config>, graph_config> walk_scheduler(conf, m);

    engine.prologue(userprogram);
    engine.run(userprogram, &walk_scheduler, sampler);
    engine.epilogue(userprogram);

    metrics_report(m);

    return 0;
}
