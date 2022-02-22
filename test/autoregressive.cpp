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
#include "apps/secondorder.hpp"

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
    metrics m("autoregressive");
    graph_driver driver(&conf, m);

    graph_walk<vid_t, SecondOrder> walk_mangager(conf.base_name, conf.nvertices, conf.nthreads, driver, blocks);
    bid_t nmblocks = get_option_int("nmblocks", blocks.nblocks);
    wid_t walks = (wid_t)get_option_int("walks", 100000);
    hid_t steps = (hid_t)get_option_int("length", 25);
    real_t alpha = (real_t)get_option_float("alpha", 0.2);
    graph_cache cache(min_value(nmblocks, blocks.nblocks), conf.blocksize);

    second_order_param_t app_param = { alpha, (real_t)(1.0 - alpha), (real_t)(1.0 - alpha), (real_t)(1.0 - alpha)};
    second_order_conf_t app_conf = {walks, steps, app_param};
    userprogram_t<second_order_app_t, second_order_conf_t> userprogram(app_conf);
    graph_engine<vid_t, SecondOrder> engine(cache, walk_mangager, driver, conf, m);

    naive_sample_t naive_sampler;
    its_sample_t its_sampler;
    alias_sample_t alias_sampler;
    reject_sample_t reject_sampler;
    second_order_soopt_sample_t soopt_sampler;
    second_order_opt_alias_sample_t opt_alias_sampler;

    // scheduler *scheduler = nullptr;
    sample_policy_t *sampler = nullptr;
    std::string type = get_option_string("sample", "its");
    if (type == "its")
        sampler = &its_sampler;
    else if (type == "alias")
        sampler = &alias_sampler;
    else if (type == "reject")
        sampler = &reject_sampler;
    else if (type == "soopt")
        sampler = &soopt_sampler;
    else if (type == "opt_alias")
        sampler = &opt_alias_sampler;
    else
        sampler = &its_sampler;

    logstream(LOG_INFO) << "sample policy : " << sampler->sample_name() << std::endl;

    scheduler<second_order_scheduler_t> walk_scheduler(m);
    its_sample_t acc_its_sampler(true);
    complex_sample_context_t sample_context(sampler, &acc_its_sampler);

    engine.prologue(userprogram);
    engine.run(userprogram, &walk_scheduler, &sample_context);
    engine.epilogue(userprogram);

    metrics_report(m);

    return 0;
}
