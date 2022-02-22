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
#include "apps/pagerank.hpp"
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
    metrics m("pagerank");

    graph_walk<empty_data_t, FirstOrder> walk_mangager(conf.base_name, conf.nvertices, conf.nthreads, driver, blocks);
    bid_t nmblocks = get_option_int("nmblocks", blocks.nblocks);
    vid_t firstsource = (vid_t)get_option_int("firstsource", 10009);
    // vid_t numsources = (vid_t)get_option_int("numsources", 4847571);
    vid_t numsources = (vid_t)get_option_int("numsources", 1);
    // wid_t walkpersource = (wid_t)get_option_int("walkpersource", 1);
    wid_t walkpersource = (wid_t)get_option_int("walkpersource", 1000000);
    hid_t steps = (hid_t)get_option_int("length", 6);
    graph_cache cache(min_value(nmblocks, blocks.nblocks), conf.blocksize);

    pagerank_conf_t app_conf = { firstsource, numsources, walkpersource, steps, 0.0 };
    userprogram_t<pagerank_t, pagerank_conf_t> userprogram(app_conf);
    graph_engine<empty_data_t, FirstOrder> engine(cache, walk_mangager, driver, conf, m);

    naive_sample_t  naive_sampler;
    its_sample_t    its_sampler;
    alias_sample_t  alias_sampler;
    reject_sample_t reject_sampler;

    // scheduler *scheduler = nullptr;
    sample_policy_t *sampler = nullptr;
    std::string type = get_option_string("sample", "naive");
    if(type == "its") sampler = &its_sampler;
    else if(type == "alias") sampler = &alias_sampler;
    else if(type == "reject") sampler = &reject_sampler;
    else sampler = &naive_sampler;

    logstream(LOG_INFO) << "sample policy : " << sampler->sample_name() << std::endl;

    scheduler<drunkardmob_scheduler<graph_config>, graph_config> walk_scheduler(conf, sampler, m);
    // scheduler<graph_scheduler<graph_config>, graph_config> graph_scheduler(conf, m);
    naive_sample_context_t sample_context(sampler);

    engine.prologue(userprogram);
    engine.run(userprogram, &walk_scheduler, &sample_context);
    engine.epilogue(userprogram);

    metrics_report(m);

    return 0;
}
