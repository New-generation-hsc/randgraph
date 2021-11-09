#include <omp.h>
#include "api/constants.hpp"
#include "engine/config.hpp"
#include "engine/cache.hpp"
#include "engine/schedule.hpp"
#include "engine/walk.hpp"
#include "engine/engine.hpp"
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
    load_graph_meta(base_name, &nvertices, &nedges);

    graph_config conf = {
        base_name,
        0,
        BLOCK_SIZE,
        (tid_t)omp_get_max_threads(),
        nvertices,
        nedges
    };

    graph_block blocks(&conf);
    graph_driver driver;
    metrics m("randomwalk");
    bool is_walk_schedule = get_option_bool("-w");
    graph_scheduler block_scheduler(&conf, m);
    walk_schedule_t walk_scheduler(&conf, 0.2, m);

    graph_walk walk_mangager(conf, blocks, driver);
    int nmblocks = get_option_int("nmblocks", blocks.nblocks);
    graph_cache cache(nmblocks, conf.blocksize);

    randomwalk_t userprogram(100000, 25, 0.15, m);
    graph_engine engine(cache, walk_mangager, driver, conf, m);

    engine.prologue(userprogram);
    if(is_walk_schedule) engine.run(userprogram, walk_scheduler);
    else engine.run(userprogram, block_scheduler);
    engine.epilogue(userprogram);

    metrics_report(m);

    return 0;
}
