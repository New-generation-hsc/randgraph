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

int main(int argc, char* argv[]) {
    assert(argc >= 2);
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
    walk_schedule_t block_scheduler(&conf, 0.2);
    graph_walk walk_mangager(conf, blocks, driver);
    graph_cache cache(blocks.nblocks, conf.blocksize);
    
    randomwalk_t userprogram(10000, 25, 0.15);
    graph_engine engine(cache, walk_mangager, driver, conf);
    
    engine.prologue(userprogram);
    engine.run(userprogram, block_scheduler);
    engine.epilogue(userprogram);

    return 0;
}