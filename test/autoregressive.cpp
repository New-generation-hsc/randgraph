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
    bool reordered = get_option_bool("reordered");
    bool filter = get_option_bool("filter");
    bool dynamic = get_option_bool("dynamic");
    
    wid_t walks = (wid_t)get_option_int("walksource", 100000);
    hid_t steps = (hid_t)get_option_int("length", 25);
    real_t alpha = (real_t)get_option_float("alpha", 0.2);

    load_graph_meta(base_name, &nvertices, &nedges, weighted);

    graph_config conf = {
        base_name,
        0,
        BLOCK_SIZE,
        (tid_t)omp_get_max_threads(),
        nvertices,
        nedges,
        weighted,
        reordered,
        filter,
        dynamic
    };

    graph_block blocks(&conf);
    bid_t nmblocks = get_option_int("nmblocks", blocks.nblocks);

    metrics m("autoregressive_walksource_" + std::to_string(walks) + "_steps_" + std::to_string(steps) + "_dataset_" + argv[1]);
    graph_driver driver(&conf, m);

    graph_walk<vid_t, SecondOrder> walk_mangager(conf, driver, blocks, &m);
    
    graph_cache cache(min_value(nmblocks, blocks.nblocks), &conf);

    second_order_param_t app_param = { alpha, (real_t)(1.0 - alpha), (real_t)(1.0 - alpha), (real_t)(1.0 - alpha)};
    second_order_conf_t app_conf = {walks, steps, app_param};

    second_order_func_t app_func;
    app_func.query_equal_func = [&alpha](const vertex_t& prev_vertex, const vertex_t& cur_vertex) {
        vid_t max_degree = std::max(prev_vertex.degree, cur_vertex.degree);
        return (1.0 - alpha) * max_degree / cur_vertex.degree;
    };
    app_func.query_comm_neighbor_func = [&alpha](const vertex_t& prev_vertex, const vertex_t& cur_vertex) {
        vid_t max_degree = std::max(prev_vertex.degree, cur_vertex.degree);
        return (1.0 - alpha) * max_degree / cur_vertex.degree +  alpha * max_degree / prev_vertex.degree;
    };
    app_func.query_other_vertex_func = [&alpha](const vertex_t& prev_vertex, const vertex_t& cur_vertex) {
        vid_t max_degree = std::max(prev_vertex.degree, cur_vertex.degree);
        return (1.0 - alpha) * max_degree / cur_vertex.degree;
    };
    app_func.query_upper_bound_func = [&alpha](const vertex_t& prev_vertex, const vertex_t& cur_vertex) {
        vid_t max_degree = std::max(prev_vertex.degree, cur_vertex.degree);
        if(prev_vertex.degree > 0) return (1.0 - alpha) * max_degree / cur_vertex.degree +  alpha * max_degree / prev_vertex.degree;
        else return (1.0 - alpha) * max_degree / cur_vertex.degree;
    };
    app_func.query_lower_bound_func = [&alpha](const vertex_t& prev_vertex, const vertex_t& cur_vertex) {
        vid_t max_degree = std::max(prev_vertex.degree, cur_vertex.degree);
        return (1.0 - alpha) * max_degree / cur_vertex.degree;
    };
    app_conf.func_param = app_func;

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

    scheduler<naive_graphwalker_scheduler_t> walk_scheduler(m);
    // complex_sample_context_t sample_context(sampler, &its_sampler);
    // naive_sample_context_t sample_context(sampler);

    auto init_func = [walks, steps](graph_walk<vid_t, SecondOrder> *walk_manager)
    {
        #pragma omp parallel for schedule(static)
        for(wid_t off = 0; off < walks; off++)
        {
            vid_t vertex = rand() % walk_manager->nvertices;
            walker_t<vid_t> walker = walker_makeup<vid_t>(vertex, off, vertex, vertex, steps);
            walk_manager->move_walk(walker);
        }
    };

    engine.prologue(userprogram, init_func);
    engine.run(userprogram, &walk_scheduler, sampler);
    engine.epilogue(userprogram);

    sampler->report();
    metrics_report(m);

    return 0;
}
