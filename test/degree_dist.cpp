
#include "preprocess/graph_converter.hpp"
#include "preprocess/precompute.hpp"
#include "engine/config.hpp"
#include "api/cmdopts.hpp"

int main(int argc, const char* argv[]) {
    assert(argc >= 2);
    set_argc(argc, argv);
    logstream(LOG_INFO) << "app : " << argv[0] << ", dataset : " << argv[1] << std::endl;
    std::string input = argv[1];
    graph_converter converter(remove_extension(input));
    vid_t pivot_vertex = (vid_t)get_option_int("vertex", 4800000);
    calc_vertex_neighbor_dist(converter.get_output_filename(), 0, BLOCK_SIZE, pivot_vertex);
    logstream(LOG_INFO) << "  ================= FINISHED ======================  " << std::endl;
    return 0;
}
