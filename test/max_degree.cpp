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
    max_link_block(converter.get_output_filename(), 0, BLOCK_SIZE);
    logstream(LOG_INFO) << "  ================= FINISHED ======================  " << std::endl;
    return 0;
}
