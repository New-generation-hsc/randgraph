#include "preprocess/graph_converter.hpp"
#include "engine/config.hpp"
#include "api/cmdopts.hpp"

int main(int argc, const char* argv[]) {
    assert(argc >= 2);
    set_argc(argc, argv);
    logstream(LOG_INFO) << "app : " << argv[0] << ", dataset : " << argv[1] << std::endl;
    std::string input = argv[1];
    bool weighted = get_option_bool("weighted");
    bool sorted   = get_option_bool("sorted");
    graph_converter converter(remove_extension(input), weighted, sorted);
    convert(input, converter);
    logstream(LOG_INFO) << "  ================= FINISHED ======================  " << std::endl;
    return 0;
}