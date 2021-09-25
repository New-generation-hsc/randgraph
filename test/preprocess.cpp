#include "preprocess/graph_converter.hpp"
#include "engine/config.hpp"

int main(int argc, char* argv[]) {
    assert(argc >= 2);
    logstream(LOG_INFO) << "app : " << argv[0] << ", dataset : " << argv[1] << std::endl;
    std::string input = argv[1];
    graph_converter converter(remove_extension(input));
    convert(input, converter);
    logstream(LOG_INFO) << "  ================= FINISHED ======================  " << std::endl;
    return 0;
}