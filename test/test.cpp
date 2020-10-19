#include "preprocess/graph_converter.hpp"

int main(int argc, char* argv[]) {
    assert(argc >= 2);
    logstream(LOG_INFO) << "app : " << argv[0] << ", dataset : " << argv[1] << std::endl;
    std::string input = argv[1];
    std::string output = remove_extension(input);
    // graph_converter converter(output);
    // convert(input, converter);
    split_blocks(output, 0, BLOCK_SIZE);
    logstream(LOG_INFO) << "  ================= FINISHED ======================  " << std::endl;
    return 0;
}