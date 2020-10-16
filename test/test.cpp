#include "preprocess/graph_converter.hpp"

int main(int argc, char* argv[]) {
    assert(argc >= 2);
    std::string input = argv[1];
    std::string output = remove_extension(file_base_name(input));
    graph_converter converter(output);
    convert(input, converter);
    return 0;
}