#define TEST_SAMPLE

#include "engine/sample.hpp"
#include <iostream>
#include "metrics/metrics.hpp"

int main(int argc, const char* argv[]) {
    set_argc(argc, argv);
    metrics m("randomwalk");
    naive_sample_t naive_sampler;
    its_sample_t its_sampler;
    alias_sample_t alias_sampler;
    reject_sample_t reject_sampler;

    std::vector<real_t> weights(5);
    for (auto &w : weights) w = static_cast<real_t>(rand()) / static_cast<real_t>(RAND_MAX) * 10.0;
    for(const auto & w : weights) std::cout << w << " ";
    std::cout << std::endl;

    its_sampler.sample(weights);
    alias_sampler.sample(weights);
    return 0;
}
