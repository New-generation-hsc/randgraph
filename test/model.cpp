#include "util/lp_solver.hpp"

int main(int argc, char **argv)
{
    // std::vector<double> weights = {0, 0, 3, 2, 0, 0, 1, 4, 1, 2, 0, 6, 4, 3, 5, 0};
    // std::vector<double> weights = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0};
    std::vector<Edge_t> edges = {
        {0, 2, 3},
        {0, 3, 2},
        {1, 2, 1},
        {1, 3, 4},
        {2, 0, 1},
        {2, 1, 2},
        {2, 3, 6},
        {3, 0, 4},
        {3, 1, 3},
        {3, 2, 5}
    };

    int num_vert = 4;
    int num_edge = 10;
    int num_cache = 2;

    std::vector<size_t> verts = {0, 1, 2, 3};
    std::vector<bool> cache_blocks = {1, 1, 0, 0};

    // DataModel data(edges, verts, num_vert, num_edge, num_cache);
    DataModel data(edges, cache_blocks, num_vert, num_edge, num_cache, 1);
    std::vector<bool> ans(num_vert);
    bool ret = operations_research::lp_solve_schedule(data, ans);
    if(ret) {
        for(int i = 0; i < num_vert; i++) std::cout << "v_" << i << " : " << ans[i] << std::endl;
    }
    return EXIT_SUCCESS;
}