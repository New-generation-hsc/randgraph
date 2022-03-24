#include "util/lp_solver.hpp"

int main(int argc, char **argv)
{
    // std::vector<double> weights = {0, 0, 3, 2, 0, 0, 1, 4, 1, 2, 0, 6, 4, 3, 5, 0};
    std::vector<double> weights = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0};
    int num_vert = 4;
    int num_edge = 16;
    int num_cache = 2;

    DataModel data(weights, num_vert, num_edge, num_cache);
    std::vector<bool> ans(num_vert);
    bool ret = operations_research::lp_solve_schedule(data, ans);
    if(ret) {
        for(int i = 0; i < num_vert; i++) std::cout << "v_" << i << " : " << ans[i] << std::endl;
    }
    return EXIT_SUCCESS;
}