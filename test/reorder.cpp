#include <vector>
#include <string>

#include "api/types.hpp"
#include "api/constants.hpp"
#include "util/io.hpp"
#include "util/util.hpp"
#include "logger/logger.hpp"
#include "preprocess/split.hpp"

class graph_t {
    struct vertex_unit_t { vid_t vertex, degree; };
public:
    std::vector<eid_t> old_beg_pos, new_beg_pos;
    std::vector<vid_t> old_csr, new_csr;
    vid_t vert_num;
    eid_t edge_num;
    void make_graph(const std::string& base_name, vid_t v_num, eid_t e_num) {
        logstream(LOG_INFO) << "start to process graph : " << base_name << ", verts : " << v_num << ", edges : " << e_num << std::endl;
        old_beg_pos.resize(v_num + 1);
        new_beg_pos.resize(v_num + 1);
        old_csr.resize(e_num);
        new_csr.resize(e_num);
        vert_num = v_num, edge_num = e_num;

        std::string beg_pos_name = get_beg_pos_name(base_name, 0);
        std::string csr_name     = get_csr_name(base_name, 0);
        int vertdesc = open(beg_pos_name.c_str(), O_RDONLY);
        int edgedesc = open(csr_name.c_str(), O_RDONLY);
        pread(vertdesc, reinterpret_cast<char*>(old_beg_pos.data()), sizeof(eid_t) * (v_num + 1), 0);
        pread(edgedesc, reinterpret_cast<char*>(old_csr.data()), sizeof(vid_t) * e_num, 0);
    }

    void make_reorder() {
        logstream(LOG_INFO) << "start to make reorder." << std::endl;
        std::vector<vertex_unit_t> vertex_units(vert_num), new_vertex_units(vert_num);
        #pragma omp parallel for
        for(vid_t v = 0; v < vert_num; v++) {
            vertex_units[v].degree = old_beg_pos[v+1] - old_beg_pos[v];
            vertex_units[v].vertex = v;
        }

        logstream(LOG_INFO) << "start to counting sort." << std::endl;
        // counting sort
        vid_t max_degree = 0;
        #pragma omp parallel for reduction (max: max_degree)
        for(vid_t v = 0; v < vert_num; v++) {
            max_degree = std::max(max_degree, vertex_units[v].degree);
        }

        std::vector<vid_t> counters(max_degree + 1, 0);
        for(vid_t v = 0; v < vert_num; v++) {
            counters[vertex_units[v].degree]++;
        }

        std::vector<vid_t> prefix_counters(counters.size() + 1, 0);
        for(vid_t d = 0; d <= max_degree; d++) {
            prefix_counters[d + 1] = prefix_counters[d] + counters[d];
        }

        logstream(LOG_INFO) << "start to make new vertex unit." << std::endl;
        for(vid_t v = 0; v < vert_num; v++) {
            vid_t pos = prefix_counters[vertex_units[v].degree]++;
            new_vertex_units[vert_num - pos - 1] = vertex_units[v];
        }

        logstream(LOG_INFO) << "start to make vertex to index." << std::endl;
        std::vector<vid_t> vertex_to_index(vert_num);
        #pragma omp parallel for
        for(vid_t v = 0; v < vert_num; v++) {
            vertex_to_index[new_vertex_units[v].vertex] = v;
        }

        new_beg_pos[0] = 0;
        for(vid_t v = 0; v < vert_num; v++) {
            new_beg_pos[v+1] = new_beg_pos[v] + new_vertex_units[v].degree;
        }

        std::vector<eid_t> edge_offset(vert_num + 1);
        #pragma omp parallel for
        for(vid_t v = 0; v <= vert_num; v++) {
            edge_offset[v] = new_beg_pos[v];
        }

        logstream(LOG_INFO) << "start to make new csr." << std::endl;
        #pragma omp parallel for
        for(vid_t v = 0; v < vert_num; v++) {
            for(eid_t off = old_beg_pos[v]; off < old_beg_pos[v + 1]; off++) {
                eid_t pos = edge_offset[vertex_to_index[v]]++;
                new_csr[pos] = vertex_to_index[old_csr[off]];
            }
        }
    }

    void make_persistent(const std::string& base_name) {
        logstream(LOG_INFO) << "start to persistent new data, new_beg_pos size = " << new_beg_pos.size() << ", new_csr size = " << new_csr.size() << std::endl;
        std::string beg_pos_name = get_beg_pos_name(base_name, 0) + ".ro";
        int fd = open(beg_pos_name.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
        // write(fd, reinterpret_cast<char*>(new_beg_pos.data()), sizeof(eid_t) * new_beg_pos.size());
        dump_block_range(fd, new_beg_pos.data(), new_beg_pos.size(), 0);

        std::string csr_name = get_csr_name(base_name, 0) + ".ro";
        fd = open(csr_name.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
        // write(fd, reinterpret_cast<char*>(new_csr.data()), sizeof(vid_t) * new_csr.size());
        dump_block_range(fd, new_csr.data(), new_csr.size(), 0);
    }
};

int main(int argc, const char* argv[]) {
    assert(argc >= 2);
    logstream(LOG_INFO) << "app : " << argv[0] << ", dataset : " << argv[1] << std::endl;
    std::string input = remove_extension(argv[1]);
    std::string base_name = randgraph_output_filename(get_path_name(input), get_file_name(input), BLOCK_SIZE);

    /* graph meta info */
    vid_t nvertices;
    eid_t nedges;
    load_graph_meta(base_name, &nvertices, &nedges, false);

    graph_t graph;
    graph.make_graph(base_name, nvertices, nedges);
    graph.make_reorder();
    graph.make_persistent(base_name);

    split_blocks(base_name, 0, BLOCK_SIZE, true);

    logstream(LOG_INFO) << "successfully done!" << std::endl;
    return 0;
}
