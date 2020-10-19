#ifndef _GRAPH_UTIL_H_
#define _GRAPH_UTIL_H_

#include <string>
#include <sstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#define max_value(a, b) (((a) > (b)) ? (a) : (b))
#define min_value(a, b) (((a) < (b)) ? (a) : (b))

template<typename T>
std::string concatnate_name(std::string filename, T val) {
    std::stringstream ss;
    ss << filename;
    ss << "_" << val;
    return ss.str();
}

template<typename T>
void appendfile(std::string filename, T *array, size_t bsize) {
    int fd = open(filename.c_str(), O_WRONLY | O_APPEND | O_CREAT, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
    assert(fd >= 0);
    pwrite(fd, (char*)array, bsize * sizeof(T), 0);
    close(fd);
}

std::string base_name(std::string const & path) {
    return path.substr(path.find_last_of("/\\")+1);
}

inline std::string file_base_name(std::string const &path) {
    return base_name(path);
}

std::string remove_extension(std::string const & filename) {
    std::string::size_type const p(filename.find_last_of('.'));
    return p>0&&p!=std::string::npos ? filename.substr(0, p) : filename;
}

inline std::string get_beg_pos_name(std::string base_name, int fnum) {
    return concatnate_name(base_name, fnum) + ".beg";
}

inline std::string get_csr_name(std::string base_name, int fnum) { 
    return concatnate_name(base_name, fnum) + ".csr";
}

inline std::string get_degree_name(std::string base_name, int fnum) { 
    return concatnate_name(base_name, fnum) + ".deg";
}

inline std::string get_vert_blocks_name(std::string base_name, size_t blocksize) {
    return concatnate_name(base_name, blocksize / (1024 * 1024)) + "MB.vert.blocks";
}

inline std::string get_edge_blocks_name(std::string base_name, size_t blocksize) {
    return concatnate_name(base_name, blocksize / (1024 * 1024)) + "MB.edge.blocks";
}

#endif