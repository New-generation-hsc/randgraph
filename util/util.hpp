#ifndef _GRAPH_UTIL_H_
#define _GRAPH_UTIL_H_

#include <string>
#include <sstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <cstdio>

// for windows mkdir
#ifdef _WIN32
#include <dirent.h>
#endif

#define max_value(a, b) (((a) > (b)) ? (a) : (b))
#define min_value(a, b) (((a) < (b)) ? (a) : (b))

#ifndef NDEBUG
#define ASSERT(condition, message)                                             \
    do                                                                         \
    {                                                                          \
        if (!(condition))                                                      \
        {                                                                      \
            std::cerr << "Assertion `" #condition "` failed in " << __FILE__   \
                      << " line " << __LINE__ << ": " << message << std::endl; \
            std::terminate();                                                  \
        }                                                                      \
    } while (false)
#else
#define ASSERT(condition, message) \
    do                             \
    {                              \
    } while (false)
#endif

template<typename T>
std::string concatnate_name(std::string filename, T val) {
    std::stringstream ss;
    ss << filename;
    ss << "_" << val;
    return ss.str();
}

template<typename T>
void appendfile(std::string filename, T *array, size_t bsize) {
    int fd = open(filename.c_str(), O_RDWR | O_APPEND | O_CREAT, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
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

inline std::string get_beg_pos_name(std::string const & base_name, int fnum) {
    return concatnate_name(base_name, fnum) + ".beg";
}

inline std::string get_csr_name(std::string const & base_name, int fnum) {
    return concatnate_name(base_name, fnum) + ".csr";
}

inline std::string get_degree_name(std::string const & base_name, int fnum) {
    return concatnate_name(base_name, fnum) + ".deg";
}

inline std::string get_weights_name(std::string const & base_name, int fnum) {
    return concatnate_name(base_name, fnum) + ".wht";
}

inline std::string get_vert_blocks_name(std::string const & base_name, size_t blocksize) {
    return concatnate_name(base_name, blocksize / (1024 * 1024)) + "MB.vert.blocks";
}

inline std::string get_edge_blocks_name(std::string const & base_name, size_t blocksize) {
    return concatnate_name(base_name, blocksize / (1024 * 1024)) + "MB.edge.blocks";
}

inline std::string get_ratio_name(std::string const & base_name, int fnum) {
    return concatnate_name(base_name, fnum) + ".rat";
}

inline std::string get_walk_name(std::string const & base_name, bid_t blk) {
    return concatnate_name(base_name, blk) + ".walk";
}

inline std::string get_meta_name(std::string const & base_name) {
    return base_name + ".meta";
}

/** test a file existence */
inline bool test_exists(const std::string & filename) {
    struct stat buffer;
    return (stat(filename.c_str(), &buffer) == 0);
}

/** test a file existence and if the file exist then delete it */
inline bool test_delete(const std::string & filename) {
    if(test_exists(filename)) {
        std::remove(filename.c_str());
        return true;
    }
    return false;
}

/** given data vertex, return the block that the vertex belongs to */
bid_t get_block(std::vector<vid_t>& vblocks, vid_t v) {
    bid_t nblocks = vblocks.size() - 1;
    for(bid_t p = 0; p < nblocks; p++) {
        if(v < vblocks[p+1]) return p;
    }
    return nblocks;
}

std::string get_path_name(const std::string& s) {
    char sep = '/';
#ifdef _WIN32
    sep = '\\';
#endif
    size_t pos = s.rfind(sep, s.length());
    if(pos != std::string::npos) return s.substr(0, pos + 1);
    return "./";
}

std::string get_file_name(const std::string& s) {
    char sep = '/';
#ifdef _WIN32
    sep = '\\';
#endif
    size_t pos = s.rfind(sep, s.length());
    if (pos != std::string::npos) return s.substr(pos + 1, s.length() - pos - 1);
    return s;
}

int randgraph_mkdir(const char* path) {
#ifdef _WIN32
    return ::_mkdir(path);
#else
    return ::mkdir(path, 0777);
#endif
}

bool test_folder_exists(const std::string& folder_name) {
    struct stat st;
    int ret = stat(folder_name.c_str(), &st);
    return ret == 0 && (st.st_mode & S_IFDIR);
}

std::string randgraph_output_folder(const std::string& folder, size_t blocksize) {
    std::string output = folder + concatnate_name("randgraph", blocksize / (1024 * 1024));
    return output;
}

std::string randgraph_output_filename(const std::string& folder, const std::string& dataset_name, size_t blocksize) {
    std::string output_filename = randgraph_output_folder(folder, blocksize) + "/" + dataset_name;
    return output_filename;
}

#endif
