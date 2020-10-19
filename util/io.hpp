#ifndef _GRAPH_IO_H_
#define _GRAPH_IO_H_

#include <vector>
#include <fstream>
#include <cassert>
#include <unistd.h>
#include "api/types.hpp"
#include "util/util.hpp"

std::vector<vid_t> load_graph_blocks(std::string base_name, size_t blocksize) {
    std::string name = get_vert_blocks_name(base_name, blocksize);
    std::ifstream isfile(name.c_str());
    assert(isfile);

    std::vector<vid_t> blocks;
    vid_t rv;
    while(isfile >> rv) blocks.push_back(rv);
    return blocks;
}

template<typename T>
void load_range(int fd, T *buf, size_t count, off_t off) {
    size_t nbr = 0;  /* number of bytes has read */
    size_t total = sizeof(T) * count; /* the bytes that need to read */
    char* bufptr = (char *)buf;
    while(nbr < total) {
        size_t ret = pread(fd, bufptr, total - nbr, off);
        assert(ret > 0);
        bufptr += ret;
        nbr += ret;
    }
}

/** a general load block range function, cound load vertex data or edge data */
/** p : the partition idx
 *  blocks : either vblocks or eblocks
 */
template<typename T>
void load_block_range(int fd, int p, std::vector<int>& blocks, T *buf) {
    int pnum = blocks.size();
    assert(p + 1 < pnum);
    size_t count = blocks[p + 1] - blocks[p];
    load_range(fd, buf, count, blocks[p]);
}
#endif