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
    int fd = open(filename.c_str(), O_WRONLY | O_APPEND | O_CREAT);
    assert(fd >= 0);
    pwrite(fd, (char*)array, bsize * sizeof(T), 0);
    close(fd);
}

#endif