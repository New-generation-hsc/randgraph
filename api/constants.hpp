#ifndef _GRAPH_CONSTANTS_
#define _GRAPH_CONSTANTS_

#define VERT_SIZE   64 * 1024 * 1024 // 64M vertices in graph_buffer
#define EDGE_SIZE   256 * 1024 * 1024 // 256M edges in graph_buffer
#define FILE_SIZE   64 * 1024 * 104 * 1024 // 16GB the maximum size of a file to store the data
#define BLOCK_SIZE  64 * 1024 * 1024 // 64M edges in each block
#define MEMORY_CACHE    1 * 1024 * 1024 * 1024   // 1GB memory for block cache

#define MAX_TWALKS  4 * 1024              // one thread at most 4096 walks in memory
#define MAX_BWALKS  12 * MAX_TWALKS       // one block at most has 12 * 4096 walks in memory

#endif