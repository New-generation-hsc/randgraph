#ifndef _GRAPH_TYPES_H_
#define _GRAPH_TYPES_H_

#include <stdint.h>

typedef uint32_t vid_t;   /* vertex id */
typedef uint64_t eid_t;   /* edge id */
typedef uint32_t bid_t;   /* block id */
typedef uint32_t rank_t;  /* block rank */
typedef uint16_t hid_t;   /* walk hop */
typedef uint16_t tid_t;   /* thread id */
typedef uint32_t wid_t;   /* walk id */
typedef float    real_t;  /* edge weight */

struct walk_t {
    hid_t hop   : 16;
    vid_t pos    : 24;   /* current walk current pos vertex */
    vid_t source : 24;   /* walk source vertex */
};

#endif
