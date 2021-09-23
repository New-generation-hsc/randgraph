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
typedef uint64_t walk_t;  /* walker data type */

#define HOPSIZE  8        /* hop field size */
#define POSHIFT  8        /* pos field shift */
#define POSIZE   28       /* pos field size */
#define SOURCESHIFT 36    /* source field shift */
#define SOURCESIZE  28    /* source field size  */

#define WALKER_SOURCE(walker) ((walker >> SOURCESHIFT) & ((0x1 << SOURCESIZE) - 1))
#define WALKER_POS(walker) ((walker >> POSHIFT) & ((0X1 << POSIZE) - 1))
#define WALKER_HOP(walker) (walker & ((0x1 << HOPSIZE) - 1))
#define WALKER_MAKEUP(source, pos, hop) ((((walk_t)source & ((0x1 << SOURCESIZE) - 1)) << SOURCESHIFT) | (((walk_t)pos & ((0x1 << POSIZE) - 1)) << POSHIFT) | ((walk_t)hop & ((0x1 << HOPSIZE) - 1)))

#endif