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
typedef float    real_t;     /* edge weight */

#define HOPSIZE  8        /* hop field size */
#define POSHIFT  8        /* pos field shift */
#define POSIZE   28       /* pos field size */
#define SOURCESHIFT 36    /* source field shift */
#define SOURCESIZE  28    /* source field size  */

/**
 * The following code is initially designed for first order random walk
*/
#define WALK_SOURCE(walk) ((walk >> SOURCESHIFT) & ((0x1 << SOURCESIZE) - 1))
#define WALK_POS(walk) ((walk >> POSHIFT) & ((0X1 << POSIZE) - 1))
#define WALK_HOP(walk) (walk & ((0x1 << HOPSIZE) - 1))
#define WALK_MAKEUP(source, pos, hop) ((((walk_t)source & ((0x1 << SOURCESIZE) - 1)) << SOURCESHIFT) | (((walk_t)pos & ((0x1 << POSIZE) - 1)) << POSHIFT) | ((walk_t)hop & ((0x1 << HOPSIZE) - 1)))

enum WalkType { FirstOrder, SecondOrder };

struct empty_data_t
{

};

/**
 * This structure stores each walk info, current walk position, hop, source
 * the data may contain some additional info, e.g. previous vertex
*/
template <typename walk_data_t>
struct walker_t {
    walk_data_t data;
    wid_t walk_id;
    walk_t walk_info;
};

template <>
struct walker_t<empty_data_t> {
    wid_t walk_id;
    union {
        walk_t walk_info;
        empty_data_t data;
    };
};

#define WALKER_SOURCE(walker) WALK_SOURCE((walker.walk_info))
#define WALKER_POS(walker) WALK_POS((walker.walk_info))
#define WALKER_HOP(walker) WALK_HOP((walker.walk_info))
#define WALKER_ID(walker) (walker.walk_id)

template<typename walk_data_t>
inline walker_t<walk_data_t> walker_makeup(walk_data_t data, wid_t id, vid_t source, vid_t pos, hid_t hop) {
    walker_t<walk_data_t> walk;
    walk.data = data;
    walk.walk_id = id;
    walk.walk_info = WALK_MAKEUP(source, pos, hop);
    return walk;
}


inline walker_t<empty_data_t> walker_makeup(wid_t id, vid_t source, vid_t pos, hid_t hop)
{
    walker_t<empty_data_t> walk;
    walk.walk_id = id;
    walk.walk_info = WALK_MAKEUP(source, pos, hop);
    return walk;
}

template<typename walk_data_t>
inline vid_t get_vertex_from_walk(const walk_data_t& data) {
    return 0;
}

struct second_order_param_t {
    real_t alpha, beta, gamma, delta;
};

#endif
