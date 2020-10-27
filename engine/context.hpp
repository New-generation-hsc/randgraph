#ifndef _GRAPH_CONTEXT_H_
#define _GRAPH_CONTEXT_H_

#include <cstdlib>
#include <ctime>
#include "api/types.hpp"
#include "logger/logger.hpp"

/** graph context
 * 
 * This file define when vertex choose the next hop, the tranisition context
 */

class context { 
public:
    virtual vid_t transition() {
        return 0;
    }
};


class graph_context : public context { 
public:
    vid_t pos;
    vid_t *adj_start, *adj_end;
    float teleport;
    vid_t nvertices;

    graph_context(vid_t _pos, vid_t *_adj_start, vid_t *_adj_end, float _teleport, vid_t _nvertices) {
        this->pos = _pos;
        this->adj_start = _adj_start;
        this->adj_end = _adj_end;
        this->teleport = _teleport;
        this->nvertices = _nvertices;
    }

    vid_t transition() { 
        eid_t deg = (eid_t)(adj_end - adj_start);
        if(deg > 0 && (float)rand() / RAND_MAX > teleport) {
            vid_t off = (vid_t)rand() % deg;
            return this->adj_start[off];
        }else {
            return rand() % nvertices;
        }
    }
};

#endif