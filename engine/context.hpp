#ifndef _GRAPH_CONTEXT_H_
#define _GRAPH_CONTEXT_H_

#include <cstdlib>
#include <ctime>
#include "api/types.hpp"
#include "logger/logger.hpp"
#include "sample.hpp"

/** graph context
 * 
 * This file define when vertex choose the next hop, the tranisition context
 */

class context { 
public:
    virtual vid_t transition(sample_policy_t*, unsigned *seed) {
        return 0;
    }
};


class graph_context : public context { 
public:
    vid_t pos;
    vid_t *adj_start, *adj_end;
    real_t *weight_start, *weight_end;
    float teleport;
    vid_t nvertices;

    graph_context(vid_t _pos, vid_t *_adj_start, vid_t *_adj_end, real_t *_weight_start, real_t *_weight_end, float _teleport, vid_t _nvertices) {
        this->pos = _pos;
        this->adj_start = _adj_start;
        this->adj_end = _adj_end;
        this->weight_start = _weight_start;
        this->weight_end = _weight_end;
        this->teleport = _teleport;
        this->nvertices = _nvertices;
    }

    vid_t transition(sample_policy_t* sampler, unsigned *seed) { 
        eid_t deg = (eid_t)(adj_end - adj_start);
        if(deg > 0 && (float)rand_r(seed) / RAND_MAX > teleport) {
            
            // std::vector<real_t> weights(deg);
            // unsigned int aux_seed = static_cast<unsigned int>(time(NULL) + deg);
            // for (auto &w : weights) w = static_cast<real_t>(rand_r(&aux_seed)) / static_cast<real_t>(RAND_MAX) * 10.0;
            // return sample(sampler, weights);
            if(weight_start == nullptr || weight_end == nullptr) {
                vid_t off = (vid_t)rand_r(seed) % deg;
                return this->adj_start[off];
            }else {
                size_t off = sample(sampler, weight_start, weight_end);
                return this->adj_start[off];
            }
        }else {
            return rand_r(seed) % nvertices;
        }
    }
};

#endif