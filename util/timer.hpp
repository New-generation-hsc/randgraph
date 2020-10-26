#ifndef _GRAPH_TIMER_H_
#define _GRAPH_TIMER_H_

#include <sys/time.h>
#include <stdlib.h>

class timer_t {
private:
    timeval start, end;
public:
    timer_t() { }

    void start_time() {
        gettimeofday(&start, NULL);
    }
    
    void stop_time() {
        gettimeofday(&end, NULL);
    }

    double runtime() {
        this->stop_time();
        return end.tv_sec-start.tv_sec+ ((double)(end.tv_usec-start.tv_usec))/1.0E6;
    }
};

#endif