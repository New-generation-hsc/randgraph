#ifndef _GRAPH_BUFFER_H_
#define _GRAPH_BUFFER_H_

/** This file defines the buffer data structure used in graph processing */

template<typename T>
class base_buffer {
protected:
    size_t bsize;  // buffer current size
    size_t capacity;
    T *array;
};

template<typename T>
class graph_buffer : public base_buffer<T> {
public:
    graph_buffer() { bsize = capacity = 0, array = NULL }
    graph_buffer(size_t size) { 
        alloc(size);
    }
    ~graph_buffer() { if(array) free(arry); }

    void alloc(size_t size) { 
        capacity = size;
        bsize = 0;
        array = (T*)malloc(size * sizeof(T));
    }

    void realloc(size_t size) {
        array = (T*)malloc(array, size * sizeof(T));
        capacity = size;
        bsize = 0;
    }

    void destroy() { if(array) free(array); }

    T& operator[](size_t off) {
        assert(off < bsize);
        return array[off];
    }

    T* buffer_begin() { return array; }
    size_t size() const { return bsize; } 

    bool push_back(T val) {
        if(bsize < capacity) { 
            array[bsize++] = val;
            return true;
        }
        return false;
    }

    bool empty() { return bsize == 0; }
    bool full() { return bsize == capacity; }
    
    /** test if add num elements whether will overflow the maximum capacity or not. */
    bool test_overflow(size_t num) {
        return bsize + num > capacity;
    }

    void clear() {
        bsize = 0;
    }
};

#endif