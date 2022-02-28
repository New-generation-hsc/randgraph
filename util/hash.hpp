#pragma once
#include <fstream>

/**
 * BloomFilter is used to speedup neighborhood query in node2vec
 * this code refers to flashmob/include/hash.hpp
 */

class BloomFilter
{
    uint64_t hash_bitmask;
    uint64_t *table;
    size_t sz;
    size_t num_blocks;

    // https://qastack.cn/programming/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key
    // https://xorshift.di.unimi.it/splitmix64.c
    uint64_t get_hash(uint64_t x) {
		x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
		x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
		x = x ^ (x >> 31);
        return x & hash_bitmask;
    }

    // https://en.wikipedia.org/wiki/Fletcher%27s_checksum
    uint64_t get_bloom(uint64_t n) {
        uint64_t res = 0;
        uint32_t sum1 = 0;
        uint32_t sum2 = 0;
        int len = sizeof(uint64_t) / sizeof(uint16_t);
        uint16_t data[len];
        memcpy(data, &n, sizeof(uint64_t));

        for (int i = 0; i < len; i++)
        {
            sum1 = (sum1 + data[i]) % 65535;
            sum2 = (sum2 + sum1) % 65535;
            res |= (uint64_t) 1 << (sum2 & (uint64_t) 63);
        }
        return res;
    }

    uint64_t get_value(uint32_t v1, uint32_t v2) {
        if (v1 > v2) {
            std::swap(v1, v2);
        }
        return ((uint64_t) v1 << 32) | v2;
    }

public:
#ifdef PROFILE_BF
    uint64_t qhit_counter;
    uint64_t qmiss_counter;
#endif

    static uint64_t cal_hash_table_capacity(uint64_t item_num) {
        // min : 4
        uint64_t ht_capacity = 4;
        while (ht_capacity <= item_num / 4) {
            ht_capacity *= 2;
        }
        return ht_capacity;
    }

    static uint64_t cal_hash_table_size(uint64_t item_num) {
        return sizeof(uint64_t) * cal_hash_table_capacity(item_num);
    }

    BloomFilter() {
        table = nullptr;
        sz = 0;
        num_blocks = 0;
    }

    ~BloomFilter() {
        if(table) delete [] table;
    }

    void create(uint64_t item_num) {
        uint64_t ht_capacity = cal_hash_table_capacity(item_num);
        hash_bitmask = ht_capacity - 1;
        // table = mpool.alloc<uint64_t>(ht_capacity, MemoryInterleaved);
        table = new uint64_t[ht_capacity];
        memset(table, 0, sizeof(uint64_t) * ht_capacity);
        sz = sizeof(uint64_t) * ht_capacity;
        num_blocks = 0;
#ifdef PROFILE_BF
        qhit_counter = 0;
        qmiss_counter = 0;
#endif
    }

    void insert(uint32_t v1, uint32_t v2) {
        uint64_t value = get_value(v1, v2);
        __sync_fetch_and_or(&table[get_hash(value)], get_bloom(value));
    }

    bool exist(uint32_t v1, uint32_t v2) {
        uint64_t value = get_value(v1, v2);
        uint64_t bloom = get_bloom(value);
#ifdef PROFILE_BF
        if (bloom == (table[get_hash(value)] & bloom)) {
            __sync_fetch_and_add(&qhit_counter, 1ul);
        } else {
            __sync_fetch_and_add(&qmiss_counter, 1ul);
        }
#endif
        return bloom == (table[get_hash(value)] & bloom);
    }

    size_t size() {
        return sz;
    }

    void set_num_blocks(size_t n_blocks) { num_blocks = n_blocks; }

    size_t get_num_blocks() const { return num_blocks; }

    void load_bloom_filter(const std::string& filename) {
        auto stream = std::fstream(filename.c_str(), std::ios::in| std::ios::binary);
        stream.read(reinterpret_cast<char*>(&num_blocks), sizeof(size_t));
        stream.read(reinterpret_cast<char*>(&sz), sizeof(size_t));
        uint64_t ht_capacity = sz / sizeof(uint64_t);
        table = new uint64_t[ht_capacity];
        stream.read(reinterpret_cast<char*>(table), sz);
        hash_bitmask = ht_capacity - 1;
    }

    void dump_bloom_filter(const std::string& filename) {
        auto stream = std::fstream(filename.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
        stream.write(reinterpret_cast<char*>(&num_blocks), sizeof(size_t));
        stream.write(reinterpret_cast<char*>(&sz), sizeof(size_t));
        stream.write(reinterpret_cast<char*>(table), sz);
    }
};
