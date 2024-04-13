#pragma once

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>

#include <string.h>
#include <libpmem.h>
#include <libpmemobj.h>
#include "util/utils.h"

// 每个线程分配自己的page用于追加写，每个page 64MB，定期垃圾回收

extern int8_t global_numa_map[64];           // 64个core
extern thread_local size_t global_thread_id; // each index use it for pmem alloc

namespace nstore
{

    const size_t PPAGE_SIZE = 16UL * 1024UL * 1024UL;

    const int numa_max_node = 8;

    const int numa_node_num = 2; // current machine numa nodes

    /**
     * $ lscpu
     * NUMA node0 CPU(s):               0-15,32-47
     * NUMA node1 CPU(s):               16-31,48-63
     */
    static inline void init_numa_map()
    {
        for (int i = 0; i < 16; i++)
        {
            global_numa_map[i] = 0;
        }

        for (int i = 16; i < 32; i++)
        {
            global_numa_map[i] = 1;
        }

        for (int i = 32; i < 48; i++)
        {
            global_numa_map[i] = 0;
        }

        for (int i = 48; i < 64; i++)
        {
            global_numa_map[i] = 1;
        }
    }

    static inline int8_t get_numa_id(size_t thread_id)
    {
        return global_numa_map[thread_id];
    }

}