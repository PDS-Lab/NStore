#pragma once

#include <libpmemobj.h>
#include <libpmem.h>
#include <string>

extern int8_t global_numa_map[64];
extern thread_local size_t global_thread_id;

// numa-aware pm allocator for var value;
namespace nstore
{
    class varval_store
    {
    public:
        varval_store()
        {
            const int numa_node_num = 2;
            pop = new PMEMobjpool *[numa_node_num];
            constexpr size_t pool_size_ = ((size_t)(1024 * 1024 * 32) * 1024);
            const std::string pool_path_ = "/mnt/pmem";
            for (int i = 0; i < numa_node_num; i++)
            {
                std::string path_ = pool_path_ + std::to_string(i) + "/lbl/var_data";
                if ((pop[i] = pmemobj_create(path_.c_str(), POBJ_LAYOUT_NAME(i), pool_size_, 0666)) == NULL)
                {
                    perror("failed to create pool.\n");
                    exit(-1);
                }
            }
        }

        char *put(const std::string &value)
        {
            void *addr = alloc(value.size());
            pmem_memcpy_persist(addr, value.c_str(), value.size());
            return (char *)addr;
        }

        void get(char *val_addr, std::string &value, size_t var_length)
        {
            value.resize(var_length);
            memcpy((char *)value.c_str(), val_addr, var_length);
        }

    private:
        int8_t get_numa_id(size_t thread_id)
        {
            return global_numa_map[thread_id];
        }

        void *alloc(size_t size)
        {
            PMEMoid p;
            pmemobj_alloc(pop[get_numa_id(global_thread_id)], &p, size, 0, NULL, NULL);
            return pmemobj_direct(p);
        }

    private:
        PMEMobjpool **pop;
    };
}