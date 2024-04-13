#pragma once

#include "tree.h"
#include "varvalue_alloc.h"
#include "../third/alexol/alex.h"
#include <utility>

namespace nstore
{
    template <class T, class P>
    class alexoldb : public Tree<T, P>
    {
    public:
        typedef std::pair<T, P> V;
        alexoldb()
        {
            db_ = new alexol::Alex<T, P>();
            db_->set_max_model_node_size(1 << 24);
            db_->set_max_data_node_size(1 << 17);
        }

        ~alexoldb()
        {
            delete db_;
        }

        void bulk_load(const V values[], int num_keys)
        {
            db_->bulk_load(values, num_keys);
        }

        bool insert(const T &key, const P &payload)
        {
            return db_->insert(key, payload);
        }

        bool search(const T &key, P &payload)
        {
            auto ret = db_->get_payload(key, &payload);
            return ret;
        }

        bool erase(const T &key, uint64_t *log_offset = nullptr)
        {
            int num = db_->erase(key, log_offset);
            if (num > 0)
                return true;
            else
                return false;
        }

        bool update(const T &key, const P &payload, uint64_t *log_offset = nullptr)
        {
            return db_->update(key, payload, log_offset);
        }

        int range_scan_by_size(const T &key, uint32_t to_scan, V *&result = nullptr)
        {
            auto scan_size = db_->range_scan_by_size(key, static_cast<uint32_t>(to_scan), result);
            return to_scan;
        }

        void get_info() {}

    private:
        alexol::Alex<T, P, alexol::AlexCompare, std::allocator<std::pair<T, P>>, false> *db_;
    };
}
