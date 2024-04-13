#include <bits/stdc++.h>

namespace nstore
{

    constexpr double numa_weight = 1; // 访问权重
    constexpr int max_numa_nodes = 2;

    struct CacheEntry
    {
        uint64_t key_;
        uint64_t value_;
        int8_t numa_id_{-1};
        double weight_{0};
        int64_t oldest_ts_;
        uint64_t visit_times_[max_numa_nodes];

        CacheEntry() {}

        CacheEntry(uint64_t key, uint64_t value, int8_t numa_id_, int64_t ts)
            : key_(key), value_(value), numa_id_(numa_id_), weight_(1.0), oldest_ts_(ts)
        {
            memset(visit_times_, 0, sizeof(visit_times_));
            visit_times_[numa_id_]++;
        }

        ~CacheEntry(){};

        void Reset(uint64_t key, uint64_t value, int64_t ts, int8_t numa_id)
        {
            key_ = key;
            value_ = value;
            numa_id_ = numa_id;
            weight_ = 1.0;
            oldest_ts_ = ts;
            memset(visit_times_, 0, sizeof(visit_times_));
            visit_times_[numa_id_]++;
        }

        void Visit(int8_t numa_id)
        {
            numa_id_ = numa_id;
            visit_times_[numa_id]++;
            weight_ = 0;
            for (int i = 0; i < max_numa_nodes; ++i)
            {
                if (i == numa_id)
                {
                    weight_ += visit_times_[i];
                }
                else
                {
                    weight_ += visit_times_[i] * numa_weight;
                }
            }
        }
    };

    struct CacheEntryCmp
    {
        auto operator()(const CacheEntry *a, const CacheEntry *b) const -> bool
        { // : set的自定义比较函数必须为const
            // weight相同时淘汰最老时间戳的
            return a->weight_ == b->weight_ ? a->oldest_ts_ < b->oldest_ts_ : a->weight_ < b->weight_;
        }
    };

    class NumaCache
    {
    public:
        NumaCache(size_t cap) : capacity_(cap)
        {
            for (int i = 0; i < cap; ++i)
            {
                CacheEntry *node = new CacheEntry();
                free_entry_.push(node);
            }
        }

        ~NumaCache()
        {
            for (auto iter : entry_map_)
            {
                delete iter.second;
            }
            while (!free_entry_.empty())
            {
                delete free_entry_.top();
                free_entry_.pop();
            }
        }

        // insert/update
        void put(uint64_t key, uint64_t value, int8_t numa_id)
        {
            std::lock_guard<std::mutex> lg(latch_);
            auto iter = entry_map_.find(key);
            if (iter == entry_map_.end())
            {
                // insert
                if (size_ == capacity_)
                {
                    auto iter = entry_set_.begin();
                    CacheEntry *node = *iter;
                    entry_set_.erase(iter);
                    entry_map_.erase(node->key_);
                    node->Reset(key, value, current_ts_++, numa_id);
                    entry_set_.insert(node);
                    entry_map_[node->key_] = node;
                }
                else
                {
                    CacheEntry *node = free_entry_.top();
                    free_entry_.pop();
                    node->Reset(key, value, current_ts_++, numa_id);
                    entry_set_.insert(node);
                    entry_map_[node->key_] = node;
                    size_++;
                }
            }
            else
            {
                // update
                CacheEntry *node = iter->second;
                entry_set_.erase(node);
                node->Visit(numa_id);
                node->value_ = value;
                entry_set_.insert(node);
            }
        }

        bool get(uint64_t key, uint64_t &value, int8_t numa_id)
        {
            std::lock_guard<std::mutex> lg(latch_);
            auto iter = entry_map_.find(key);
            if (iter == entry_map_.end())
            {
                return false;
            }
            CacheEntry *node = iter->second;
            entry_set_.erase(node);
            node->Visit(numa_id);
            value = node->value_;
            entry_set_.insert(node);
            return true;
        }

        // if key exist, return true; else return false
        bool evict(uint64_t key)
        {
            std::lock_guard<std::mutex> lg(latch_);
            auto iter = entry_map_.find(key);
            if (iter == entry_map_.end())
            {
                return false;
            }
            CacheEntry *node = iter->second;
            entry_set_.erase(node);
            entry_map_.erase(iter);
            free_entry_.push(node);
            return true;
        }

    private:
        size_t capacity_;
        size_t size_{0};
        std::mutex latch_;
        int64_t current_ts_{0};
        std::unordered_map<uint64_t, CacheEntry *> entry_map_; // key -> entry
        std::set<CacheEntry *, CacheEntryCmp> entry_set_;
        std::stack<CacheEntry *> free_entry_;
    };

}