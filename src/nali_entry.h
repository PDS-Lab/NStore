#pragma once

#include <atomic>
#include <cstdint>
#include <iostream>
#include <cstddef>
#include <vector>
#include <shared_mutex>
#include <cmath>
#include <assert.h>
#include <string.h>

#include "util/bitops.h"
#include "util/utils.h"

using namespace std;

// #define USE_FINGER
#define USE_UNSORT_BUNCKET

#define bitScan(x) __builtin_ffs(x)
#define countBit(x) __builtin_popcount(x)

namespace nstore
{
    // calculate fingerprint
    static inline char hashcode1B(uint64_t x)
    {
        x ^= x >> 32;
        x ^= x >> 16;
        x ^= x >> 8;
        return (char)(x & 0x0ffULL);
    }

    enum status
    {
        Failed = -1,
        OK = 0,
        Full,
        Exist,
        NoExist,
    };

#define USE_DELETE_0

    /**
     * @brief 有序的C层节点，
     *
     * @tparam bucket_size
     * @tparam value_size
     * @tparam key_size
     * @tparam max_entry_count
     */
    template <const size_t bucket_size = 256, const size_t value_size = 8, const size_t key_size = 8,
              const size_t max_entry_count = 256>
    class __attribute__((aligned(64))) SortBuncket
    { // without SortBuncket main key
        struct kventry;

        size_t maxEntrys(int idx) const
        {
            return max_entries;
        }

        void *pkey(int idx) const
        {
            return (void *)&records[idx].key;
        }

        void *pvalue(int idx) const
        {
            return (void *)&records[idx].ptr;
        }

        // 插入一个KV对，仿照FastFair写的，先移动指针，再移动key
        status PutBufKV(uint64_t new_key, uint64_t value, int &data_index, bool flush = true);

#ifdef USE_DELETE_0
        bool remove_key(uint64_t key, uint64_t *value);
#else
        bool remove_key(uint64_t key, uint64_t *value);
#endif

        status SetValue(int pos, uint64_t value)
        {
            memcpy(pvalue(pos), &value, value_size);
            return status::OK;
        }

    public:
        class BuncketIter;

        SortBuncket(uint64_t key, int prefix_len) : entries(0), last_pos(0), next_bucket(nullptr)
        {
            next_bucket = nullptr;
            max_entries = std::min(buf_size / (value_size + key_size), max_entry_count);
            // std::cout << "Max Entry size is:" <<  max_entries << std::endl;
        }

        explicit SortBuncket(uint64_t key, uint64_t value, int prefix_len);

        void *operator new(size_t size)
        {
            return aligned_alloc(64, size);
        }

        ~SortBuncket()
        {
        }

        status Load(uint64_t *keys, uint64_t *values, int count);

        // 节点分裂
        status Expand_(SortBuncket *&next, uint64_t &split_key, int &prefix_len, char *old_finger = nullptr, char *new_finger = nullptr);

        SortBuncket *Next()
        {
            return next_bucket;
        }

        int Find(uint64_t target, bool &find) const;

        int LinearFind(uint64_t target, bool &find) const
        {
            int i = 0;
            for (; i < last_pos && target > records[i].key; i++)
                ;
            find = (i < last_pos) && (target == records[i].key);
            return i;
        }

        uint64_t value(int idx) const
        {
            // the const bit mask will be generated during compile
            return records[idx].ptr;
        }

        uint64_t key(int idx) const
        {
            return records[idx].key;
        }

        uint64_t min_key() const
        {
#ifdef USE_DELETE_0
            for (int i = 0; records[i].key != 0; i++)
            {
                return records[0].key;
            }
#else
            return records[0].key;
#endif
        }

        status Put(uint64_t key, uint64_t value, char *fingerprint = nullptr);

        status Update(uint64_t key, uint64_t value, char *fingerprint = nullptr);

        status Get(uint64_t key, uint64_t &value, char *fingerprint) const;

        status Scan(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results, bool if_first) const;

        status Delete(uint64_t key, uint64_t *value, char *fingerprint);

        uint16_t get_num_entries() { return entries; }

        void Show() const
        {
            std::cout << "This: " << this << ", kventry count: " << entries << std::endl;
            for (int i = 0; i < entries; i++)
            {
                std::cout << "key: " << key(i) << ", value: " << value(i) << std::endl;
            }
        }

        uint64_t EntryCount() const
        {
            return entries;
        }

        void SetInvalid() {}
        bool IsValid() { return false; }

    private:
        // Frist 8 byte head
        struct kventry
        {
            uint64_t key;
            uint64_t ptr;
        };

        const static size_t buf_size = bucket_size - (8 + 4);
        const static size_t entry_size = (key_size + value_size);
        const static size_t entry_count = (buf_size / entry_size);

        SortBuncket *next_bucket;
        union
        {
            uint32_t header;
            struct
            {
                uint16_t last_pos : 8;    // 最后一个位置
                uint16_t entries : 8;     // 键值对个数
                uint16_t max_entries : 8; // MSB
            };
        };
        // char buf[buf_size];
        kventry records[entry_count];

    public:
        class BuncketIter
        {
        public:
            BuncketIter() {}

            BuncketIter(const SortBuncket *bucket, uint64_t prefix_key, uint64_t start_key)
                : cur_(bucket), prefix_key(prefix_key)
            {
                if (unlikely(start_key <= prefix_key))
                {
                    idx_ = 0;
                    return;
                }
                else
                {
                    bool find = false;
                    idx_ = cur_->Find(start_key, find);
                }
            }

            BuncketIter(const SortBuncket *bucket, uint64_t prefix_key)
                : cur_(bucket), prefix_key(prefix_key)
            {
                idx_ = 0;
            }

            uint64_t key() const
            {
                return cur_->key(idx_);
            }

            uint64_t value() const
            {
                return cur_->value(idx_);
            }

            // return false if reachs end
            bool next()
            {
                idx_++;
                while (cur_->key(idx_) == 0 && idx_ < cur_->last_pos)
                {
                    idx_++;
                }
                if (idx_ >= cur_->last_pos)
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }

            bool end() const
            {
                return cur_ == nullptr ? true : (idx_ >= cur_->last_pos ? true : false);
            }

            bool operator==(const BuncketIter &iter) const { return idx_ == iter.idx_ && cur_ == iter.cur_; }
            bool operator!=(const BuncketIter &iter) const { return idx_ != iter.idx_ || cur_ != iter.cur_; }

        private:
            uint64_t prefix_key;
            const SortBuncket *cur_;
            int idx_; // current index in node
        };
    };

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status SortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        PutBufKV(uint64_t new_key, uint64_t value, int &data_index, bool flush)
    {
        if (entries >= max_entries)
        {
            return status::Full;
        }
        if (entries == 0)
        { // this page is empty
            records[0].key = new_key;
            records[0].ptr = value;
            records[1].ptr = 0;
            return status::OK;
        }
        {
            if (last_pos >= max_entries - 1 && last_pos != entries)
            {
                for (int i = last_pos - 1; i >= 0; i--)
                {
                    if (key(i) == 0)
                    {
                        for (; i < last_pos; i++)
                        {
                            records[i].ptr = records[i + 1].ptr;
                            records[i].key = records[i + 1].key;
                        }
                        break;
                    }
                }
                last_pos--;
            }

            int inserted = 0;
            for (int i = last_pos - 1; i >= 0; i--)
            {
                if (new_key < records[i].key)
                {
                    records[i + 1].ptr = records[i].ptr;
                    records[i + 1].key = records[i].key;
                }
                else
                {
                    records[i + 1].ptr = records[i].ptr;
                    records[i + 1].key = new_key;
                    records[i + 1].ptr = value;
                    inserted = 1; // zzy: todo
                    break;
                }
            }
            if (inserted == 0)
            {
                records[0].key = new_key;
                records[0].ptr = value;
            }
        }
        return status::OK;
    }

#ifdef USE_DELETE_0
    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    bool SortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        remove_key(uint64_t key, uint64_t *value)
    {
        for (int i = 0; records[i].key != 0 && i < last_pos; ++i)
        {
            if (records[i].key == key)
            {
                // simply set zero
                records[i].key = 0;
                records[i].ptr = 0;
                return true;
            }
        }
        return false;
    }
#else
    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    bool SortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        remove_key(uint64_t key, uint64_t *value)
    {
        bool shift = false;
        int i;
        for (i = 0; records[i].ptr != 0; ++i)
        {
            if (!shift && records[i].key == key)
            {
                if (value)
                    *value = records[i].ptr;
                if (i != 0)
                {
                    records[i].ptr = records[i - 1].ptr;
                }
                shift = true;
            }

            if (shift)
            {
                records[i].key = records[i + 1].key;
                records[i].ptr = records[i + 1].ptr;
                uint64_t records_ptr = (uint64_t)(&records[i]);
                int remainder = records_ptr % CACHE_LINE_SIZE;
                bool do_flush = (remainder == 0) ||
                                ((((int)(remainder + sizeof(kventry)) / CACHE_LINE_SIZE) == 1) &&
                                 ((remainder + sizeof(kventry)) % CACHE_LINE_SIZE) != 0);
                if (do_flush)
                {
                    clflush((char *)records_ptr);
                }
            }
        }
        return shift;
    }
#endif

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    SortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        SortBuncket(uint64_t key, uint64_t value, int prefix_len) : entries(0), last_pos(0), next_bucket(nullptr)
    {
        next_bucket = nullptr;
        max_entries = std::min(buf_size / (value_size + key_size), max_entry_count);
        // std::cout << "Max Entry size is:" <<  max_entries << std::endl;
        Put(nullptr, key, value);
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status SortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Load(uint64_t *keys, uint64_t *values, int count)
    {
        assert(entries == 0 && count < max_entries);

        for (int target_idx = 0; target_idx < count; target_idx++)
        {
            assert(pvalue(target_idx) > pkey(target_idx));
            memcpy(pkey(target_idx), &keys[target_idx], key_size);
            memcpy(pvalue(target_idx), &values[count - target_idx - 1], value_size);
            entries++;
            last_pos++;
        }
        return status::OK;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status SortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Expand_(SortBuncket *&next, uint64_t &split_key, int &prefix_len, char *old_finger, char *new_finger)
    {
        next = new SortBuncket(key(last_pos / 2), prefix_len);
        // int idx = 0;
        split_key = key(last_pos / 2);
        prefix_len = 0;
        int idx = 0;
        int m = (int)ceil(last_pos / 2);
        for (int i = m; i < last_pos; i++)
        {
            // next->Put(nullptr, key(i), value(i));
            if (key(i) != 0)
            {
                next->PutBufKV(key(i), value(i), idx, false);
                next->entries++;
                next->last_pos++;
            }
        }
        next->next_bucket = this->next_bucket;

        records[m].ptr = 0;
        this->next_bucket = next;

        entries = entries - next->entries;
        last_pos = m;
        return status::OK;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    int SortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Find(uint64_t target, bool &find) const
    {
        int left = 0;
        int right = entries - 1;
        while (left <= right)
        {
            int middle = (left + right) / 2;
            uint64_t mid_key = key(middle);
            if (mid_key == target)
            {
                find = true;
                return middle;
            }
            else if (mid_key > target)
            {
                right = middle - 1;
            }
            else
            {
                left = middle + 1;
            }
        }
        find = false;
        return left;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status SortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Put(uint64_t key, uint64_t value, char *fingerprint)
    {
        status ret = status::OK;
        int idx = 0;
        ret = PutBufKV(key, value, idx);
        if (ret != status::OK)
        {
            return ret;
        }
        entries++;
        last_pos++;
        return status::OK;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status SortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Update(uint64_t key, uint64_t value, char *fingerprint)
    {
        bool find = false;
        int pos = Find(key, find);
        if (!find || this->value(pos) == 0)
        {
            // Show();
            return status::NoExist;
        }
        SetValue(pos, value);
        return status::OK;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status SortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Get(uint64_t key, uint64_t &value, char *fingerprint) const
    {
        bool find = false;
        // int pos = Find(key, find);
        int pos = LinearFind(key, find);
        if (!find || this->value(pos) == 0)
        {
            // Show();
            // assert(0);
            return status::NoExist;
        }
        value = this->value(pos);
        return status::OK;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status SortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Scan(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results, bool if_first) const
    {
        int pos = 0;
        if (start_key != 0)
        {
            bool find = false;
            // int pos = Find(key, find);
            int pos = LinearFind(start_key, find);
            if (!find || this->value(pos) == 0)
            {
                return status::NoExist;
            }
        }
        for (; pos < this->last_pos && len > 0; ++pos)
        {
            if (this->key(pos) != 0)
            {
                results.push_back({this->key(pos), this->value(pos)});
                --len;
            }
        }
        if (this->next_bucket == nullptr)
        {
            if (len > 0)
            {
                return status::Failed;
            }
            return status::OK;
        }
        SortBuncket *next_ = this->next_bucket;
        while (len > 0)
        {
            int i = 0;
            for (; i < next_->last_pos && len > 0; i++)
            {
                if (next_->key(i) != 0)
                {
                    results.push_back({next_->key(i), next_->value(i)});
                    --len;
                }
            }
            if (next_->next_bucket == nullptr)
            {
                break;
            }
            next_ = next_->next_bucket;
        }
        // cout << "in kventry:" << len << endl;
        if (len > 0)
        {
            return status::Failed;
        }
        return status::OK;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status SortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Delete(uint64_t key, uint64_t *value, char *fingerprint)
    {
        auto ret = remove_key(key, value);
        if (!ret)
        {
            return status::NoExist;
        }
        entries--;
        return status::OK;
    }

    template <const size_t bucket_size = 256, const size_t value_size = 8, const size_t key_size = 8,
              const size_t max_entry_count = 64>
    class __attribute__((aligned(64))) UnSortBuncket
    {
        struct kventry;

        size_t maxEntrys(int idx) const
        {
            return max_entries;
        }

        void *pkey(int idx) const
        {
            return (void *)&records[idx].key;
        }

        void *pvalue(int idx) const
        {
            return (void *)&records[idx].value;
        }

        inline status PutBufKV(uint64_t new_key, uint64_t value, int &data_index, char *fingerprint)
        {
            if (unlikely(data_index >= max_entries))
            {
                return status::Full;
            }
            records[data_index].key = new_key;
            records[data_index].value = value;
#ifdef USE_FINGER
            fingerprint[data_index] = hashcode1B(new_key);
#endif
            return status::OK;
        }

        bool remove_key(uint64_t key, uint64_t *value, int idx, char *fingerprint = nullptr);

        status SetValue(int pos, uint64_t value)
        {
            memcpy(pvalue(pos), &value, value_size);
            return status::OK;
        }

        int getSortedIndex(int sorted_index[]) const;

    public:
        class BuncketIter;

        UnSortBuncket(uint64_t key, int prefix_len) : entries(0), next_bucket(nullptr)
        {
            next_bucket = nullptr;
            max_entries = std::min(buf_size / (value_size + key_size), max_entry_count);
            // std::cout << "Max Entry size is:" <<  max_entries << std::endl;
        }

        explicit UnSortBuncket(uint64_t key, uint64_t value, int prefix_len);

        void *operator new(size_t size)
        {
            return aligned_alloc(64, size);
        }

        ~UnSortBuncket()
        {
        }

        status Load(uint64_t *keys, uint64_t *values, int count);

        status Expand_(UnSortBuncket *&next, uint64_t &split_key, int &prefix_len, char *old_finger = nullptr, char *new_finger = nullptr);

        UnSortBuncket *Next()
        {
            return next_bucket;
        }

        int Find(uint64_t target, bool &find, const char *fingerprint = nullptr) const;

        uint64_t value(int idx) const
        {
            // the const bit mask will be generated during compile
            return records[idx].value;
        }

        uint64_t key(int idx) const
        {
            return records[idx].key;
        }

        uint64_t min_key() const
        {
            uint64_t min_key = key(0);
            for (int i = 1; i < entries; i++)
            {
                if (key(i) < min_key)
                {
                    min_key = key(i);
                }
            }
            return min_key;
        }

        status Put(uint64_t key, uint64_t value, char *fingerprint = nullptr);

        status Update(uint64_t key, uint64_t value, uint64_t *old_offset, char *fingerprint = nullptr);

        status Get(uint64_t key, uint64_t &value, const char *fingerprint = nullptr) const;

        status Scan(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results, bool if_first) const;

        status Delete(uint64_t key, uint64_t *value, char *fingerprint = nullptr);

        void Show() const
        {
            std::cout << "This: " << this << ", kventry count: " << entries << std::endl;
            for (int i = 0; i < entries; i++)
            {
                std::cout << "key: " << key(i) << ", value: " << value(i) << std::endl;
            }
        }

        uint64_t EntryCount() const
        {
            return entries;
        }

        void SetInvalid() {}
        bool IsValid() { return false; }

        uint16_t get_num_entries() { return entries; }

    private:
        struct kventry
        {
            uint64_t key;
            uint64_t value;
        };

        // Frist 8 byte head and 8 byte next_buncket ptr
        UnSortBuncket *next_bucket;
        union
        {
            uint64_t header;
            struct
            {
                uint16_t entries : 8;
                uint16_t max_entries : 8; // MSB
            };
        };

        const static size_t buf_size = bucket_size - (8 + 8);
        const static size_t entry_size = (key_size + value_size);
        const static size_t entry_count = (buf_size / entry_size);

        kventry records[entry_count];

    public:
        class BuncketIter
        {
        public:
            BuncketIter() {}

            BuncketIter(const UnSortBuncket *bucket, uint64_t prefix_key, uint64_t start_key)
                : cur_(bucket), prefix_key(prefix_key)
            {
                // std::cout << "iter call getSortedIndex" << std::endl;
                cur_->getSortedIndex(sorted_index_);
                if (unlikely(start_key <= prefix_key))
                {
                    idx_ = 0;
                    return;
                }
                else
                {
                    for (int i = 0; i < cur_->entries; i++)
                    {
                        if (cur_->key(sorted_index_[i]) >= start_key)
                        {
                            idx_ = i;
                            return;
                        }
                    }
                    idx_ = cur_->entries;
                    return;
                }
            }

            BuncketIter(const UnSortBuncket *bucket, uint64_t prefix_key)
                : cur_(bucket), prefix_key(prefix_key)
            {
                // std::cout << "iter2 call getSortedIndex" << std::endl;
                cur_->getSortedIndex(sorted_index_);
                idx_ = 0;
            }

            uint64_t key() const
            {
                if (idx_ < cur_->entries)
                    return cur_->key(sorted_index_[idx_]);
                else
                    return 0;
            }

            uint64_t value() const
            {
                if (idx_ < cur_->entries)
                    return cur_->value(sorted_index_[idx_]);
                else
                    return 0;
            }

            // return false if reaches end
            bool next()
            {
                if (idx_ >= cur_->entries - 1)
                {
                    return false;
                }
                else
                {
                    idx_++;
                    return true;
                }
            }

            bool end() const
            {
                return cur_ == nullptr ? true : (idx_ >= cur_->entries ? true : false);
            }

            bool operator==(const BuncketIter &iter) const { return idx_ == iter.idx_ && cur_ == iter.cur_; }
            bool operator!=(const BuncketIter &iter) const { return idx_ != iter.idx_ || cur_ != iter.cur_; }

        private:
            uint64_t prefix_key;
            const UnSortBuncket *cur_;
            int idx_; // current index in sorted_index_
            int sorted_index_[16];
        };
    };

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    bool UnSortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        remove_key(uint64_t key, uint64_t *value, int idx, char *fingerprint)
    {
        bool find = false;
        int pos = Find(key, find, fingerprint);
        if (find)
        {
            if (pos == idx - 1)
            {
                if (value)
                    *value = records[pos].value;
                return true;
            }
            else
            {
                if (value)
                    *value = records[pos].value;
                records[pos].key = records[idx - 1].key;
                records[pos].value = records[idx - 1].value;
#ifdef USE_FINGER
                fingerprint[pos] = fingerprint[idx - 1];
#endif
                return true;
            }
        }
        return false;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    int UnSortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        getSortedIndex(int sorted_index[]) const
    {
        uint64_t keys[entries];
        for (int i = 0; i < entries; ++i)
        {
            keys[i] = key(i); // prefix does not matter
            sorted_index[i] = i;
        }
        std::sort(&sorted_index[0], &sorted_index[entries],
                  [&keys](uint64_t a, uint64_t b)
                  { return keys[a] < keys[b]; });
        return entries;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    UnSortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        UnSortBuncket(uint64_t key, uint64_t value, int prefix_len) : entries(0), next_bucket(nullptr)
    {
        next_bucket = nullptr;
        max_entries = std::min(buf_size / (value_size + key_size), max_entry_count);
        // std::cout << "Max Entry size is:" <<  max_entries << std::endl;
        Put(nullptr, key, value);
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status UnSortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Load(uint64_t *keys, uint64_t *values, int count)
    {
        assert(entries == 0 && count < max_entries);

        for (int target_idx = 0; target_idx < count; target_idx++)
        {
            assert(pvalue(target_idx) > pkey(target_idx));
            memcpy(pkey(target_idx), &keys[target_idx], key_size);
            memcpy(pvalue(target_idx), &values[count - target_idx - 1], value_size);
            entries++;
        }
        return status::OK;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status UnSortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Expand_(UnSortBuncket *&next, uint64_t &split_key, int &prefix_len, char *old_finger, char *new_finger)
    {
        // int expand_pos = entries / 2;
        int sorted_index_[entry_count];
        // std::cout << "expand call getSortedIndex" << std::endl;
        getSortedIndex(sorted_index_);
        split_key = key(sorted_index_[entries / 2]);
        // std::cout << "entries:" << entries << std::endl;
        //  for(int i = 0; i < entries; i++) {
        //  std::cout << "key(" << i << "):" << key(i) << std::endl;
        //  std::cout << "Sorted_index_[" << i << "]):" << sorted_index_[i] << std::endl;
        // }
        //  if (key(sorted_index_[0]) >= split_key)
        //  {
        //      std::cerr << "split_key_index" << entries / 2 << "split_key:" << split_key << "fisrt key: " << key(sorted_index_[0]) << std::endl;
        //      std::cout << "split_key is not the middle key" << std::endl;
        //  }
        assert(key(sorted_index_[0]) < split_key);
        next = new UnSortBuncket(split_key, prefix_len);
        int idx = 0;
        prefix_len = 0;
        for (int i = entries / 2; i < entries; i++)
        {
            next->PutBufKV(key(sorted_index_[i]), value(sorted_index_[i]), idx, new_finger);
            idx++;
        }
        next->entries = entries - entries / 2;
        next->next_bucket = this->next_bucket;
        idx = entries;
        for (int i = entries / 2; i < entries; i++)
        {
            remove_key(key(sorted_index_[i]), nullptr, idx, old_finger);
            idx--;
        }
        entries = entries / 2;
        this->next_bucket = next;
        return status::OK;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    int UnSortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Find(uint64_t target, bool &find, const char *fingerprint) const
    {
#ifdef USE_FINGER
        char t = hashcode1B(target);
        // for(int i = 0; i < entries; i++) {
        //     if (fingerprint[i] == t && key(i) == target)
        //     {
        //         find = true;
        //         return i;
        //     }
        // }

        // 使用SIMD指令加速
        // a. set every byte to key_hash in a 16B register
        __m128i key_16B = _mm_set1_epi8(t);
        // b. load meta into another 16B register
        __m128i fgpt_16B = _mm_load_si128((const __m128i *)fingerprint);
        // c. compare them
        __m128i cmp_res = _mm_cmpeq_epi8(key_16B, fgpt_16B);
        // d. generate a mask
        unsigned int mask = (unsigned int)
            _mm_movemask_epi8(cmp_res); // 1: same; 0: diff
        while (mask)
        {
            int jj = bitScan(mask) - 1; // next candidate
            if (key(jj) == target)
            {
                find = true;
                return jj;
            }
            mask &= ~(0x1 << jj); // remove this bit
            /*  UBSan: implicit conversion from int -65 to unsigned int
            changed the value to 4294967231 (32-bit, unsigned)      */
        }
#else
        for (int i = 0; i < entries; i++)
        {
            if (key(i) == target)
            {
                find = true;
                return i;
            }
        }
#endif
        find = false;
        return entries;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status UnSortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Put(uint64_t key, uint64_t value, char *fingerprint)
    {
        status ret = status::OK;
        int idx = entries;
        ret = PutBufKV(key, value, idx, fingerprint);
        if (ret != status::OK)
        {
            return ret;
        }
        entries++;
        return status::OK;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status UnSortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Update(uint64_t key, uint64_t value, uint64_t *old_offset, char *fingerprint)
    {
        bool find = false;
        int pos = Find(key, find, fingerprint);
        if (!find)
        {
            return status::NoExist;
        }
        *old_offset = this->value(pos);
        SetValue(pos, value);
#ifdef USE_FINGER
        fingerprint[pos] = hashcode1B(key);
#endif
        return status::OK;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status UnSortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Get(uint64_t key, uint64_t &value, const char *fingerprint) const
    {
        bool find = false;
        int pos = Find(key, find, fingerprint);
        if (!find)
        {
            return status::NoExist;
        }
        value = this->value(pos);
        // if (key != this->key(pos))
        //     return status::NoExist;
        return status::OK;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status UnSortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Scan(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results, bool if_first) const
    {
        if (if_first)
        {
            // scan from start_key;
            int sorted_index_[entries];
            getSortedIndex(sorted_index_);
            for (int i = sorted_index_[0]; i < entries && len > 0; i++)
            {
                if (this->key(sorted_index_[i]) >= start_key)
                {
                    results.push_back({this->key(sorted_index_[i]), this->value(sorted_index_[i])});
                    --len;
                }
            }
        }
        else
        {
            int pos = 0;
            if (len >= this->entries - pos)
            {
                for (; pos < this->entries; pos++)
                {
                    results.push_back({this->key(pos), this->value(pos)});
                    --len;
                }
            }
            else
            {
                int sorted_index_[entries];
                getSortedIndex(sorted_index_);
                while (len > 0)
                {
                    results.push_back({this->key(pos), this->value(pos)});
                    --len;
                }
            }
        }
        if (len > 0)
        {
            return status::Failed;
        }
        return status::OK;
    }

    template <const size_t bucket_size, const size_t value_size, const size_t key_size,
              const size_t max_entry_count>
    status UnSortBuncket<bucket_size, value_size, key_size, max_entry_count>::
        Delete(uint64_t key, uint64_t *value, char *fingerprint)
    {
        auto ret = remove_key(key, value, entries, fingerprint);
        if (!ret)
        {
            return status::NoExist;
        }
        entries--;
        return status::OK;
    }

#ifdef USE_UNSORT_BUNCKET
    typedef UnSortBuncket<256, 8> buncket_t;
#else
    typedef SortBuncket<256, 8> buncket_t;
#endif
    // c层节点的定义， C层节点需支持Put，Get，Update，Delete
    // C层节点内部需要实现一个Iter作为迭代器，

    static_assert(sizeof(buncket_t) == 256);

    class __attribute__((packed)) BuncketPointer
    {

#define READ_SIX_BYTE(addr) ((*(uint64_t *)addr) & 0x0000FFFFFFFFFFFFUL)
        uint8_t pointer_[6]; // uint48_t, LSB == 1 means NULL
    public:
        BuncketPointer() { pointer_[0] = 1; }

        bool HasSetup() const { return !(pointer_[0] & 1); };

        void Setup(uint64_t key, int prefix_len)
        {
            buncket_t *buncket = new buncket_t(key, prefix_len);
            memcpy(pointer_, &buncket, sizeof(pointer_));
        }

        void Setup(buncket_t *buncket, uint64_t key, int prefix_len)
        {
            memcpy(pointer_, &buncket, sizeof(pointer_));
        }

        buncket_t *pointer() const
        {
            return (buncket_t *)(READ_SIX_BYTE(pointer_));
        }
    };

    /**
     * @brief B 层数组，包括4个C层节点，entry_count 可以控制C层节点个数
     * @ BuncketPointer C层节点指针的封装，仿照Combotree以6bit的偏移代替8字节的指针
     */
    struct eentry
    {
        uint64_t entry_key;
        BuncketPointer pointer; // 6B指针
        union
        {
            uint16_t meta;
            struct
            {
                uint16_t prefix_bytes : 4; // LSB
                uint16_t suffix_bytes : 4;
                uint16_t entries : 8;
                // uint16_t max_entries : 4; // MSB
            };

        } buf;
#ifdef USE_FINGER
        char fingerprint[16];
#endif
        uint64_t DataCount() const
        {
            return pointer.pointer()->EntryCount();
        }
        void SetInvalid() { buf.meta = 0; }
        bool IsValid() const { return buf.meta != 0; }
    };

    struct PointerBEntry
    {
        static const int pb_entry_count = 4;
        union
        {
            struct
            {
                uint64_t entry_key; // min key
                char pointer[6];
                union
                {
                    uint16_t meta;
                    struct
                    {
                        uint16_t prefix_bytes : 4; // LSB
                        uint16_t suffix_bytes : 4;
                        uint16_t entries : 8; // 实际记录的是entrys的数目
                        // uint16_t max_entries : 4; // MSB
                    };
                } buf;
#ifdef USE_FINGER
                char fingerprint[16];
#endif
            };
            struct eentry entrys[pb_entry_count];
        };

        buncket_t *Pointer(int i) const
        {
            return entrys[i].pointer.pointer();
        }

        PointerBEntry()
        {
            memset(this, 0, sizeof(PointerBEntry));
        }

        PointerBEntry(uint64_t key, int prefix_len);

        PointerBEntry(uint64_t key, uint64_t value, int prefix_len);

        PointerBEntry(const eentry *kventry);

        void *operator new(size_t size)
        {
            return aligned_alloc(64, size);
        }

        /**
         * @brief 调整第一个entry的entry_key，用于可能存在插入比当前最小key还要小的情况
         *
         */
        void AdjustEntryKey()
        {
            entry_key = Pointer(0)->min_key();
        }

        /**
         * @brief 根据key找到C层节点位置
         *
         * @param key
         * @return int
         */
        // zzy: 线性查找,这里的IsValid()的意图是啥呢？优化成了二分查找
        int Find_pos(uint64_t key) const;

        size_t get_num_keys() const
        {
            size_t num_keys = 0;
            for (int i = 0; i < buf.entries; i++)
            {
                num_keys += entrys[i].pointer.pointer()->get_num_entries();
            }
            return num_keys;
        }

        inline status Put(uint64_t key, uint64_t value, bool *split = nullptr)
        {
        retry:
            // Common::timers["ALevel_times"].start();
            int pos = Find_pos(key);
            bool flag = false;
            assert(entrys[pos].IsValid());
            // if (unlikely(!entrys[pos].IsValid()))
            // {
            //     cout << "invalid" << endl;
            //     flag = true;
            //     entrys[pos].pointer.Setup(mem, key, entrys[pos].buf.prefix_bytes);
            // }
            // Common::timers["ALevel_times"].end();
            // std::cout << "Put key: " << key << ", value " << value << std::endl;
#ifdef USE_FINGER
            auto ret = entrys[pos].pointer.pointer()->Put(key, value, entrys[pos].fingerprint);
#else
            auto ret = entrys[pos].pointer.pointer()->Put(key, value, nullptr);
#endif
            // if(ret == status::Full){
            //     std::cout << entrys[0].buf.entries << std::endl;
            // }
            if (ret == status::Full && entrys[0].buf.entries < pb_entry_count)
            { // 节点满的时候进行扩展
                buncket_t *next = nullptr;
                uint64_t split_key;
                int prefix_len = 0;
                for (int i = entrys[0].buf.entries - 1; i > pos; i--)
                {
                    entrys[i + 1] = entrys[i];
                }
#ifdef USE_FINGER
                entrys[pos].pointer.pointer()->Expand_(next, split_key, prefix_len, entrys[pos].fingerprint, entrys[pos + 1].fingerprint);
#else
                entrys[pos].pointer.pointer()->Expand_(next, split_key, prefix_len, nullptr, nullptr);
#endif
                entrys[pos + 1].entry_key = split_key;
                entrys[pos + 1].pointer.Setup(next, key, prefix_len);
                entrys[pos + 1].buf.prefix_bytes = prefix_len;
                entrys[pos + 1].buf.suffix_bytes = 8 - prefix_len;
                entrys[0].buf.entries++;
                // std::cout << entrys[0].buf.entries << std::endl;
                // this->Show(mem);
                if (split)
                    *split = true;
                goto retry;
            }

            if (ret == status::OK && entry_key > key)
            {
                entry_key = key;
            }

            // if (!entrys[pos].IsValid() && flag)
            // {
            //     cout << "should be valid" << endl;
            // }
            return ret;
        }

        bool Update(uint64_t key, uint64_t value, uint64_t *old_offset);

        bool Get(uint64_t key, uint64_t &value) const;

        bool Scan(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results, bool if_first) const;

        bool Delete(uint64_t key, uint64_t *value);

        void Show()
        {
            for (int i = 0; i < buf.entries && entrys[i].IsValid(); i++)
            {
                std::cout << "Entry key: " << entrys[i].entry_key << std::endl;
            }
        }

        void SetInvalid() { entrys[0].buf.meta = 0; }
        bool IsValid() { return entrys[0].buf.meta != 0; }

        class PointerBEntryIter
        {
        public:
            PointerBEntryIter() {}

            PointerBEntryIter(const PointerBEntry *kventry)
                : entry_(kventry)
            {
                if (!entry_)
                    return;
                if (entry_->entrys[0].IsValid())
                {
                    new (&biter_) buncket_t::BuncketIter(entry_->Pointer(0), entry_->entrys[0].buf.prefix_bytes);
                }
                cur_idx = 0;
            }

            PointerBEntryIter(const PointerBEntry *kventry, uint64_t start_key)
                : entry_(kventry)
            {
                int pos = entry_->Find_pos(start_key);
                if (unlikely(!entry_->entrys[pos].IsValid()))
                {
                    cur_idx = pb_entry_count;
                    return;
                }
                cur_idx = pos;
                if (entry_->entrys[pos].IsValid())
                {
                    new (&biter_) buncket_t::BuncketIter(entry_->Pointer(pos), entry_->entrys[pos].buf.prefix_bytes, start_key);
                    if (biter_.end())
                    {
                        next();
                    }
                }
            }

            uint64_t key() const
            {
                return biter_.key();
            }

            uint64_t value() const
            {
                return biter_.value();
            }

            bool next()
            {
                if (cur_idx < entry_->buf.entries && biter_.next())
                {
                    return true;
                }
                else if (cur_idx < entry_->buf.entries - 1)
                {
                    cur_idx++;
                    new (&biter_) buncket_t::BuncketIter(entry_->Pointer(cur_idx), entry_->entrys[cur_idx].buf.prefix_bytes);
                    if (biter_.end())
                        return false;
                    return true;
                }
                cur_idx = entry_->buf.entries;
                return false;
            }

            bool end() const
            {
                return cur_idx >= entry_->buf.entries;
            }

        private:
            const PointerBEntry *entry_;
            int cur_idx;
            buncket_t::BuncketIter biter_;
        };

        class EntryIter
        {
        public:
            EntryIter() {}

            EntryIter(const PointerBEntry *kventry)
                : entry_(kventry)
            {
                cur_idx = 0;
            }

            const eentry &operator*() { return entry_->entrys[cur_idx]; }

            bool next()
            {
                // std::cout << "cur_idx:" << cur_idx << std::endl;
                cur_idx++;
                // std::cout << "cur_idx:" << cur_idx << std::endl;
                // std::cout << "entry_->buf.entries:" << entry_->entrys[0].buf.entries << std::endl;
                if (cur_idx < entry_->entrys[0].buf.entries)
                {
                    return true;
                }
                cur_idx = entry_->entrys[0].buf.entries;
                return false;
            }

            bool end() const
            {
                return cur_idx >= entry_->entrys[0].buf.entries;
            }

        private:
            const PointerBEntry *entry_;
            int cur_idx;
        };
    };

    PointerBEntry::PointerBEntry(uint64_t key, int prefix_len)
    {
        memset(this, 0, sizeof(PointerBEntry));
        entrys[0].buf.prefix_bytes = prefix_len;
        entrys[0].buf.suffix_bytes = 8 - prefix_len;
        entrys[0].buf.entries = 1;
        entrys[0].entry_key = key;
        entrys[0].pointer.Setup(key, prefix_len);
    }

    PointerBEntry::PointerBEntry(uint64_t key, uint64_t value, int prefix_len)
    {
        memset(this, 0, sizeof(PointerBEntry));
        entrys[0].buf.prefix_bytes = prefix_len;
        entrys[0].buf.suffix_bytes = 8 - prefix_len;
        entrys[0].buf.entries = 1;
        entrys[0].entry_key = key;
        entrys[0].pointer.Setup(key, prefix_len);
        entrys[0].pointer.pointer()->Put(key, value);
    }

    PointerBEntry::PointerBEntry(const eentry *kventry)
    {
        memset(this, 0, sizeof(PointerBEntry));
        entrys[0] = *kventry;
        entrys[0].buf.entries = 1;
        // std::cout << "Entry key: " << key << std::endl;
    }

    int PointerBEntry::Find_pos(uint64_t key) const
    {
        int pos = 0;
        while (pos < buf.entries && entrys[pos].entry_key <= key)
            pos++;
        pos = pos == 0 ? pos : pos - 1;
        return pos;
    }

    bool PointerBEntry::Update(uint64_t key, uint64_t value, uint64_t *old_offset)
    {
        int pos = Find_pos(key);
        if (unlikely(pos >= pb_entry_count || !entrys[pos].IsValid()))
        {
            return false;
        }
#ifdef USE_FINGER
        auto ret = entrys[pos].pointer.pointer()->Update(key, value, old_offset, entrys[pos].fingerprint);
#else
        auto ret = entrys[pos].pointer.pointer()->Update(key, value, old_offset, nullptr);
#endif
        return ret == status::OK;
    }

    bool PointerBEntry::Get(uint64_t key, uint64_t &value) const
    {
        int pos = Find_pos(key);
        if (unlikely(pos >= pb_entry_count || !entrys[pos].IsValid()))
        {
            return false;
        }
#ifdef USE_FINGER
        auto ret = entrys[pos].pointer.pointer()->Get(key, value, (const char *)entrys[pos].fingerprint);
#else
        auto ret = entrys[pos].pointer.pointer()->Get(key, value, nullptr);
#endif
        // if(ret != status::OK) {
        //     printf("get key false\n");
        // }
        return ret == status::OK;
    }

    bool PointerBEntry::Scan(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results, bool if_first) const
    {
        int pos = 0;
        status ret;
        if (if_first)
        {
            pos = 0;
            ret = entrys[pos].pointer.pointer()->Scan(0, len, results, if_first);
        }
        else
        {
            int pos = Find_pos(start_key);
            if (unlikely(pos >= pb_entry_count || !entrys[pos].IsValid()))
            {
                return false;
            }
            ret = entrys[pos].pointer.pointer()->Scan(start_key, len, results, false);
        }
        pos++;
        while (ret == status::Failed && pos < entrys[0].buf.entries)
        {
            ret = entrys[pos].pointer.pointer()->Scan(start_key, len, results, false);
        }
        return ret == status::OK ? true : false;
    }

    bool PointerBEntry::Delete(uint64_t key, uint64_t *value)
    {
        int pos = Find_pos(key);
        if (unlikely(pos >= pb_entry_count || !entrys[pos].IsValid()))
        {
            return false;
        }
#ifdef USE_FINGER
        auto ret = entrys[pos].pointer.pointer()->Delete(key, value, entrys[pos].fingerprint);
#else
        auto ret = entrys[pos].pointer.pointer()->Delete(key, value, nullptr);
#endif
        return ret == status::OK;
    }
} // namespace nstore
