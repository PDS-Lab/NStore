// #pragma once

// #include <unistd.h>
// #include <atomic>
// #include <pthread.h>
// #include <shared_mutex>

// #include "nali_entry.h"
// #include "rmi_model.h"

// #define MULTI_THREAD
// // #define STASTISTIC_NALI_CDF

// namespace nstore
// {
//     #ifdef STASTISTIC_NALI_CDF
//     extern size_t test_total_keys;
//     #endif

//     class __attribute__((aligned(64))) Spinlock {
//         public:
//         void lock() {
//             while (flag.test_and_set(std::memory_order_acquire)) {
//             }
//         }

//         void unlock() { flag.clear(std::memory_order_release); }

//         void *operator new(size_t size) {
//             return aligned_alloc(64, size);
//         }

//         private:
//         std::atomic_flag flag = ATOMIC_FLAG_INIT;
//         char padding[63];
//     };

//     static const size_t max_entry_count = 1024;
//     static const size_t min_entry_count = 64;

//     /**
//      * @brief 根模型，采用的是两层RMI模型，
//      * 1. 目前实现需要首先 Load一定的数据作为初始化数据；
//      * 2. EXPAND_ALL 宏定义控制采用每次扩展所有EntryGroup，还是采用重复指针一次扩展一个EntryGroup
//      */
//     class Nali;
//     class __attribute__((aligned(64))) group
//     {
//         // class  group {
//     public:
//         class Iter;
//         class BEntryIter;
//         class EntryIter;
//         friend class Nali;
//         group() : nr_entries_(0), next_entry_count(0) {}

//         ~group()
//         {
//             if (entry_space) {
//                 delete [] entry_space;
//             }
//         }

//         void *operator new(size_t size) {
//             return aligned_alloc(64, size);
//         }

//         void Init();

//         void bulk_load(std::vector<std::pair<uint64_t, uint64_t>> &data);

//         void bulk_load(std::vector<std::pair<uint64_t, uint64_t>> &data, size_t start, size_t count);

//         void bulk_load(const std::pair<uint64_t, uint64_t> data[], size_t start, size_t count);

//         void append_entry(const eentry *entry);

//         inline void inc_entry_count()
//         {
//             next_entry_count++;
//         }

//         void reserve_space();

//         void re_tarin();

//         int find_entry(const uint64_t &key) const;

//         int exponential_search_upper_bound(int m, const uint64_t &key) const;

//         // int linear_search_upper_bound(int m, const uint64_t & key) const;

//         int binary_search_upper_bound(int l, int r, const uint64_t &key) const;

//         int linear_search_upper_bound(int l, int r, const uint64_t &key) const;

//         int binary_search_lower_bound(int l, int r, const uint64_t &key) const;

//         status Put(uint64_t key, uint64_t value);

//         bool Get(uint64_t key, uint64_t &value) const;

//         bool Scan(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results, bool if_first) const;

//         bool fast_fail(uint64_t key, uint64_t &value);

//         bool scan_fast_fail(uint64_t key);

//         bool Update(uint64_t key, uint64_t value, uint64_t *old_offset);

//         bool Delete(uint64_t key, uint64_t *old_log_offset);

//         static inline uint64_t get_entry_key(const PointerBEntry &entry)
//         {
//             return entry.entry_key;
//         }

//         void AdjustEntryKey();

//         void expand();

//         void Show();

//         void Info();

//         size_t get_num_keys() {
//             size_t num_keys = 0;
//             for (int i = 0; i < nr_entries_; i++) {
//                 num_keys += entry_space[i].get_num_keys();
//             }
//             return num_keys;
//         }

//     private:
//         int nr_entries_; // entry个数
//         int next_entry_count; // 下一次扩展的entry个数
//         uint64_t min_key; // 最小key
//         PointerBEntry *entry_space; // entry nvm space
//         LearnModel::rmi_line_model<uint64_t> model;
//         uint8_t reserve[24];
//     }; // 每个group 64B

//     void group::Init()
//     {

//         nr_entries_ = 1;

//         entry_space = new PointerBEntry(0, 8);

//         model.init<PointerBEntry *, PointerBEntry>(entry_space, 1, 1, get_entry_key);

//         next_entry_count = 1;
//     }

//     void group::bulk_load(std::vector<std::pair<uint64_t, uint64_t>> &data)
//     {
//         nr_entries_ = data.size();

//         PointerBEntry *new_entry_space = (PointerBEntry *)aligned_alloc(64, nr_entries_ * sizeof(PointerBEntry));
//         size_t new_entry_count = 0;
//         for (size_t i = 0; i < data.size(); i++)
//         {
//             new_entry_space[new_entry_count++] = PointerBEntry(data[i].first, data[i].second);
//         }
//         model.init<PointerBEntry *, PointerBEntry>(new_entry_space, new_entry_count,
//                                             std::ceil(1.0 * new_entry_count / 100), get_entry_key);
//         entry_space = new_entry_space;
//         next_entry_count = nr_entries_;
//     }

//     void group::bulk_load(std::vector<std::pair<uint64_t, uint64_t>> &data, size_t start, size_t count)
//     {
//         nr_entries_ = count;
//         size_t new_entry_count = 0;
//         for (size_t i = 0; i < count; i++)
//         {
//             entry_space[new_entry_count++] = PointerBEntry(data[start + i].first,
//                                                             data[start + i].second, 0);
//         }
//         min_key = data[0].first;
//         model.init<PointerBEntry *, PointerBEntry>(entry_space, new_entry_count,
//                                             std::ceil(1.0 * new_entry_count / 100), get_entry_key);
//         next_entry_count = nr_entries_;
//     }

//     void group::bulk_load(const std::pair<uint64_t, uint64_t> data[], size_t start, size_t count)
//     {
//         nr_entries_ = count;
//         size_t new_entry_count = 0;
//         for (size_t i = 0; i < count; i++)
//         {
//             entry_space[new_entry_count++] = PointerBEntry(data[start + i].first,
//                                                             data[start + i].second, 0);
//         }
//         min_key = data[0].first;
//         model.init<PointerBEntry *, PointerBEntry>(entry_space, new_entry_count,
//                                             std::ceil(1.0 * new_entry_count / 100), get_entry_key);
//         next_entry_count = nr_entries_;
//     }

//     void group::append_entry(const eentry *entry)
//     {
//         entry_space[nr_entries_++] = PointerBEntry(entry);
//     }

//     void group::reserve_space()
//     {
//         entry_space = new PointerBEntry[next_entry_count];
//     }

//     void group::re_tarin()
//     {
//         assert(nr_entries_ <= next_entry_count);
//         model.init<PointerBEntry *, PointerBEntry>(entry_space, nr_entries_,
//                                             std::ceil(1.0 * nr_entries_ / 100), get_entry_key);
//         min_key = entry_space[0].entry_key;
//     }

//     // alex指数查找
//     int group::find_entry(const uint64_t &key) const
//     {
//         int m = model.predict(key);
//         m = std::min(std::max(0, m), (int)nr_entries_ - 1);

//         return exponential_search_upper_bound(m, key);
//     }

//     int group::exponential_search_upper_bound(int m, const uint64_t &key) const
//     {
//         int bound = 1;
//         int l, r; // will do binary search in range [l, r)
//         if (entry_space[m].entry_key > key)
//         {
//             int size = m;
//             while (bound < size && (entry_space[m - bound].entry_key > key))
//             {
//                 bound *= 2;
//             }
//             l = m - std::min<int>(bound, size);
//             r = m - bound / 2;
//         }
//         else
//         {
//             int size = nr_entries_ - m;
//             while (bound < size && (entry_space[m + bound].entry_key <= key))
//             {
//                 bound *= 2;
//             }
//             l = m + bound / 2;
//             r = m + std::min<int>(bound, size);
//         }
//         if (r - l < 6)
//         {
//             return std::max(linear_search_upper_bound(l, r, key) - 1, 0);
//         }
//         return std::max(binary_search_upper_bound(l, r, key) - 1, 0);
//     }

//     int group::binary_search_upper_bound(int l, int r, const uint64_t &key) const
//     {
//         while (l < r)
//         {
//             int mid = l + (r - l) / 2;
//             if (entry_space[mid].entry_key <= key)
//             {
//                 l = mid + 1;
//             }
//             else
//             {
//                 r = mid;
//             }
//         }
//         return l;
//     }

//     int group::linear_search_upper_bound(int l, int r, const uint64_t &key) const
//     {
//         while (l < r && entry_space[l].entry_key <= key)
//             l++;
//         return l;
//     }

//     int group::binary_search_lower_bound(int l, int r, const uint64_t &key) const
//     {
//         while (l < r)
//         {
//             int mid = l + (r - l) / 2;
//             if (entry_space[mid].entry_key < key)
//             {
//                 l = mid + 1;
//             }
//             else
//             {
//                 r = mid;
//             }
//         }
//         return l;
//     }

//     status group::Put(uint64_t key, uint64_t value)
//     {
//     retry0:
//         int entry_id = find_entry(key);
//         bool split = false;
//         auto ret = entry_space[entry_id].Put(key, value, &split);

//         if (split)
//         {
//             next_entry_count++;
//         }

//         if (ret == status::Full)
//         { // LearnGroup数组满了，需要扩展
//             if (next_entry_count > max_entry_count)
//             {
//                 // LOG(Debug::INFO, "Need expand tree: group entry count %d.", next_entry_count);
//                 return ret;
//             }
//             expand();
//             split = false;
//             goto retry0;
//         }
//         return ret;
//     }

//     bool group::Get(uint64_t key, uint64_t &value) const
//     {
//         // Common::g_metic.tracepoint("None");
//         int entry_id = find_entry(key);
//         // Common::g_metic.tracepoint("FindEntry");
//         auto ret = entry_space[entry_id].Get(key, value);
//         if (!ret)
//         {
//             assert(false);
//         }
//         return ret;
//     }

//     bool group::Scan(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results, bool if_first) const
//     {
//         int entry_id;
//         bool ret;
//         if (if_first)
//         {
//             entry_id = 0;
//             ret = entry_space[entry_id].Scan(start_key, len, results,if_first);
//         }
//         else
//         {
//             entry_id = find_entry(start_key);
//             ret = entry_space[entry_id].Scan(start_key, len, results,false);
//         }
//         while (!ret && entry_id < nr_entries_ - 1)
//         {
//             ++entry_id;
//             ret = entry_space[entry_id].Scan(start_key, len, results,false);
//         }
//         //cout << "in group:" << len << endl;
//         return ret;
//     }

//     bool group::fast_fail(uint64_t key, uint64_t &value)
//     {
//         if (nr_entries_ <= 0 || key < min_key)
//             return false;
//         return Get(key, value);
//     }

//     bool group::scan_fast_fail(uint64_t key)
//     {
//         if (nr_entries_ <= 0 || key < min_key)
//             return false;
//         return true;
//     }

//     bool group::Update(uint64_t key, uint64_t value, uint64_t *old_offset)
//     {
//         int entry_id = find_entry(key);
//         auto ret = entry_space[entry_id].Update(key, value, old_offset);
//         return ret;
//     }

//     bool group::Delete(uint64_t key, uint64_t *old_log_offset)
//     {
//         int entry_id = find_entry(key);
//         auto ret = entry_space[entry_id].Delete(key, old_log_offset);
//         return ret;
//     }

//     void group::expand()
//     {
//         PointerBEntry::EntryIter it;
//         // Meticer timer;
//         // timer.Start();
//         // PointerBEntry *new_entry_space = (PointerBEntry *)malloc(next_entry_count * sizeof(PointerBEntry));
//         PointerBEntry * new_entry_space = new PointerBEntry[next_entry_count];
//         size_t new_entry_count = 0;
//         entry_space[0].AdjustEntryKey();
//         for (size_t i = 0; i < nr_entries_; i++)
//         {
//             new (&it) PointerBEntry::EntryIter(&entry_space[i]);
//             while (!it.end())
//             {
//                 //std::cout << entry_space[i].buf.entries << std::endl;
//                 new_entry_space[new_entry_count++] = PointerBEntry(&(*it));
//                 //std::cout << entry_space[i].buf.entries << std::endl;
//                 it.next();
//             }
//         }
//         if(next_entry_count != new_entry_count){
//             std::cout << "next_entry_count = " << next_entry_count << " new_entry_count = "
//                         << new_entry_count << " nr_entries = " << nr_entries_<< std::endl;
//             assert(next_entry_count == new_entry_count);
//         }

//         model.init<PointerBEntry *, PointerBEntry>(new_entry_space, new_entry_count,
//                                             std::ceil(1.0 * new_entry_count / 100), get_entry_key);
//         delete [] entry_space;
//         entry_space = new_entry_space;
//         nr_entries_ = new_entry_count;
//         next_entry_count = nr_entries_;
//     }

//     void group::AdjustEntryKey()
//     {
//         entry_space[0].AdjustEntryKey();
//     }

//     void group::Show()
//     {
//         std::cout << "Entry count:" << nr_entries_ << std::endl;
//         for (int i = 0; i < nr_entries_; i++)
//         {
//             entry_space[i].Show();
//         }
//     }

//     void group::Info()
//     {
//         std::cout << "nr_entrys: " << nr_entries_ << "\t";
//         std::cout << "entry size:" << sizeof(PointerBEntry) << "\t";
//         //clevel_mem_->Usage();
//     }

//     static_assert(sizeof(group) == 64);

//     class group::BEntryIter
//     {
//     public:
//         using difference_type = ssize_t;
//         using value_type = const uint64_t;
//         using pointer = const uint64_t *;
//         using reference = const uint64_t &;
//         using iterator_category = std::random_access_iterator_tag;

//         BEntryIter(group *root) : root_(root) {}
//         BEntryIter(group *root, uint64_t idx) : root_(root), idx_(idx) {}
//         ~BEntryIter()
//         {
//         }
//         uint64_t operator*()
//         {
//             return root_->entry_space[idx_].entry_key;
//         }

//         BEntryIter &operator++()
//         {
//             idx_++;
//             return *this;
//         }

//         BEntryIter operator++(int)
//         {
//             return BEntryIter(root_, idx_++);
//         }

//         BEntryIter &operator--()
//         {
//             idx_--;
//             return *this;
//         }

//         BEntryIter operator--(int)
//         {
//             return BEntryIter(root_, idx_--);
//         }

//         uint64_t operator[](size_t i) const
//         {
//             if ((i + idx_) > root_->nr_entries_)
//             {
//                 std::cout << "索引超过最大值" << std::endl;
//                 // 返回第一个元素
//                 return root_->entry_space[root_->nr_entries_ - 1].entry_key;
//             }
//             return root_->entry_space[i + idx_].entry_key;
//         }

//         bool operator<(const BEntryIter &iter) const { return idx_ < iter.idx_; }
//         bool operator==(const BEntryIter &iter) const { return idx_ == iter.idx_ && root_ == iter.root_; }
//         bool operator!=(const BEntryIter &iter) const { return idx_ != iter.idx_ || root_ != iter.root_; }
//         bool operator>(const BEntryIter &iter) const { return idx_ < iter.idx_; }
//         bool operator<=(const BEntryIter &iter) const { return *this < iter || *this == iter; }
//         bool operator>=(const BEntryIter &iter) const { return *this > iter || *this == iter; }
//         size_t operator-(const BEntryIter &iter) const { return idx_ - iter.idx_; }

//         const BEntryIter &base() { return *this; }

//     private:
//         group *root_;
//         uint64_t idx_;
//     };

//     class group::Iter
//     {
//     public:
//         Iter(group *root) : root_(root), idx_(0)
//         {
//             if (root->nr_entries_ == 0)
//                 return;
//             new (&biter_) PointerBEntry::PointerBEntryIter(&root_->entry_space[idx_]);
//         }
//         Iter(group *root, uint64_t start_key) : root_(root)
//         {
//             if (root->nr_entries_ == 0)
//                 return;
//             idx_ = root->find_entry(start_key);
//             new (&biter_) PointerBEntry::PointerBEntryIter(&root_->entry_space[idx_], start_key);
//             if (biter_.end())
//             {
//                 next();
//             }
//         }
//         ~Iter()
//         {
//         }

//         uint64_t key()
//         {
//             return biter_.key();
//         }

//         uint64_t value()
//         {
//             return biter_.value();
//         }

//         bool next()
//         {
//             if (idx_ < root_->nr_entries_ && biter_.next())
//             {
//                 return true;
//             }
//             // idx_ = root_->NextGroupId(idx_);
//             idx_++;
//             if (idx_ < root_->nr_entries_)
//             {
//                 new (&biter_) PointerBEntry::PointerBEntryIter(&root_->entry_space[idx_]);
//                 return true;
//             }
//             return false;
//         }

//         bool end()
//         {
//             return idx_ >= root_->nr_entries_;
//         }

//     private:
//         group *root_;
//         PointerBEntry::PointerBEntryIter biter_;
//         uint64_t idx_;
//     };

//     class group::EntryIter
//     {
//     public:
//         EntryIter(group *group) : group_(group), cur_idx(0)
//         {
//             new (&biter_) PointerBEntry::EntryIter(&group_->entry_space[cur_idx]);
//         }
//         const eentry &operator*() { return *biter_; }

//         bool next()
//         {
//             if (cur_idx < group_->nr_entries_ && biter_.next())
//             {
//                 return true;
//             }
//             cur_idx++;
//             if (cur_idx >= group_->nr_entries_)
//             {
//                 return false;
//             }
//             new (&biter_) PointerBEntry::EntryIter(&group_->entry_space[cur_idx]);
//             return true;
//         }

//         bool end() const
//         {
//             return cur_idx >= group_->nr_entries_;
//         }

//     private:
//         group *group_;
//         PointerBEntry::EntryIter biter_;
//         uint64_t cur_idx;
//     };

//     class EntryIter;

//     class Nali
//     {
//     public:
//         friend class EntryIter;
//         class Iter;

//     public:
//         Nali() : nr_groups_(0), root_expand_times(0)
// #ifdef MULTI_THREAD
//         , is_tree_expand(false)
// #endif
//         {}

//         ~Nali()
//         {
//             if (group_space) {
//                 delete [] group_space;
//             }

//             #ifdef MULTI_THREAD
//             if(lock_space)
//                 delete [] lock_space;
//             #endif
//         }

//         void Init()
//         {
//             nr_groups_ = 1;
//             group_space = new group[1];
// #ifdef MULTI_THREAD
//             lock_space = new Spinlock[1];
// #endif
//             group_space[0].Init();
//         }

//         static inline uint64_t first_key(const std::pair<uint64_t, uint64_t> &kv)
//         {
//             return kv.first;
//         }

//         void bulk_load(std::vector<std::pair<uint64_t, uint64_t>> &data);

//         void bulk_load(const std::pair<uint64_t, uint64_t> data[], int size);

//         status Put(uint64_t key, uint64_t value);

//         bool Update(uint64_t key, uint64_t value, uint64_t *old_offset);

//         bool Get(uint64_t key, uint64_t &value);

//         bool Scan(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results);

//         bool Delete(uint64_t key, uint64_t *old_log_offset);

//         int find_group(const uint64_t &key) const;

//         bool find_fast(uint64_t key, uint64_t &value) const;

//         bool scan_fast(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results) const;

//         bool find_slow(uint64_t key, uint64_t &value) const;

// #ifdef MULTI_THREAD
//         void trans_begin() {
//             if(is_tree_expand.load(std::memory_order_acquire)) {
//                 std::unique_lock<std::mutex> lock(expand_wait_lock);
//                 expand_wait_cv.wait(lock);
//             }
//         }
// #endif

//         bool scan_slow(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results, int fast_group_id = 0) const;

//         void ExpandTree();

//         void Show()
//         {
//             for (int i = 0; i < nr_groups_; i++)
//             {
//                 std::cout << "Group [" << i << "].\n";
//                 group_space[i].Show();
//             }
//         }

//         void Info() {
//             std::cout << "root_expand_times : " << root_expand_times << std::endl;
//             std::cout << "total groups : " << nr_groups_ << std::endl;
//             #ifdef STASTISTIC_NALI_CDF
//             size_t num_keys = 0;
//             size_t cnt = 0;
//             size_t jump = test_total_keys / 10000;
//             for (int i = 0; i < nr_groups_; i++) {
//                 group *group_ = &group_space[i];
//                 size_t old_num_keys = num_keys;
//                 size_t group_num_keys = group_->get_num_keys();
//                 num_keys += group_num_keys;
//                 double model_a = 0.0, model_b = 0.0;
//                 group_->model.get_model_info(model_a, model_b);
//                 size_t last_key = group_->min_key;
//                 bool flag = false;
//                 while (num_keys > cnt) {
//                     cnt += jump;
//                     std::cout << "cdf: " << "group#" << i << " min_key: " << last_key
//                     << " model_a: " << model_a << " model_b: " << model_b << std::endl;
//                     last_key += (group_num_keys/1.0/jump)/model_a;
//                     if (flag)
//                         std::cout << "*****" << std::endl;
//                     flag = true;
//                 }

//             }
//             #endif
//         }

//     private:
//         group *group_space;
//         int nr_groups_;
//         LearnModel::rmi_line_model<uint64_t> model;
//         int entries_per_group = min_entry_count;
//         uint64_t root_expand_times;

// #ifdef MULTI_THREAD
//         Spinlock *lock_space;
//         std::mutex expand_wait_lock;
//         std::condition_variable expand_wait_cv;
//         std::atomic_bool is_tree_expand;
// #endif
//     };

//     void nstore::bulk_load(std::vector<std::pair<uint64_t, uint64_t>> &data)
//     {
//         size_t size = data.size();
//         int group_id = 0;

//         if(nr_groups_ != 0) {
//             if(group_space) {
//                 delete [] group_space;
//             }
// #ifdef MULTI_THREAD
//             if(lock_space)
//                 delete [] lock_space;
// #endif
//         }

//         model.init<std::vector<std::pair<uint64_t, uint64_t>> &, std::pair<uint64_t, uint64_t>>(data, size, size / 256, first_key);
//         nr_groups_ = size / min_entry_count;
//         // group_space = (group *)malloc(nr_groups_ * sizeof(group));
//         group_space = new group[nr_groups_];
// #ifdef MULTI_THREAD
//         // lock_space = new std::mutex[nr_groups_];
//         // lock_space = new pthread_mutex_t[nr_groups_];
//         // for (int i = 0; i < nr_groups_; i++)
//             // lock_space[i] = PTHREAD_MUTEX_INITIALIZER;
//         lock_space = new Spinlock[nr_groups_];
//         // lock_space = new std::shared_mutex[nr_groups_];
// #endif
//         // pmem_memset_persist(group_space, 0, nr_groups_ * sizeof(group));
//         memset(group_space, 0, nr_groups_ * sizeof(group));
//         for (int i = 0; i < size; i++)
//         {
//             group_id = model.predict(data[i].first) / min_entry_count;
//             group_id = std::min(std::max(0, group_id), (int)nr_groups_ - 1);

//             group_space[group_id].inc_entry_count();
//         }
//         size_t start = 0;
//         for (int i = 0; i < nr_groups_; i++)
//         {
//             if (group_space[i].next_entry_count == 0)
//                 continue;
//             group_space[i].reserve_space();
//             group_space[i].bulk_load(data, start, group_space[i].next_entry_count);
//             start += group_space[i].next_entry_count;
//         }
//     }

//     void nstore::bulk_load(const std::pair<uint64_t, uint64_t> data[], int size)
//     {
//         int group_id = 0;

//         if(nr_groups_ != 0) {
//             if(group_space) {
//                 delete [] group_space;
//             }
// #ifdef MULTI_THREAD
//             if(lock_space)
//                 delete [] lock_space;
// #endif
//         }

//         model.init<const std::pair<uint64_t, uint64_t>[], std::pair<uint64_t, uint64_t>>(data, size, size / 256, first_key);
//         nr_groups_ = size / min_entry_count;
//         // group_space = (group *)malloc(nr_groups_ * sizeof(group));
//         group_space = new group[nr_groups_];
// #ifdef MULTI_THREAD
//         // lock_space = new std::mutex[nr_groups_];
//         // lock_space = new pthread_mutex_t[nr_groups_];
//         // for (int i = 0; i < nr_groups_; i++)
//         //    lock_space[i] = PTHREAD_MUTEX_INITIALIZER;
//         lock_space = new Spinlock[nr_groups_];
//         // lock_space = new std::shared_mutex[nr_groups_];
// #endif
//         memset(group_space, 0, nr_groups_ * sizeof(group));
//         for (int i = 0; i < size; i++)
//         {
//             group_id = model.predict(data[i].first) / min_entry_count;
//             group_id = std::min(std::max(0, group_id), (int)nr_groups_ - 1);
//             group_space[group_id].inc_entry_count();
//         }
//         size_t start = 0;
//         for (int i = 0; i < nr_groups_; i++)
//         {
//             if (group_space[i].next_entry_count == 0)
//                 continue;
//             group_space[i].reserve_space();
//             group_space[i].bulk_load(data, start, group_space[i].next_entry_count);
//             start += group_space[i].next_entry_count;
//         }
//     }

//     status nstore::Put(uint64_t key, uint64_t value)
//     {
//         status ret = status::Failed;
//     retry0:
// #ifdef MULTI_THREAD
//         trans_begin();
// #endif
//         {
//             int group_id = find_group(key);
// #ifdef MULTI_THREAD
//             lock_space[group_id].lock();
//             if(unlikely(is_tree_expand.load(std::memory_order_acquire))) {
//                 lock_space[group_id].unlock();
//                 goto retry0;
//             }//存在本线程阻塞在lock，然后另一个线程释放lock并进行ExpandTree的situation

// #endif
//             ret = group_space[group_id].Put(key, value);
// #ifdef MULTI_THREAD
//             lock_space[group_id].unlock();
// #endif
//         }

//         if (ret == status::Full)
//         { // LearnGroup 太大了
//             ExpandTree();
//             goto retry0;
//         }
//         return ret;
//     }

//     bool nstore::Update(uint64_t key, uint64_t value, uint64_t *old_offset)
//     {
// #ifdef MULTI_THREAD
//         trans_begin();
// #endif
//         int group_id = find_group(key);
// #ifdef MULTI_THREAD
//         lock_space[group_id].lock();
// #endif
//         auto ret = group_space[group_id].Update(key, value, old_offset);
// #ifdef MULTI_THREAD
//         lock_space[group_id].unlock();
// #endif
//         return ret;
//     }

//     bool nstore::Get(uint64_t key, uint64_t &value)
//     {
// #ifdef MULTI_THREAD
//         trans_begin();
// #endif
//         if (find_fast(key, value))
//         {
//             return true;
//         }
//         return find_slow(key, value);
//     }

//     bool nstore::Scan(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results)
//     {
// #ifdef MULTI_THREAD
//         trans_begin();
// #endif
//         if (scan_fast(start_key, len, results))
//         {
//             return true;
//         }
//         return scan_slow(start_key, len, results);
//     }

//     bool nstore::Delete(uint64_t key, uint64_t *old_log_offset)
//     {
// #ifdef MULTI_THREAD
//         trans_begin();
// #endif
//         int group_id = find_group(key);
// #ifdef MULTI_THREAD
//         lock_space[group_id].lock();
// #endif
//         auto ret = group_space[group_id].Delete(key, old_log_offset);
// #ifdef MULTI_THREAD
//         lock_space[group_id].unlock();
// #endif
//         return ret;
//     }

//     //zzy: 内部节点寻找group需要线性查找, 这里如何优化呢？
//     int nstore::find_group(const uint64_t &key) const
//     {
//         int group_id = model.predict(key) / min_entry_count;
//         group_id = std::min(std::max(0, group_id), (int)nr_groups_ - 1);

//         // int gg = group_id;

//         // /** 忽略空的group和边界 **/
//         // while (group_id > 0 && key < group_space[group_id].entry_space[0].entry_key)
//         // {
//         //     // 这里测试了一下感觉不可能出现group_space[group_id].nr_entries_ == 0的情况，可以优化成二分或指数搜索
//         //     // if (group_space[group_id].nr_entries_ == 0)
//         //     //     std::cout << "group entries is empty" << std::endl;
//         //     group_id--;
//         // }

//         while(group_id > 0 && (group_space[group_id].nr_entries_ == 0 ||
//                 (key < group_space[group_id].entry_space[0].entry_key))) {
//             group_id --;
//         }

//         while (group_space[group_id].nr_entries_ == 0)
//             group_id++;

//         return group_id;

//         // // 二分搜索
//         // if (gg == 0)
//         //     return 0;
//         // int left = 1;
//         // int right = gg + 1;
//         // int ppos = 0;
//         // while (left < right) {
//         //     ppos = (left + right) >> 1;
//         //     // assert(entrys[ppos].IsValid());
//         //     if(group_space[ppos].entry_space[0].entry_key == key) {
//         //         // assert(ppos == pos);
//         //         return ppos;
//         //     }

//         //     if (group_space[ppos].entry_space[0].entry_key > key)
//         //         right = ppos;
//         //     else
//         //         left = ppos + 1;
//         // }
//         // ppos = left - 1;

//         // // assert(ppos == group_id);
//         // return ppos;
//     }

//     bool nstore::find_fast(uint64_t key, uint64_t &value) const
//     {
//         int group_id = model.predict(key) / min_entry_count;
//         group_id = std::min(std::max(0, group_id), (int)nr_groups_ - 1);
// #ifdef MULTI_THREAD
//         // std::unique_lock<std::mutex> lock(lock_space[group_id]);
//         // pthread_mutex_lock(&lock_space[group_id]);
//         // lock_space[group_id].lock();
//         // ReadLock l(lock_space[group_id]);
// #endif
//         auto ret = group_space[group_id].fast_fail(key, value);
//         // pthread_mutex_unlock(&lock_space[group_id]);
// #ifdef MULTI_THREAD
//         // lock_space[group_id].unlock();
// #endif
//         return ret;
//     }

//     bool nstore::find_slow(uint64_t key, uint64_t &value) const
//     {
//         int group_id = find_group(key);
// #ifdef MULTI_THREAD
//         // std::unique_lock<std::mutex> lock(lock_space[group_id]);
//         // pthread_mutex_lock(&lock_space[group_id]);
//         // lock_space[group_id].lock();
//         // ReadLock l(lock_space[group_id]);
// #endif
//         auto ret = group_space[group_id].Get(key, value);
//         // pthread_mutex_unlock(&lock_space[group_id]);
// #ifdef MULTI_THREAD
//         // lock_space[group_id].unlock();
// #endif
//         return ret;
//     }

//     bool nstore::scan_fast(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results) const
//     {
//         int group_id = model.predict(start_key) / min_entry_count;
//         group_id = std::min(std::max(0, group_id), (int)nr_groups_ - 1);
// #ifdef MULTI_THREAD
//         // std::unique_lock<std::mutex> lock(lock_space[group_id]);
// #endif
//         if (group_space[group_id].scan_fast_fail(start_key))
//         {
//             scan_slow(start_key, len, results, group_id);
//             return true;
//         }
//         return false;
//     }

//     bool nstore::scan_slow(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results, int fast_group_id) const
//     {
//         int group_id = 0;
//         if (fast_group_id != 0)
//         {
//             group_id = fast_group_id;
//         }
//         else
//         {
//             group_id = find_group(start_key);
//         }
// #ifdef MULTI_THREAD
//         // std::unique_lock<std::mutex> lock(lock_space[group_id]);
// #endif
//         auto ret = group_space[group_id].Scan(start_key, len, results,true);
//         while (!ret && group_id < nr_groups_ - 1)
//         {
//             ++group_id;
// #ifdef MULTI_THREAD
//         // std::unique_lock<std::mutex> lock(lock_space[group_id]);
// #endif
//             while (group_space[group_id].nr_entries_ == 0)
//             {
//                 ++group_id;
//             }
//             ret = group_space[group_id].Scan(start_key, len, results,false);
//         }
//         // cout << len << endl;
//         if (len > 0)
//         {
//             std::cout << len << std::endl;
//         }
//         return 0;
//     }

//     void nstore::ExpandTree()
//     {
//         size_t entry_count = 0;
//         int entry_seq = 0;

//         // Show();
// #ifdef MULTI_THREAD
//         // if(is_tree_expand.load(std::memory_order_acquire))
//         //     return ;
//         // is_tree_expand.store(true);
//         bool b1 = false, b2 = true;
//         if (!is_tree_expand.compare_exchange_strong(b1, b2, std::memory_order_acquire))
//             return;
// #endif

//         // Meticer timer;
//         // timer.Start();
//         {
//             /*采用一层线性模型*/
//             LearnModel::rmi_line_model<uint64_t>::builder_t bulder(&model);
//             // 遍历一遍group的所有entry的entrykey生成模型
//             for (int i = 0; i < nr_groups_; i++)
//             {
//                 if (group_space[i].next_entry_count == 0)
//                     continue;
//                 entry_count += group_space[i].next_entry_count;
//                 group_space[i].AdjustEntryKey();
//                 group::EntryIter e_iter(&group_space[i]);
//                 // int sample = std::ceil(1.0 * group_space[i].next_entry_count / min_entry_count);
//                 while (!e_iter.end())
//                 {
//                     bulder.add((*e_iter).entry_key, entry_seq);
//                     e_iter.next();
//                     entry_seq++;
//                 }
//             }
//             bulder.build();
//         }

//         // {
//         //     std::vector<uint64_t> entry_keys;
//         //     for(int i = 0; i < nr_groups_; i++) {
//         //         if(group_space[i].next_entry_count == 0) continue;
//         //         entry_count += group_space[i].next_entry_count;
//         //         group_space[i].AdjustEntryKey(clevel_mem_);
//         //         group::EntryIter e_iter(&group_space[i]);
//         //         while (!e_iter.end())
//         //         {
//         //         entry_keys.push_back((*e_iter).entry_key);
//         //         e_iter.next();
//         //         }
//         //     }
//         //     model.init(entry_keys, entry_keys.size(), entry_keys.size() / 256);
//         // }
//         // std::cout << "Entry count: " <<
//         //LOG(Debug::INFO, "Entry_count: %ld, %d", entry_count, entry_seq);
//         // int new_nr_groups = std::ceil(1.0 * entry_count / min_entry_count);
//         int new_nr_groups = std::ceil(1.0 * entry_count / min_entry_count);
//         // group *new_group_space = (group *)malloc(new_nr_groups * sizeof(group));
//         group *new_group_space = new group[new_nr_groups];
// #ifdef MULTI_THREAD
//         // std::mutex *new_lock_space = new std::mutex[new_nr_groups];
//         // pthread_mutex_t *new_lock_space = new pthread_mutex_t[new_nr_groups];
//         // for (int i = 0; i < new_nr_groups; i++)
//         //     new_lock_space[i] = PTHREAD_MUTEX_INITIALIZER;
//         Spinlock * new_lock_space = new Spinlock[new_nr_groups];
//         // std::shared_mutex * new_lock_space = new std::shared_mutex[new_nr_groups];
// #endif
//         // pmem_memset_persist(new_group_space, 0, new_nr_groups * sizeof(group));
//         memset(new_group_space, 0, new_nr_groups * sizeof(group));
//         int group_id = 0;

//         // int prev_group_id  = 0;
//         // for(int  i = 1; i < entry_keys.size(); i ++) {
//         //     assert(entry_keys[i] > entry_keys[i-1]);
//         //     group_id = model.predict(entry_keys[i]) / min_entry_count;
//         //     group_id = std::min(std::max(0, group_id), (int)new_nr_groups - 1);
//         //     if(group_id < prev_group_id) {
//         //         model.predict(entry_keys[i-1]);
//         //         model.predict(entry_keys[i]);
//         //         assert(0);
//         //     }
//         //     prev_group_id = group_id;
//         // }

//         for (int i = 0; i < nr_groups_; i++)
//         {
//             group::EntryIter e_iter(&group_space[i]);
//             while (!e_iter.end())
//             {
//                 group_id = model.predict((*e_iter).entry_key) / min_entry_count;
//                 group_id = std::min(std::max(0, group_id), (int)new_nr_groups - 1);
//                 new_group_space[group_id].inc_entry_count();
//                 e_iter.next();
//             }
//         }

//         for (int i = 0; i < new_nr_groups; i++)
//         {
//             if (new_group_space[i].next_entry_count == 0)
//                 continue;
//             new_group_space[i].reserve_space();
//         }

//         // uint64_t prev_key = 0;
//         for (int i = 0; i < nr_groups_; i++)
//         {
//             group::EntryIter e_iter(&group_space[i]);
//             while (!e_iter.end())
//             {
//                 group_id = model.predict((*e_iter).entry_key) / min_entry_count;
//                 group_id = std::min(std::max(0, group_id), (int)new_nr_groups - 1);
//                 new_group_space[group_id].append_entry(&(*e_iter));
//                 // assert((*e_iter).entry_key >= prev_key);
//                 // prev_key = (*e_iter).entry_key;
//                 e_iter.next();
//             }
//         }

//         for (int i = 0; i < new_nr_groups; i++)
//         {
//             // std::cerr << "Group [" << i << "]: entry count " << new_group_space[i].next_entry_count << ".\n";
//             if (new_group_space[i].next_entry_count == 0)
//                 continue;
//             new_group_space[i].re_tarin();
//         }
//         // pmem_persist(new_group_space, new_nr_groups * sizeof(group));
//         if(nr_groups_ != 0) {
//             if(group_space) {
//                 // NVM::data_alloc->Free(group_space, nr_groups_ * sizeof(group));
//                 // free(group_space);
//                 delete [] group_space;
//             }

// #ifdef MULTI_THREAD
//             if(lock_space)
//                 delete [] lock_space;
// #endif
//         }
//         std::cout << "root_expand, old_groups: " << nr_groups_ << " new_groups: " << new_nr_groups << std::endl;
//         nr_groups_ = new_nr_groups;
//         group_space = new_group_space;
//         root_expand_times++;
// #ifdef MULTI_THREAD
//         lock_space = new_lock_space;
//         is_tree_expand.store(false);
//         expand_wait_cv.notify_all();
// #endif
//         // uint64_t expand_time = timer.End();
//         // LOG(Debug::INFO, "Finish expanding group, new group count %ld,  expansion time is %lfs",
//         //nr_groups_, (double)expand_time/1000000.0);
//         // Show();
//     }

//     class nstore::Iter
//     {
//     public:
//         Iter(Nali *tree) : tree_(tree), group_id_(0), giter(&tree->group_space[0])
//         {
//             while (giter.end())
//             {
//                 group_id_++;
//                 if (group_id_ >= tree_->nr_groups_)
//                     break;
//                 new (&giter) group::Iter(&tree_->group_space[group_id_]);
//             }
//         }

//         Iter(Nali *tree, uint64_t start_key) : tree_(tree), group_id_(0), giter(&tree->group_space[0])
//         {
//             group_id_ = tree->find_group(start_key);
//             new (&giter) group::Iter(&tree->group_space[group_id_], start_key);
//         }

//         ~Iter()
//         {
//         }

//         uint64_t key()
//         {
//             return giter.key();
//         }

//         uint64_t value()
//         {
//             return giter.value();
//         }

//         bool next()
//         {
//             if (group_id_ < tree_->nr_groups_ && giter.next())
//             {
//                 return true;
//             }
//             // idx_ = root_->NextGroupId(idx_);
//             while (giter.end())
//             {
//                 group_id_++;
//                 if (group_id_ >= tree_->nr_groups_)
//                     break;
//                 new (&giter) group::Iter(&tree_->group_space[group_id_]);
//             }

//             return group_id_ < tree_->nr_groups_;
//         }

//         bool end()
//         {
//             return group_id_ >= tree_->nr_groups_;
//         }

//     private:
//         Nali *tree_;
//         group::Iter giter;
//         int group_id_;
//     };

// } // namespace nstore
