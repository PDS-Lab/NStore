#include "getopt.h"
#include "db_interface.h"
#include "logdb.h"
#include "util/sosd_util.h"
#include "utils.h"

using namespace std;
using namespace nstore;

using KEY_TYPE = size_t;
using VALUE_TYPE = size_t;
size_t VALUE_LENGTH = 8;

int8_t global_numa_map[64];
thread_local size_t global_thread_id = -1;
bool begin_gc = false;
std::atomic<size_t> gc_complete(0);

size_t PER_THREAD_POOL_THREADS = 0;
size_t NSTORE_VERSION_SHARDS = 128;
double GC_THRESHOLD = 0.1;

#ifdef USE_CACHE
uint64_t hit_cnt[64];
#endif

void show_help(char *prog)
{
    cout << "Usage: " << prog << " [options]" << endl
         << endl
         << "  Option:" << endl
         << "    --bulkload-size          LOAD_SIZE" << endl
         << "    --put-size               PUT_SIZE" << endl
         << "    --get-size               GET_SIZE" << endl
         << "    --thread-num               THREAD_NUM_PER_SOCKET" << endl
         << "    --help[-h]               show help" << endl;
}

vector<size_t> generate_uniform_random(size_t op_num)
{
    vector<size_t> data;
    data.resize(op_num);
    const size_t ns = util::timing([&]
                                   {
                                     Random rnd(0, UINT64_MAX);
                                     for (size_t i = 0; i < op_num; ++i)
                                     {
                                       data[i] = rnd.Next();
                                     } });

    const size_t ms = ns / 1e6;
    cout << "generate " << data.size() << " values in "
         << ms << " ms (" << static_cast<double>(data.size()) / 1000 / ms
         << " M values/s)" << endl;
    return data;
}

int main(int argc, char *argv[])
{
    size_t LOAD_SIZE = 10000000;
    size_t PUT_SIZE = 10000000;
    size_t GET_SIZE = 10000000;
    int numa0_thread_num = 16;
    int numa1_thread_num = 16;

    static struct option opts[] = {
        {"load-size", required_argument, NULL, 0},
        {"put-size", required_argument, NULL, 0},
        {"get-size", required_argument, NULL, 0},
        {"thread-num", required_argument, NULL, 't'},
        {"help", no_argument, NULL, 'h'},
        {NULL, 0, NULL, 0}};

    // parse arguments
    int c;
    int opt_idx;

    while ((c = getopt_long(argc, argv, "t:h",
                            opts, &opt_idx)) != -1)
    {
        switch (c)
        {
        case 0:
            switch (opt_idx)
            {
            case 0:
                LOAD_SIZE = atoi(optarg);
                break;
            case 1:
                PUT_SIZE = atoi(optarg);
                break;
            case 2:
                GET_SIZE = atoi(optarg);
                break;
            default:
                abort();
            }
        case 't':
            numa0_thread_num = atoi(optarg);
            numa1_thread_num = numa0_thread_num;
            break;
        case 'h':
            show_help(argv[0]);
            return 0;
        case '?':
            break;
        default:
            cout << (char)c << endl;
            abort();
        }
    }
    cout << "LOAD_SIZE:             " << LOAD_SIZE << endl;
    cout << "PUT_SIZE:              " << PUT_SIZE << endl;
    cout << "GET_SIZE:              " << GET_SIZE << endl;
    cout << "NUMA0 THREAD NUMBER:   " << numa0_thread_num << endl;
    cout << "NUMA1 THREAD NUMBER:   " << numa1_thread_num << endl;

    // generate thread_ids
    assert(numa0_thread_num >= 0 && numa0_thread_num <= 16);
    assert(numa1_thread_num >= 0 && numa1_thread_num <= 16);
    vector<vector<int>> thread_ids(nstore::numa_max_node, vector<int>());
    vector<int> thread_id_arr;
    for (int i = 0; i < numa0_thread_num; i++)
    {
        thread_ids[0].push_back(i);
        thread_id_arr.push_back(i);
    }
    for (int i = 0; i < numa1_thread_num; i++)
    {
        thread_ids[1].push_back(16 + i);
        thread_id_arr.push_back(16 + i);
    }

    // init database
    Tree<KEY_TYPE, VALUE_TYPE> *db = nullptr;
    Tree<KEY_TYPE, VALUE_TYPE> *real_db = new nstore::alexoldb<KEY_TYPE, VALUE_TYPE>();
    db = new nstore::logdb<KEY_TYPE, VALUE_TYPE>(real_db);

    vector<KEY_TYPE> data_base = generate_uniform_random(LOAD_SIZE + PUT_SIZE * 10);
    // bulk load
    LOG_INFO("START BULKLOAD");

    auto values = new std::pair<KEY_TYPE, VALUE_TYPE>[LOAD_SIZE];
    for (size_t i = 0; i < LOAD_SIZE; i++)
    {
        values[i].first = data_base[i];
        values[i].second = data_base[i];
    }
    std::sort(values, values + LOAD_SIZE, [&](auto const &a, auto const &b)
              { return a.first < b.first; });
    db->bulk_load(values, LOAD_SIZE);
    delete[] values;
    size_t load_pos = LOAD_SIZE;
    // test put
    LOG_INFO("START PUT");
    std::vector<std::thread> threads;
    std::atomic<size_t> thread_idx_count(0);
    int total_thread_num = numa0_thread_num + numa1_thread_num;
    size_t per_thread_size = PUT_SIZE / total_thread_num;

    auto ts = TIME_NOW;

    for (int i = 0; i < thread_id_arr.size(); i++)
    {
        threads.emplace_back([&]()
                             {
                                     size_t idx = thread_idx_count.fetch_add(1);
                                     global_thread_id = thread_id_arr[idx];
                                     bindCore(global_thread_id);
                                     size_t size = (idx == thread_id_arr.size() - 1) ? (PUT_SIZE - idx * per_thread_size) : per_thread_size;
                                     size_t start_pos = idx * per_thread_size + LOAD_SIZE;
                                     for(size_t j = 0; j < size; j++)
                                     {
                                        auto ret = db->insert(data_base[start_pos+j], data_base[start_pos+j]);
                                        if(idx == 0 && (j + 1) % 100000 == 0) {
                                            std::cerr << "Operate: " << j + 1 << '\r';
                                        }
                                     } });
    }

    for (auto &t : threads)
    {
        t.join();
    }

    auto te = TIME_NOW;
    auto use_seconds = std::chrono::duration_cast<std::chrono::microseconds>(te - ts).count() * 1.0 / 1000 / 1000;
    LOG_INFO("put %lu kvs in %.2f seconds, %.2e iops/s", PUT_SIZE, use_seconds, PUT_SIZE * 1.0 / use_seconds);
    load_pos += PUT_SIZE;

    // test get
    LOG_INFO("START GET");
    vector<KEY_TYPE> rand_pos;
    util::FastRandom ranny(18);

    for (size_t i = 0; i < GET_SIZE; i++)
    {
        rand_pos.push_back(ranny.RandUint32(0, load_pos - 1));
    }

    int wrong_get = 0;
    VALUE_TYPE value = 0;
    for (size_t i = 0; i < GET_SIZE; i++)
    {
        db->search(data_base[rand_pos[i]], value);
        if (value != data_base[rand_pos[i]])
        {
            wrong_get++;
        }
    }
    LOG_INFO("get %lu kvs, with %d wrong value.", GET_SIZE, wrong_get);

    delete db;
    return 0;
}