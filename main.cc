#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <random>

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>

#include <glog/logging.h>
#include <gflags/gflags.h>

using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::open_document;

DEFINE_string(uri, "mongodb://localhost:27017/", "Database URI");
DEFINE_string(workload, "insert", "Workload");

DEFINE_string(db_name, "testdb", "Database name");
DEFINE_string(coll_name, "testcoll", "Collection name");

DEFINE_int64(num_operations, 65536, "Database operations");
DEFINE_int64(num_documents, 16384, "Number of database documents");
DEFINE_int32(num_fields, 10, "Number of fields");
DEFINE_int32(field_length, 100, "Value length in bytes");
DEFINE_int32(num_threads, 2, "Number of threads");

DEFINE_bool(reset, false, "Reset database prior to run");

static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:;";

static const int MAX_THREADS = 256;

enum Stats {SUCCESSES=0, FAILURES, LAST_STAT};

static struct Statistics {
    std::atomic_int64_t stats[LAST_STAT];
} __attribute__((__aligned__(64))) threads_stats[MAX_THREADS];

#define INCREMENT_STAT(INDEX, STAT) (threads_stats[INDEX].stats[STAT] = threads_stats[INDEX].stats[STAT] + 1)

/*
static std::atomic<bool> shutdown_requested{false};

static inline int64_t RandomDocumentIndex() {
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int64_t> distribution(0, FLAGS_num_documents - 1);
    return distribution(generator);
}
*/

static void SummarizeStats(Statistics *summary_stats) {

    for (size_t fi=0; fi < LAST_STAT; ++fi) {
        for (int32_t ti=0; ti < FLAGS_num_threads; ++ti) {
            summary_stats->stats[fi] = summary_stats->stats[fi] + threads_stats[ti].stats[fi];
        }
    }
}

static std::string CreateRandomString(size_t length) {
    auto randchar = []() -> char {
        const size_t max_index = sizeof(charset) - 1;
        return charset[rand() % max_index];
    };

    std::string ret(length, 0);
    std::generate_n(ret.begin(), length, randchar);
    return ret;
}

static bsoncxx::document::value CreateRandomDocument(int64_t id) {
    document doc{};
    doc << "_id" << bsoncxx::types::b_int64{id};
    for (int i=0; i < FLAGS_num_fields; i++) {
        char key_num = '0' + i;
        std::string key = "key";
        key += key_num;
        doc << key << CreateRandomString(FLAGS_field_length);
    }
    return doc << finalize;
}

static void InsertWorkload(mongocxx::pool *pool, int32_t index) {
    auto client = pool->acquire();
    mongocxx::collection collection = (*client)[FLAGS_db_name][FLAGS_coll_name];

    const int64_t first = index * FLAGS_num_documents / FLAGS_num_threads;
    const int64_t last = first + FLAGS_num_documents / FLAGS_num_threads;

    for (int64_t _id = first; _id < last; ++_id) {
        bsoncxx::document::value x = CreateRandomDocument(_id);
        collection.insert_one(x.view());
        INCREMENT_STAT(index, SUCCESSES);
    }
}

/*
static void FetchAllWorkload(int32_t index) {
    bsoncxx::types::b_int64 index = {j};
    coll.insert_one(bsoncxx::builder::basic::make_document(kvp("x", index)));

    mongocxx::collection collection = conn[FLAGS_db_name][FLAGS_coll_name];

    //    Statistics *stats = &statistics[index];
    const int64_t first = index * FLAGS_num_documents / FLAGS_num_threads;
    const int64_t last = first + FLAGS_num_documents / FLAGS_num_threads;

    doc << "_id" << bsoncxx::types::b_int64{id};
    for (int64_t _id = first; _id < last; ++_id) {
        bsoncxx::document::value x = CreateRandomDocument(_id);
        collection.insert_one(x.view());
        INCREMENT_STAT(index, SUCCESSES);
    }
}
*/

static void ResetDatabase() {
    mongocxx::client conn{mongocxx::uri{FLAGS_uri}};
    mongocxx::collection collection = conn[FLAGS_db_name][FLAGS_coll_name];
    collection.drop();
}

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    mongocxx::instance inst{};
    mongocxx::pool pool{mongocxx::uri{FLAGS_uri}};

    if (FLAGS_reset) {
        ResetDatabase();
    }

    std::function<void(mongocxx::pool *, int32_t)> func = InsertWorkload;
    std::vector<std::thread> threads{};

    auto start = std::chrono::steady_clock::now();

    for (int32_t t=0; t < FLAGS_num_threads; ++t) {
        threads.push_back(std::thread{InsertWorkload, &pool, t});
    }

    for (auto &t : threads) {
        t.join();
    }

    auto end = std::chrono::steady_clock::now();
    auto ms =  std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    Statistics stats{};
    SummarizeStats(&stats);
    double thousand = 1000;
    std::cout << "Operations: " << stats.stats[SUCCESSES] << " Elapsed time in milliseconds: "
              << ms << " ms " <<  (thousand * stats.stats[SUCCESSES] / ms) << " req/sec"
              << std::endl;

}
