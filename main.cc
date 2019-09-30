#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>

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

//DEFINE_int64(operations, 0, "Database operations");
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

static void SummarizeStats(Statistics *summary_stats) {

    for (size_t fi=0; fi < LAST_STAT; ++fi) {
        for (int32_t ti=0; ti < FLAGS_num_threads; ++ti) {
            summary_stats->stats[fi] = summary_stats->stats[fi] + threads_stats[ti].stats[fi];
        }
    }
}

static std::string RandomString(size_t length) {
    auto randchar = []() -> char {
        const size_t max_index = sizeof(charset) - 1;
        return charset[rand() % max_index];
    };

    std::string ret(length, 0);
    std::generate_n(ret.begin(), length, randchar);
    return ret;
}

static bsoncxx::document::value RandomDocument(int64_t id) {
    document doc{};
    doc << "_id" << bsoncxx::types::b_int64{id};
    for (int i=0; i < FLAGS_num_fields; i++) {
        char key_num = '0' + i;
        std::string key = "key";
        key += key_num;
        doc << key << RandomString(FLAGS_field_length);
    }
    return doc << finalize;
}

static void InsertWorkload(int32_t index) {
    mongocxx::client conn{mongocxx::uri{FLAGS_uri}};
    mongocxx::collection collection = conn[FLAGS_db_name][FLAGS_coll_name];

    //    Statistics *stats = &statistics[index];
    const int64_t first = index * FLAGS_num_documents / FLAGS_num_threads;
    const int64_t last = first + FLAGS_num_documents / FLAGS_num_threads;

    for (int64_t _id = first; _id < last; ++_id) {
        bsoncxx::document::value x = RandomDocument(_id);
        collection.insert_one(x.view());
        INCREMENT_STAT(index, SUCCESSES);
    }
}

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

    if (FLAGS_reset) {
        ResetDatabase();
    }

    std::function<void(int32_t)> func = InsertWorkload;
    std::vector<std::thread> threads{};

    auto start = std::chrono::steady_clock::now();

    for (int32_t t=0; t < FLAGS_num_threads; ++t) {
        threads.push_back(std::thread{InsertWorkload, t});
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
