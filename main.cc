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
DEFINE_int64(num_documents, 1024, "Number of database documents");
DEFINE_int32(num_fields, 10, "Number of fields");
DEFINE_int32(field_length, 100, "Value length in bytes");
DEFINE_int32(num_threads, 2, "Number of threads");

DEFINE_bool(reset, false, "Reset database prior to run");

static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:;";

struct Statistics {
    int64_t successes;
    int64_t failures;
} __attribute__((__aligned__(64)));

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

    const int64_t first = index * FLAGS_num_documents / FLAGS_num_threads;
    const int64_t last = first + FLAGS_num_documents / FLAGS_num_threads;

    for (int64_t _id = first; _id < last; ++_id) {
        bsoncxx::document::value x = RandomDocument(_id);
        collection.insert_one(x.view());
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
    for (int32_t t=0; t < FLAGS_num_threads; ++t) {
        threads.push_back(std::thread{InsertWorkload, t});
    }

    for (auto &t : threads) {
        t.join();
    }
    //    LOG(INFO) << "Starting abench";
    
    //    auto cursor = collection.find({});

    //    for (const auto& doc : cursor) {
    //  std::cout << bsoncxx::to_json(doc) << std::endl;
    //    }
}
