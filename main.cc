#include <iostream>
#include <memory>

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

//DEFINE_int64(operations, 0, "Database operations");
DEFINE_int64(num_documents, 1024, "Number of database documents");
DEFINE_int32(num_fields, 10, "Number of fields");
DEFINE_int32(field_length, 100, "Value length in bytes");
DEFINE_int32(num_threads, 1, "Number of threads");

template<typename T>       // declaration only for TD;
class TD;

static std::string RandomString(size_t length) {
    auto randchar = []() -> char {
        static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:;";
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

class Workload {
  public:
    virtual ~Workload(){};
    virtual void Run() = 0;
};

class InsertWorkload : public Workload {
  public:
    InsertWorkload(int32_t index) {}
    virtual void Run() {
        mongocxx::instance inst{};
        mongocxx::client conn{mongocxx::uri{FLAGS_uri}};        
        mongocxx::collection collection = conn["testdb"]["testcollection"];

        const int64_t first = index_ * FLAGS_num_documents / FLAGS_num_threads;
        const int64_t last = first + FLAGS_num_documents / FLAGS_num_threads;

        for (int64_t _id = first; _id < last; ++_id) {
            bsoncxx::document::value x = RandomDocument(_id);
            collection.insert_one(x.view());
        }
        //        std::cout << "Insert ID: " << res->inserted_id().get_int64() << std::endl;
    }

  private:
    int32_t index_;
};
    
std::unique_ptr<Workload> CreateWorkload(int32_t index) {
    return std::unique_ptr<Workload>{new InsertWorkload{index}};
}

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    auto x = CreateWorkload(0);
    x->Run();
    //    LOG(INFO) << "Starting abench";
    
    //    auto cursor = collection.find({});

    //    for (const auto& doc : cursor) {
    //  std::cout << bsoncxx::to_json(doc) << std::endl;
    //    }
}
