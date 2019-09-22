#include <iostream>

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>

#include <glog/logging.h>
#include <gflags/gflags.h>

DEFINE_bool(big_menu, true, "Include 'advanced' options in the menu listing");

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    LOG(INFO) << "Starting abench";
    
    mongocxx::instance inst{};
    mongocxx::client conn{mongocxx::uri{}};
    bsoncxx::builder::stream::document document{};

    auto collection = conn["testdb"]["testcollection"];
    document << "hello" << "world";
    collection.insert_one(document.view());

    auto cursor = collection.find({});

    for (const auto& doc : cursor) {
        std::cout << bsoncxx::to_json(doc) << std::endl;
    }
}
