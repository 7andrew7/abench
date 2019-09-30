cmake_minimum_required(VERSION 3.11)

project(abench VERSION 1.0 LANGUAGES CXX)

project(ModernCMakeExample VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "-O0 -g -Wall -Werror ${CMAKE_CXX_FLAGS}")
find_package(libmongocxx REQUIRED)
find_package(gflags REQUIRED)

add_executable(abench "${PROJECT_SOURCE_DIR}/main.cc")

target_include_directories(abench PRIVATE
   "${PROJECT_SOURCE_DIR}"
   "/usr/local/include"
   ${LIBMONGOCXX_INCLUDE_DIRS}
)

find_library(LIBGFLAGS gflags)
target_link_libraries(abench gflags ${LIBMONGOCXX_LIBRARIES})

