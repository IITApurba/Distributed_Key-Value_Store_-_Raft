cmake_minimum_required(VERSION 3.16)
project(keyper_cpp CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

find_package(Threads REQUIRED)

include_directories(${CMAKE_SOURCE_DIR}/src)

add_library(keyper_core
  src/raft/raft_node.cpp
)
target_link_libraries(keyper_core PUBLIC Threads::Threads)

add_executable(keyper_server src/server/main.cpp)
target_link_libraries(keyper_server PRIVATE keyper_core)

enable_testing()
add_subdirectory(tests)
