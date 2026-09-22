cmake_minimum_required(VERSION 3.20)
project(mk8_sava_cpp_compatibility LANGUAGES CXX)

find_package(azure-storage-blobs-cpp CONFIG REQUIRED)

add_executable(compatibility main.cpp)
target_compile_features(compatibility PRIVATE cxx_std_17)
target_compile_options(compatibility PRIVATE -Wall -Wextra -Wpedantic -Werror)
target_link_libraries(compatibility PRIVATE Azure::azure-storage-blobs)
