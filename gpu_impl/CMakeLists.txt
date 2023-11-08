cmake_minimum_required(VERSION 3.19)
set(CMAKE_CUDA_COMPILER /usr/local/cuda-11.2/bin/nvcc)
project(gpuimpl CUDA)
enable_testing()

set(CMAKE_CUDA_STANDARD 14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CUDA_FLAGS "--extended-lambda -rdc=true --default-stream per-thread --expt-relaxed-constexpr --compiler-options='-O3 -fopenmp -mavx2 -mbmi2' -DSM520 -Xptxas='-dlcm=cg' -lineinfo -Xcudafe -\# ")
set(LINK_DIRECTORIES ${PROJECT_SOURCE_DIR}/lib)

file(GLOB source "src/**/*.cu" "src/**/**/*.cu" "src/*.cu" "src/**/*.cpp" "src/*.cpp")

add_library(gpuimpl SHARED ${source} src/Operator/SortMergeJoinPredicate.cu inc/Operator/SortMergeJoinPredicate.hpp src/Operator/HashJoinPredicate.cu inc/Operator/HashJoinPredicate.hpp)

set(JAVA_HOME $ENV{JAVA_HOME})
set(CUDA_HOME $ENV{CUDA_HOME})

set_target_properties(
        gpuimpl
        PROPERTIES
        CUDA_SEPARABLE_COMPILATION ON
        CUDA_ARCHITECTURES 61)
target_include_directories(gpuimpl
        PUBLIC ${CUDA_HOME}/samples/common/inc
        PUBLIC ${CUDA_HOME}/include/cub
        PUBLIC ${CUDA_HOME}/include/cub/test
        PUBLIC ${JAVA_HOME}/include
        PUBLIC ${JAVA_HOME}/include/linux
        PUBLIC inc)

target_link_directories(gpuimpl
        PUBLIC lib)
target_link_libraries(gpuimpl
        PUBLIC curand
        PUBLIC gomp
        PUBLIC numa
        PUBLIC hdfs)

file(GLOB TEST_SOURCES test/*.cu)
foreach (TEST_SOURCE_FILE ${TEST_SOURCES})
    # I used a simple string replace, to cut off .cpp.
    STRING(REGEX REPLACE ".+/(.+)\\..*" "\\1" TEST_NAME ${TEST_SOURCE_FILE})
    add_executable(${TEST_NAME} ${TEST_SOURCE_FILE})
    target_include_directories(${TEST_NAME}
            PUBLIC ${CUDA_HOME}/samples/common/inc
            PUBLIC ${CUDA_HOME}/include/cub
            PUBLIC ${CUDA_HOME}/include/cub/test
            PUBLIC ${JAVA_HOME}/include
            PUBLIC ${JAVA_HOME}/include/linux
            PUBLIC inc)
    target_link_directories(${TEST_NAME}
            PUBLIC ${JAVA_HOME}/jre/lib/amd64/server)
    target_link_libraries(${TEST_NAME}
            PUBLIC gtest
            PUBLIC gtest_main
            PUBLIC gpuimpl
            PUBLIC jvm)
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME} --gtest_list_tests)
    set_target_properties(
            ${TEST_NAME}
            PROPERTIES
            CUDA_SEPARABLE_COMPILATION ON
            CUDA_ARCHITECTURES 61)
endforeach (TEST_SOURCE_FILE ${TEST_SOURCES})

