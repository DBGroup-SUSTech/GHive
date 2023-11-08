//
// Created by Jiash on 2021/1/27.
//

#ifndef DEMOCLIONCUDA__AGGREGATE_CUH
#define DEMOCLIONCUDA__AGGREGATE_CUH

#include <cuda.h>
#include <curand.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <algorithm>
#include <cstdint>
#include <cub/util_allocator.cuh>
#include <iostream>

// Below data type modification for atomicMax refer to
// https://docs.nvidia.com/cuda/cuda-math-api/
__device__ static double atomicMax(double* address, double val);

__device__ static double atomicMin(double* address, double val);

__device__ static float atomicMax(float* address, float val);

__device__ static float atomicMin(float* address, float val);

/**
 * Convert sum to avg by division.
 * @tparam I The data type of sum.
 * @param data The sum array.
 * @param gpu_group_sizes The size of each group.
 * @param gpu_group_num Num of groups.
 */
template <typename I>
__global__ void sum2avg_(I* data, size_t* gpu_group_sizes,
                         size_t gpu_group_num);

template <typename O, typename I>
__global__ void aggregate_min_gpu_(O* gpu_output, I* gpu_input, size_t row_num,
                                   size_t* gpu_group_tag,
                                   size_t* gpu_group_offset);

template <typename O, typename I>
void aggregate_min_gpu(O* output, I* input, size_t row_num, size_t col_num,
                       size_t group_num, size_t* group_tag,
                       size_t* group_offset);

template <typename O, typename I>
__global__ void aggregate_max_gpu_(O* gpu_output, I* gpu_input, size_t row_num,
                                   size_t* gpu_group_tag,
                                   size_t* gpu_group_offset);

template <typename O, typename I>
void aggregate_max_gpu(O* output, I* input, size_t row_num, size_t col_num,
                       size_t group_num, size_t* group_tag,
                       size_t* group_offset);

template <typename O, typename I>
void aggregate_avg_gpu(O* output, I* input, size_t row_num, size_t col_num,
                       size_t group_num, size_t* group_sizes, size_t* group_tag,
                       size_t* group_offset);

/**
 * This is the wrapper of gpu implementation for aggregation sum.
 * TODO: col_num is not properly used.
 * @tparam O The output data type.
 * @tparam I The input data type.
 * @param output The output col.
 * @param input The input col.
 * @param row_num Number of rows.
 * @param col_num Number of cols.
 * @param group_num Number of groups.
 * @param group_tag Tags of groups.
 * @param group_offset Permutation.
 */
template <typename O, typename I>
void aggregate_sum_gpu(O* output, I* input, size_t row_num, size_t col_num,
                       size_t group_num, size_t* group_tag,
                       size_t* group_offset);

/**
 * Kernel function to compute aggregation sum.
 * @tparam O The output data type.
 * @tparam I The input data type.
 * @param output The output col.
 * @param input The input col.
 * @param row_num Number of rows.
 * @param gpu_group_tag Group tag in GPU.
 * @param gpu_group_offset Group offset in GPU.
 */
template <typename O, typename I>
__global__ void aggregate_sum_gpu_(O* output, I* input, size_t row_num,
                                   size_t* gpu_group_tag,
                                   size_t* gpu_group_offset);

//__global__ void aggregate_sum(int* data,
//    int num_tuples,
//    int* group_tag,
//    int* gpu_offset,
//    double* result)
//{
//  int idx = blockIdx.x * blockDim.x + threadIdx.x;
//
//  if (idx < num_tuples)
//  {
//    int tag = group_tag[idx];
//    atomicAdd(&result[tag], data[gpu_offset[idx]]);
//    //printf(" idx:%d    group tag: %d    data: %d    sum:%lf \n", idx, tag,
//    data[ gpu_offset[idx]], result[tag]);
//  }
//}

//__global__ void aggregate_sum_count(int* data,
//    int num_tuples,
//    int* group_tag,
//    int* gpu_offset,
//    double* result,
//    int* group_count)
//{
//  int idx = blockIdx.x * blockDim.x + threadIdx.x;
//
//  if (idx < num_tuples)
//  {
//    int tag = group_tag[idx];
//    atomicAdd(&result[tag], data[gpu_offset[idx]]);
//    atomicAdd(&group_count[tag], 1);
//    //printf(" idx:%d    group tag: %d    data: %d    AVG:%d    Count:%d\n",
//    idx, tag, data[ gpu_offset[idx]], result[tag], group_count[tag]);
//  }
//}

//__global__ void aggregate_avg(int gpu_num, double* result, int* group_count)
//{
//  int idx = blockIdx.x * blockDim.x + threadIdx.x;
//
//  if (idx < gpu_num)
//  {
//    result[idx] /= group_count[idx];
//    //printf(" idx:%d    group_count: %d   AVG:%d \n", idx, group_count[idx],
//    result[idx]);
//  }
//}

//__global__ void aggregate_max(int* data,
//    int num_tuples,
//    int* group_tag,
//    int* gpu_offset,
//    double* result)
//{
//  int idx = blockIdx.x * blockDim.x + threadIdx.x;
//
//  if (idx < num_tuples)
//  {
//    int tag = group_tag[idx];
//    atomicMax(&result[tag], (double)data[gpu_offset[idx]]);
//    //printf(" idx:%d    group tag: %d    data: %d    max:%ld \n", idx, tag,
//    data[ gpu_offset[idx]], result[tag]);
//  }
//}

//__global__ void aggregate_max_int(int* data,
//    int num_tuples,
//    int* group_tag,
//    int* gpu_offset,
//    int* result)
//{
//  int idx = blockIdx.x * blockDim.x + threadIdx.x;
//
//  if (idx < num_tuples)
//  {
//    int tag = group_tag[idx];
//    atomicMax(&result[tag], data[gpu_offset[idx]]);
//    //printf(" idx:%d    group tag: %d    data: %d    max:%d \n", idx, tag,
//    data[ gpu_offset[idx]], result[tag]);
//  }
//}

//__global__ void aggregate_min(int* data,
//    int num_tuples,
//    int* group_tag,
//    int* gpu_offset,
//    double* result)
//{
//  int idx = blockIdx.x * blockDim.x + threadIdx.x;
//
//  if (idx < num_tuples)
//  {
//    int tag = group_tag[idx];
//    atomicMin(&result[tag], (double)data[gpu_offset[idx]]);
//    //printf(" idx:%d    group tag: %d    data: %d    min:%ld \n", idx, tag,
//    data[ gpu_offset[idx]], result[tag]);
//  }
//}

//__global__ void aggregate_min_int(int* data,
//    int num_tuples,
//    int* group_tag,
//    int* gpu_offset,
//    int* result)
//{
//  int idx = blockIdx.x * blockDim.x + threadIdx.x;
//
//  if (idx < num_tuples)
//  {
//    int tag = group_tag[idx];
//    atomicMin(&result[tag], data[gpu_offset[idx]]);
//    //printf(" idx:%d    group tag: %d    data: %d    min:%d \n", idx, tag,
//    data[ gpu_offset[idx]], result[tag]);
//  }
//}

///**
// * Apply permutation to shuffle input data.
// * @tparam T The type of data. Typically uint64_t or double.
// * @param result Permutation result.
// * @param input Input for permutation.
// * @param permutation The permutation arr.
// * @param row_num Number of rows.
// */
// template<typename T>
//__global__ void permute_gpu_(T* result, T* input, size_t* permutation, size_t
// row_num);

/**
 * Kernel function to extend group tags.
 * @tparam T The type of input group tags. uint32_t or uint64_t.
 * @param start Pos to start extending.
 * @param groupTag The tag.
 * @param length The total length.
 */
template <typename T>
__global__ void extend_group_tag_gpu_(T* start, T groupTag, T length);

/**
 * Wrapper of kernel function to extend group tags.
 * @tparam T The type of input group tags. uint32_t or uint64_t.
 * @param output Extended group tag arr to output.
 * @param input Input non-output group tags.
 * @param row_num Number of rows.
 * @param group_num Number of groups.
 */
template <typename T>
void extend_group_tag_gpu(T* output, T* input, const size_t row_num,
                          const size_t group_num);

/**
 * Do aggregation after permutation.
 * @deprecated
 * @param function
 * @param data
 * @param num_columns
 * @param num_tuples
 * @param group_tag
 * @param group_offset
 * @param result
 */
// void aggregate_old(int function,
//    int* data,
//    int num_columns,
//    int num_tuples,
//    int* group_tag,
//    int* group_offset,
//    double* result);

/**
 * TODO: Use real template or type code to solve min/max init for different data
 * types. Do aggregation after permutation.
 * @tparam O Output input type.
 * @tparam I Input input type.
 * @param output Aggregation output.
 * @param input Aggregation input.
 * @param function Aggregation function.
 * @param row_num Number of rows.
 * @param col_num Number of columns.
 * @param group_num Number of groups.
 * @param group_sizes Size of each group in an array.
 * @param group_tag Extended group tags.
 * @param group_offset Permutation.
 */
template <typename O, typename I>
void aggregate(O* output, I* input, int function, size_t row_num,
               size_t col_num, size_t group_num, size_t* group_sizes,
               size_t* group_tag, size_t* group_offset, Profiler& profiler);

#endif  // DEMOCLIONCUDA__AGGREGATE_CUH
