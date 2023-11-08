//
// Created by Jiash on 2021/1/27.
//

#ifndef DEMOCLIONCUDA__GROUPBY_CUH
#define DEMOCLIONCUDA__GROUPBY_CUH

#include <cuda.h>
#include <curand.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <thrust/device_vector.h>
#include <thrust/gather.h>
#include <thrust/generate.h>
#include <thrust/host_vector.h>
#include <thrust/random.h>
#include <thrust/sequence.h>
#include <thrust/sort.h>
#include <time.h>

#include <algorithm>
#include <iostream>

template <typename KeyVector, typename PermutationVector>
void update_permutation(KeyVector &keys, size_t offset_begin, size_t offset_end,
                        PermutationVector &permutation);

template <typename KeyVector, typename PermutationVector>
void apply_permutation(KeyVector &keys, size_t offset_begin, size_t offset_end,
                       PermutationVector &permutation);

thrust::device_vector<size_t> reduce_by_group(int64_t *encode_keys,
                                              size_t num_tuples);

/**
 * @tparam Type The type of data. Typically int64_t.
 * @param array The array of data.
 * @param key The output key of a row..
 * @param num_tuples The num of tuples.
 * @param num_columns The num of columns.
 */
template <typename Type>
__global__ void encode_key_gpu_(Type *array, int64_t *key, size_t num_tuples,
                                size_t num_columns);

/**
 * This is the wrapper of encode_key_gpu_.
 * @tparam T The type of data. Typically int64_t.
 * @param host_key The key in host.
 * @param array The array of data.
 * @param num_tuples The num of tuples.
 * @param num_cols The num of columns.
 */
template <typename T>
void encode_key_gpu(int64_t *host_key, T *array, size_t num_tuples,
                    size_t num_cols);

/**
 * TODO: Unit test.
 * Eliminate potential collision in bitmap. The collision is caused by
 * encode_key_gpu_.
 * @tparam T The type of data. Typically int64_t.
 * @param bitmap The bitmap which is no longer than num_tuples, used as both
 * input and output.
 * @param arr The array of data.
 * @param num_tuples The num of tuples.
 * @param num_cols The num of columns.
 */
template <typename T>
void eliminate_collision(size_t *&bitmap, T *arr, size_t num_tuples,
                         size_t num_cols);

/**
 * TODO: Unit test with collision.
 * TODO: DO NOT MIX size_t, uint32_t, int64_t and uint64_t.
 * @tparam Type The keys of group by. Typically int64_t.
 * @param data The array of data.
 * @param num_columns The number of columns.
 * @param num_tuples The number of tuples.
 * @param group_offset Output permutation.
 * @param bitmap Output array of group size.
 */
template <typename Type>
void group_by(Type *data, size_t num_columns, size_t num_tuples,
              size_t *group_offset, size_t *bitmap, Profiler& profiler);

#endif
