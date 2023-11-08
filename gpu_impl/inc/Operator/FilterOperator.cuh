#ifndef FILTER_CUH
#define FILTER_CUH

#include <cuda.h>
#include <curand.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#include <algorithm>
#include <iostream>

#include "Operator/FilterPredicate.hpp"
#include "Util/crystal.cuh"

//#define TILE_ITEMS 2
//#define BLOCK_THREADS_NUM 1024
//#define TILE_SIZE (BLOCK_THREADS * ITEMS_PER_THREAD)
//
//template <typename T, int BLOCK_THREADS, int ITEMS_PER_THREAD>
//__global__ void Range(T *data, size_t num_tuples, T lb, T ub, bool *flag);
//
//template <typename T, int BLOCK_THREADS, int ITEMS_PER_THREAD>
//__global__ void RangeEqual(T *data, size_t num_tuples, T lb, T ub, bool *flag);
//
//template <typename T, int BLOCK_THREADS, int ITEMS_PER_THREAD>
//__global__ void NotRange(T *data, size_t num_tuples, T lb, T ub, bool *flag);
//
//template <typename T, int BLOCK_THREADS, int ITEMS_PER_THREAD>
//__global__ void NotRangeEqual(T *data, size_t num_tuples, T lb, T ub,
//                              bool *flag);
//
//template <typename T, int BLOCK_THREADS, int ITEMS_PER_THREAD>
//__global__ void Equal(T *data, size_t num_tuples, T bound, bool *flag);
//
//template <typename T, int BLOCK_THREADS, int ITEMS_PER_THREAD>
//__global__ void NotEqual(T *data, size_t num_tuples, T bound, bool *flag);
//
//template <typename T, int BLOCK_THREADS, int ITEMS_PER_THREAD>
//__global__ void LargeEqual(T *data, size_t num_tuples, T bound, bool *flag);
//
//template <typename T, int BLOCK_THREADS, int ITEMS_PER_THREAD>
//__global__ void Large(T *data, size_t num_tuples, T bound, bool *flag);
//
//template <typename T, int BLOCK_THREADS, int ITEMS_PER_THREAD>
//__global__ void LessEqual(T *data, size_t num_tuples, T bound, bool *flag);
//
//template <typename T, int BLOCK_THREADS, int ITEMS_PER_THREAD>
//__global__ void Less(T *data, size_t num_tuples, T bound, bool *flag);
//
//template <typename T>
//bool *select(T *data, PredicateMode model, T *predicate, int predicate_num,
//             size_t num_tuples);
//
//extern bool *select(long *data, PredicateMode model, long *predicate, int predicate_num,
//             size_t num_tuples);
//
//template <typename T>
//bool *select(T *data, int model, T *predicate, int predicate_num,
//             size_t num_tuples);

#endif