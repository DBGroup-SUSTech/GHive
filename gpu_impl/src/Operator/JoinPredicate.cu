#include <iostream>
#include "Operator/JoinPredicate.hpp"
#include "Operator/Operator.hpp"
#include "DataFlow/DataFlow.hpp"
//#include "Operator/hashjoin.cuh"
#include "unordered_map"
//#include "Util/crystal.cuh"
//#include <cub/cub.cuh>
//#include <cub/util_allocator.cuh>
//#include <cub/block/block_shuffle.cuh>

/*
 * This method will call GPU has join.
 * */
//int *hashJoin(int *tableR, int num_tuples_R, int *tableS, int num_tuples_S,
//              int *result_count, Profiler &profiler) {
//
//  int *tagR = (int *) malloc(num_tuples_R * sizeof(int));
//  int *tagS = (int *) malloc(num_tuples_S * sizeof(int));
//
//  for (int i = 0; i < num_tuples_R; i++) {
//    tagR[i] = i;
//  }
//
//  for (int i = 0; i < num_tuples_S; i++) {
//    tagS[i] = i;
//  }
//
//  int *join_res;
//  if (num_tuples_R < 128000001 & num_tuples_S < 128000001) {
//    join_res =
//        inGPU_Hashjoin(tableR, tagR, num_tuples_R, tableS, tagS, num_tuples_S,
//                       log_parts1, log_parts2, 5 + p_d, result_count, profiler);
//    return join_res;
//  } else {
//    std::cout << "Out-of-GPU Hash join is not supported currently.";
//    return nullptr;
//  }
//}

JoinPredicate::JoinPredicate() {
  this->join_condition = UNKNOWN_CONDITION;
  this->join_type = UNKNOWN_TYPE;
  this->left_join_col = 0xffffffff;
  this->right_join_col = 0xffffffff;
}

JoinPredicate::JoinPredicate(JoinCondition join_condition, JoinType join_type,
                             uint32_t left_join_col, uint32_t right_join_col) :
    join_condition(join_condition), join_type(join_type),
    left_join_col(left_join_col), right_join_col(right_join_col) {}


/*
 * Receives the pointer of the data from two tables.
 * The rows with join keys the same will be merged together.
 * The right table will be after the left table.
 * */
//
// // GPU hash join impl with crystal
// // ---------------------------------------------------------------------------------------------------
//
// /**
//  * @brief build_hash_table hash_table of R
//  * @tparam BT BLOCK_THREADS
//  * @tparam IPT ITEMS_PER_THREAD
//  * @param keyR [input]
//  * @param idxR [input]
//  * @param rowNumR [input]
//  * @param hashTableR [output]
//  * @param slotNum [input] todo: optimized
//  *
//  * @note TILE <---> BLOCK
//  * num_slots为项数
//  * hash_table大小设置为2倍
//  */
// /// todo input selection flags
// template<uint32_t BT, uint32_t IPT>
// __global__ void
// buildHashTable(int64_t *keyR, int64_t *idxR, int rowNumR, int64_t *hashTableR, int slotNum) {
//
//     using ull_t = unsigned long long int; // uint64_t cast to unsigned long long int
//
//     ull_t keyItems[IPT];
//     ull_t idxItems[IPT];
//     int selectionFlags[IPT];
//
//     static const uint32_t TILE_SIZE = BT * IPT;
//     uint32_t tileOffset = blockIdx.x * TILE_SIZE;
//     uint32_t tileNum = (rowNumR + TILE_SIZE - 1) / TILE_SIZE;
//     uint32_t itemNumPerTile = TILE_SIZE;
//     if (blockIdx.x == tileNum - 1) {
//         itemNumPerTile = rowNumR - tileOffset; // the last block
//     }
//
//     InitFlags<BT, IPT>(selectionFlags);
//     BlockLoad<ull_t, BT, IPT>(reinterpret_cast<ull_t *>(keyR + tileOffset), keyItems, itemNumPerTile);
//     BlockLoad<ull_t, BT, IPT>(reinterpret_cast<ull_t *>(idxR + tileOffset), idxItems, itemNumPerTile);
//
//     // init hash table
//     int64_t *blockItr = hashTableR + tileOffset * 3;
//     int64_t *threadItr = blockItr + threadIdx.x * 3 * IPT;
// #pragma unroll
//     for (int i = 0; i < IPT; ++i) {
//         if (threadIdx.x * IPT + i < itemNumPerTile) {
//             threadItr[3 * i] = 0;
//             threadItr[3 * i + 1] = 0;
//             threadItr[3 * i + 2] = slotNum;
//         }
//     }
//
//     __syncthreads();
//
//     BlockBuildSelectivePHT_3<ull_t, ull_t, BT, IPT>(keyItems, idxItems, selectionFlags,
//                                                     reinterpret_cast<ull_t *>(hashTableR), slotNum,
//                                                     (int) itemNumPerTile);
// }
//
// /**
//  * @brief For each row in S, use R's hashTable to probe and count matched entries.
//  * @tparam BT BLOCK_THREADS
//  * @tparam IPT ITEMS_PER_THREAD
//
//  * @param keyS [input]
//  * @param idxS [input]
//  * @param rowNumS [input]
//  *
//  * @param hashTableR [input]
//  * @param slotNum [input]
//  *
//  * @param prefixSumS [output] blockWide
//  // * @param flagsS [output] todo: can be optimized.
//  * @param counter [input & output] length of final result
//  * @param writeOffset [output] every block write from this. length = number of block
//  */
// template<uint32_t BT, uint32_t IPT>
// __global__ void
// probeAndCount(int64_t *keyS, int64_t *idxS, int rowNumS,
//               int64_t *hashTableR, int slotNum,
//               int *prefixSumS, int *counter, int *writeOffset) {
//
//     using ull_t = unsigned long long int; // cast uint64_t to unsigned long long int
//     using BlockScanT = cub::BlockScan<int, BT, cub::BLOCK_SCAN_RAKING>;
//
//     __shared__ struct TempStorage {
//         ull_t out[BT * IPT]; // not enough
//         // int outOffset;
//         typename BlockScanT::TempStorage scan;
//     } tempStorage;
//
//     ull_t keyItemS[IPT];
//     ull_t idxItemS[IPT];
//     int selectionFlagsS[IPT];
//     int matchCountS[IPT];
//     int matchCountPrefixSumS[IPT];
//     int selectionNum;
//
//     static const uint32_t TILE_SIZE = BT * IPT;
//     uint32_t tileOffset = blockIdx.x * TILE_SIZE;
//     uint32_t tileNum = (rowNumS + TILE_SIZE - 1) / TILE_SIZE;
//     uint32_t itemNumPerTile = TILE_SIZE;
//     if (blockIdx.x == tileNum - 1) {
//         itemNumPerTile = rowNumS - tileOffset; // the last block
//     }
//
//     // printf("tileOffset = %d blockIdx = %d, itemNumPerTile = %d TILE_SIZE = %d tileNum = %d rowNumS = %d\n", tileOffset,
//     //        blockIdx.x,
//     //        itemNumPerTile, TILE_SIZE, tileNum, rowNumS);
//
//     BlockLoad<ull_t, BT, IPT>(reinterpret_cast<ull_t *>(keyS + tileOffset), keyItemS, itemNumPerTile);
//     BlockLoad<ull_t, BT, IPT>(reinterpret_cast<ull_t *>(idxS + tileOffset), idxItemS, itemNumPerTile);
//
//     InitCounts<BT, IPT>(matchCountS);
//     BlockProbeAndDo_3<ull_t, ull_t, BT, IPT>(
//             keyItemS, reinterpret_cast<ull_t *>(hashTableR), slotNum, itemNumPerTile,
//             [&]__device__(int i, ull_t k, ull_t v) { matchCountS[i]++; }
//     );
//
//     InitFlags<BT, IPT>(selectionFlagsS);
// #pragma unroll
//     for (int i = 0; i < IPT; ++i) {
//         selectionFlagsS[i] = !!matchCountS[i];
//     }
//     // calculate prefix sum per block =====================================================================
//
//     // int selectionFlagPrefixSum[IPT]; // block-wide prefix sum
//
//     BlockScanT(tempStorage.scan).ExclusiveSum(matchCountS, matchCountPrefixSumS, selectionNum);
//
//     // printf("**debug block scan: sum=%d \n", selectionNum);
//     // // for (int i = threadIdx.x * IPT; i < (threadIdx.x + 1) * IPT; ++i) {
//     // for (int i = 0; i < IPT; ++i) {
//     //     printf("blockID = %d selectionNum = %d matchCount = %d prefixSumS = %d key = %d\n",
//     //            blockIdx.x, selectionNum, matchCountS[i], matchCountPrefixSumS[i], keyItemS[i]);
//     // }
//     // printf("\n");
//
//     if (threadIdx.x == 0) {
//         writeOffset[blockIdx.x] = atomicAdd(counter, selectionNum);
//     }
//
//     BlockStore<int, BT, IPT>(prefixSumS + tileOffset, matchCountPrefixSumS, itemNumPerTile);
//     // BlockStore<int, BT, IPT>(flagsS + tileOffset, selectionFlagsS, itemNumPerTile);
//     //
//     // for (auto i = threadIdx.x; i < BT * IPT; i += blockDim.x) tempStorage.out[i] = 0;
//     // __syncthreads();
//     // BlockShuffle<ull_t, BT, IPT>(keyItemS, selectionFlagsS, selectionFlagPrefixSum, tempStorage.out);
//     // if (threadIdx.x == 0) {
//     //     memcpy(keyS + tempStorage.outOffset, tempStorage.out, sizeof(int64_t) * selectionNum);
//     // }
//     //
//     // for (auto i = threadIdx.x; i < BT * IPT; i += blockDim.x) tempStorage.out[i] = 0;
//     // __syncthreads();
//     // BlockShuffle<ull_t, BT, IPT>(idxItemS, selectionFlagsS, selectionFlagPrefixSum, tempStorage.out);
//     // if (threadIdx.x == 0) {
//     //     memcpy(idxS + tempStorage.outOffset, tempStorage.out, sizeof(int64_t) * selectionNum);
//     // }
//     //
//     // for (auto i = threadIdx.x; i < BT * IPT; i += blockDim.x) tempStorage.out[i] = 0;
//     // __syncthreads();
//     // BlockShuffle<ull_t, BT, IPT>(joinValueR, selectionFlagsS, selectionFlagPrefixSum, tempStorage.out);
//     // if (threadIdx.x == 0) {
//     //     memcpy(idxRJoinByS + tempStorage.outOffset, tempStorage.out, sizeof(int64_t) * selectionNum);
//     // }
//
//     // for (auto i = threadIdx.x; i < BT * IPT; i += blockDim.x) tempStorage.out[i] = 0;
//     // __syncthreads();
//     // BlockShuffle<int, BT, IPT>(selectionFlagsS, selectionFlagsS, selectionFlagPrefixSum, tempStorage.out);
//     // if (threadIdx.x == 0) {
//     //     memcpy(flagsS + tempStorage.outOffset, tempStorage.out, sizeof(int) * selectionNum);
//     // }
//     // --------------------------------------------------------------------------------------------------------
//
//     // for (int item = 0; item < IPT; ++item) {
//     //     if (threadIdx.x + BT * item < itemNumPerTile) {
//     //         // printf("item = %d, flag = %d: ", item, selectionFlagsS[item]);
//     //         if (selectionFlagsS[item]) {
//     //             printf("idxR: %4ld, key: %4ld, idxS: %4ld\n", joinValueR[item], keyItemS[item], idxItemS[item]);
//     //         }
//     //     }
//     // }
// }
//
// /**
//  * @brief
//  * @tparam BT BLOCK_THREADS
//  * @tparam IPT ITEMS_PER_THREAD
//  *
//  * @param keyS [input]
//  * @param idxS [input]
//  * @param rowNumS [input]
//  * @param prefixSumS [input]
//  * @param writeOffsetS [input] block wide
//  *
//  * @param hashTableR [input]
//  * @param slotNum [input]
//  *
//  * @param keyJoin [output]
//  * @param idxRJoin [output]
//  * @param idxSJoin [output]
//  */
// template<uint32_t BT, uint32_t IPT>
// __global__ void
// probeAndGenerate(int64_t *keyS, int64_t *idxS, int rowNumS, int *prefixSumS, int *writeOffsetS,
//                  int64_t *hashTableR, int slotNum,
//                  int64_t *keyJoin, int64_t *idxRJoin, int64_t *idxSJoin) {
//     using ull_t = unsigned long long int; // cast uint64_t to unsigned long long int
//
//     ull_t keyItemS[IPT];
//     ull_t idxItemS[IPT];
//     int prefixSumItemS[IPT]; // block wide
//     // int flagsItemS[IPT];
//
//     static const uint32_t TILE_SIZE = BT * IPT;
//     uint32_t tileOffset = blockIdx.x * TILE_SIZE;
//     uint32_t tileNum = (rowNumS + TILE_SIZE - 1) / TILE_SIZE;
//     uint32_t itemNumPerTile = TILE_SIZE;
//     if (blockIdx.x == tileNum - 1) {
//         itemNumPerTile = rowNumS - tileOffset; // the last block
//     }
//
//     BlockLoad<ull_t, BT, IPT>(reinterpret_cast<ull_t *>(keyS + tileOffset), keyItemS, itemNumPerTile);
//     BlockLoad<ull_t, BT, IPT>(reinterpret_cast<ull_t *>(idxS + tileOffset), idxItemS, itemNumPerTile);
//     BlockLoad<int, BT, IPT>(prefixSumS + tileOffset, prefixSumItemS, itemNumPerTile);
//     // BlockLoad<ull_t, BT, IPT>(reinterpret_cast<ull_t *>(flagsS + tileOffset), flagsItemS, itemNumPerTile);
//
//     int blockWriteOffset = writeOffsetS[blockIdx.x];
//
//     auto callback = [&]__device__(int i, ull_t k, ull_t v) {
//         int offset = blockWriteOffset + prefixSumItemS[i];
//
//         idxRJoin[offset] = (int64_t) v;
//         keyJoin[offset] = (int64_t) k; // keyItemS[i]
//         idxSJoin[offset] = (int64_t) idxItemS[i];
//         prefixSumItemS[i]++;
//
//         printf("Callback: hashtable[%d] = [%lld->%lld],\t\tgen row[%d=%d+%d]=[%lld %lld %lld]\n",
//                i, k, v,
//                offset, blockWriteOffset, prefixSumItemS[i] - 1,
//                idxRJoin[offset], keyJoin[offset], idxSJoin[offset]);
//     };
//
//     BlockProbeAndDo_3<ull_t, ull_t, BT, IPT>(
//             keyItemS, reinterpret_cast<ull_t *>(hashTableR), slotNum, itemNumPerTile,
//             callback
//     );
// }
//
//
// // --------------------------------------------------------------------------------------
// struct TableWithIndex {
//     int64_t *d_key;
//     std::vector<int64_t *> h_values;
//     uint32_t row_num;
//     cub::CachingDeviceAllocator &g_allocator;
//
//     TableWithIndex(int64_t *d_key_col, std::vector<int64_t *> &&h_val_cols, uint32_t row_num,
//                    cub::CachingDeviceAllocator &g_allocator)
//             : d_key(d_key_col),
//               h_values(h_val_cols),
//               row_num(row_num),
//               g_allocator(g_allocator) {}
//
//     TableWithIndex(DataFlow *df, uint32_t key_col, cub::CachingDeviceAllocator &g_allocator)
//             : g_allocator(g_allocator) {
//         // assert(join_type == IJ);
//         // assert(join_condition == EQ);
//
//         assert(key_col < df->longColNum + df->doubleColNum);
//
//         uint32_t key_index = df->sequence[key_col];
//         assert(key_index < df->longColNum);
//         //  Assert that 2 cols used for join are of the same data type -> long type
//
//         this->row_num = df->get_row_num();
//
//         int64_t *h_key = df->get_long_cols()[key_index];
//
//         this->d_key = nullptr;
//         CubDebugExit(this->g_allocator.DeviceAllocate((void **) &this->d_key, sizeof(int64_t) * row_num));
//         CubDebugExit(cudaMemcpy(this->d_key, h_key, sizeof(int64_t) * row_num, cudaMemcpyHostToDevice));
//
//         int64_t *h_idx;
//         h_idx = new int64_t[row_num];
//         init_index_sequence(h_idx, h_idx + row_num);
//
//         this->h_values.push_back(h_idx);
//         // printf("table constructed\n");
//         // print();
//     }
//
//     ~TableWithIndex() {
//         clear_memory();
//     }
//
//     void print() {
//         auto *h_key = new int64_t[row_num];
//         cudaMemcpy(h_key, d_key, sizeof(int64_t) * row_num, cudaMemcpyDeviceToHost);
//         auto col_num_join = h_values.size();
//         printf("With %lu cols, first col is key\n", col_num_join);
//
//         printf("+~~~~~~~~~+");
//         for (int c = 0; c < col_num_join; ++c) {
//             printf("---------+");
//         }
//         printf("\n");
//
//         for (int r = 0; r < row_num; ++r) {
//
//             printf("|");
//             printf(" %7ld |", h_key[r]);
//             for (int c = 0; c < col_num_join; ++c) {
//                 printf(" %7ld |", h_values[c][r]);
//             }
//             printf("\n");
//
//             printf("+~~~~~~~~~+");
//             for (int c = 0; c < col_num_join; ++c) {
//                 printf("---------+");
//             }
//             printf("\n");
//         }
//
//         delete[] h_key;
//     }
//
//     void clear_memory() {
//         // printf("Memory cleared:\n");
//         // print();
//
//         CubDebugExit(this->g_allocator.DeviceFree(d_key));
//         for (auto i: h_values) delete[] i;
//     }
//
// };
//
// TableWithIndex
// gpuHashJoinTwoTable(const TableWithIndex &R, const TableWithIndex &S, cub::CachingDeviceAllocator &gAllocator) {
//     int64_t *hIdxR, *hIdxS;
//     int64_t *dIdxR, *dIdxS;
//     hIdxR = new int64_t[R.row_num];
//     hIdxS = new int64_t[S.row_num];
//     init_index_sequence(hIdxR, hIdxR + R.row_num);
//     init_index_sequence(hIdxS, hIdxS + S.row_num);
//
//     CubDebugExit(gAllocator.DeviceAllocate((void **) &dIdxR, sizeof(int64_t) * R.row_num));
//     CubDebugExit(gAllocator.DeviceAllocate((void **) &dIdxS, sizeof(int64_t) * S.row_num));
//     CubDebugExit(cudaMemcpy(dIdxR, hIdxR, sizeof(int64_t) * R.row_num, cudaMemcpyHostToDevice));
//     CubDebugExit(cudaMemcpy(dIdxS, hIdxS, sizeof(int64_t) * S.row_num, cudaMemcpyHostToDevice));
//
//     int64_t *dHashTableR;       // 使用R建立的hash table
//     // int64_t *dIdxRJoinByS;      // 与Sjoin后的IdxR
//     // int *dSelectionFlagsR;   // R是build的时候输入用的。
//     int *dSelectionFlagsS;      // S是用S去probe之后输出用的。
//     int *dPrefixSum;            // 中间结果,每个threadBlock对应的prefixSum
//     int *dCounter;              // 中间用于同步的counter
//     int *dWriteOffset;          // 每个block写入最终结果的offset
//
//     CubDebugExit(gAllocator.DeviceAllocate((void **) &dHashTableR, sizeof(int64_t) * R.row_num * 3));
//     // CubDebugExit(g_allocator.DeviceAllocate((void **) &dIdxRJoinByS, sizeof(int64_t) * rowNumS));
//     // CubDebugExit(g_allocator.DeviceAllocate((void **) &dSelectionFlagsR, sizeof(int) * rowNumR));
//     CubDebugExit(gAllocator.DeviceAllocate((void **) &dSelectionFlagsS, sizeof(int) * S.row_num));
//     CubDebugExit(gAllocator.DeviceAllocate((void **) &dPrefixSum, sizeof(int) * S.row_num));
//     CubDebugExit(gAllocator.DeviceAllocate((void **) &dCounter, sizeof(int)));
//     CubDebugExit(gAllocator.DeviceAllocate((void **) &dWriteOffset, sizeof(int)));
//
//     cudaMemset(dHashTableR, 0, sizeof(int64_t) * R.row_num * 3);
//     // cudaMemset(dSelectionFlagsR, 1, sizeof(int) * rowNumR);
//     cudaMemset(dSelectionFlagsS, 1, sizeof(int) * S.row_num);
//     // cudaMemset(dIdxRJoinByS, 1, sizeof(int64_t) * rowNumR);
//     cudaMemset(dCounter, 0, sizeof(int));
//
//     static const uint32_t BLOCK_THREADS = 6;
//     static const uint32_t ITEMS_PER_THREAD = 1;
//     static const uint32_t BLOCKS_PER_GRID =
//             (R.row_num + BLOCK_THREADS * ITEMS_PER_THREAD - 1) / (BLOCK_THREADS * ITEMS_PER_THREAD);
//
//     int slotNum = (int) R.row_num;
//
//     // printf("buildHashTable<%u, %u><<<%u, %u>>>()\n", BLOCK_THREADS, ITEMS_PER_THREAD, BLOCKS_PER_GRID, BLOCK_THREADS);
//
//     buildHashTable<BLOCK_THREADS, ITEMS_PER_THREAD><<<BLOCKS_PER_GRID, BLOCK_THREADS>>>(
//             R.d_key, dIdxR, R.row_num, dHashTableR,
//             slotNum
//     );
//
//     cudaDeviceSynchronize();
//
//     // print hash table------------------------------------------------------------------------
//     auto *hHashTableR = new int64_t[R.row_num * 3];
//     cudaMemcpy(hHashTableR, dHashTableR, sizeof(int64_t) * R.row_num * 3, cudaMemcpyDeviceToHost);
//     printf("hash table R: ");
//     for (int i = 0; i < R.row_num; ++i) {
//         printf("[%ld -> %ld -> %ld] ", hHashTableR[i * 3], hHashTableR[i * 3 + 1], hHashTableR[i * 3 + 2]);
//     }
//     printf("\n");
//     delete[] hHashTableR;
//     //-----------------------------------------------------------------------------------------
//
//     probeAndCount<BLOCK_THREADS, ITEMS_PER_THREAD><<<BLOCKS_PER_GRID, BLOCK_THREADS>>>(
//             S.d_key, dIdxS, S.row_num, dHashTableR,
//             slotNum,
//             dPrefixSum, dCounter, dWriteOffset
//     );
//
//     cudaDeviceSynchronize();
//
//     int hCounter;
//     cudaMemcpy(&hCounter, dCounter, sizeof(int), cudaMemcpyDeviceToHost);
//
//     int64_t *dKeyJoin;
//     int64_t *dIdxRJoin;
//     int64_t *dIdxSJoin;
//     CubDebugExit(gAllocator.DeviceAllocate((void **) &dKeyJoin, sizeof(int64_t) * hCounter));
//     CubDebugExit(gAllocator.DeviceAllocate((void **) &dIdxRJoin, sizeof(int64_t) * hCounter));
//     CubDebugExit(gAllocator.DeviceAllocate((void **) &dIdxSJoin, sizeof(int64_t) * hCounter));
//
//     probeAndGenerate<BLOCK_THREADS, ITEMS_PER_THREAD><<<BLOCKS_PER_GRID, BLOCK_THREADS>>>(
//             S.d_key, dIdxS, S.row_num, dPrefixSum, dWriteOffset,
//             dHashTableR, slotNum,
//             dKeyJoin, dIdxRJoin, dIdxSJoin
//     );
//
//     cudaDeviceSynchronize();
//
//     auto *hIdxRJoin = new int64_t[hCounter];
//     auto *hIdxSJoin = new int64_t[hCounter];
//
//     cudaMemcpy(hIdxRJoin, dIdxRJoin, sizeof(int64_t) * hCounter, cudaMemcpyDeviceToHost);
//     cudaMemcpy(hIdxSJoin, dIdxSJoin, sizeof(int64_t) * hCounter, cudaMemcpyDeviceToHost);
//
//     // | R.val | S.val |
//     int rowNumJoin = hCounter;
//     std::vector<int64_t *> hValuesJoin;
//
//     for (auto col: R.h_values) {
//         auto *colJoin = new int64_t[rowNumJoin];
//         for (int i = 0; i < rowNumJoin; ++i) {
//             colJoin[i] = col[hIdxRJoin[i]];
//         }
//         hValuesJoin.push_back(colJoin);
//     }
//
//     for (auto col: S.h_values) {
//         auto *colJoin = new int64_t[rowNumJoin];
//         for (int i = 0; i < rowNumJoin; ++i) {
//             colJoin[i] = col[hIdxSJoin[i]];
//         }
//         hValuesJoin.push_back(colJoin);
//     }
//
//     delete[]hIdxRJoin;
//     delete[]hIdxSJoin;
//
//     // CubDebugExit(g_allocator.DeviceFree(dKeyJoin));
//     CubDebugExit(gAllocator.DeviceFree(dIdxRJoin));
//     CubDebugExit(gAllocator.DeviceFree(dIdxSJoin));
//
//     CubDebugExit(gAllocator.DeviceFree(dHashTableR));
//     // CubDebugExit(g_allocator.DeviceFree(dIdxRJoinByS));
//     // CubDebugExit(g_allocator.DeviceFree(dSelectionFlagsR));
//     CubDebugExit(gAllocator.DeviceFree(dSelectionFlagsS));
//     CubDebugExit(gAllocator.DeviceFree(dPrefixSum));
//     CubDebugExit(gAllocator.DeviceFree(dCounter));
//     CubDebugExit(gAllocator.DeviceFree(dWriteOffset));
//
//     CubDebugExit(gAllocator.DeviceFree(dIdxR));
//     CubDebugExit(gAllocator.DeviceFree(dIdxS));
//     delete[] hIdxR;
//     delete[] hIdxS;
//
//     return TableWithIndex{dKeyJoin, std::move(hValuesJoin), (uint32_t) rowNumJoin, gAllocator};
//
//
//     // delete[] hIdxRJoin;
//     // delete[] hIdxSJoin;
//
//     // // print final table: --------------------------------------------------------------
//     // auto *hKeyJoin = new int64_t[hCounter];
//     // cudaMemcpy(hKeyJoin, dKeyJoin, sizeof(int64_t) * hCounter, cudaMemcpyDeviceToHost);
//     // // printf("Table R: \n");
//     // // printf("+---------+---------+\n");
//     // // printf("| idxR    | keyR    |\n");
//     // // printf("+---------+---------+\n");
//     // // for (int i = 0; i < R.row_num; ++i) {
//     // //     printf("| %7ld | %7ld |\n", hIdxR[i], hKeyR[i]);
//     // //     printf("+---------+---------+\n");
//     // // }
//     // //
//     // // printf("Table S: \n");
//     // // printf("+---------+---------+\n");
//     // // printf("| idxS    | keyS    |\n");
//     // // printf("+---------+---------+\n");
//     // // for (int i = 0; i < rowNumS; ++i) {
//     // //     printf("| %7ld | %7ld |\n", hIdxS[i], hKeyS[i]);
//     // //     printf("+---------+---------+\n");
//     // // }
//     // printf("Final table: \n");
//     // printf("+---------+---------+---------+\n");
//     // printf("| idxR    | keyR/S  | idxS    |\n");
//     // printf("+---------+---------+---------+\n");
//     // for (int i = 0; i < hCounter; ++i) {
//     //     printf("| %7ld | %7ld | %7ld |\n", hIdxRJoin[i], hKeyJoin[i], hIdxSJoin[i]);
//     //     printf("+---------+---------+---------+\n");
//     // }
//     // delete[] hKeyJoin;
//     // // ---------------------------------------------------------------------------------------
//
//
//
//     // print result ---------------------------------------------------------------------------
//     // auto *hPrefixSum = new int[rowNumS];
//     // auto *hSelectionFlagsS = new int[rowNumS];
//     // auto *hIdxRJoinByS = new int64_t[rowNumS];
//     // cudaMemcpy(hPrefixSum, dPrefixSum, sizeof(int) * rowNumS, cudaMemcpyDeviceToHost);
//     // cudaMemcpy(hSelectionFlagsS, dSelectionFlagsS, sizeof(int) * rowNumS, cudaMemcpyDeviceToHost);
//     // // cudaMemcpy(hIdxRJoinByS, dIdxRJoinByS, sizeof(int64_t) * rowNumS, cudaMemcpyDeviceToHost);
//     //
//     // cudaMemcpy(hKeyS, dKeyS, sizeof(int64_t) * rowNumS, cudaMemcpyDeviceToHost);
//     // cudaMemcpy(hIdxS, dIdxS, sizeof(int64_t) * rowNumS, cudaMemcpyDeviceToHost);
//     // cudaMemcpy(&rowNumS, dCounter, sizeof(int), cudaMemcpyDeviceToHost);
//     //
//     // printf("Final Table:\n%10s %10s %10s %10s %10s\n", "flag", "w_idx", "R.idx", "(R/S).key", "S.idx");
//     // for (int i = 0; i < rowNumS; ++i) {
//     //     printf("%10d %10d %10ld %10ld %10ld\n", hSelectionFlagsS[i], hPrefixSum[i], hIdxRJoinByS[i], hKeyS[i],
//     //            hIdxS[i]);
//     // }
//     //
//     // delete[] hPrefixSum;
//     // delete[] hIdxRJoinByS;
//     // ----------------------------------------------------------------------------------------
//
// }
//
// DataFlow *JoinPredicate::gpu_hash_join(DataFlow *left_ptr, DataFlow *right_ptr, Profiler &profiler) {
//
//     /**
//      * @TODO template support
//      * @TODO multi key support
//      * @DONE multi table support
//      * @note
//      *  1. build index table for every dataflow
//      *  2. gpuHashJoinTwoTable() join all dataflow
//      *  3. link with other cols
//      * @DONE multi-val hash table support
//      * @note
//      *  Linear probeAndCount.
//      *  Use CAS for each slot.
//      *  Partitioned and shared hash table.
//      *  unsolved problems:
//      *  1. how to probeAndCount with partition:
//      *     1. Probe once and create thread-wide matchCountS[IPT];
//      *     2. Count block-wide prefixSum[BT * IPT] and selectionNum.
//      *     3. Use selectionNum to allocate global memory to store result
//      *     3. Probe again and write to global result;
//      *
//      *  shuffle or not ?
//      *  allocate how many space ?
//      *  DONE [list]
//      *      1. 简化输入输出接口 [done]
//      *      2. 实现近乎完整的两表join [done]
//      *      3. 思考多表join的思路 [done]
//      *      4. 讨论空间分配问题 [done]
//      * TODO distinct 0 and NULL
//      */
//     cub::CachingDeviceAllocator gAllocator(true);
//
//     TableWithIndex R(left_ptr, left_join_col, gAllocator), S(right_ptr, right_join_col, gAllocator);
//     TableWithIndex(gpuHashJoinTwoTable(R, S, gAllocator)).print();
//
//     // joinRS.print();
//     /*
//     // print RS: ------------------------------------------------------------------------------
//     // auto *hKeyJoin = new int64_t[joinRS.row_num];
//     // cudaMemcpy(hKeyJoin, joinRS.d_key, sizeof(int64_t) * joinRS.row_num, cudaMemcpyDeviceToHost);
//     // auto colNumJoin = joinRS.h_values.size();
//     // printf("Final table: with %lu cols\n", colNumJoin);
//     // printf("+---------+---------+---------+\n");
//     // printf("| idxR    | keyR/S  | idxS    |\n");
//     // printf("+---------+---------+---------+\n");
//     // for (int i = 0; i < joinRS.row_num; ++i) {
//     //     printf("| %7ld | %7ld | %7ld |\n",
//     //            joinRS.h_values[colNumJoin - 2][i], hKeyJoin[i], joinRS.h_values[colNumJoin - 1][i]);
//     //     printf("+---------+---------+---------+\n");
//     // }
//     // delete[] hKeyJoin;
//     // ----------------------------------------------------------------------------------------------
//
//     // free result memory
//     // joinRS.clearMemory(g_allocator);
//     // R.clearMemory(g_allocator);
//     // S.clear_memory(g_allocator);
//     // free device memory
//     */
//     // todo build dataflow
//     return nullptr;
// }
//
// /**
//  * @brief gpu_hash_join for N tables and different join types
//  * @param tables  length=Ns
//  * @param key_cols length=N
//  * @param types   length=N - 1;
//  * @return
//  */
// DataFlow *
// JoinPredicate::gpu_hash_join(const std::vector<DataFlow *> &tables, const std::vector<std::vector<uint32_t>> &key_cols,
//                              const std::vector<JoinType> &types) {
//     /**
//      * todo: add join type support
//      * todo: add multi key support
//      */
//     cub::CachingDeviceAllocator gAllocator(true);
//
//     auto tableNum = tables.size();
//     std::vector<TableWithIndex *> indexTables;
//
//     for (int i = 0; i < tableNum; ++i) {
//         // printf("table %d. key = %d\n", i, key_cols[i][0]);
//         indexTables.push_back(new TableWithIndex(tables[i], key_cols[i][0], gAllocator));
//     }
//
//     printf("First table: ");
//     indexTables[0]->print();
//     printf("Join table: ");
//     indexTables[1]->print();
//
//     auto *lastJoinResult = new TableWithIndex(gpuHashJoinTwoTable(*indexTables[0], *indexTables[1], gAllocator));
//     lastJoinResult->print();
//
//     for (int i = 2; i < tableNum; ++i) {
//         printf("Join table: ");
//         indexTables[i]->print();
//
//         auto *joinResult = new TableWithIndex(gpuHashJoinTwoTable(*lastJoinResult, *indexTables[i], gAllocator));
//         delete lastJoinResult;
//         lastJoinResult = joinResult;
//
//         lastJoinResult->print();
//     }
//
//
//     DataFlow *dfJoin = nullptr;
//     // todo: build dataflow
//
//     delete lastJoinResult;
//     for (auto ptr: indexTables) {
//         delete ptr;
//     }
//
//     return dfJoin;
// }
