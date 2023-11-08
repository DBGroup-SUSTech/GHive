#ifndef RSADECRYPT_H
#define RSADECRYPT_H

#include <cstdint>
#include <iostream>
#include <string>
#include <functional>
#include <utility>
#include <thrust/device_vector.h>

#include "DataFlow/Column.hpp"
#include "../../DataFlow/Column.hpp"

#define OPTIMIZE


struct rsa_decrypt_functor {


    struct gpu_operator {
        char exp[128] = {57,-102,8,56,10,80,39,91,-97,96,-35,-39,-16,-83,-123,36,94,45,13,55,-70,43,100,-111,-3,24,-116,-6,84,2,-24,-86,-18,-63,123,-40,78,78,114,18,80,54,6,-86,51,49,115,-44,-125,31,4,-66,53,-28,-14,2,18,-77,81,-32,63,60,66,83,-20,71,103,-23,-16,-50,8,-51,-18,-8,-126,-22,-122,95,75,-40,-14,14,90,60,91,106,-1,-16,-50,-66,-18,-8,25,-90,-25,41,91,-115,107,46,-43,72,86,115,-68,-40,55,79,-73,105,-84,51,-19,-88,6,124,-59,-54,65,-34,12,-25,72,88,-54,-91,-82,107};
        char mod[128] = {61,-58,51,-39,76,32,-62,10,39,83,30,103,-102,-60,37,-120,69,94,-62,68,97,-14,-25,-122,-56,-40,82,-117,120,-82,-128,-110,-120,5,-86,85,67,-47,18,-123,-103,84,51,26,4,66,123,31,-33,-59,-85,-7,-128,127,-121,-62,113,-32,79,7,120,-80,33,-27,-8,-27,-12,-18,-6,16,-28,-18,-80,-64,82,-13,97,8,-9,85,-115,79,10,59,25,117,104,-62,-4,109,98,-31,-43,-19,58,-79,2,122,-56,-50,-27,101,-81,46,118,-55,108,10,50,-88,-61,108,53,-42,-24,42,28,-36,77,90,-58,30,74,91,10,-96,10,-112};
        char *data;
        int *idx;
        gpu_operator(void *data, void *idx): data((char *)data), idx((int *)idx) { }

        __host__ __device__
        void big_num_mul(unsigned short w[], unsigned short u[],
                    unsigned short v[], int m, int n) {
            unsigned int k, t;
            int i, j;

            for (i = 0; i < m; i++)
                w[i] = 0;

            for (j = 0; j < n; j++) {
                k = 0;
                for (i = 0; i < m; i++) {
                    t = u[i] * v[j] + w[i + j] + k;
                    w[i + j] = t & 0xFFFF;          // (I.e., t & 0xFFFF).
                    k = t >> 16;
                }
                w[j + m] = k;
            }
        }


        __host__ __device__
        bool big_num_rsa1024(unsigned long res[], unsigned long data[], unsigned long expo[],unsigned long key[])
        {
            unsigned int i,j,expo_len;
            unsigned long mod_data[16]={0},result[16]={0};
            unsigned long temp_expo;

            big_num_mod(mod_data,data,key,16, 16);
            result[0] = 1;
//            expo_len = big_num_bit_length(expo,16) /64;
            expo_len = big_num_bit_size((unsigned *)expo, 32) / 64;
            for(i=0;i<expo_len+1;i++)
            {
                temp_expo = expo[i];
                for(j=0;j<64;j++)
                {
                    if(temp_expo & 0x1UL)
                        big_num_mod_multiply_1024(result,result,mod_data,key);

                    big_num_mod_multiply_1024(mod_data,mod_data,mod_data,key);
                    temp_expo = temp_expo >> 1;
                }
            }
            for(i=0;i<16;i++)
                res[i]=result[i];
            return 1;
        }



        __host__ __device__
        bool big_num_mod_multiply_1024(unsigned long res[], unsigned long op1[], unsigned long op2[],unsigned long mod[]) //optimized
        {
            int i;
            unsigned long result[32];
            big_num_mul((unsigned short *)result, (unsigned short *)op1, (unsigned short *)op2, 64, 64);

            unsigned long mod_data[32] = {0};
            big_num_mod(mod_data,result,mod,32, 16);
            //modbignum(result,result,xmod,33);
            for(i=0;i<16;i++)
                res[i]=mod_data[i];

            return 0;
        }


        __host__ __device__
        unsigned big_num_bit_size (unsigned op[], unsigned n) {
            for (unsigned i = n; i > 0; i --) {
                if (op[i - 1]) {
                    return i * 32 - nlz(op[i - 1]);
                }
            }
            return 0;
        }


        __host__ __device__
        unsigned int big_num_bit_length(unsigned long op[],unsigned int n)
        {

            unsigned int len=0;
            unsigned int i;
            unsigned long unit = 1;
            for( ;n>0;n--)
            {
                if(op[n-1]==0)
                    continue;
                for(i=64;i>0;i--)
                {
                    if(op[n-1] & (unit<<(i-1)))
                    {
                        len = (64*(n-1)) + i;
                        break;
                    }

                }
                if(len)
                    break;
            }
            return len;
        }

        __host__ __device__
        int nlz(unsigned x) {
            int n;

            if (x == 0) return(32);
            n = 0;
            if (x <= 0x0000FFFF) {n = n +16; x = x <<16;}
            if (x <= 0x00FFFFFF) {n = n + 8; x = x << 8;}
            if (x <= 0x0FFFFFFF) {n = n + 4; x = x << 4;}
            if (x <= 0x3FFFFFFF) {n = n + 2; x = x << 2;}
            if (x <= 0x7FFFFFFF) {n = n + 1;}
            return n;
        }

        __host__ __device__
        int big_num_div(unsigned q[], unsigned r[],
                   const unsigned u[], const unsigned v[],
                   const int m, const int n) {

            const unsigned long long b = 4294967296LL; // Number base (2**32).
            unsigned long long qhat;                   // Estimated quotient digit.
            unsigned long long rhat;                   // A remainder.
            unsigned long long p;                      // Product of two digits.
            long long t, k;
            int s, i, j;

            if (m < n || n <= 1 || v[n-1] == 0)
                return 1;                         // Return if invalid param.
            /* Normalize by shifting v left just enough so that its high-order
            bit is on, and shift u left the same amount. We may have to append a
            high-order digit on the dividend; we do that unconditionally. */

            s = nlz(v[n-1]);             // 0 <= s <= 31.
            unsigned vn[32] = {0};
            for (i = n - 1; i > 0; i--)
                vn[i] = (v[i] << s) | ((unsigned long long)v[i-1] >> (32-s));
            vn[0] = v[0] << s;

            unsigned un[64] = {0};
            un[m] = (unsigned long long)u[m-1] >> (32-s);
            for (i = m - 1; i > 0; i--)
                un[i] = (u[i] << s) | ((unsigned long long)u[i-1] >> (32-s));
            un[0] = u[0] << s;

            for (j = m - n; j >= 0; j--) {       // Main loop.
                // Compute estimate qhat of q[j].
                qhat = (un[j+n]*b + un[j+n-1])/vn[n-1];
#ifdef OPTIMIZE
                rhat = (un[j+n]*b + un[j+n-1])%vn[n-1];
#else // ORIGINAL
                rhat = (un[j+n]*b + un[j+n-1]) - qhat*vn[n-1];
#endif
                again:
                    if (qhat >= b ||
                        #ifdef OPTIMIZE
                        (unsigned)qhat*(unsigned long long)vn[n-2] > b*rhat + un[j+n-2]) {
                        #else // ORIGINAL
                        qhat*vn[n-2] > b*rhat + un[j+n-2]) {
                        #endif
                        qhat = qhat - 1;
                        rhat = rhat + vn[n-1];
                        if (rhat < b) goto again;
                    }

                // Multiply and subtract.
                k = 0;
                for (i = 0; i < n; i++) {
#ifdef OPTIMIZE
                    p = (unsigned)qhat*(unsigned long long)vn[i];
#else // ORIGINAL
                    p = qhat * vn[i];
#endif
                    t = un[i+j] - k - (p & 0xFFFFFFFFLL);
                    un[i+j] = t;
                    k = (p >> 32) - (t >> 32);
                }
                t = un[j+n] - k;
                un[j+n] = t;

                q[j] = qhat;              // Store quotient digit.
                if (t < 0) {              // If we subtracted too
                    q[j] = q[j] - 1;       // much, add back.
                    k = 0;
                    for (i = 0; i < n; i++) {
                        t = (unsigned long long)un[i+j] + vn[i] + k;
                        un[i+j] = t;
                        k = t >> 32;
                    }
                    un[j+n] = un[j+n] + k;
                }
            } // End j.
            // If the caller wants the remainder, unnormalize
            // it and pass it back.
//            if (r != NULL) {
                for (i = 0; i < n-1; i++)
                    r[i] = (un[i] >> s) | ((unsigned long long)un[i+1] << (32-s));
                r[n-1] = un[n-1] >> s;
//            }
            return 0;
        }


        // op1 / op2 -> p mod r, res[] is r
        __host__ __device__
        bool big_num_mod(unsigned long res[],unsigned long op1[], unsigned long op2[],unsigned int m, unsigned int n)//optimized
        {

            int op1_len, op2_len = 0;
            for (int i = 0; i < m; i ++) {
                op1_len = (op1[i] == 0)? op1_len: i + 1;
            }
            for (int i = 0; i < n; i ++) {
                op2_len = op2[i] == 0? op2_len: i + 1;
            }

            //printf("op1_len: %d\n", op1_len);
            //printf("op2_len: %d\n", op2_len);

            unsigned long q[16];

            unsigned long r[16];
            big_num_div((unsigned *)q, (unsigned *)r, (unsigned *)op1, (unsigned *)op2, op1_len * 2, op2_len * 2);
            for (int i = 0; i < 16; i ++) {
                res[i] = r[i];
            }
            return 1;
        }

        __host__ __device__
        int operator()(const int &offset) {
            int start = idx[2 * offset];
            int end = idx[2 * offset + 1];
            if (start == -1 || end == -1) {
                return -1;
            }
            void * pData = data + start;
            char buffer[128];
            int length = end - start;
            big_num_rsa1024((unsigned long *)(pData), (unsigned long *)(pData), (unsigned long *)exp, (unsigned long *)mod);

            for (int i = start; i < end; i++) {
                if (*(data + i) == 0) {
                    idx[2 * offset + 1] = i;
                    end = i;
                    length = i - start;
                    break;
                }
            }
            for (int i = 0; i < length; i ++) {
                // Transform the big-endian to little-endian schema.
                buffer[i] = data[end - i - 1];
            }
//            printf("length: %d", length);
            for (int i = 0; i < length; i ++) {
                data[start + i] = buffer[i];
            }
            return  0;
        }
    };

    // RSA decryption: string -> string
    Column* operator()(Column *col) {
        assert(col->type = STRING);

        // Allocate and copy the data from host to device.
        char *dDataPtr;
        int *dIdxPtr;
        cudaMalloc((void **) &dDataPtr, sizeof(char) * col->char_size);
        cudaMalloc((void **) &dIdxPtr, sizeof(int) * col->row_num * 2);
        CubDebugExit(cudaMemcpy(dDataPtr, col->data_ptr, sizeof(char) * col->char_size, cudaMemcpyHostToDevice));
        CubDebugExit(cudaMemcpy(dIdxPtr, col->data_ptr_aux, sizeof(int) * col->row_num * 2, cudaMemcpyHostToDevice));
        std::cout << "GHive-CPP [RSADecrypt-operator()]: done malloc and pcie h2d" << std::endl;


        thrust::counting_iterator<int> iter(0);
        // res stores the char size of the decrypted data.
        thrust::device_vector<int> res(col->row_num);
        thrust::transform(iter, iter + col->row_num, res.begin(),
                          gpu_operator(dDataPtr, dIdxPtr));
        std::cout << "GHive-CPP [RSADecrypt-operator()]: done processing" << std::endl;

        char *hDataPtr = new char[col->char_size];
        int *hIdxPtr = new int[col->row_num * 2];
        CubDebugExit(cudaMemcpy(hDataPtr, dDataPtr, sizeof(char) * col->char_size, cudaMemcpyDeviceToHost));
        CubDebugExit(cudaMemcpy(hIdxPtr, dIdxPtr, sizeof(int) * col->row_num * 2, cudaMemcpyDeviceToHost));

        std::cout << "GHive-CPP [RSADecrypt-operator()]: done pcie d2h" << std::endl;

        return new Column(STRING, col->row_num, hDataPtr, hIdxPtr, col->char_size);
    }
};

#endif