#include <gtest/gtest.h>
#include <thrust/transform.h>
#include <thrust/iterator/counting_iterator.h>
#include <thrust/device_vector.h>

using namespace  std;
class GPUTest : public ::testing::Test {

};


struct c2l {


    char *start;

    c2l(char *_start): start(_start) {}

    __host__ __device__
    long operator() (int i) {
        return *(long *)start;

    }
};

TEST_F(GPUTest, EndianTest) {

    char c[8] = {1, 1, 0, 0, 0, 0, 0, 0};
    char *dDataPtr;
    cudaMalloc((void **) &dDataPtr, sizeof(char) * 8);
    cudaMemcpy(dDataPtr, c, sizeof(char) * 8, cudaMemcpyHostToDevice);


    thrust::counting_iterator<int> iter(0);
    thrust::device_vector<long> res(1);
    thrust::transform(iter, iter + 1, res.begin(), c2l(dDataPtr));
    std::cout << res[0] << std::endl;


    char x[8] = {1};
    long *a = (long *)x;
    std::cout << *a << std::endl;





}
