#include <iostream>
#include <random> // Add this line to include the <random> header
#include <algorithm>
#include <chrono>
#include "join.h"

#define DATA_SIZE 10000

int main()
{
    CHashJoin *join = new CHashJoin();
    std::vector<input_tuple_t> relR;
    std::vector<input_tuple_t> relS;

    std::vector<output_tuple_t> relRS;

    std::cout << "Loading tuples..." << std::endl;
    for (int i = 0; i < DATA_SIZE; i++)
    {
        relR.push_back({i, i});
        relS.push_back({i, DATA_SIZE + i});
    }



    // randomize relR
    std::mt19937 g(1000);
    std::shuffle(relR.begin(), relR.end(), g); // Use std::shuffle instead of std::random_shuffle

    std::cout << "Joining..." << std::endl;
    auto kernel_start = std::chrono::high_resolution_clock::now();
    join->Join(relR, relS, relRS);
    auto kernel_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> kernel_time = kernel_end - kernel_start;
    std::cout << "Kernel time: " << kernel_time.count() << " s" << std::endl;
    double throughput = (double)2*DATA_SIZE / kernel_time.count();
    std::cout << "Throughput: " << throughput << " tuples/s" << std::endl;

    std::cout << "Printing..." << std::endl;
    // print out relRS
    for (size_t i = 0; i < relRS.size(); i++)
    {
        std::cout << relRS[i].rid1 << " " << relRS[i].rid2 << " " << relRS[i].key << std::endl;
    }
    return 0;
}