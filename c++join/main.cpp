#include <iostream>
#include <random> // Add this line to include the <random> header
#include <algorithm>
#include <chrono>
#include "join.h"

#define DATA_SIZE 30000

int main()
{
    CHashJoin *join = new CHashJoin();
    std::vector<input_tuple_t> relR;
    std::vector<input_tuple_t> relS;

    std::vector<output_tuple_t> relRS;

    std::cout << "Loading tuples..." << std::endl;
    for (int i = 0; i < DATA_SIZE; i++)
    {
        relR.push_back(input_tuple_t());
        relR[i].key = i;
        relR[i].rid = i;
        relS.push_back(input_tuple_t());
        relS[i].key = i;
        relS[i].rid = DATA_SIZE + i;
    }

    // randomize relR
    std::mt19937 g(1000);
    std::shuffle(relR.begin(), relR.end(), g); // Use std::shuffle instead of std::random_shuffle

    std::cout << "Joining..." << std::endl;
    clock_t time_req;
    time_req = clock();

    join->Join(relR, relS, relRS);

    time_req = clock() - time_req;
    double time_taken = ((double)time_req) / CLOCKS_PER_SEC; // calculate the elapsed time
    std::cout << "Kernel time: " << time_taken << " s" << std::endl;
    double throughput = (double)2 * DATA_SIZE / time_taken;
    std::cout << "Throughput: " << (int)throughput << " tuples/s" << std::endl;

    std::cout << "Printing..." << std::endl;
    return 0;
}