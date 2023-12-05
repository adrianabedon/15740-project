#include <iostream>
#include "join.h"

void load_tuples1(std::vector<input_tuple_t> &relR)
{
    input_tuple_t tuple;
    tuple.key = 1;
    tuple.rid = 1;
    relR.push_back(tuple);
    tuple.key = 2;
    tuple.rid = 2;
    relR.push_back(tuple);
    tuple.key = 3;
    tuple.rid = 3;
    relR.push_back(tuple);
    tuple.key = 4;
    tuple.rid = 4;
    relR.push_back(tuple);
    tuple.key = 5;
    tuple.rid = 5;
    relR.push_back(tuple);
    tuple.key = 6;
    tuple.rid = 6;
    relR.push_back(tuple);
    tuple.key = 7;
    tuple.rid = 7;
    relR.push_back(tuple);
    tuple.key = 8;
    tuple.rid = 8;
    relR.push_back(tuple);
    tuple.key = 9;
    tuple.rid = 9;
    relR.push_back(tuple);
    tuple.key = 10;
    tuple.rid = 10;
    relR.push_back(tuple);
    tuple.key = 11;
    tuple.rid = 11;
    relR.push_back(tuple);
    tuple.key = 12;
    tuple.rid = 12;
    relR.push_back(tuple);
    tuple.key = 13;
    tuple.rid = 13;
    relR.push_back(tuple);
    tuple.key = 14;
    tuple.rid = 14;
    relR.push_back(tuple);
    tuple.key = 15;
    tuple.rid = 15;
    relR.push_back(tuple);
    tuple.key = 16;
    tuple.rid = 16;
    relR.push_back(tuple);
    tuple.key = 17;
    tuple.rid = 17;
    relR.push_back(tuple);
    tuple.key = 18;
    tuple.rid = 18;
    relR.push_back(tuple);
    tuple.key = 19;
    tuple.rid = 19;
    relR.push_back(tuple);
    tuple.key = 20;
    tuple.rid = 20;
    relR.push_back(tuple);
    tuple.key = 21;
    tuple.rid = 21;
    relR.push_back(tuple);
    tuple.key = 22;
    tuple.rid = 22;
    relR.push_back(tuple);
}

void load_tuples2(std::vector<input_tuple_t> &relS)
{
    input_tuple_t tuple;
    tuple.key = 1;
    tuple.rid = 1;
    relS.push_back(tuple);
    tuple.key = 1;
    tuple.rid = 2;
    relS.push_back(tuple);
    tuple.key = 3;
    tuple.rid = 3;
    relS.push_back(tuple);
    tuple.key = 3;
    tuple.rid = 4;
    relS.push_back(tuple);
    tuple.key = 5;
    tuple.rid = 5;
    relS.push_back(tuple);
    tuple.key = 5;
    tuple.rid = 6;
    relS.push_back(tuple);
    tuple.key = 7;
    tuple.rid = 7;
    relS.push_back(tuple);
    tuple.key = 8;
    tuple.rid = 8;
    relS.push_back(tuple);
    tuple.key = 9;
    tuple.rid = 9;
    relS.push_back(tuple);
    tuple.key = 10;
    tuple.rid = 10;
    relS.push_back(tuple);
    tuple.key = 11;
    tuple.rid = 11;
    relS.push_back(tuple);
    tuple.key = 12;
    tuple.rid = 12;
    relS.push_back(tuple);
    tuple.key = 13;
    tuple.rid = 13;
    relS.push_back(tuple);
    tuple.key = 14;
    tuple.rid = 14;
    relS.push_back(tuple);
    tuple.key = 15;
    tuple.rid = 15;
    relS.push_back(tuple);
    tuple.key = 16;
    tuple.rid = 16;
    relS.push_back(tuple);
    tuple.key = 17;
    tuple.rid = 17;
    relS.push_back(tuple);
    tuple.key = 18;
    tuple.rid = 18;
    relS.push_back(tuple);
    tuple.key = 19;
    tuple.rid = 19;
    relS.push_back(tuple);
    tuple.key = 20;
    tuple.rid = 20;
    relS.push_back(tuple);
    tuple.key = 21;
    tuple.rid = 21;
    relS.push_back(tuple);
    tuple.key = 22;
    tuple.rid = 22;
    relS.push_back(tuple);
}

int main() {
    CHashJoin *join = new CHashJoin();
    std::vector<input_tuple_t> relR;
    std::vector<input_tuple_t> relS;
    std::vector<output_tuple_t> relRS;

    std::cout << "Loading tuples..." << std::endl;
    load_tuples1(relR);
    load_tuples2(relS);

    std::cout << "Joining..." << std::endl; 
    join->Join(relR, relS, relRS);

    std::cout << "Printing..." << std::endl;
    // print out relRS
    for (size_t i = 0; i < relRS.size(); i++)
    {
        std::cout << relRS[i].rid1 << " " << relRS[i].rid2 << " " << relRS[i].key << std::endl;
    }
    return 0;
}