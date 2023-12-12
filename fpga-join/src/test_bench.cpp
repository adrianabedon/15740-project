#include <stdio.h>
#include "kml_join.hpp"
#include "check_result.hpp"

#define NUM_TRANS 1
#define DATA_SIZE NUM_TUPLES

int main()
{
    // Data storage
    input_tuple_t *a = (input_tuple_t *)calloc(DATA_SIZE, sizeof(input_tuple_t));
    input_tuple_t *b = (input_tuple_t *)calloc(DATA_SIZE, sizeof(input_tuple_t));
    output_tuple_t *c = (output_tuple_t *)calloc(DATA_SIZE, sizeof(output_tuple_t));
    int output_size;

    int retval = 0, i, i_trans, tmp;
    // Load input data from files
    for (i = 0; i < DATA_SIZE; i++)
    {
        a[i].key = i;
        a[i].rid = i;
        b[i].key = i;
        b[i].rid = DATA_SIZE + i;
    }

    // Execute the function multiple times (multiple transactions)
    for (i_trans = 0; i_trans < NUM_TRANS; i_trans++)
    {
        kml_join(a, b, c, &output_size, DATA_SIZE, DATA_SIZE);
    }

    retval = check_result(a, b, c, DATA_SIZE, DATA_SIZE, output_size);

    // Print Results
    if (retval == 0)
    {
        printf(" *** *** *** *** \n");
        printf(" Results are good \n");
        printf(" *** *** *** *** \n");
    }
    else
    {
        printf(" xxx xxx xxx xxx \n");
        printf(" Mismatch");
        printf(" xxx xxx xxx xxx \n");
    }

    // Return 0 if outputs are correct
    return retval;
}