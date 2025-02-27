#include "tests.h"
#include <stdio.h>

int main(void)
{
    int testsuites = 3;
    int outcomes   = 0;

    printf("\n");
    outcomes += timeseries_test();
    printf("\n");
    outcomes += parser_test();
    printf("\n");
    outcomes += encoding_test();
    printf("\n");

    printf("\nTests summary: %d passed, %d failed\n", testsuites - outcomes,
           outcomes == 0 ? 0 : testsuites - (outcomes * -1));

    return outcomes == 0 ? 0 : -1;
}
