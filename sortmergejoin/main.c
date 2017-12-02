#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include "types.h"
#include "sortmergejoin_560.h"

relation_t* createRelation(int tuple_count);

void main() {
    srand(time(0));
    relation_t* relR = createRelation(1000000);
    relation_t* relS = createRelation(1000000);
printf("tables are ready.\n");
    
    result_t* result = (result_t*)malloc(sizeof(result_t));
    result = sortmergejoin_560(relR, relS);
}

relation_t* createRelation(int tuple_count) {
    relation_t* result = (relation_t*)malloc(sizeof(relation_t));
    result->tuples = (tuple_t*)malloc(tuple_count * sizeof(tuple_t));
    result->num_tuples = tuple_count;
    int ndx = 0;
 
    for (ndx = 0; ndx < tuple_count; ndx++) {
        result->tuples[ndx].payload = (value_t)rand();
        result->tuples[ndx].key = ndx;
    }

    return result;
}
