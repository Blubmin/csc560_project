#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

#include "types.h"

#define MEMORY_SIZE 10000

typedef struct pat_t pat_t;
typedef struct scheduler scheduler;
typedef struct pair pair;

struct pat_t {
    relation_t* relations;
    int num_relations;
};

struct scheduler {
    int* currents;
    int* originals;
    int count;
};

struct pair {
    tuple_t tuple;
    int relation_id;
};

//void RJ(relation_t * relR);
int64_t 
bucket_chaining_join(const relation_t * const R, 
                     const relation_t * const S,
                     relation_t * const tmpR);

void radix_cluster_nopadding(relation_t * outRel, relation_t * inRel, uint32_t * tuples_per_cluster, int D);

int64_t hashjoin_560(relation_t* relR, relation_t* relS);



