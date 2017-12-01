#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

#include "types.h"

#define MEMORY_SIZE 512

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

result_t* sortmergejoin_560(relation_t* relR, relation_t* relS);
pat_t* partition_phase(relation_t* rel);
pat_t* sorting_phase(pat_t* rel);
relation_t* merging_phase(pat_t* r, pat_t* s, int maximum_tuples);

scheduler* initScheduler(pat_t* pat);
pair* nextTuple(pat_t* pat, scheduler* sche);
scheduler* incrementScheduler(scheduler* sche, pair* p);
scheduler* copyScheduler(scheduler* sche);

void printRelation(relation_t* rel);
void printPat(pat_t* pat);
void printScheduler(scheduler* sche);
void printPair(pair* pair);
