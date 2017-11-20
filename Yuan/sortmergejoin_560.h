#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

#include "../types.h"

#define MEMORY_SIZE 500

typedef struct pat_t pat_t;
typedef struct scheduler scheduler;

struct pat_t {
    relation_t* relations;
    int num_relations;
};

struct scheduler {
    int* currents;
    int* originals;
    int cursor;
    int count;
};

result_t* sortmergejoin_560(relation_t* relR, relation_t* relS);
pat_t* partition_phase(relation_t* rel);
pat_t* sorting_phase(pat_t* rel);
relation_t* merging_phase(pat_t* r, pat_t* s, int maximum_tuples);
relation_t* nextBlock(pat_t* pat, scheduler* sche);
scheduler* initScheduler(pat_t* pat);

void printRelation(relation_t* rel);
void printPat(pat_t* pat);
void printScheduler(scheduler* sche);
int countLeft(scheduler* sche);
tuple_t nextIndex(pat_t* pat, scheduler* sche);
int incrementCursor(int current, int total);
void shrinkBlock(int min, relation_t** block, relation_t** result);
int findMin(relation_t* block);
int checkBlockIsEmpty(relation_t* block);
relation_t* refillBlock(pat_t* pat, scheduler* sche, relation_t* block);
int checkresult(relation_t* block);
int checkCurrentNumber(relation_t* block);
void clearScheduler(relation_t** block, scheduler** sche, pat_t* pat);
