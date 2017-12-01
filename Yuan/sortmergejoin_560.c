#include "sortmergejoin_560.h"

int match = 0;
int shrinked = 0;

result_t* sortmergejoin_560(relation_t* relR, relation_t* relS) {
    result_t* joinresult = (result_t*)malloc(sizeof(result_t));
    relation_t* joint;
    int maximum_tuples = relR->num_tuples + relS->num_tuples;

    /********** PARTITION **************/
    pat_t* patR = partition_phase(relR);
    pat_t* patS = partition_phase(relS);

    /********** SORT *******************/
    patR = sorting_phase(patR);
    patS = sorting_phase(patS);

    /********** JOIN *******************/
    joint = merging_phase(patR, patS, maximum_tuples);
printf("total match is %d\n", match);
    return joinresult;
}

pat_t* partition_phase(relation_t* rel) {
    int memory_tuples = MEMORY_SIZE / sizeof(tuple_t);
    int partition_count = (rel->num_tuples + memory_tuples - 1) / memory_tuples;
    pat_t* result = (pat_t*)malloc(sizeof(pat_t));
    int left = rel->num_tuples;    
    int ndx = 0, i = 0;

    result->num_relations = partition_count;
    result->relations = (relation_t*)malloc(partition_count * sizeof(relation_t));

    for (ndx = 0; ndx < partition_count; ndx++) {
        result->relations[ndx].tuples = (tuple_t*)malloc(memory_tuples * sizeof(tuple_t));
        if (left > memory_tuples) {
            for (i = 0; i < memory_tuples; i++) {
                memcpy(&result->relations[ndx].tuples[i], 
                       &rel->tuples[ndx*memory_tuples + i], 
                       sizeof(tuple_t));
            } 
            result->relations[ndx].num_tuples = memory_tuples;
            left -= memory_tuples;
        } else {
            for (i = 0; i < left; i++) {
                memcpy(&result->relations[ndx].tuples[i], 
                       &rel->tuples[ndx*memory_tuples + i], 
                       sizeof(tuple_t));
            }
            result->relations[ndx].num_tuples = left; 
        }
    }

    return result;
}

/* Bubble Sort  */
pat_t* sorting_phase(pat_t* pat) {
    int ndx = 0;
    int i = 0, j = 0;
    tuple_t* temp = (tuple_t*)malloc(sizeof(tuple_t));

    for (ndx = 0; ndx < pat->num_relations; ndx++) {
        relation_t* rel = &pat->relations[ndx];
        for (i = 0; i < rel->num_tuples - 1; i++) {
            for (j = 0; j < rel->num_tuples - 1 - i; j++) {
                if (rel->tuples[j].payload >= rel->tuples[j+1].payload) {
                    memcpy(&temp, &rel->tuples[j], sizeof(tuple_t));
                    memcpy(&rel->tuples[j], &rel->tuples[j+1], sizeof(tuple_t));
                    memcpy(&rel->tuples[j+1], &temp, sizeof(tuple_t));
                }
            }
        }
    }

    return pat;
}

relation_t* merging_phase(pat_t* patR, pat_t* patS, int maximum_tuples) {
    scheduler* scheR = initScheduler(patR);
    scheduler* scheS = initScheduler(patS);
    scheduler* scheS_temp = initScheduler(patS);
 
    relation_t* result = (relation_t*)malloc(sizeof(relation_t));
    result->tuples = (tuple_t*)malloc(maximum_tuples * sizeof(tuple_t));
    result->num_tuples = 0;

    while (!schedulerIsEmpty(scheR)){
        pair* pairR = nextTuple(patR, scheR);
        pair* pairS = nextTuple(patS, scheS);
 
        if (pairS->relation_id == -1) {
            incrementScheduler(scheR, pairR);
            free(scheS);
            scheS = copyScheduler(scheS_temp);
        } else if (pairR->tuple.payload < pairS->tuple.payload) {
            incrementScheduler(scheR, pairR);
            free(scheS);
            scheS = copyScheduler(scheS_temp);
        } else if (pairR->tuple.payload > pairS->tuple.payload) {
            incrementScheduler(scheS, pairS);
            free(scheS_temp);
            scheS_temp = copyScheduler(scheS);
        } else {
            memcpy(&result->tuples[result->num_tuples], &pairR->tuple, sizeof(tuple_t));
            result->num_tuples += 1;
printf("%d %d: %d\n", pairR->tuple.key, pairS->tuple.key, pairR->tuple.payload);
            match += 1;

            incrementScheduler(scheS, pairS);
        }
    } 
        
    return result;
}



/****** HELPER FUNCTIONS **************/

scheduler* copyScheduler(scheduler* sche) {
    int ndx = 0;
    scheduler* result = (scheduler*)malloc(sizeof(scheduler));
    result->currents = (int*)malloc(sche->count * sizeof(int));
    result->originals = (int*)malloc(sche->count * sizeof(int));
    result->count = sche->count;
    for (ndx = 0; ndx < sche->count; ndx++) {
        result->currents[ndx] = sche->currents[ndx];
        result->originals[ndx] = sche->originals[ndx];
    }
    return result;
}

// 1 means empty
int schedulerIsEmpty(scheduler* sche) {
    int ndx = 0;
    for (ndx = 0; ndx < sche->count; ndx++) {
        if (sche->currents[ndx] < sche->originals[ndx]) {
            return 0;
        }
    }
    return 1;
}

scheduler* incrementScheduler(scheduler* sche, pair* p) {
    sche->currents[p->relation_id]++;
    return sche;
}

pair* nextTuple(pat_t* pat, scheduler* sche) {
    int ndx = 0, count = 0;
    pair* result = (pair*)malloc(sizeof(pair));
    result->tuple = pat->relations[0].tuples[sche->currents[0]];
    result->relation_id = 0;
    for (ndx = 0; ndx < sche->count; ndx++) {
        if (sche->currents[ndx] >= sche->originals[ndx]) {
            count++;
            continue;
        } else if ( result->tuple.payload > pat->relations[ndx].tuples[sche->currents[ndx]].payload) {
            result->tuple = pat->relations[ndx].tuples[sche->currents[ndx]];
            result->relation_id = ndx;
        }
    }
    if (count == sche->count) {
        //printf("count is %d, original is %d\n", count, sche->count);
        result->relation_id = -1;
    }
    return result;
}

scheduler* initScheduler(pat_t* pat) {
    int ndx = 0;
    scheduler* sche = (scheduler*)malloc(sizeof(scheduler));
    sche->currents = (int*)malloc(pat->num_relations * sizeof(int));
    sche->originals = (int*)malloc(pat->num_relations * sizeof(int));
    sche->count = pat->num_relations;
    for (ndx = 0; ndx < pat->num_relations; ndx++) {
        sche->currents[ndx] = 0;
        sche->originals[ndx] = pat->relations[ndx].num_tuples;
    }
    return sche;
}

void printRelation(relation_t* rel) {
    int ndx = 0;
    for (ndx = 0; ndx < rel->num_tuples; ndx++) {
        printf("tuple %d, value %d\n", rel->tuples[ndx].key, rel->tuples[ndx].payload);
    }
}

void printPat(pat_t* pat) {
    int ndx = 0;
    for (ndx = 0; ndx < pat->num_relations; ndx++) {
        printf("partition %d:\n", ndx);
        printRelation(&pat->relations[ndx]);
    }
}

void printScheduler(scheduler* sche) {
    int ndx = 0;
    for (ndx = 0; ndx < sche->count; ndx++) {
        printf("current is %d, original is %d\n", sche->currents[ndx], sche->originals[ndx]);
    }
}

void printPair(pair* pair) {
    printf("tuple value is %d, relation_id is %d\n", pair->tuple.payload, pair->relation_id);
}






