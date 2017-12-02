#include "sortmergejoin_560.h"

double match = 0;

result_t* sortmergejoin_560(relation_t* relR, relation_t* relS) {
    result_t* joinresult = (result_t*)malloc(sizeof(result_t));
    relation_t* joint;
    int maximum_tuples = relR->num_tuples + relS->num_tuples;
    
    struct timeval start, end;
    uint64_t timer1, timer2, timer3, timer4;

    /********** PARTITION **************/
    gettimeofday(&start, NULL);
    startTimer(&timer1);
    startTimer(&timer2);

    pat_t* patR = partition_phase(relR);
    pat_t* patS = partition_phase(relS);

    stopTimer(&timer2);

    /********** SORT *******************/
    startTimer(&timer3);

    patR = sorting_phase(patR);
    patS = sorting_phase(patS);
    
    stopTimer(&timer3);

    /********** JOIN *******************/
    startTimer(&timer4);

    joint = merging_phase(patR, patS, maximum_tuples);

    stopTimer(&timer4);
    stopTimer(&timer1);
    gettimeofday(&end, NULL);

    print_timing(timer1, timer2, timer3, timer4, relR->num_tuples+relS->num_tuples,
                 match, &start, &end);

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

/* Quick Sort  */
pat_t* sorting_phase(pat_t* pat) {
    int ndx = 0;
    int i = 0, j = 0;
    
    for (ndx = 0; ndx < pat->num_relations; ndx++) {
        relation_t* rel = &pat->relations[ndx];
        tuple_t* in = rel->tuples;
        qsort(in, rel->num_tuples, sizeof(tuple_t), compare_tuples);
    }

    return pat;
}

relation_t* merging_phase(pat_t* patR, pat_t* patS, int maximum_tuples) {
    scheduler* scheR = initScheduler(patR);
    scheduler* scheS = initScheduler(patS);
    scheduler* scheS_temp;

    relation_t* result = (relation_t*)malloc(sizeof(relation_t));
    result->tuples = (tuple_t*)malloc(maximum_tuples * sizeof(tuple_t));
    result->num_tuples = 0;

    while (!schedulerIsEmpty(scheR) && !schedulerIsEmpty(scheS)) {
        pair* pairR = nextTuple(patR, scheR);
        pair* pairS = nextTuple(patS, scheS);
        pair* pairS_temp;

        if (pairR->tuple.payload < pairS->tuple.payload) {
            scheR = incrementScheduler(scheR, pairR);      
        } else if (pairR->tuple.payload > pairS->tuple.payload) {
            scheS = incrementScheduler(scheS, pairS);
        } else {
            while (pairR->relation_id != -1 && pairR->tuple.payload == pairS->tuple.payload) {
                scheS_temp = copyScheduler(scheS);
                pairS_temp = nextTuple(patS, scheS_temp);
                while (pairS_temp->relation_id != -1 && pairR->tuple.payload == pairS_temp->tuple.payload) {
                   // printf("%d %d: %d\n", pairR->tuple.key, pairS_temp->tuple.key, pairR->tuple.payload);
                    match++;
                    scheS_temp = incrementScheduler(scheS_temp, pairS_temp);
                    pairS_temp = nextTuple(patS, scheS_temp);
                } 
                scheR = incrementScheduler(scheR, pairR);
                pairR = nextTuple(patR, scheR);
            } 
            scheS = copyScheduler(scheS_temp);
            pairS = nextTuple(patS, scheS);
        }
    }
      
    return result;
}



/****** HELPER FUNCTIONS **************/

static inline int compare_tuples(const void* a, const void* b) {
    return (((tuple_t*)a)->payload > ((tuple_t*)b)->payload);
}

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

int firstValidRelation(scheduler* sche) {
    int ndx = 0;
    for (ndx = 0; ndx < sche->count; ndx++) {
        if (sche->currents[ndx] < sche->originals[ndx]) {
            return ndx; 
        }
    }
    return -1;
}

// If there is no next_tuple, the returned pair's relation_id will be -1.
pair* nextTuple(pat_t* pat, scheduler* sche) {
    int ndx = 0;
    int first = firstValidRelation(sche);
    pair* result = (pair*)malloc(sizeof(pair));
    if (first == -1) {
        result->relation_id = -1;
        return result; 
    }
    result->tuple = pat->relations[first].tuples[sche->currents[first]];
    result->relation_id = first;
    for (ndx = first; ndx < sche->count; ndx++) { 
        if (result->tuple.payload > pat->relations[ndx].tuples[sche->currents[ndx]].payload) {
            result->tuple = pat->relations[ndx].tuples[sche->currents[ndx]];
            result->relation_id = ndx;
        }
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

void 
print_timing(uint64_t total, uint64_t partition, uint64_t sorting, uint64_t merging,
             uint64_t numtuples, int64_t result,
             struct timeval * start, struct timeval * end) {
    double diff_usec = (((*end).tv_sec*1000000L + (*end).tv_usec)
                        - ((*start).tv_sec*1000000L+(*start).tv_usec));
    double cyclestuple = total;
    cyclestuple /= numtuples;
    fprintf(stdout, "RUNTIME TOTAL,   PARTITION,       SORTING,        MERGING: \n");
    fprintf(stderr, "%lu \t %lu \t %lu \t %lu ", 
            total, partition, sorting, merging);
    fprintf(stdout, "\n");
    fprintf(stdout, "TOTAL-TIME-USECS, TOTAL-TUPLES, CYCLES-PER-TUPLE: \n");
    fprintf(stdout, "%.4lf \t    %lu \t ", diff_usec, result);
    fflush(stdout);
    fprintf(stderr, "%.4lf ", cyclestuple);
    fflush(stderr);
    fprintf(stdout, "\n");

}




