#include "sortmergejoin_560.h"

int global = 0;
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
    printRelation(joint);
printf("global is %d\n", global);
    return joinresult;
}

pat_t* partition_phase(relation_t* rel) {
    int memory_tuples = MEMORY_SIZE / sizeof(tuple_t);
    int partition_count = rel->num_tuples / memory_tuples;
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
                       &rel->tuples[ndx*partition_count + i], 
                       sizeof(tuple_t));
            } 
            result->relations[ndx].num_tuples = memory_tuples;
            left -= memory_tuples;
        } else {
            for (i = 0; i < left; i++) {
                memcpy(&result->relations[ndx].tuples[i], 
                       &rel->tuples[ndx*partition_count + i], 
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

relation_t* sorting_relation(relation_t* block) {
    int i = 0, j = 0;
    tuple_t* temp = (tuple_t*)malloc(sizeof(tuple_t));
    for (i = 0; i < block->num_tuples - 1; i++) {
        for (j = 0; j < block->num_tuples - 1 - i; j++) {
            if (block->tuples[j].payload >= block->tuples[j+1].payload) {
                memcpy(&temp, &block->tuples[j], sizeof(tuple_t));
                memcpy(&block->tuples[j], &block->tuples[j+1], sizeof(tuple_t));
                memcpy(&block->tuples[j+1], &temp, sizeof(tuple_t));
            }
        }
    }
    return block;
}

relation_t* merging_phase(pat_t* patR, pat_t* patS, int maximum_tuples) {
    int memory_tuples = MEMORY_SIZE / sizeof(tuple_t) / 2;
    scheduler* scheR = initScheduler(patR);
    scheduler* scheS = initScheduler(patS);
    relation_t* blockR = NULL;
    relation_t* blockS = NULL;
    int i = 0, j = 0, trackR = 0, trackS = 0;
int temp = 0;

    relation_t* result = (relation_t*)malloc(sizeof(relation_t));
    result->tuples = (tuple_t*)malloc(maximum_tuples * sizeof(tuple_t));
    result->num_tuples = 0;

    while (countLeft(scheR) != 0 && countLeft(scheS) != 0) {
        if (blockR == NULL) {
            blockR = nextBlock(patR, scheR);
        } 
        if (blockS == NULL) {
            blockS = nextBlock(patS, scheS);
        }
        if (checkCurrentNumber(blockR) != blockR->num_tuples ||
            checkCurrentNumber(blockS) != blockS->num_tuples) {
            refillBlock(patR, scheR, blockR);
            refillBlock(patS, scheS, blockS);
        }
        if ((checkCurrentNumber(blockR) == blockR->num_tuples &&
             findMin(blockS) == -1) ||
            (checkCurrentNumber(blockS) == blockS->num_tuples &&
             findMin(blockR) == -1)) {
            break;
        }
        blockR = sorting_relation(blockR);
        blockS = sorting_relation(blockS);
        for (i = 0; i < blockR->num_tuples; i++) {
            if (blockR->tuples[i].payload == -1) {
                continue;
            }
            tuple_t tempR = blockR->tuples[i];
            for (j = 0; j < blockS->num_tuples; j++) {
                if (blockS->tuples[j].payload == -1) {
                    continue;
                }
                tuple_t tempS = blockS->tuples[j];
                if (tempR.payload == tempS.payload) {
                    memcpy(&result->tuples[result->num_tuples], &tempR, sizeof(tuple_t));
                    result->num_tuples += 1;
printf("result # %d, normal join\n", result->num_tuples);
temp++;
                    blockR->tuples[i].payload = -1;
                    blockS->tuples[j].payload = -1;
                    break;
                }
            }
        }
        int minR = findMin(blockR);
        shrinkBlock(minR, &blockS, &result);
        int minS = findMin(blockS);
        shrinkBlock(minS, &blockR, &result);

        if (checkBlockIsEmpty(blockR) == 0) {
            blockR = NULL;
        }
        if (checkBlockIsEmpty(blockS) == 0) {
            blockS = NULL;
        }
    }

    clearScheduler(&result, &scheR, patR);
    clearScheduler(&result, &scheS, patS);

printf("total normal is %d\n", temp);
printf("total shrinked is %d\n", shrinked);
printf("final tuples are %d\n", result->num_tuples);
if (blockR != NULL) {
    printf("blockR is not NULL, it still has %d tuples\n", checkCurrentNumber(blockR));
}
if (blockS != NULL) {
    printf("blockS is not NULL, it still has %d tuples\n", checkCurrentNumber(blockS));
}
printf("scheR left %d, scheS left %d\n", countLeft(scheR), countLeft(scheS));

    return result;
}

/****** HELPER FUNCTIONS **************/

void clearScheduler(relation_t** block, scheduler** sche, pat_t* pat) {
    int left = countLeft(*sche);
    int ndx = 0;

    for (ndx = 0; ndx < left; ndx++) {
        tuple_t temp = nextIndex(pat, *sche);
        memcpy(&(*block)->tuples[(*block)->num_tuples], &temp, sizeof(tuple_t));
        (*block)->num_tuples += 1;
    }
}

int checkresult(relation_t* block) {
    int result = 0, ndx = 0;
    for (ndx = 0; ndx < block->num_tuples; ndx++) {
        if (block->tuples[ndx].payload == -1) {
            result++;
        }
    }
    return result;
}

void shrinkBlock(int min, relation_t** block, relation_t** result) {
    int ndx = 0;
    for (ndx = 0; ndx < (*block)->num_tuples; ndx++) {
        tuple_t temp = (*block)->tuples[ndx];
        if (temp.payload != -1 && temp.payload < min) {
           memcpy(&((*result)->tuples[(*result)->num_tuples]), &temp, sizeof(tuple_t));
           (*result)->num_tuples += 1;
printf("result # %d\n", (*result)->num_tuples);
shrinked++;
           (*block)->tuples[ndx].payload = -1;
        }
    }
}

int findMin(relation_t* block) {
    int result = 1000000;
    int temp = 0, ndx = 0;
    for (ndx = 0; ndx < block->num_tuples; ndx++) {
        temp = block->tuples[ndx].payload;
        if (temp != -1 && temp < result) {
            result = temp;
        }
    }
    if (result == 1000000) {
        result = -1;
    }
printf("min is %d\n", result);
    return result;
}

/* 0 means empty */
int checkBlockIsEmpty(relation_t* block) {
    int ndx = 0;
    for (ndx = 0; ndx < block->num_tuples; ndx++) {
        if (block->tuples[ndx].payload != -1) {
            return 1;
        }
    }
printf("ISEMPTY!\n");
    return 0;
}

int checkCurrentNumber(relation_t* block) {
    int result = 0, ndx = 0;
    for (ndx = 0; ndx < block->num_tuples; ndx++) {
        if (block->tuples[ndx].payload != -1) {
            result++;
        }
    }
    return result;
}

relation_t* refillBlock(pat_t* pat, scheduler* sche, relation_t* block) {
    int left = countLeft(sche);
    int ndx = 0, number = checkCurrentNumber(block);
    for (ndx = 0; ndx < block->num_tuples; ndx++) {
        if (left == 0) {
            block->num_tuples = number;
            return block;
        }
        if (block->tuples[ndx].payload == -1) {
            number++;
            tuple_t temp = nextIndex(pat, sche);
            memcpy(&block->tuples[ndx], &temp, sizeof(tuple_t));
            left--;
        } 
    }
    block->num_tuples = number;
    return block;
}

relation_t* nextBlock(pat_t* pat, scheduler* sche) {
    int left = countLeft(sche);
    int memory_tuples = MEMORY_SIZE / sizeof(tuple_t) / 2;
    int ndx = 0;

    relation_t* result = (relation_t*)malloc(sizeof(relation_t));
    if (left >= memory_tuples) {
        result->tuples = (tuple_t*)malloc(memory_tuples * sizeof(tuple_t));
        result->num_tuples = memory_tuples;
    } else if (left > 0) {
        result->tuples = (tuple_t*)malloc(left * sizeof(tuple_t));
        result->num_tuples = left;
    } else {
printf("HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH\n");
        return NULL;
    }

    for (ndx = 0; ndx < result->num_tuples; ndx++) {
        tuple_t temp = nextIndex(pat, sche);
        memcpy(&result->tuples[ndx], &temp, sizeof(tuple_t));
    }

    return result;
}

tuple_t nextIndex(pat_t* pat, scheduler* sche) {

global++;
    int current_index = sche->currents[sche->cursor];
    tuple_t result = pat->relations[sche->cursor].tuples[current_index];
    
    sche->cursor = incrementCursor(sche->cursor, sche->count);
    sche->currents[sche->cursor]+=1;

    return result;
}

int incrementCursor(int current, int total) {
    if (current + 1 == total) {
        return 0;
    }
    return current+1;
}

int countLeft(scheduler* sche) {
    int result = 0;
    int ndx = 0;
    for (ndx = 0; ndx < sche->count; ndx++) {
        result += sche->originals[ndx] - sche->currents[ndx];
    }
    return result;
}

void printRelation(relation_t* rel) {
    int ndx = 0;
    for (ndx = 0; ndx < rel->num_tuples; ndx++) {
        printf("tuple %d, value %d\n", ndx, rel->tuples[ndx]);
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

scheduler* initScheduler(pat_t* pat) {
    int ndx = 0;
    scheduler* result = (scheduler*)malloc(sizeof(scheduler));
    result->currents = (int*)malloc(pat->num_relations * sizeof(int));
    result->originals = (int*)malloc(pat->num_relations * sizeof(int));
    result->cursor = 0;
    result->count = pat->num_relations;
    for (ndx = 0; ndx < pat->num_relations; ndx++) {
        result->currents[ndx] = 0;
        result->originals[ndx] = pat->relations[ndx].num_tuples;
    }
    return result;
}








