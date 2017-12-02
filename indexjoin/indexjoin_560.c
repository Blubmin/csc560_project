#include "indexjoin_560.h"
#include "types.h"
#include "rdtsc.h" 
#include <sys/time.h>           /* gettimeofday */

double match = 0;

/** print out the execution time statistics of the join */
static void 
print_timing(uint64_t total, uint64_t build, uint64_t part,
            uint64_t numtuples, int64_t result,
            struct timeval * start, struct timeval * end)
{
    double diff_usec = (((*end).tv_sec*1000000L + (*end).tv_usec)
                        - ((*start).tv_sec*1000000L+(*start).tv_usec));
    double cyclestuple = total;
    cyclestuple /= numtuples;
    fprintf(stdout, "RUNTIME TOTAL, BUILD, PART (cycles): \n");
    fprintf(stderr, "%lu \t %lu \t %lu ", 
            total, build, part);
    fprintf(stdout, "\n");
    fprintf(stdout, "TOTAL-TIME-USECS, TOTAL-TUPLES, CYCLES-PER-TUPLE: \n");
    fprintf(stdout, "%.4lf \t %lu \t ", diff_usec, result);
    fflush(stdout);
    fprintf(stderr, "%.4lf ", cyclestuple);
    fflush(stderr);
    fprintf(stdout, "\n");

}

result_t* indexjoin_560(relation_t* relR, relation_t* relS) {
    result_t* joinresult = (result_t*)malloc(sizeof(result_t));
    relation_t* joint;

    struct timeval start, end;
    uint64_t timer1, timer2, timer3;

    int maximum_tuples = relR->num_tuples + relS->num_tuples;

    /********** PARTITION MM size block fit in the memory **************/

    gettimeofday(&start, NULL);
    startTimer(&timer1);
    startTimer(&timer2); 

    int64_t matches = 0;
    struct hashtable_t ht;
    int num_ht_buckets = (relR->num_tuples)*sizeof(tuple_t)/MEMORY_SIZE + 1;
    printf("number of bucket it needed is %d\n", num_ht_buckets);
    ht.buckets = (struct bucket_t*) calloc(num_ht_buckets, sizeof(struct bucket_t));
    int64_t tuple_limit = MEMORY_SIZE/sizeof(tuple_t);
    printf("max_tuple number in MM is %ld\n", tuple_limit);
    /*
    for(int i=0; i < relR->num_tuples; i++){       
        int32_t idx = (relR->tuples[i].key)*sizeof(tuple_t)/MEMORY_SIZE;
        int32_t offset = relR->tuples[i].key%(MEMORY_SIZE/sizeof(tuple_t));
        //printf("%d,  r key: %d, idx: %d, offset: %d\n", i, relR->tuples[i].key, idx, offset);
        (ht.buckets+idx)->tuples[offset] = relR->tuples[i];
    }*/

    int64_t min_index = relR->tuples[0].key;
    int64_t max_index = relR->tuples[relR->num_tuples-1].key;   

    for(int i=0; i < num_ht_buckets; i++){ 
        memcpy((ht.buckets+i)->tuples, relR->tuples+i*MEMORY_SIZE/sizeof(tuple_t), MEMORY_SIZE);
        printf("%d,  first key in this bucket: %d\n", i, (relR->tuples+i*MEMORY_SIZE/sizeof(tuple_t))[0].key);
        /*
        int32_t idx = (relR->tuples[i].key)*sizeof(tuple_t)/MEMORY_SIZE;
        int32_t offset = relR->tuples[i].key%(MEMORY_SIZE/sizeof(tuple_t));
        //printf("%d,  r key: %d, idx: %d, offset: %d\n", i, relR->tuples[i].key, idx, offset);
        (ht.buckets+idx)->tuples[offset] = relR->tuples[i];
        */
    }
    printf("got here\n");
    stopTimer(&timer2); /* for build */
    
    for (int i = 0; i < relS->num_tuples; i++)
    {
        if(relS->tuples[i].key>=min_index && relS->tuples[i].key<=max_index){
            int32_t idx = (relS->tuples[i].key)*sizeof(tuple_t)/MEMORY_SIZE;
            int32_t offset = relS->tuples[i].key%(MEMORY_SIZE/sizeof(tuple_t));
            //printf("%d,  s key: %d, idx: %d, offset: %d\n", i, relS->tuples[i].key, idx, offset);
            //printf("ht-value %d\n", ((ht.buckets+idx)->tuples[offset]).key);
            if(((ht.buckets+idx)->tuples[offset]).key == (relS->tuples[i]).key){
                //printf("find a match\n");
                matches++;
            }
        }
    }

    stopTimer(&timer1); /* over all */
    gettimeofday(&end, NULL);
    /* now print the timing results: */
    print_timing(timer1, timer2, timer3, relS->num_tuples, matches, &start, &end);

    printf("final match: %ld\n", matches);
    return joinresult;
}









