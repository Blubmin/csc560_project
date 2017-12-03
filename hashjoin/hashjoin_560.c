#include "hashjoin_560.h"
#include "prj_params.h" 
#include "rdtsc.h" 
#include <sys/time.h>           /* gettimeofday */
#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)

#ifndef NEXT_POW_2
#define NEXT_POW_2(V)                           \
    do {                                        \
        V--;                                    \
        V |= V >> 1;                            \
        V |= V >> 2;                            \
        V |= V >> 4;                            \
        V |= V >> 8;                            \
        V |= V >> 16;                           \
        V++;                                    \
    } while(0)
#endif

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
    fprintf(stderr, "%llu \t %llu \t %llu ", 
            total, build, part);
    fprintf(stdout, "\n");
    fprintf(stdout, "TOTAL-TIME-USECS, TOTAL-TUPLES, CYCLES-PER-TUPLE: \n");
    fprintf(stdout, "%.4lf \t %llu \t ", diff_usec, result);
    fflush(stdout);
    fprintf(stderr, "%.4lf ", cyclestuple);
    fflush(stderr);
    fprintf(stdout, "\n");

}

int match = 0;
int shrinked = 0;
int i;
int num_radix_bits;

int64_t hashjoin_560(relation_t* relR, relation_t* relS) {
    
    num_radix_bits = 1;
    uint64_t radix_size;
    while(num_radix_bits < NUM_RADIX_BITS){
        radix_size = 1<<num_radix_bits;
        if(radix_size>MEMORY_SIZE/2){
            break;
        }
        num_radix_bits++;
    }
    printf("current radix_num_bits is %d \n", num_radix_bits);

    int64_t result = 0;
    result_t* joinresult = (result_t*)malloc(sizeof(result_t));//prepare to materialize the result;
    relation_t* joint;
    int maximum_tuples = relR->num_tuples + relS->num_tuples;

    struct timeval start, end;
    uint64_t timer1, timer2, timer3;
    gettimeofday(&start, NULL);
    startTimer(&timer1);
    startTimer(&timer2); 
    startTimer(&timer3);

    //re-arrange data table, one partition cluster after another
    relation_t *outRelR, *outRelS;
    
    outRelR = (relation_t*) malloc(sizeof(relation_t));
    outRelS = (relation_t*) malloc(sizeof(relation_t));
    
    /* allocate temporary space for partitioning */
    /* TODO: padding problem */
    size_t sz = relR->num_tuples * sizeof(tuple_t) + RELATION_PADDING;
    outRelR->tuples     = (tuple_t*) malloc(sz);
    outRelR->num_tuples = relR->num_tuples;
    
    sz = relS->num_tuples * sizeof(tuple_t) + RELATION_PADDING;
    outRelS->tuples     = (tuple_t*) malloc(sz);
    outRelS->num_tuples = relS->num_tuples;

    uint32_t * R_count_per_cluster = (uint32_t*)calloc((1<<num_radix_bits), sizeof(uint32_t));
    uint32_t * S_count_per_cluster = (uint32_t*)calloc((1<<num_radix_bits), sizeof(uint32_t));

    /********** PARTITION by hashing **************/

    radix_cluster_nopadding(outRelR, relR, R_count_per_cluster, num_radix_bits);
    relR = outRelR;

    /* apply radix-clustering on relation S for pass-1 */
    radix_cluster_nopadding(outRelS, relS, S_count_per_cluster, num_radix_bits);
    relS = outRelS;
    stopTimer(&timer3);

    /********** HASH one pass *******************/


    /* compute number of tuples per cluster */
    for( i=0; i < relR->num_tuples; i++ ){
        //printf("Table R: tuple %d,  key is %d\n", i, relR->tuples[i].key);
        uint32_t idx = (relR->tuples[i].key) & ((1<<num_radix_bits)-1);
        R_count_per_cluster[idx] ++;
    }
    for( i=0; i < relS->num_tuples; i++ ){
        //printf("Table S: tuple %d,  key is %d\n", i, relS->tuples[i].key);
        uint32_t idx = (relS->tuples[i].key) & ((1<<num_radix_bits)-1);
        S_count_per_cluster[idx] ++;
    }


    /* build hashtable on inner */
    int r, s; /* start index of next clusters */
    r = s = 0;
    for( i=0; i < (1<<num_radix_bits); i++ ){
        relation_t tmpR, tmpS;

        if(R_count_per_cluster[i] > 0 && S_count_per_cluster[i] > 0){

            tmpR.num_tuples = R_count_per_cluster[i];
            tmpR.tuples = relR->tuples + r;
            r += R_count_per_cluster[i];

            tmpS.num_tuples = S_count_per_cluster[i];
            tmpS.tuples = relS->tuples + s;
            s += S_count_per_cluster[i];
            
            /********** JOIN *******************/
            result += bucket_chaining_join(&tmpR, &tmpS, NULL);
        }
        else {
            r += R_count_per_cluster[i];
            s += S_count_per_cluster[i];
        }
    }
    stopTimer(&timer2);/* build finished */
    stopTimer(&timer1);/* probe finished */
    gettimeofday(&end, NULL);
    /* now print the timing results: */
    print_timing(timer1, timer2, timer3, relS->num_tuples, result, &start, &end);
    /* clean-up temporary buffers 
    free(S_count_per_cluster);
    free(R_count_per_cluster);*/

    /* clean up temporary relations 
    free(outRelR->tuples);
    free(outRelS->tuples);
    free(outRelR);
    free(outRelS);*/

    return result;

   
}



int64_t 
bucket_chaining_join(const relation_t * const R, 
                     const relation_t * const S,
                     relation_t * const tmpR)
{
    int * next, * bucket;
    const uint32_t numR = R->num_tuples;
    uint32_t N = numR;
    int64_t matches = 0;

    NEXT_POW_2(N);
    
    /* N <<= 1; */
    const uint32_t MASK = (N-1) << (num_radix_bits);
    //printf("current numR is %d, N is %d, MASK is %x\n",numR, N, MASK);


    next   = (int*) malloc(sizeof(int) * numR);
    bucket = (int*) calloc(N, sizeof(int));

    const tuple_t * const Rtuples = R->tuples;
    for(uint32_t i=0; i < numR; ){
        uint32_t idx = HASH_BIT_MODULO(R->tuples[i].key, MASK, num_radix_bits);
        next[i]      = bucket[idx];
        bucket[idx]  = ++i;     /* we start pos's from 1 instead of 0 */

        /* Enable the following tO avoid the code elimination
           when running probe only for the time break-down experiment */
        /* matches += idx; */
    }

    const tuple_t * const Stuples = S->tuples;
    const uint32_t        numS    = S->num_tuples;

    /* Disable the following loop for no-probe for the break-down experiments */
    /* PROBE- LOOP */
    for(uint32_t i=0; i < numS; i++ ){

        uint32_t idx = HASH_BIT_MODULO(Stuples[i].key, MASK, num_radix_bits);

        for(int hit = bucket[idx]; hit > 0; hit = next[hit-1]){

            if(Stuples[i].key == Rtuples[hit-1].key){
                /* TODO: copy to the result buffer, we skip it */
                //printf("found a match: R key: %d,   S key: %d\n", Rtuples[hit-1].key, Stuples[i].key);
                matches ++;
            }
        }
    }
    /* PROBE-LOOP END  */
    
    /* clean up temp */
    free(bucket);
    free(next);

    return matches;
}


/** 
 * Radix clustering algorithm which does not put padding in between
 * clusters. This is used only by single threaded radix join implementation RJ.
 * 
 * @param outRel 
 * @param inRel 
 * @param hist 
 * @param R 
 * @param D 
 */
 void 
 radix_cluster_nopadding(relation_t * outRel, relation_t * inRel, uint32_t * tuples_per_cluster, int D)
 {
     tuple_t ** dst;
     tuple_t * input;
     uint32_t i;
     uint32_t offset;
     const uint32_t M = ((1 << D) - 1);
     const uint32_t fanOut = 1 << D;
     const uint32_t ntuples = inRel->num_tuples;
 
     dst     = (tuple_t**)malloc(sizeof(tuple_t*)*fanOut);
 
     input = inRel->tuples;
     /* count tuples per cluster */
     for( i=0; i < ntuples; i++ ){
         uint32_t idx = (uint32_t)(HASH_BIT_MODULO(input->key, M, 0));
         tuples_per_cluster[idx]++;
         input++;
     }
 
     offset = 0;
     /* determine the start and end of each cluster depending on the counts. */
     for ( i=0; i < fanOut; i++ ) {
         dst[i]      = outRel->tuples + offset;
         offset     += tuples_per_cluster[i];
     }
 
     input = inRel->tuples;
     /* copy tuples to their corresponding clusters at appropriate offsets */
     for( i=0; i < ntuples; i++ ){
         uint32_t idx   = (uint32_t)(HASH_BIT_MODULO(input->key, M, 0));
         *dst[idx] = *input;
         ++dst[idx];
         input++;
     }

     free(dst);
 }








