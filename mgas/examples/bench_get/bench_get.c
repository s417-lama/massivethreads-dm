
#include <mgas.h>
//#include <mgas_prof.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <math.h>
#include <sys/time.h>
#include <mpi.h>

typedef uint64_t tsc_t;

static inline tsc_t rdtsc(void)
{
#if (defined __i386__) || (defined __x86_64__)
    uint32_t hi,lo;
    asm volatile("lfence\nrdtsc" : "=a"(lo),"=d"(hi));
    return (tsc_t)((uint64_t)hi)<<32 | lo;
#elif (defined __sparc__) && (defined __arch64__)
    uint64_t tick;
    asm volatile("rd %%tick, %0" : "=r" (tick));
    return (tsc_t)tick;
#else
#warning "rdtsc() is not implemented."
        return 0;
#endif
}

static inline double now(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec*1e-6;
}

int main(int argc, char **argv)
{
    mgas_initialize(&argc, &argv);

    mgas_proc_t me = mgas_get_pid();
    size_t n_procs = mgas_get_n_procs();
 
    if (argc != 6) {
        fprintf(stderr, "%s block_size n_owners n_initiators [ rand | neighb | me ] n_trials\n", argv[0]);
        exit(1);
    }
    
    size_t block_size = (size_t)atoi(argv[1]);
    size_t n_owners = (size_t)atoi(argv[2]);
    size_t n_initiators = (size_t)atoi(argv[3]);
    
    assert(n_owners <= n_procs);
    assert(n_initiators <= n_procs);
    
    const char* access_method = argv[4];
    
    size_t n_trials = (size_t)atoi(argv[5]);
    
    int access_method_tag = 0;
    if (strcmp(access_method, "rand") == 0)        access_method_tag = 0;
    else if (strcmp(access_method, "neighb") == 0) access_method_tag = 1;
    else if (strcmp(access_method, "me") == 0)     access_method_tag = 2;
    
    mgasptr_t global_ptr = mgas_all_dmalloc(block_size * n_owners, 1, &block_size, &n_owners);
    void* buf = malloc(block_size);
    
    if (me < n_owners) {
        mgas_get(buf, global_ptr + me*block_size, block_size); // first touch
    }
    
    tsc_t* results = malloc(n_trials * sizeof(tsc_t));
    
    mgas_barrier();
    
    if (me < n_initiators) {
        size_t i;
        for (i = 0 ; i < n_trials; i++) {
            mgas_proc_t target = me;
            if (access_method_tag == 1)
                target = (me + 1) % n_owners;
            else if (access_method_tag == 2)
                target = me;
            else {
                do {
                    target = rand() % n_owners;
                } while (target == me && n_owners > 1);
            }
            
            tsc_t start_clock = rdtsc();
            mgas_get(buf, global_ptr + target*block_size, block_size);
            tsc_t end_clock = rdtsc();
            
            results[i] = end_clock - start_clock;
        }
    }
    
    mgas_barrier();
    
    double sum = 0.0;
    if (me < n_initiators) {
        size_t i;
        for (i = 0; i < n_trials; i++)
            sum += (double)results[i];
    }
    const double my_average = sum / (double)n_trials;
    
    double var = 0.0;
    if (me < n_initiators) {
        size_t i;
        for (i = 0; i < n_trials; i++) {
            double v = (double)results[i] - my_average;
            var += v * v;
        }
    }
    
    /*mgas_proc_t p;
    for (p = 0; p < n_initiators; p++) {
        if (p == me) {
            char filename[1024];
            sprintf(filename, "bench_get.out");
            
            FILE *fp = fopen(filename, p == 0 ? "w" : "a");
            fprintf(fp, "pid=%3u, clock=%lf\n", (unsigned int)me, sum / (double)n_trials);
            fclose(fp);
        }
        
        mgas_barrier();
    }*/
    
    mgas_barrier();
    
    double* sums = me == 0 ? (double*)malloc(n_procs * sizeof(double)) : NULL;
    MPI_Gather(&sum, 1, MPI_DOUBLE, sums, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    
    double* vars = me == 0 ? (double*)malloc(n_procs * sizeof(double)) : NULL;
    MPI_Gather(&var, 1, MPI_DOUBLE, vars, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    
    if (me == 0) {
        mgas_proc_t p;
        for (p = 0; p < n_initiators; p++)
            printf("pid=%3u, clock=%lf, deviation= %lf\n", (unsigned int)p, sums[p] / (double)n_trials, sqrt(vars[p] / (double)(n_trials - 1)));
    }
    
    mgas_all_free(global_ptr);

    mgas_finalize();
    return 0;
}
