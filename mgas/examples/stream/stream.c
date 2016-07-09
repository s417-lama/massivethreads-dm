
#include <mgas.h>
#include <mgas_prof.h>
#include <stdio.h>
#include <stdlib.h>
#include <sysexits.h>


enum params {
    N_LEAF = 4096,
    N_LOCAL = 1 * 1024 * 1024,
};

typedef double elem_t;
typedef mgasptr_t ptr_t;

typedef struct {
    ptr_t A;
    ptr_t B;
    ptr_t C;
    size_t N;
} daxpy_t;

static void daxpy_rec(ptr_t A, ptr_t B, ptr_t C, size_t N);

static void daxpy_leaf(elem_t *A, elem_t *B, elem_t *C, size_t N)
{
    size_t i;
    for (i = 0; i < N; i++)
        C[i] += A[i] * B[i];
}

static void daxpy_rec(ptr_t A, ptr_t B, ptr_t C, size_t N)
{
    size_t size = sizeof(elem_t) * N;

    void *a, *b, *c;
    mgas_handle_t h = MGAS_HANDLE_INIT;
    if (N <= N_LOCAL) {
//        DPUTS("N = %zu", N);

        a = mgas_localize(A, size, MGAS_REUSE, &h);
        b = mgas_localize(B, size, MGAS_REUSE, &h);
        c = mgas_localize(C, size, MGAS_UPDATE | MGAS_OWN, &h);

//        DPUTS("N = %zu end", N);
    }

    if (N <= N_LEAF) {
        daxpy_leaf(a, b, c, N);

        mgas_commit(C, c, size);
    } else {
        size_t N_half = N / 2;
        daxpy_rec(A, B, C, N_half);
        daxpy_rec(A + sizeof(elem_t) * N_half, B, C, N - N_half);
    }
}

int main(int argc, char **argv)
{
    mgas_initialize(&argc, &argv);

    mgas_proc_t me = mgas_get_pid();
    size_t n_procs = mgas_get_n_procs();

    if (argc != 2) {
        if (me == 0)
            fprintf(stderr, "Usage: %s [size]\n", argv[0]);
        exit(EX_USAGE);
    }

    size_t N = (size_t)atoi(argv[1]);

    size_t size = sizeof(elem_t) * N;

    size_t mod_ = size % 4096;
    MGAS_ASSERT(mod_ == 0);
    
    size_t n_dims = 1;
    size_t block_size[1] = { 4096 };
    size_t n_blocks[1] = { size / block_size[0] };
    
    ptr_t A = mgas_all_dmalloc(size, n_dims, block_size, n_blocks);
    ptr_t B = mgas_all_dmalloc(size, n_dims, block_size, n_blocks);
    ptr_t C = mgas_all_dmalloc(size, n_dims, block_size, n_blocks);

    size_t subN = N / n_procs;
    size_t mod = N % n_procs;
    
    size_t idx = subN * me;

    if (me < mod) {
        subN += 1;
        idx += me;
    } else {
        idx += mod;
    }

    mgasptr_t subA = A + sizeof(elem_t) * idx;
    mgasptr_t subB = B + sizeof(elem_t) * idx;
    mgasptr_t subC = C + sizeof(elem_t) * idx;

#if 1
    mgas_set(subA, 2, sizeof(elem_t) * subN);
    mgas_set(subB, 4, sizeof(elem_t) * subN);
    mgas_set(subC, 6, sizeof(elem_t) * subN);
#endif
        
    mgas_barrier();
    mgas_prof_start();

    double t0 = now();
        
    daxpy_rec(subA, subB, subC, subN);

    double t1 = now();

    mgas_barrier();
    mgas_prof_stop();

    if (me == 0) {
        printf("N = %2zu, n_procs = %4zd, time = %.6f\n",
               N, n_procs, t1 - t0);
    }

    double t2 = now();
    mgas_all_free(A);
    mgas_all_free(B);
    mgas_all_free(C);
    double t3 = now();

    printf("free time: %f\n", t3 - t2);

    mgas_finalize();
    return 0;
}
