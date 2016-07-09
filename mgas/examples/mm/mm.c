#define USE_MGAS  1
#include "mm.h"
#include <mgas.h>
#include <mgas_prof.h>
#include <mgas_debug.h>


static void mm_rec(const mm_t *mm, size_t key, size_t rest_depth,
                   size_t begin, size_t end)
{
    if (key < (begin >> rest_depth) || key > (end >> rest_depth))
        return;

    ptr_t A = mm->A, B = mm->B, C = mm->C;
    size_t M = mm->M, N = mm->N, K = mm->K, ld = mm->ld;
    
    size_t elem_size = sizeof(elem_t);
    size_t stride = ld * elem_size;
    size_t countA[] = { M, K * elem_size };
    size_t countB[] = { K, N * elem_size };
    size_t countC[] = { M, N * elem_size };

    elem_t *a = NULL, *b = NULL, *c = NULL;
    mgas_handle_t h = MGAS_HANDLE_INIT;

    if (rest_depth == 0 || M + N + K <= 3 * g_cache_size) {
        a = mgas_localize_s(A, stride, countA, MGAS_REUSE, &h);
        b = mgas_localize_s(B, stride, countB, MGAS_REUSE, &h);
        c = mgas_localize_s(C, stride, countC, MGAS_REUSE | MGAS_OWN, &h);

        value_check(A, a, M, K, ld);
        value_check(B, b, K, N, ld);
        value_check_C(C, c, M, N, ld);
    }

    if (rest_depth == 0) {
        mm_kernel(a, b, c, M, N, K, ld);
        mgas_commit_s(C, c, stride, countC);
    } else {
        mm_t submm[2];
        divide_force(mm, submm);
        if (K == submm[0].K) {
            // divide with M or N
            mm_rec(&submm[0], key * 2 + 0, rest_depth - 1, begin, end);
            mm_rec(&submm[1], key * 2 + 1, rest_depth - 1, begin, end);
        } else {
            // divide with K
            mm_rec(&submm[0], key, rest_depth, begin, end);
            mm_rec(&submm[1], key, rest_depth, begin, end);
        }
    }

    mgas_unlocalize(&h);
}

int main(int argc, char **argv)
{
    mgas_initialize(&argc, &argv);

    mgas_proc_t me = mgas_get_pid();
    size_t n_procs = mgas_get_n_procs();

    // parse arguments
    if (argc != 5) {
        if (me == 0)
            fprintf(stderr, "Usage: %s N block_size leaf_size cache_size\n",
                    argv[0]);
        mgas_barrier();
        return 1;
    }
    size_t n = (size_t)atol(argv[1]);
    size_t block_size = (size_t)atol(argv[2]);
    size_t leaf_size = (size_t)atol(argv[3]);
    size_t cache_size = (size_t)atol(argv[4]);

    if (me == 0) {
        mgas_conf_output(stdout);
        mm_conf_output(stdout);
        MM_DPUTS("n = %zd", n);
    }

    // initialize
    mm_t mm;
    initialize(&mm, n, block_size, leaf_size, cache_size);

    // divide computation into n_procs parts
    size_t max_depth = max_depth_of_C_tree(&mm, 0);
    size_t n_virtual_leaves = 0x1llu << max_depth;

    size_t per_proc = n_virtual_leaves / n_procs;
    size_t mod = n_virtual_leaves % n_procs;
    size_t begin = (me * per_proc + (me < mod ? me : mod));
    size_t end = ((me + 1) * per_proc + (me + 1 < mod ? me + 1 : mod)) - 1;

    MM_DPUTS("begin = %zu, end = %zu", begin, end);

    // do computation
    mgas_barrier();
    mgas_prof_start();
    double t0 = now();

    mm_rec(&mm, 0, max_depth, begin, end);

    mgas_barrier();
    mgas_prof_stop();
    double t1 = now();

    if (me == 0) {
        printf("# %3s, %3s, %4s, %s\n", "N", "blk", "np", "time");
        printf("%5zd, %3zu, %4zu, %.6f\n", n, g_block_size, n_procs, t1 - t0);
    }

    if (me == 0)
        verify(&mm, n);

    char filename[FILENAME_MAX];

    // output profiling results
    sprintf(filename, "mm.%03zu.prof", me);
    FILE *f = fopen(filename, "w");
    mgas_prof_output(f);
    fclose(f);
/*
    // output log
    sprintf(filename, "mm.%03zu.sslog", me);
    FILE *g = fopen(filename, "w");
    mgas_log_output(g);
    fclose(g);
*/
    // finalize
    finalize(&mm);
    mgas_finalize();
    return 0;
}
