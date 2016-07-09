#define USE_MGAS  0
#include "../../../mgas/examples/mm/mm.h"

#include <madm.h>
#include <madm-dtbb.h>
#include <signal.h>

#define PRE_LOCALIZE  0

static void mm_rec(mm_t mm)
{
    madm_ptr_t A = mm.A, B = mm.B, C = mm.C;
    size_t M = mm.M, N = mm.N, K = mm.K, ld = mm.ld;
    
    size_t elem_size = sizeof(elem_t);
    size_t stride = ld * elem_size;
    size_t countA[] = { M, K * elem_size };
    size_t countB[] = { K, N * elem_size };
    size_t countC[] = { M, N * elem_size };
    
    elem_t *a = NULL, *b = NULL, *c = NULL;
    if (M + N + K <= 3 * g_cache_size) {
        mm_debug_print(&mm);
#if !PRE_LOCALIZE
        a = (elem_t *)madm_localize_s(A, stride, countA, MADM_REUSE);
        b = (elem_t *)madm_localize_s(B, stride, countB, MADM_REUSE);
        c = (elem_t *)madm_localize_s(C, stride, countC,
                                      (madm_flag_t)(MADM_UPDATE | MADM_OWN));
#else
        a = (elem_t *)madm_localize_s(A, stride, countA, MADM_UPDATE);
        b = (elem_t *)madm_localize_s(B, stride, countB, MADM_UPDATE);
        // FIXME: type of MADM_UPDATE | MADM_OWN becomes int...
        c = (elem_t *)madm_localize_s(C, stride, countC,
                                      (madm_flag_t)(MADM_UPDATE | MADM_OWN));
#endif
        value_check(A, a, M, K, ld);
        value_check(B, b, K, N, ld);
        value_check_C(C, c, M, N, ld);
    }

    mm_t submm[2];
    if (!divide(&mm, submm)) {
        mm_kernel(a, b, c, M, N, K, ld);
        madm_commit_s(C, c, stride, countC);
    } else if (submm[0].K == K) {
        // divide with M or N
        madm::dtbb::parallel_invoke(
            [=] { mm_rec(submm[0]); },
            [=] { mm_rec(submm[1]); }
        );
    } else {
        // divide with K
        mm_rec(submm[0]);
        mm_rec(submm[1]);
    }
}

void real_main(int argc, char **argv)
{
    size_t i;

    madm::pid_t me = madm::get_pid();
    size_t n_procs = madm::get_n_procs();

    // parse arguments
    if (argc < 5) {
        if (me == 0)
            fprintf(stderr,
                    "Usage: %s N block_size leaf_size cache_size "
                    "n_pre_execs=0\n",
                    argv[0]);
        return;
    }
    size_t n = (size_t)atol(argv[1]);
    size_t block_size = (size_t)atol(argv[2]);
    size_t leaf_size = (size_t)atol(argv[3]);
    size_t cache_size = (size_t)atol(argv[4]);
    size_t n_preexecs = (argc >= 6) ? (size_t)atol(argv[5]) : 0;

    if (me == 0) {
        MM_DPUTS("n = %zd", n);
    }

    // initialize
    mm_t mm;
    initialize(&mm, n, block_size, leaf_size, cache_size);
    
    madm_unlocalize();
    madm::barrier();

#if PRE_LOCALIZE
    if (me == 0)
        printf("*** ENABLE PRE_LOCALIZE ***\n");

    // pre-caching
    madm_ptr_t A = mm.A, B = mm.B, C = mm.C;
    size_t M = mm.M, N = mm.N, K = mm.K, ld = mm.ld;
    
    size_t elem_size = sizeof(elem_t);
    size_t stride = ld * elem_size;
    size_t countA[] = { M, K * elem_size };
    size_t countB[] = { K, N * elem_size };
    size_t countC[] = { M, N * elem_size };

    madm_localize_s(A, stride, countA, MADM_UPDATE);
    madm_localize_s(B, stride, countB, MADM_UPDATE);
    madm_localize_s(C, stride, countC, MADM_UPDATE);

    madm::barrier();
#endif

    for (i = 0; i < n_preexecs; i++) {
        if (me == 0) {
            madm::thread<void> th(mm_rec, mm);
            th.join();
        }
        madm::barrier();
    }

    madm::barrier();

    // compute
    if (me == 0) {
        double t0 = madm::time();

        madm::thread<void> th(mm_rec, mm);
        th.join();

        double t1 = madm::time();

        printf("# %3s, %5s, %5s, %5s, %4s, %s, %s\n", 
               "N", "block", "cache", "leaf", "np", "time", "gflops/core");
        printf("%5zd, %5zu, %5zu, %5zu, %4zu, %.6f, %.6f\n", 
               n, g_block_size, g_cache_size, g_leaf_size, n_procs,
               t1 - t0,
               1e-9 * (double)(2 * n * n * n) / ((t1 - t0) * (double)n_procs));
    }
    
    madm::barrier();
    madm_unlocalize();
    
    if (me == 0)
        verify(&mm, n);

    finalize(&mm);
}

int main(int argc, char **argv)
{
    madm::start(real_main, argc, argv);
    return 0;
}


