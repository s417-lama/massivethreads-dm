#ifndef MM_H
#define MM_H

#include "config.h"

#define MM_TOUCH_CENTER      0
#define MM_TOUCH_1DBLOCK     1
#define MM_TOUCH_INTERLEAVE  2

#define MM_KERNEL_NAIVE 0
#define MM_KERNEL_BLAS  1
#define MM_KERNEL_CO    2


#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <time.h>

#if MM_KERNEL == MM_KERNEL_BLAS
#include <cblas.h>
#endif

#if MM_DEBUG
#define MM_DPUTS(fmt, ...)    DPUTS(fmt, ##__VA_ARGS__);
#else
#define MM_DPUTS(fmt, ...)
#endif

#if USE_MGAS
#include <mgas.h>
#include <mgas_prof.h>
#include <mgas_debug.h>

typedef mgasptr_t ptr_t;

#define REUSE  MGAS_RO
#define UPDATE MGAS_RWS

typedef mgas_proc_t proc_t;

#define GET_PID          mgas_get_pid
#define GET_N_PROCS      mgas_get_n_procs

#define DIST_MALLOC_ALL  mgas_all_dmalloc
#define DIST_FREE_ALL    mgas_all_free
#define BARRIER          mgas_barrier
#define GLOBAL_SET       mgas_set

#define LOCALIZE_PREPARE                        \
    mgas_handle_t h = MGAS_HANDLE_INIT;
#define LOCALIZE(mp, size, option)                      \
    mgas_localize(mp, size, option, &h)
#define LOCALIZE_S(mp, stride, count, option)           \
    mgas_localize_s(mp, stride, count, option, &h)
#define COMMIT_S(mp, p, stride, count)          \
    mgas_commit_s(mp, p, stride, count)
#define UNLOCALIZE \
    mgas_unlocalize(&h)

#define POLL             mgas_poll
#define ASSERT           MGAS_ASSERT

#else
#include <madm.h>
#include <madm_debug.h>
#include <madm_misc.h>
//#include <mgas_prof.h>

using namespace madi;

typedef madm_ptr_t ptr_t;

#define REUSE  MADM_REUSE
#define UPDATE MADM_UPDATE

typedef mgas_proc_t proc_t;

#define GET_PID          madm_get_pid
#define GET_N_PROCS      madm_get_n_procs

#define DIST_MALLOC_ALL  madm_all_dmalloc
#define DIST_FREE_ALL    madm_all_free
#define BARRIER          madm_barrier
#define GLOBAL_SET       madm_set

#define LOCALIZE_PREPARE  
#define LOCALIZE(mp, size, option)                      \
    madm_localize(mp, size, option)
#define LOCALIZE_S(mp, stride, count, option)           \
    madm_localize_s(mp, stride, count, option)
#define COMMIT_S(mp, p, stride, count)          \
    madm_commit_s(mp, p, stride, count)
#define UNLOCALIZE

#define POLL             madm_poll
#define ASSERT           MADI_ASSERT

#endif

typedef float elem_t;

#define EPSILON    FLT_EPSILON

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))


typedef struct {
    ptr_t A;
    ptr_t B;
    ptr_t C;
    size_t M;
    size_t N;
    size_t K;
    size_t ld;
} mm_t;

static size_t g_block_size = 0;
static size_t g_leaf_size = 0;
static size_t g_cache_size = 0;

static ptr_t sub(ptr_t A, size_t i, size_t j, size_t ld)
{
    return A + (uint64_t)(sizeof(elem_t) * (ld * i + j));
}

static elem_t get_value(ptr_t A)
{
    return (elem_t)(A % 128);
}

static void value_check(ptr_t A, elem_t *a, size_t M, size_t N, size_t ld)
{
#if MM_DEBUG
    size_t i, j;
    for (i = 0; i < M; i++)
        for (j = 0; j < N; j++) {
            elem_t value = get_value(A + sizeof(elem_t) * (ld * i + j));

            if (fabs(a[i * ld + j] - value) >= EPSILON) {
                DPUTSZ(i);
                DPUTSZ(j);
                DPUTS("a[%3zu, %3zu] (%p) = %f (correct: %f)",
                      i, j, &a[i*ld+j], a[i*ld+j], value);
                ASSERT(fabs(a[i * ld + j] - value) < EPSILON);
            }
        }
#endif
}

static void value_check_C(ptr_t C, elem_t *c, size_t M, size_t N, size_t ld)
{
#if MM_DEBUG
    size_t i, j;
    for (i = 0; i < M; i++)
        for (j = 0; j < N; j++) {
            if (c[i * ld + j] >= 10000000.0) {
                DPUTSZ(i);
                DPUTSZ(j);
                DPUTS("c[..] = %f", c[i*ld+j]);
                ASSERT(0);
            }
        }
#endif
}

static void value_check_C_init(ptr_t C, elem_t *c, size_t M, size_t N,
                               size_t ld)
{
#if MM_DEBUG
    size_t i, j;
    for (i = 0; i < M; i++)
        for (j = 0; j < N; j++) {
            if (fabs(c[i * ld + j]) >= EPSILON) {
                DPUTSZ(i);
                DPUTSZ(j);
                DPUTS("c[%3zu, %3zu] = %e", i, j, c[i * ld + j]);
                ASSERT(0);
            }
        }
#endif
}

static void mm_compare(elem_t *c0, elem_t *c1, size_t M, size_t N)
{
    size_t i, j;
    for (i = 0; i < M; i++) {
        for (j = 0; j < N; ++j) {
            elem_t e0 = c0[M * i + j];
            elem_t e1 = c1[M * i + j];
            if (fabs(e0 - e1) >= EPSILON) {
                printf("error: C[%3zu, %3zu] = %10.0f (correct: %10.0f)\n",
                       i, j, e0, e1);
                ASSERT("verification" == NULL);
            }
        }
    }

    printf("MM verified.\n");
}

static void mm_print(const mm_t *mm)
{
    ptr_t A = mm->A, B = mm->B, C = mm->C;
    size_t M = mm->M, N = mm->N, K = mm->K, ld = mm->ld;

    size_t elem_size = sizeof(elem_t);
    size_t Ai = (0xFFFFFFFF & A) / (elem_size * ld);
    size_t Aj = (0xFFFFFFFF & A) % (elem_size * ld) / elem_size;
    size_t Bi = (0xFFFFFFFF & B) / (elem_size * ld);
    size_t Bj = (0xFFFFFFFF & B) % (elem_size * ld) / elem_size;
    size_t Ci = (0xFFFFFFFF & C) / (elem_size * ld);
    size_t Cj = (0xFFFFFFFF & C) % (elem_size * ld) / elem_size;

    printf("A[%3zu,%3zu..%3zu,%3zu], "
             "B[%3zu,%3zu..%3zu,%3zu], "
             "C[%3zu,%3zu..%3zu,%3zu]\n",
             Ai, Aj, Ai+M, Aj+K, Bi, Bj, Bi+K, Bj+N,
             Ci, Cj, Ci+M, Cj+N);
}

static void mm_debug_print(const mm_t *mm)
{
    ptr_t A = mm->A, B = mm->B, C = mm->C;
    size_t M = mm->M, N = mm->N, K = mm->K, ld = mm->ld;

    size_t elem_size = sizeof(elem_t);
    size_t Ai = (0xFFFFFFFF & A) / (elem_size * ld);
    size_t Aj = (0xFFFFFFFF & A) % (elem_size * ld) / elem_size;
    size_t Bi = (0xFFFFFFFF & B) / (elem_size * ld);
    size_t Bj = (0xFFFFFFFF & B) % (elem_size * ld) / elem_size;
    size_t Ci = (0xFFFFFFFF & C) / (elem_size * ld);
    size_t Cj = (0xFFFFFFFF & C) % (elem_size * ld) / elem_size;

    MM_DPUTS("A[%3zu,%3zu..%3zu,%3zu], "
             "B[%3zu,%3zu..%3zu,%3zu], "
             "C[%3zu,%3zu..%3zu,%3zu]",
             Ai, Aj, Ai+M, Aj+K, Bi, Bj, Bi+K, Bj+N,
             Ci, Cj, Ci+M, Cj+N);
}

static void mm_naive(elem_t *a, elem_t *b, elem_t *c,
                      size_t M, size_t N, size_t K, size_t ld)
{
    const size_t threshold =
        MM_POLL_THRESHOLD * MM_POLL_THRESHOLD * MM_POLL_THRESHOLD;
    size_t counter = 0;

    size_t i, j, k;
    for (i = 0; i < M; i++) {        
        for (j = 0; j < N; j++) {            
            for (k = 0; k < K; k++) {

                if (counter++ % threshold == (threshold - 1))
                    POLL();
                
                c[ld * i + j] += a[ld * i + k] * b[ld * k + j];
            }
        }
    }
}

static void mm_kernel(elem_t *a, elem_t *b, elem_t *c,
                      size_t M, size_t N, size_t K, size_t ld)
{
#if MM_KERNEL == MM_KERNEL_NAIVE
    mm_naive(a, b, c, M, N, K, ld);
#elif MM_KERNEL == MM_KERNEL_BLAS
    cblas_dgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans,
                (int)M, (int)N, (int)K,
                1., a, (int)ld, b, (int)ld, 1., c, (int)ld);
#elif MM_KERNEL == MM_KERNEL_CO

    const size_t N_LEAF = 48;
    static size_t counter = 0;
    
    if (M + N + K <= 3 * N_LEAF) {

        const size_t threshold =
            MM_POLL_THRESHOLD * MM_POLL_THRESHOLD * MM_POLL_THRESHOLD;

        size_t i, j, k;
        for (i = 0; i < M; i++) {        
            for (j = 0; j < N; j++) {            
                for (k = 0; k < K; k++) {                
                    c[ld * i + j] += a[ld * i + k] * b[ld * k + j];
                }

                counter += K;
                if (counter > threshold) {
                    POLL();
                    counter = 0;
                }
            }
        }

    } else if (M >= N && M >= K) {
        size_t M_half = (M >> 1) - (M >> 1) % 4;
        mm_kernel(&a[0], &b[0], &c[0], M_half, N, K, ld);
        mm_kernel(&a[ld * M_half], &b[0], &c[ld * M_half],
                  M - M_half, N, K, ld);
    } else if (N >= M && N >= K) {
        size_t N_half = (N >> 1) - (N >> 1) % 4;
        mm_kernel(&a[0], &b[0], &c[0], M, N_half, K, ld);
        mm_kernel(&a[0], &b[N_half], &c[N_half],
                  M, N - N_half, K, ld);
    } else {
        size_t K_half = (K >> 1) - (K >> 1) % 4;
        mm_kernel(&a[0], &b[0], &c[0], M, N, K_half, ld);
        mm_kernel(&a[K_half], &b[ld * K_half], &c[0],
                  M, N, K - K_half, ld);
    }
#endif
}

static size_t half(size_t size)
{
    size_t half = size >> 1;

#if MM_BLOCK_ALIGN
    if (half <= g_block_size) {
//        return half - half % 8;
        return half;
    } else {
        size_t value = half - half % g_block_size;
        ASSERT(value != 0);
        ASSERT(value != size);
        return value;
    }
#else
    return half - half % 8;
#endif
}

static size_t half_M(size_t size)
{
    return half(size);
}

static size_t half_N(size_t size)
{
    return half(size);
}

static size_t half_K(size_t size)
{
    return half(size);
}

static void divide_force(const mm_t *mm, mm_t submm[2])
{
    ptr_t A = mm->A, B = mm->B, C = mm->C;
    size_t M = mm->M, N = mm->N, K = mm->K, ld = mm->ld;

    if (M >= N && M >= K) {
        size_t M_half = half_M(M);
        mm_t submm0 = { A, B, C, M_half, N, K, ld };
        mm_t submm1 = { sub(A,M_half,0UL,ld), B, sub(C,M_half,0UL,ld),
                        M - M_half, N, K, ld };
        submm[0] = submm0;
        submm[1] = submm1;
    } else if (N >= M && N >= K) {
        size_t N_half = half_N(N);
        mm_t submm0 = { A, B, C, M, N_half, K, ld };
        mm_t submm1 = { A, sub(B,0UL,N_half,ld), sub(C,0UL,N_half,ld),
                        M, N - N_half, K, ld };
        submm[0] = submm0;
        submm[1] = submm1;
    } else {
        size_t K_half = half_K(K);
        mm_t submm0 = { A, B, C, M, N, K_half, ld };
        mm_t submm1 = { sub(A,0UL,K_half,ld), sub(B, K_half, 0UL, ld), C,
                        M, N, K - K_half, ld };
        submm[0] = submm0;
        submm[1] = submm1;
    }
}

static int divide(const mm_t *mm, mm_t submm[2])
{
    size_t M = mm->M, N = mm->N, K = mm->K;

    if (M + N + K <= 3 * g_leaf_size)
        return 0;

    divide_force(mm, submm);
    return 1;
}

static void put_values(ptr_t A, elem_t *a, size_t size)
{
    size_t i;
    time_t t;
    time(&t);
    srand((unsigned)t);

    for (i = 0; i < size / sizeof(elem_t); i++)
        a[i] = get_value(A + sizeof(elem_t) * i);
//        localA[i] = rand() % 100;
}

static void put_random(ptr_t A, size_t size)
{
    LOCALIZE_PREPARE;

    size_t stride = size;
    size_t count[] = { 1, size };

    elem_t *localA = (elem_t *)LOCALIZE_S(A, stride, count, REUSE);

    put_values(A, localA, size);

    COMMIT_S(A, localA, stride, count);

    UNLOCALIZE;
}

static void touch_with_center(ptr_t A, ptr_t B, ptr_t C, size_t N)
{
    proc_t me = GET_PID();
    
    size_t size = sizeof(elem_t) * N * N;

    if (me == 0) {
        put_random(A, size);
        put_random(B, size);
        GLOBAL_SET(C, 0, size);
    }
}

static void touch_with_block(ptr_t A, ptr_t B, ptr_t C, size_t N)
{
    proc_t me = GET_PID();
    size_t n_procs = GET_N_PROCS();
    
    size_t n_elems = N * N;
    
    size_t sub_elems = n_elems / n_procs;
    size_t mod_elems = n_elems % n_procs;
    
    size_t offset = sizeof(elem_t) * (sub_elems * me + MIN(me, mod_elems));

    if (me < mod_elems)
        sub_elems += 1;

    size_t sub_size = sizeof(elem_t) * sub_elems;
    
    put_random(A + offset, sub_size);
    put_random(B + offset, sub_size);
    GLOBAL_SET(C + offset, 0, sub_size);
}

static void touch_rec(const mm_t *mm, size_t key, size_t rest_depth,
                      size_t begin, size_t end)
{
    if (key < (begin >> rest_depth) || (end >> rest_depth) < key)
        return;

    ptr_t A = mm->A, B = mm->B, C = mm->C;
    size_t M = mm->M, N = mm->N, K = mm->K, ld = mm->ld;
    
    size_t elem_size = sizeof(elem_t);
    size_t stride = ld * elem_size;
    size_t countA[] = { M, N * elem_size };
    size_t countB[] = { M, N * elem_size };
    size_t countC[] = { M, N * elem_size };

    elem_t *a, *b, *c;
    LOCALIZE_PREPARE;

    if (M + N <= 2 * g_leaf_size) {
        MM_DPUTS("A=0x%" PRIx64 ", B=0x%" PRIx64 ", C=0x%" PRIx64 ", "
                 "M=%zu, N=%zu",
                 A, B, C, M, N);

        a = (elem_t *)LOCALIZE_S(A, stride, countA, REUSE);
        b = (elem_t *)LOCALIZE_S(B, stride, countB, REUSE);
        c = (elem_t *)LOCALIZE_S(C, stride, countC, REUSE);

        size_t i;
        for (i = 0; i < M; i++)
            put_values(A + stride * i, a + ld * i, N * elem_size);
        for (i = 0; i < M; i++)
            put_values(B + stride * i, b + ld * i, N * elem_size);
        for (i = 0; i < M; i++)
            memset(c + ld * i, 0, N * elem_size);

        COMMIT_S(A, a, stride, countA);
        COMMIT_S(B, b, stride, countB);
        COMMIT_S(C, c, stride, countC);

    } else if (M >= N) {
        size_t M_half = half_M(M);
        mm_t submm[] = {
            { A, B, C, M_half, N, K, ld },
            { sub(A,M_half,0UL,ld), sub(B,M_half,0UL,ld), sub(C,M_half,0UL,ld),
              M - M_half, N, K, ld }
        };
        touch_rec(&submm[0], key * 2 + 0, rest_depth - 1, begin, end);
        touch_rec(&submm[1], key * 2 + 1, rest_depth - 1, begin, end);
    } else {
        size_t N_half = half_N(N);
        mm_t submm[] = {
            { A, B, C, M, N_half, K, ld },
            { sub(A,0UL,N_half,ld), sub(B,0UL,N_half,ld), sub(C,0UL,N_half,ld),
              M, N - N_half, K, ld }
        };
        touch_rec(&submm[0], key * 2 + 0, rest_depth - 1, begin, end);
        touch_rec(&submm[1], key * 2 + 1, rest_depth - 1, begin, end);
    }

    UNLOCALIZE;
}

static size_t max_depth_of_C_tree(const mm_t *mm, size_t depth)
{
    mm_t submm[2];
    if (!divide(mm, submm)) {
        return depth;
    } else if (submm[0].K == mm->K) {
        size_t depth0 = max_depth_of_C_tree(&submm[0], depth + 1);
        size_t depth1 = max_depth_of_C_tree(&submm[1], depth + 1);
        return MAX(depth0, depth1);
    } else {
        size_t depth0 = max_depth_of_C_tree(&submm[0], depth);
        size_t depth1 = max_depth_of_C_tree(&submm[1], depth);
        return MAX(depth0, depth1);
    }
}

static void touch_with_sfc(const mm_t *mm)
{
    proc_t me = GET_PID();
    size_t n_procs = GET_N_PROCS();

    size_t max_depth = max_depth_of_C_tree(mm, 0);
    size_t n_virtual_leaves = 0x1llu << max_depth;

    size_t per_proc = n_virtual_leaves / n_procs;
    size_t mod = n_virtual_leaves % n_procs;
    size_t begin = (me * per_proc + (me < mod ? me : mod));
    size_t end = ((me + 1) * per_proc + (me + 1 < mod ? me + 1 : mod)) - 1;

    touch_rec(mm, 0, max_depth, begin, end);
}

static void initialize(mm_t *mm, size_t n, size_t blk_size, size_t leaf_size,
                       size_t cache_size)
{
    proc_t me = GET_PID();
    size_t n_procs = GET_N_PROCS();

    size_t M = n;
    size_t N = n;
    size_t K = n;
    size_t ld = n;

    size_t da_size = sizeof(elem_t) * n * n;

#if 0
    size_t n_dims = 1;
    size_t block_size[] = { sizeof(elem_t) * blk_size };
    size_t n_blocks[] = { (da_size + block_size[0] - 1) / block_size[0] };
#else
    size_t n_dims = 2;

    size_t mod = n % blk_size;
    ASSERT(mod == 0);

    size_t elem_size = sizeof(elem_t);
    size_t block_size[] = { blk_size, elem_size * blk_size };
    size_t n_blocks[] = { n / block_size[0], elem_size * n / block_size[1] };
#endif

#if 0
    if (me == 0) {
        fprintf(stdout,
                "# block_size={%zu,%zu}, n_blocks={%zu,%zu}\n",
                block_size[0], block_size[1], n_blocks[0], n_blocks[1]);
    }
#endif
    
    ptr_t A = DIST_MALLOC_ALL(da_size, n_dims, block_size, n_blocks);
    ptr_t B = DIST_MALLOC_ALL(da_size, n_dims, block_size, n_blocks);
    ptr_t C = DIST_MALLOC_ALL(da_size, n_dims, block_size, n_blocks);

    mm_t mm_ = { A, B, C, M, N, K, ld };
    *mm = mm_;

    g_block_size = blk_size;
    g_leaf_size = leaf_size;
    g_cache_size = cache_size;

#if MM_TOUCH == MM_TOUCH_CENTER
    touch_with_center(A, B, C, n);
#elif MM_TOUCH == MM_TOUCH_1DBLOCK
    touch_with_block(A, B, C, n);
#else
    touch_with_sfc(mm);
#endif

    BARRIER();
}

static void verify(const mm_t *mm, size_t n)
{
#if MM_VERIFY
    size_t size = sizeof(elem_t) * n * n;

    size_t stride = size;
    size_t count[] = { 1, size };

    LOCALIZE_PREPARE;
    
    elem_t *a = (elem_t *)LOCALIZE(mm->A, size, REUSE);
    elem_t *b = (elem_t *)LOCALIZE(mm->B, size, REUSE);
    elem_t *c = (elem_t *)LOCALIZE(mm->C, size, REUSE);

    // calculate locally
    elem_t *blas_c = (elem_t *)malloc(size);
    memset(blas_c, 0, size);

    double t = now();
    mm_naive(a, b, blas_c, n, n, n, n);
    printf("sequential time: %f\n", now() - t);

    // compare global and local result
    mm_compare(c, blas_c, n, n);

    free(blas_c);
    UNLOCALIZE;
#endif    
}

static void finalize(mm_t *mm)
{
    DIST_FREE_ALL(mm->A);
    DIST_FREE_ALL(mm->B);
    DIST_FREE_ALL(mm->C);
}

static void mm_conf_output(FILE *f)
{
    fprintf(f, "MM_DEBUG = %d, MM_BLOCK_ALIGN = %d, MM_TOUCH = %d, "
            "MM_KERNEL = %d\n",
            MM_DEBUG, MM_BLOCK_ALIGN, MM_TOUCH, MM_KERNEL);
}

#endif
