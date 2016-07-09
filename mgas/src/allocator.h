#ifndef ALLOCATOR_H
#define ALLOCATOR_H

#define MGAS_KR_MALLOC 1

#if MGAS_KR_MALLOC

#include "../include/mgas_debug.h"
#include "kr_malloc.h"

struct mgas_alc {
    size_t n_threads;
    mgas_kr_malloc_t *kr_alc;
};

typedef struct mgas_alc * mgas_alc_t;

/* memory allocator functions */
static mgas_alc_t mgas_alc_create(size_t n_threads,
                                  mgas_mmap_t my_mmap,
                                  mgas_aligned_mmap_t aligned_mmap)
{
    size_t i;

    mgas_kr_malloc_t *kr_alc = malloc(sizeof(mgas_kr_malloc_t) * n_threads);

    for (i = 0; i < n_threads; i++)
        mgas_kr_malloc_init(&kr_alc[i], my_mmap, aligned_mmap);

    mgas_alc_t alc = malloc(sizeof(struct mgas_alc));
    alc->n_threads = n_threads;
    alc->kr_alc = kr_alc;

    return alc;
}

static void mgas_alc_destroy(mgas_alc_t alc)
{
    size_t i;

    for (i = 0; i < alc->n_threads; i++)
        mgas_kr_malloc_finalize(&alc->kr_alc[i]);

    free(alc);
}

static void *mgas_alc_malloc(mgas_alc_t alc, size_t size)
{
    size_t tid = mgas_get_tid();

    void *p = mgas_kr_malloc_allocate(&alc->kr_alc[tid], size);
    
    MGAS_CHECK(p != NULL);

    return p;
}

static void mgas_alc_free(mgas_alc_t alc, void *p)
{
    size_t tid = mgas_get_tid();

    mgas_kr_malloc_deallocate(&alc->kr_alc[tid], p);
}

static void *mgas_alc_aligned_malloc(mgas_alc_t alc, size_t size, size_t align)
{
    // FIXME
    return mgas_alc_malloc(alc, size);
}

static void mgas_alc_aligned_free(mgas_alc_t alc, void *p)
{
    mgas_alc_free(alc, p);
}

#else
#include "../include/mgas_debug.h"
#include "myth_alloc.h"

enum { PAGESIZE = 4096 };

/* memory allocator type */
typedef void *(*mgas_mmap_t)(size_t len);
typedef void *(*mgas_aligned_mmap_t)(size_t len, size_t align);
typedef myth_alloc_t mgas_alc_t;

/* memory allocator functions */
static inline mgas_alc_t mgas_alc_create(size_t n_threads, mgas_mmap_t my_mmap,
                                         mgas_aligned_mmap_t aligned_mmap)
{ return myth_alloc_create(n_threads, my_mmap, aligned_mmap); }

static inline void mgas_alc_destroy(mgas_alc_t alc)
{ myth_alloc_destroy(alc); }

static inline void *mgas_alc_malloc(mgas_alc_t alc, size_t size)
{
    size_t tid = mgas_get_tid();
    size_t real_size = sizeof(size_t) + size;
    size_t *p = myth_alloc_malloc(alc, tid, real_size);
    MGAS_CHECK(p != NULL);
    p[0] = real_size;
    return &p[1];
}

static inline void mgas_alc_free(mgas_alc_t alc, void *p)
{
    size_t tid = mgas_get_tid();
    size_t *ptr = p;
    size_t real_size = ptr[-1];
    myth_alloc_free(alc, tid, real_size, &ptr[-1]);
}

static inline void *mgas_alc_aligned_malloc(mgas_alc_t alc, size_t size,
                                            size_t align)
{
    size_t tid = mgas_get_tid();
    size_t real_size = PAGESIZE + (size + PAGESIZE - 1) / align * align;
    uint8_t *p = myth_alloc_aligned_malloc(alc, tid, real_size, align);
    MGAS_CHECK(p != NULL);
    size_t *header = (size_t *)p;
    header[0] = real_size;
    return p + PAGESIZE;
}

static inline void mgas_alc_aligned_free(mgas_alc_t alc, void *p)
{
    size_t tid = mgas_get_tid();
    size_t *header = (size_t *)((uint8_t *)p - PAGESIZE);
    size_t real_size = header[0];
    myth_alloc_aligned_free(alc, tid, real_size, header);
}
#endif

#endif
