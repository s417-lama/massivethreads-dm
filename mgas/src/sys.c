#include "sys.h"
#include "comm.h"
#include "../include/mgas_prof.h"
#include "../include/mgas_debug.h"

#include <stdlib.h>


void mgas_die(const char *s)
{
    mgas_proc_t me = globals_get_pid();
    
    fprintf(stderr, "%zu: %s\n", me, s);
    fflush(stderr);
    
    comm_exit(1);
}

#if MGAS_MYTH_MALLOC

static mgas_alc_t g_alc = NULL;

static void *sys_mmap(size_t len)
{
    return malloc(len);
}

static void *sys_aligned_mmap(size_t len, size_t align)
{
    void *p;
    int error = posix_memalign(&p, align, len);
    MGAS_ASSERT(error == 0);

    return p;
}

void mgas_sys_initialize(size_t n_threads)
{
    g_alc = mgas_alc_create(n_threads, sys_mmap, sys_aligned_mmap);
}

void mgas_sys_finalize(void)
{
    mgas_alc_destroy(g_alc);
    g_alc = NULL;
}

void *mgas_sys_malloc(size_t size)
{
    void *p;
    if (g_alc != NULL)
        p = mgas_alc_malloc(g_alc, size);
    else
        p = malloc(size);

    return p;
}

void mgas_sys_free(void *p)
{
    if (g_alc != NULL)
        mgas_alc_free(g_alc, p);
    else
        free(p);
}

void *mgas_sys_realloc(void *p, size_t size)
{
    MGAS_UNDEFINED;
}

void *mgas_sys_memalign(size_t size, size_t alignment)
{
    void *p = NULL;
    if (g_alc != NULL) {
        p = mgas_alc_aligned_malloc(g_alc, size, alignment);
    } else {
        int error = posix_memalign(&p, alignment, size);
        MGAS_ASSERT(error == 0);
    }

    return p;
}

#else

void mgas_sys_initialize(size_t n_threads)
{
}

void mgas_sys_finalize(void)
{
}

void *mgas_sys_malloc(size_t size)
{
    void *p = malloc(size);

    return p;
}

void mgas_sys_free(void *p)
{
    free(p);
}

void *mgas_sys_realloc(void *p, size_t size)
{
    void *p2 = realloc(p, size);

    return p2;
}

void *mgas_sys_memalign(size_t size, size_t alignment)
{
    void *p = NULL;
    posix_memalign(&p, alignment, size);

    return p;
}

#endif
