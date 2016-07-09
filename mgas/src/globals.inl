#ifndef GLOBALS_INL
#define GLOBALS_INL


/* if you include this file, you have to define GLOBALS_INLINE. */

#include "threadsafe.h"
#include "../include/mgas_prof.h"
#include <pthread.h>

typedef struct th_data {
    comm_handler_t *handler;
    mgas_alloc_t *alloc;
} th_data_t;

typedef struct proc_data {
    mgas_proc_t pid;
    size_t n_procs;

    spinlock_t giant_lock;
    comm_t *comm;
    gmt_t *gmt;

    spinlock_t tid_lock;   // lock for tid allocation
    idpool_t *tidpool;
    pthread_key_t tid_key;
    size_t n_max_threads;

    profile_t profile;
} proc_data_t;


extern proc_data_t g_proc;
extern th_data_t *g_thread;
extern __thread mgas_thread_t g_tid;


static proc_data_t *mgas_proc_data(void) { return &g_proc; }

static th_data_t *mgas_th_data(void)
{
    mgas_thread_t tid = globals_get_tid();
    return &g_thread[tid];
}

GLOBALS_INLINE mgas_proc_t globals_get_pid(void)
{
    return mgas_proc_data()->pid;
}

GLOBALS_INLINE size_t globals_get_n_procs(void)
{
    return mgas_proc_data()->n_procs;
}

#if MGAS_ENABLE_GIANT_LOCK
GLOBALS_INLINE void globals_giant_lock(void)
{
    spinlock_lock(&mgas_proc_data()->giant_lock);
}
GLOBALS_INLINE void globals_giant_unlock(void)
{
    spinlock_unlock(&mgas_proc_data()->giant_lock);
}
#else
GLOBALS_INLINE void globals_giant_lock(void) {}
GLOBALS_INLINE void globals_giant_unlock(void) {}
#endif

GLOBALS_INLINE comm_t *globals_get_comm(void)
{
    return mgas_proc_data()->comm;
}

GLOBALS_INLINE gmt_t *globals_get_gmt(void)
{
    return mgas_proc_data()->gmt;
}

GLOBALS_INLINE profile_t *globals_get_profile(void)
{
    return &mgas_proc_data()->profile;
}

GLOBALS_INLINE comm_handler_t *globals_get_handler(void)
{
    return mgas_th_data()->handler;
}

GLOBALS_INLINE mgas_alloc_t *globals_get_alloc(void)
{
    return mgas_th_data()->alloc;
}

static void cleanup_thread(void *tid_buf)
{
    mgas_thread_t tid = *(mgas_thread_t *)tid_buf;
    if (tid != MGAS_INVALID_TID) {
        proc_data_t *pdata = mgas_proc_data();
        idpool_t *pool = pdata->tidpool;

        spinlock_lock(&pdata->tid_lock);
        idpool_put(pool, tid);
        spinlock_unlock(&pdata->tid_lock);
    }

    free(tid_buf);
}

static void globals_set_new_tid(void)
{
    proc_data_t *pdata = mgas_proc_data();

    // This statement may be called on GASNet's no-interrupt sections.
    spinlock_lock_pure(&pdata->tid_lock);
    mgas_thread_t tid = idpool_get(pdata->tidpool);
    spinlock_unlock(&pdata->tid_lock);

    MGAS_CHECK(tid < pdata->n_max_threads);

    // simulate atexit function for pthread
    {
        // this malloc'd memory must be alive after MGAS is finished,
        // so we have to use normal malloc.
        mgas_thread_t *tid_buf = (mgas_thread_t *)malloc(sizeof(mgas_thread_t));
        *tid_buf = tid;

        MGAS_CHECK(pthread_setspecific(pdata->tid_key, tid_buf) == 0);
    }

    g_tid = tid;
}

GLOBALS_INLINE mgas_thread_t globals_get_tid(void)
{
    if (g_tid == MGAS_INVALID_TID) {
        globals_set_new_tid();
    }
    return g_tid;
}

GLOBALS_INLINE size_t globals_get_n_max_threads(void)
{
    return mgas_proc_data()->n_max_threads;
}

GLOBALS_INLINE FILE *globals_get_debug_out(void)
{
    return stderr;
}

#endif
