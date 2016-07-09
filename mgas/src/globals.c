#include "globals.h"

#include "../include/mgas_config.h"
#include "../include/mgas_prof.h"
#include "comm.h"
#include "threadsafe.h"

#if !MGAS_INLINE_GLOBALS
#define GLOBALS_INLINE extern
#include "globals.inl"
#endif

proc_data_t g_proc;
th_data_t *g_thread;
__thread mgas_thread_t g_tid = MGAS_INVALID_TID;

//-- process/thread local data access ------------------------------------------

static void *globals_gheap_mmap(size_t len);
static void *globals_gheap_aligned_mmap(size_t len, size_t align);

void globals_initialize(mgas_proc_t me, size_t n_procs, size_t n_threads,
                        comm_t *comm)
{
    // setup process local storage
    proc_data_t *pdata = &g_proc;
    pdata->pid = me;
    pdata->n_procs = n_procs;
    spinlock_init(&pdata->giant_lock);
    pdata->comm = comm;
    pdata->gmt = gmt_create();
    spinlock_init(&pdata->tid_lock);
    pdata->tidpool = idpool_create();
    MGAS_CHECK(pthread_key_create(&pdata->tid_key, cleanup_thread) == 0);
    pdata->n_max_threads = n_threads;

    mgas_prof_initialize(&pdata->profile);

    size_t i;
    th_data_t *tdata = array_create(th_data_t, n_threads);
    for(i = 0; i < n_threads; i++) {
        tdata[i].handler = comm_handler_create();
        tdata[i].alloc = mgas_alloc_create();
    }

    g_thread = tdata;
}

void globals_finalize(void)
{
    size_t i;
    size_t n_threads = globals_get_n_max_threads();

    proc_data_t *pdata = &g_proc;
    gmt_destroy(pdata->gmt);
    idpool_destroy(pdata->tidpool);
    MGAS_CHECK(pthread_key_delete(pdata->tid_key) == 0);

    mgas_prof_finalize(&pdata->profile);

    th_data_t *tdata = g_thread;
    for(i = 0; i < n_threads; i++) {
        comm_handler_destroy(tdata[i].handler);
        mgas_alloc_destroy(tdata[i].alloc);
    }
    array_destroy(tdata);
}

mgas_thread_t mgas_get_raw_tid(void)
{
    return g_tid;
}
