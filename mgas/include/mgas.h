#ifndef MGAS_H
#define MGAS_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include "mgas_misc.h"

#ifdef __cplusplus
extern "C" {
#endif
    
enum  {
    MGAS_MAX_PROCS = 1UL << 20,
};

/* miscellaneous types */
typedef enum mgas_bool {
    MGAS_FALSE, MGAS_TRUE
} mgas_bool_t;

/* process id */
typedef size_t mgas_proc_t;

/* thread id */
typedef size_t mgas_thread_t;

/* global pointer */
typedef uint64_t mgasptr_t;

static const mgasptr_t MGAS_NULL = 0UL;


/*
 *  initialization/finalization
 */
mgas_bool_t mgas_initialize(int *argc, char ***argv);
mgas_bool_t mgas_initialize_with_threads(int *argc, char ***argv,
                                         size_t n_threads);
void mgas_finalize(void);
void mgas_exit(int status) MGAS_NORETURN;
void mgas_abort(void) MGAS_NORETURN;


/*
 * process/thread functions
 */
mgas_proc_t mgas_get_pid(void);
size_t mgas_get_n_procs(void);
mgas_thread_t mgas_get_tid(void);
size_t mgas_get_max_threads(void);


/*
 * global memory allocation/deallocation
 */

/* global object allocation */
mgasptr_t mgas_malloc(size_t size);

/* distributed array allocation (one-sided operation) */
mgasptr_t mgas_dmalloc(size_t size);

/* deallocation of global objects and distributed arrays
   (one-sided operation) */
void mgas_free(mgasptr_t p);

/* FIXME: temporary implementation of mgas_free corresponding to mgas_malloc. */
void mgas_free_small(mgasptr_t mp, size_t size);


/*
 * global memory access operations
 */

typedef enum {
    MGAS_RO  = 1,               /* read only            : use cached data */
    MGAS_RWE = MGAS_RO  << 1,   /* read/write exclusive : use cached data */
    MGAS_RWS = MGAS_RWE << 1,   /* read/write shared    : update cache */

    MGAS_OWN = MGAS_RWS << 1,   /* page migration */

    MGAS_REUSE  = MGAS_RO,
    MGAS_UPDATE = MGAS_RWS,
} mgas_flag_t;

/* internal data structure */
struct mgas_local;

/* localize handle */
typedef struct mgas_handle {
    struct mgas_local *locals;
} mgas_handle_t;

#define MGAS_HANDLE_INIT  { NULL }
void mgas_handle_init(mgas_handle_t *handle);

void *mgas_localize(mgasptr_t mp, size_t size, mgas_flag_t flags,
                    mgas_handle_t *handle);
void mgas_commit(mgasptr_t mp, void *p, size_t size);
void mgas_unlocalize(mgas_handle_t *handle);

void mgas_put(mgasptr_t mp, void *p, size_t size);
void mgas_get(void *p, mgasptr_t mp, size_t size);
void mgas_set(mgasptr_t mp, int value, size_t size);


/*
 * vector global memory access operations
 */

/* global pointer vector */
typedef struct mgas_vector {
    mgasptr_t mp;
    size_t size;
} mgas_vector_t;

/* local pointer vector */
typedef struct mgas_memvec_t {
    void *p;
    size_t size;
} mgas_memvec_t;

void *mgas_localize_v(mgasptr_t mp, const mgas_vector_t mvs[], size_t n_mvs,
                      mgas_flag_t flags, mgas_handle_t *handle);
void mgas_commit_v(mgasptr_t mp, void *p, const mgas_vector_t mvs[],
                   size_t n_mvs);


/*
 * strided global memory access operations
 */
void *mgas_localize_s(mgasptr_t mp, size_t stride, const size_t count[2],
                      mgas_flag_t flags, mgas_handle_t *handle);
void mgas_commit_s(mgasptr_t mp, void *p, size_t stride, const size_t count[2]);


/*
 * collective operations
 */
mgasptr_t mgas_all_dmalloc(size_t size, size_t n_dims,
                           const size_t block_size[],
                           const size_t n_blocks[]);
void mgas_all_free(mgasptr_t mp);
void mgas_barrier(void);
void mgas_broadcast(void *p, size_t size, mgas_proc_t root);
void mgas_gather(void *dst, void *src, size_t size, mgas_proc_t root);
void mgas_reduce_sum_long(long *dst, long *src, size_t size, mgas_proc_t root);
    
/*
 * Synchronization operations
 */

/* read-modify-write operation */
typedef void (*mgas_rmw_func_t)(void *p, size_t size,
                                const void *param_in, size_t param_in_size,
                                void *param_out, size_t param_out_size);
void mgas_rmw(mgas_rmw_func_t f, mgasptr_t mp, size_t size,
              const void *param_in, size_t param_in_size,
              void *param_out, size_t param_out_size);

/* synchronized variables */
typedef mgasptr_t mgas_syncvar_t;
mgas_syncvar_t mgas_syncvar_create(size_t size);
void mgas_syncvar_destroy(mgas_syncvar_t sv, size_t data_size);
void mgas_syncvar_put(mgas_syncvar_t sv, void *p, size_t size);
void mgas_syncvar_get(mgas_syncvar_t sv, void *p, size_t size);
mgas_bool_t mgas_syncvar_try_get(mgas_syncvar_t sv, void *p, size_t size);


/* internal API */
void mgas_poll(void);
mgas_bool_t mgas_owned(mgasptr_t mp);
mgas_proc_t mgas_home(mgasptr_t mp);
void mgas_conf_output(FILE *f);

/* deprecated */
mgasptr_t mgas_alloc(size_t size);
mgasptr_t mgas_all_alloc(size_t size);


/* customize interface */
typedef void (*mgas_poll_t)(void);
void mgas_barrier_with_poll(mgas_poll_t poll);
void mgas_broadcast_with_poll(void *p, size_t size, mgas_proc_t root,
                              mgas_poll_t poll);
void mgas_gather_with_poll(void *dst, void *src, size_t size, mgas_proc_t root,
                           mgas_poll_t poll);
void mgas_reduce_sum_long_with_poll(long *dst, long *src, size_t size,
                                   mgas_proc_t root, mgas_poll_t poll);

#ifdef __cplusplus
}
#endif
    
#endif
