#ifndef MGAS_COMM_H
#define MGAS_COMM_H

#include "../include/mgas.h"
#include "../include/mgas_am.h"
#include "globals.h"
#include "memory_v.h"
#include "misc.h"
#include <stddef.h>

#pragma GCC diagnostic ignored "-Wredundant-decls"

typedef struct comm_complete comm_complete_t;

comm_t *comm_initialize(int *argc, char ***argv, size_t n_threads);
void comm_finalize(comm_t *comm);

void comm_exit(int status);

pthread_t comm_spawn_helper_thread(void);
void comm_join_helper_thread(pthread_t th);

const char *comm_get_segment_name(void);

mgas_proc_t comm_get_pid(void);
size_t comm_get_n_procs(void);

comm_handler_t *comm_handler_create(void);
void comm_handler_destroy(comm_handler_t *handler);

void *comm_malloc(size_t size);
void comm_free(void *p);
void *comm_aligned_malloc(size_t size, size_t align);
void comm_aligned_free(void *p);

void comm_put_u64(uint64_t *p, mgas_proc_t proc, uint64_t value);
void comm_atomic_add_i64(int64_t *p, mgas_proc_t target, int64_t value);

void comm_put_v(const mgas_memvec_t dst[], size_t n_dst, mgas_proc_t dstproc,
                const mgas_memvec_t src[], size_t n_src);
void comm_get_v(const mgas_memvec_t dst[], size_t n_dst,
                const mgas_memvec_t src[], size_t n_src, mgas_proc_t srcproc);

void comm_poll(void);

void mpi_barrier(void);
void comm_barrier(void);
void comm_barrier_with_poll(mgas_poll_t poll);

void comm_broadcast(void *buffer, size_t size, mgas_proc_t root);
void comm_broadcast_with_poll(void *p, size_t size, mgas_proc_t root,
                              mgas_poll_t poll);

void comm_gather(void *dst, void *src, size_t size, mgas_proc_t root);
void comm_gather_with_poll(void *dst, void *src, size_t size, mgas_proc_t root,
                           mgas_poll_t poll);
void comm_reduce_sum_long(long *dst, long *src, size_t size, mgas_proc_t root);
void comm_reduce_sum_long_with_poll(long *dst, long *src, size_t size,
                                   mgas_proc_t root, mgas_poll_t poll);

// inter-process synchronization object
typedef struct comm_handle comm_handle_t;

comm_handle_t *comm_handle_create(size_t count);
void comm_handle_destroy(comm_handle_t *handle);
void comm_handle_notify(const comm_handle_t *handle, size_t count);
void comm_handle_wait(const comm_handle_t *handle);

// internal messaging
typedef struct msg msg_t;

mgasptr_t comm_request_alloc(size_t size);
void comm_request_free(mgasptr_t mp);

void comm_request_stride_access(const mgas_memvec_t local_vs[],
                                size_t n_local_vs,
                                mgasptr_t mp, size_t stride,
                                const size_t count[2],
                                mgas_access_t access,
                                const comm_handle_t *handle,
                                mgas_proc_t initiator,
                                mgas_proc_t target);
void comm_request_stride_owner_change(mgasptr_t mp, size_t stride,
                                      const size_t count[2], mgas_proc_t owner,
                                      mgas_proc_t target);

typedef struct {
    mgas_proc_t owner;
    size_t block_size;
} owner_result_t;

void comm_request_owners(const mgasptr_t mps[], size_t n_mps,
                         mgas_access_t access, join_counter_t *jc,
                         owner_result_t *result_buf, mgas_proc_t target);
void comm_reply_owners(const msg_t *msg, const owner_result_t results[],
                       size_t n_results);

void comm_request_owner_change(const mgasptr_t mps[], size_t n_mps,
                               mgas_proc_t target);

struct data_rep_arg;
void comm_request_data_transfer(const mem_pair_t pairs[], size_t n_pairs,
                                mgas_access_t access,
                                const struct data_rep_arg *rep_arg,
                                mgas_proc_t target);
void comm_reply_data_transfer(const msg_t *msg, const size_t retry_indices[],
                              size_t n_retry_indices);

mgas_bool_t comm_request_rmw(mgas_rmw_func_t f, mgasptr_t mp, size_t size,
                             const void *param_in, size_t param_in_size,
                             void *param_out, size_t param_out_size,
                             mgas_proc_t target);
void comm_reply_rmw(const msg_t *msg, mgas_bool_t success,
                    const void *param_out_data);

struct mgas_am {
    const msg_t *msg;
    void *token;
};

void comm_request_active_message(mgas_am_func_t f, const void *p, size_t size,
                                 mgas_proc_t target);
void comm_reply_active_message(const mgas_am_t *am, mgas_am_func_t f,
                               const void *p, size_t size);

mgas_proc_t comm_get_initiator(const mgas_am_t *am);

mgas_bool_t comm_handler_processing(comm_handler_t *handler);


#pragma GCC diagnostic warning "-Wredundant-decls"


#if 1
static void *allocate_page(size_t size)
{
    return comm_malloc(size);
}

static void free_page(void *p)
{
    comm_free(p);
}

#else

static void *allocate_page(size_t size)
{
    // this code frequently causes page fault (clear_page_c)...
    return comm_aligned_malloc(size, OS_PAGE_SIZE);
}

static void free_page(void *p)
{
    comm_aligned_free(p);
}

#endif

#endif
