#ifndef MGAS_HANDLERS_H
#define MGAS_HANDLERS_H

#include "comm.h"
#include "threadsafe.h"

typedef struct data_rep_arg {
    const mem_pair_t *pairs;
    size_t n_pairs;
    mgas_access_t access;
    spinlock_t *lock;
    varray_t *done_list;
    varray_t *retry_list;
    join_counter_t *jc;
} data_rep_arg_t;


// active message handlers
void mgas_process_alloc(size_t size, mgasptr_t *mp_ptr,
                       const comm_handle_t *handle,
                       mgas_proc_t initiator);
void mgas_process_free(mgasptr_t mp);


void mgas_process_owner_request(const msg_t *msg, const mgasptr_t mps[],
                                size_t n_mps, mgas_access_t access,
                                mgas_proc_t initiator);

void mgas_process_owner_reply(const owner_result_t results[], size_t n_results,
                              join_counter_t *jc, owner_result_t *result_buf);

void mgas_process_owner_change(const mgasptr_t mps[], size_t n_mps,
                               mgas_proc_t proc);

void mgas_process_data_transfer_request(const msg_t *msg,
                                        const mem_pair_t pairs[],
                                        size_t n_pairs, mgas_access_t access,
                                        mgas_proc_t initiator);

void mgas_process_data_transfer_reply(const size_t retry_indices[],
                                      size_t n_retry_indices,
                                      const data_rep_arg_t *v);

void mgas_process_rmw_request(const msg_t *msg, mgas_rmw_func_t f,
                              mgasptr_t mp, size_t size,
                              const void *param_in, size_t param_in_size,
                              void *param_out, size_t param_out_size);

void mgas_process_rmw_reply(mgas_bool_t success, void *param_out,
                            const void *param_out_data, size_t param_out_size,
                            mgas_bool_t *result_buf);

void mgas_am_process_request(const msg_t *msg, mgas_am_func_t f,
                             const void *p, size_t size);

#endif
