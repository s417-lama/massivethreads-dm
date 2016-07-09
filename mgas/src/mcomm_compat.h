#ifndef MGAS_MCOMM_COMPAT_H
#define MGAS_MCOMM_COMPAT_H

#include "gasnet_ext.h"

#define MGAS_MCOMM_COMPAT 1
#define MADI_COMM_GASNET_HANDLER_LAST  135   //128

#ifndef MGAS_MCOMM_COMPAT
#define MGAS_MCOMM_COMPAT         0
#define MADI_COMM_GASNET_HANDLER_LAST  128
#endif


typedef enum handler_tag {
    HANDLER_QUEUE_MSG = MADI_COMM_GASNET_HANDLER_LAST,
    HANDLER_QUEUE_LARGE_MSG,
    HANDLER_PUT_V_PACKED,
    HANDLER_ATOMIC_ADD_I64,
    HANDLER_MGAS_AM,
    HANDLER_LAST__,
} handler_tag_t;

enum {
    N_HANDLERS = HANDLER_LAST__ - MADI_COMM_GASNET_HANDLER_LAST,
};

static void handler_queue_msg(gasnet_token_t token, void *buf, size_t nbytes);
static void handler_queue_large_msg(gasnet_token_t token,
                                    void *buf, size_t nbytes,
                                    gasnet_handlerarg_t arg0,
                                    gasnet_handlerarg_t arg1,
                                    gasnet_handlerarg_t arg2,
                                    gasnet_handlerarg_t arg3);
static void handler_put_v_packed(gasnet_token_t token,
                                 void *buf, size_t nbytes,
                                 gasnet_handlerarg_t arg0);
static void handler_atomic_add_i64(gasnet_token_t token,
                                   gasnet_handlerarg_t p_high,
                                   gasnet_handlerarg_t p_low,
                                   gasnet_handlerarg_t value_high,
                                   gasnet_handlerarg_t value_low);
static void comm_process_active_message(gasnet_token_t token,
                                        void *buf, size_t nbytes,
                                        gasnet_handlerarg_t arg0,
                                        gasnet_handlerarg_t arg1);
static void reduce_long_fn(void *results, size_t result_count,
                           const void *left_operands, size_t left_count,
                           const void *right_operands,
                           size_t elem_size, int flags, int arg);


// a thin wrapper of GASNet API

#if MGAS_MCOMM_COMPAT
// #define MCOMM_PID_OF_GASNET(pid)  (mcomm_pid_of_gasnet(pid))
// #define MCOMM_NP_OF_GASNET(np)    (mcomm_n_procs_of_gasnet(np))
// #define GASNET_PID_OF_MCOMM(pid)  (mcomm_gasnet_pid_of(pid))
// #define GASNET_NP_OF_MCOMM(np)    (mcomm_gasnet_n_procs_of(np))
// FIXME
#define MCOMM_PID_OF_GASNET(pid)  (pid)
#define MCOMM_NP_OF_GASNET(np)    (np)
#define GASNET_PID_OF_MCOMM(pid)  (pid)
#define GASNET_NP_OF_MCOMM(np)    (np)
#else
#define MCOMM_PID_OF_GASNET(pid)  (pid)
#define MCOMM_NP_OF_GASNET(np)    (np)
#define GASNET_PID_OF_MCOMM(pid)  (pid)
#define GASNET_NP_OF_MCOMM(np)    (np)
#endif

size_t mgas_init_gasnet_handlers(gasnet_handlerentry_t *handlers,
                               size_t start_idx)
{
    gasnet_handlerentry_t mgas_handlers[N_HANDLERS] = {
        { HANDLER_QUEUE_MSG, handler_queue_msg },
        { HANDLER_QUEUE_LARGE_MSG, handler_queue_large_msg },
        { HANDLER_PUT_V_PACKED, handler_put_v_packed },
        { HANDLER_ATOMIC_ADD_I64, handler_atomic_add_i64 },
        { HANDLER_MGAS_AM, comm_process_active_message },
    };

    MGAS_CHECK(start_idx + N_HANDLERS <= 255);

    memcpy(handlers + start_idx, mgas_handlers,
           sizeof(mgas_handlers[0]) * N_HANDLERS);

    size_t n_total_entries = start_idx + N_HANDLERS;

    return n_total_entries;
}

static void mgas_initialize_gasnet(int *argc, char ***argv)
{
#if MGAS_MCOMM_COMPAT
    // GASNet intialization is done by MCOMM

    // FIXME
    if (getenv("MADI_GASNET_INITIALIZED") == NULL) {
        fprintf(stderr, "MADI_GASNET_INITIALIZED is NULL\n");

        // the case MGAS is not executed with MCOMM
        GASNET_SAFE(gasnet_init(argc, argv));

        gasnet_handlerentry_t handlers[N_HANDLERS];
        mgas_init_gasnet_handlers(handlers, 0);

        uintptr_t max_segsize = gasnet_getMaxLocalSegmentSize();
        uintptr_t minheapoffset = GASNET_PAGESIZE;

        uintptr_t segsize = max_segsize / GASNET_PAGESIZE * GASNET_PAGESIZE;

        GASNET_SAFE(gasnet_attach(handlers, N_HANDLERS,
                                  segsize, minheapoffset));

        mgas_proc_t me = gasnet_mynode();
        size_t n_procs = gasnet_nodes();

        // initialize GASNet collectives
        // - images = NULL: 1 image per node
        gasnet_coll_fn_entry_t entries[1] = {
            { reduce_long_fn, 0 },
        };
        size_t n_entries = sizeof(entries) / sizeof(entries[0]);
        gasnet_coll_init(NULL, (gasnet_image_t)me, entries, n_entries, 0);
    }
#else
    // initialize GASNet core/extended

    GASNET_SAFE(gasnet_init(argc, argv));

    gasnet_handlerentry_t handlers[N_HANDLERS];
    mgas_init_gasnet_handlers(handlers, 0);

    uintptr_t max_segsize = gasnet_getMaxLocalSegmentSize();
    uintptr_t minheapoffset = GASNET_PAGESIZE;

    mgas_proc_t me = gasnet_mynode();
    size_t n_procs = gasnet_nodes();

    uintptr_t segsize = max_segsize / GASNET_PAGESIZE * GASNET_PAGESIZE;

    GASNET_SAFE(gasnet_attach(handlers, N_HANDLERS,
                              segsize, minheapoffset));

    // initialize GASNet collectives
    // - images = NULL: 1 image per node

    gasnet_coll_fn_entry_t entries[1] = {
        { reduce_long_fn, 0 },
    };
    size_t n_entries = sizeof(entries) / sizeof(entries[0]);
    gasnet_coll_init(NULL, (gasnet_image_t)me, entries, n_entries, 0);
#endif 
}

static gasnet_node_t mgas_gasnet_mynode(void)
{
    return MCOMM_PID_OF_GASNET(gasnet_mynode());
}

static gasnet_node_t mgas_gasnet_nodes(void)
{
    return MCOMM_NP_OF_GASNET(gasnet_nodes());
}

static int mgas_gasnet_getSegmentInfo(gasnet_seginfo_t *seginfo_table,
                                      int numentries)
{
#if MGAS_MCOMM_COMPAT
    // FIXME
    return gasnet_getSegmentInfo(seginfo_table, numentries);
#else
    return gasnet_getSegmentInfo(seginfo_table, numentries);
#endif
}

static uintptr_t mgas_gasnet_getMaxLocalSegmentSize(void)
{
#if MGAS_MCOMM_COMPAT
    // FIXME
    return gasnet_getMaxLocalSegmentSize();
#else
    return gasnet_getMaxLocalSegmentSize();
#endif
}

static void mgas_gasnet_exit(int exitcode)
{
    gasnet_exit(exitcode);
}

static void mgas_gasnet_AMPoll(void)
{
    gasnet_AMPoll();
}

static void mgas_gasnet_hold_interrupts(void)
{
    gasnet_hold_interrupts();
}

static void mgas_gasnet_resume_interrupts(void)
{
    gasnet_resume_interrupts();
}

static int mgas_gasnet_AMRequestMedium0(gasnet_node_t dest,
                                        gasnet_handler_t handler,
                                        void *source_addr, size_t nbytes)
{
    return gasnet_AMRequestMedium0(GASNET_PID_OF_MCOMM(dest), handler,
                                   source_addr, nbytes);
}

static int mgas_gasnet_AMRequestMedium1(gasnet_node_t dest,
                                        gasnet_handler_t handler,
                                        void *source_addr, size_t nbytes,
                                        gasnet_handlerarg_t arg0)
{
    return gasnet_AMRequestMedium1(GASNET_PID_OF_MCOMM(dest), handler,
                                   source_addr, nbytes,
                                   arg0);
}

static int mgas_gasnet_AMRequestMedium2(gasnet_node_t dest,
                                        gasnet_handler_t handler,
                                        void *source_addr, size_t nbytes,
                                        gasnet_handlerarg_t arg0,
                                        gasnet_handlerarg_t arg1)
{
    return gasnet_AMRequestMedium2(GASNET_PID_OF_MCOMM(dest), handler,
                                   source_addr, nbytes,
                                   arg0, arg1);
}

static int mgas_gasnet_AMRequestMedium4(gasnet_node_t dest,
                                        gasnet_handler_t handler,
                                        void *source_addr, size_t nbytes,
                                        gasnet_handlerarg_t arg0,
                                        gasnet_handlerarg_t arg1,
                                        gasnet_handlerarg_t arg2,
                                        gasnet_handlerarg_t arg3)
{
    return gasnet_AMRequestMedium4(GASNET_PID_OF_MCOMM(dest), handler,
                                   source_addr, nbytes,
                                   arg0, arg1, arg2, arg3);
}

static int mgas_gasnet_AMReplyMedium2(gasnet_token_t token,
                                      gasnet_handler_t handler,
                                      void *source_addr, size_t nbytes,
                                      gasnet_handlerarg_t arg0,
                                      gasnet_handlerarg_t arg1)
{
    return gasnet_AMReplyMedium2(token, handler, source_addr, nbytes,
                                 arg0, arg1);
}

static size_t mgas_gasnet_AMMaxMedium(void)
{
    return gasnet_AMMaxMedium();
}

static size_t mgas_gasnet_AMMaxLongRequest(void)
{
    return gasnet_AMMaxLongRequest();
}

static int mgas_gasnet_AMGetMsgSource(gasnet_token_t token,
                                      gasnet_node_t *srcindex)
{
    gasnet_node_t node;
    int r = gasnet_AMGetMsgSource(token, &node);

    *srcindex = MCOMM_PID_OF_GASNET(node);
    return r;
}

static void mgas_gasnet_putv_bulk(gasnet_node_t dstnode,
                                  size_t dstcount, gasnet_memvec_t dstlist[],
                                  size_t srccount, gasnet_memvec_t srclist[])
{
    gasnet_putv_bulk(GASNET_PID_OF_MCOMM(dstnode), dstcount, dstlist,
                     srccount, srclist);
}

static void mgas_gasnet_getv_bulk(size_t dstcount,gasnet_memvec_t dstlist[], 
                                  gasnet_node_t srcnode,
                                  size_t srccount, gasnet_memvec_t srclist[])
{
    gasnet_getv_bulk(dstcount, dstlist,
                     GASNET_PID_OF_MCOMM(srcnode), srccount, srclist);
}

static void mgas_gasnet_put_val(gasnet_node_t node, void *dest,
                                gasnet_register_value_t value, size_t nbytes)
{
    gasnet_put_val(node, GASNET_PID_OF_MCOMM(dest), value, nbytes);
}

static void mgas_gasnet_barrier_notify(int id, int flags)
{
#if MGAS_MCOMM_COMPAT
    // FIXME
    gasnet_barrier_notify(id, flags);
#else
    gasnet_barrier_notify(id, flags);
#endif
}

static int mgas_gasnet_barrier_try(int id, int flags)
{
#if MGAS_MCOMM_COMPAT
    // FIXME
    return gasnet_barrier_try(id, flags);
#else
    return gasnet_barrier_try(id, flags);
#endif
}

#define MGAS_GASNET_TEAM_ALL GASNET_TEAM_ALL

static gasnet_coll_handle_t
mgas_gasnet_coll_broadcast_nb(gasnet_team_handle_t team,
                              void *dst, gasnet_image_t srcimage, void *src,
                              size_t nbytes, int flags)
{
    // team is always GASNET_TEAM_ALL in GASNet-2.24.2

#if MGAS_MCOMM_COMPAT
    // FIXME
    return gasnet_coll_broadcast_nb(team, dst, srcimage, src, nbytes, flags);
#else
    return gasnet_coll_broadcast_nb(team, dst, srcimage, src, nbytes, flags);
#endif
}

static gasnet_coll_handle_t
mgas_gasnet_coll_gather_nb(gasnet_team_handle_t team,
                           gasnet_image_t dstimage, void *dst,
                           void *src, size_t nbytes, int flags)
{
#if MGAS_MCOMM_COMPAT
    // FIXME
    return gasnet_coll_gather_nb(team, dstimage, dst, src, nbytes, flags);
#else
    return gasnet_coll_gather_nb(team, dstimage, dst, src, nbytes, flags);
#endif
}

static gasnet_coll_handle_t
mgas_gasnet_coll_reduce_nb(gasnet_team_handle_t team,
                           gasnet_image_t dstimage, void *dst,
                           void *src, size_t src_blksz, size_t src_offset,
                           size_t elem_size, size_t elem_count,
                           gasnet_coll_fn_handle_t func, int func_arg,
                           int flags)
{
#if MGAS_MCOMM_COMPAT
    // FIXME
    return gasnet_coll_reduce_nb(team, dstimage, dst,
                                 src, src_blksz, src_offset,
                                 elem_size, elem_count, func, func_arg,
                                 flags);
#else
    return gasnet_coll_reduce_nb(team, dstimage, dst,
                                 src, src_blksz, src_offset,
                                 elem_size, elem_count, func, func_arg,
                                 flags);
#endif
}

static int mgas_gasnet_coll_try_sync(gasnet_coll_handle_t handle)
{
    return gasnet_coll_try_sync(handle);
}

static void mgas_native_barrier(void)
{
#if MGAS_MCOMM_COMPAT
    // FIXME
    MPI_Barrier(MPI_COMM_WORLD);
#else
    MPI_Barrier(MPI_COMM_WORLD);
#endif
}

static void mgas_native_broadcast(void *p, size_t size, mgas_proc_t root)
{
#if MGAS_MCOMM_COMPAT
    // FIXME
    MPI_Bcast(p, (int)size, MPI_BYTE, (int)root, MPI_COMM_WORLD);
#else
    MPI_Bcast(p, (int)size, MPI_BYTE, (int)root, MPI_COMM_WORLD);
#endif
}

#endif
