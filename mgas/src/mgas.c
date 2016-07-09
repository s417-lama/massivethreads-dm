#include "../include/mgas.h"
#include "../include/mgas_debug.h"
#include "../include/mgas_prof.h"

#include "globals.h"
#include "gmt.h"
#include "sys.h"
#include "comm.h"
#include "allocator.h"

#include <string.h>
#include <mpi.h>


static pthread_t mgas_g_comm_thread;

//-- initialization/finalization -----------------------------------------------

static void handler_reply_warmup(const mgas_am_t *am,
                                 const void *buf, size_t nbytes)
{
    int * const *msg = buf;
    int *done = *msg;
    *done = 1;
}

static void handler_request_warmup(const mgas_am_t *am,
                                   const void *buf, size_t nbytes)
{
    mgas_am_reply(handler_reply_warmup, buf, nbytes, am);
}

static void warmup(mgas_proc_t me, size_t n_procs)
{
    size_t i;

    size_t n_warmups = 100;

    mgas_proc_t proc;
    for (proc = 0; proc < n_procs; proc++) {
        for (i = 0; i < n_warmups; i++) {
            DPUTS("proc = %d, i = %ld", proc, i);
            int done = 0;
            int *msg = &done;
            mgas_am_request(handler_request_warmup, &msg, sizeof(msg), proc);

            while (!done)
                mgas_poll();
        }
    }

    mgas_barrier();
}

mgas_bool_t mgas_initialize_with_threads(int *argc, char ***argv,
                                         size_t n_threads)
{
    comm_t *comm = comm_initialize(argc, argv, n_threads);

    mgas_proc_t me = comm_get_pid();
    size_t n_procs = comm_get_n_procs();
    
    globals_initialize(me, n_procs, n_threads, comm);
    mgas_sys_initialize(n_threads);
    
    mgas_g_comm_thread = comm_spawn_helper_thread();

#if MGAS_WARMUP
//    warmup(me, n_procs);
#endif
    
    return MGAS_TRUE;
}

mgas_bool_t mgas_initialize(int *argc, char ***argv)
{
    return mgas_initialize_with_threads(argc, argv, 1);
}

void mgas_finalize(void)
{
    mgas_barrier();

    // FIXME
    mgas_sys_finalize();

    comm_join_helper_thread(mgas_g_comm_thread);
    
    comm_t *comm = globals_get_comm();
    globals_finalize();
    comm_finalize(comm);
}

// for suppressing noreturn warning
#pragma GCC diagnostic ignored "-Wredundant-decls"
int MPI_Abort(MPI_Comm comm, int status) MGAS_NORETURN;
void comm_exit(int status) MGAS_NORETURN;
#pragma GCC diagnostic warning "-Wredundant-decls"

void mgas_exit(int status)
{
    comm_exit(status);
}

void mgas_abort(void)
{
    MPI_Abort(MPI_COMM_WORLD, -1);
}

mgas_proc_t mgas_get_pid(void)
{
    return globals_get_pid();
}

size_t mgas_get_n_procs(void)
{
    return globals_get_n_procs();
}

mgas_thread_t mgas_get_tid(void)
{
    return globals_get_tid();
}

size_t mgas_get_max_threads(void)
{
    return globals_get_n_max_threads();
}

void mgas_conf_output(FILE *f)
{
    const char *seg_config = comm_get_segment_name();

#ifdef DEBUG
    const int debug = 1;
#else
    const int debug = 0;
#endif

    fprintf(f, "GASNET_SEGMENT=%s, DEBUG=%d, MGAS_NO_LOOPBACK=%d,\n"
            "MGAS_INLINE_GLOBALS=%d, MGAS_PROFILE=%d, \n",
            seg_config, debug, MGAS_NO_LOOPBACK, MGAS_INLINE_GLOBALS,
            MGAS_PROFILE);
}

FILE *mgas_get_debug_out(void)
{
    return globals_get_debug_out();
}

void mgas_barrier(void)
{
    comm_barrier();
}

void mgas_barrier_with_poll(mgas_poll_t poll)
{
    comm_barrier_with_poll(poll);
}

void mgas_broadcast(void *p, size_t size, mgas_proc_t root)
{
    comm_broadcast(p, size, root);
}

void mgas_broadcast_with_poll(void *p, size_t size, mgas_proc_t root,
                              mgas_poll_t poll)
{
    comm_broadcast_with_poll(p, size, root, poll);
}

void mgas_gather(void *dst, void *src, size_t size, mgas_proc_t root)
{
    comm_gather(dst, src, size, root);
}


void mgas_gather_with_poll(void *dst, void *src, size_t size, mgas_proc_t root,
                           mgas_poll_t poll)
{
    comm_gather_with_poll(dst, src, size, root, poll);
}

void mgas_reduce_sum_long(long *dst, long *src, size_t size, mgas_proc_t root)
{
    comm_reduce_sum_long(dst, src, size, root);
}

void mgas_reduce_sum_long_with_poll(long *dst, long *src, size_t size,
                                   mgas_proc_t root, mgas_poll_t poll)
{
    comm_reduce_sum_long_with_poll(dst, src, size, root, poll);
}

void mgas_poll(void)
{
    comm_poll();
}

mgas_bool_t mgas_owned(mgasptr_t mp)
{
    gmt_t *gmt = globals_get_gmt();
    return gmt_owned(gmt, mp);
}

mgas_proc_t mgas_home(mgasptr_t mp)
{
    gmt_t *gmt = globals_get_gmt();
    return gmt_calc_home(gmt, mp);
}
