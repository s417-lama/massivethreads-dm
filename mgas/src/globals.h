#ifndef MGAS_GLOBALS_H
#define MGAS_GLOBALS_H

#include "../include/mgas_config.h"
#include "gmt.h"
#include "allocator.h"
#include "mgas_alloc.h"

#include <stdio.h>
#include <stdint.h>

#pragma GCC diagnostic ignored "-Wredundant-decls"

typedef struct comm comm_t;
typedef struct comm_handler comm_handler_t;

void globals_initialize(mgas_proc_t me, size_t n_procs, size_t n_threads,
                        comm_t *comm);
void globals_finalize(void);


enum {
    MGAS_INVALID_PID = UINT64_MAX,
    MGAS_INVALID_TID = UINT64_MAX,

    MGAS_PID_MIGRATING = UINT64_MAX - 1,
};


#if MGAS_INLINE_GLOBALS
#define GLOBALS_INLINE static
#else
#define GLOBALS_INLINE extern
#endif

GLOBALS_INLINE mgas_proc_t globals_get_pid(void);
GLOBALS_INLINE size_t globals_get_n_procs(void);
GLOBALS_INLINE void globals_giant_lock(void);
GLOBALS_INLINE void globals_giant_unlock(void);
GLOBALS_INLINE comm_t *globals_get_comm(void);
GLOBALS_INLINE gmt_t *globals_get_gmt(void);

GLOBALS_INLINE comm_handler_t *globals_get_handler(void);
GLOBALS_INLINE mgas_alloc_t *globals_get_alloc(void);

GLOBALS_INLINE mgas_thread_t globals_get_tid(void);
GLOBALS_INLINE mgas_thread_t globals_get_raw_tid(void);
GLOBALS_INLINE size_t globals_get_n_max_threads(void);
GLOBALS_INLINE FILE *globals_get_debug_out(void);

#pragma GCC diagnostic warning "-Wredundant-decls"

#if MGAS_INLINE_GLOBALS
#include "globals.inl"
#endif

#endif
