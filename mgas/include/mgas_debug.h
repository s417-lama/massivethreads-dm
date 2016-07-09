#ifndef MOND_DEBUG_H
#define MOND_DEBUG_H

#include "mgas_config.h"
#include "mgas.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <inttypes.h>
#include <sys/time.h>

FILE *mgas_get_debug_out(void);
mgas_thread_t mgas_get_raw_tid(void);

#define MGAS_DEBUG_PRINT(type, format, ...)                             \
    (((mgas_get_raw_tid() != UINT64_MAX)                                \
      ? fprintf(mgas_get_debug_out(),                                   \
                "%c %05zd:%2zd: %-20.20s :: " format "\n",              \
                (type), mgas_get_pid(), mgas_get_raw_tid(),             \
                __FUNCTION__, ##__VA_ARGS__)                            \
      : fprintf(mgas_get_debug_out(),                                   \
                "%c %05zd: -: %-20.20s :: " format "\n",                \
                (type), mgas_get_pid(),                                 \
                __FUNCTION__, ##__VA_ARGS__)),                          \
     fflush(mgas_get_debug_out()),                                      \
     0)


#ifdef DEBUG

#define DPUTS(fmt, ...)    MGAS_DEBUG_PRINT('D', fmt, ##__VA_ARGS__)

static int abort_(void)
{
    mgas_abort();
    return 0;
}

#define MGAS_ASSERT(cond)                                               \
    ((cond) ? 1                                                         \
     : (MGAS_DEBUG_PRINT('E', "assertion " #cond " failed at %s:%d.\n", \
                         __FILE__, __LINE__), abort_()))

#else
#define DPUTS(fmt, ...) do {} while (0)
#define MGAS_ASSERT(cond) 0
#endif

#define PRI_COLOR_BEGIN(color_num)  "\x1b[" #color_num "m"
#define PRI_COLOR_END               PRI_COLOR_BEGIN(39)

#define DPUTSR(fmt, ...) DPUTS(PRI_COLOR_BEGIN(31) fmt PRI_COLOR_END, \
                               ##__VA_ARGS__)

#define MGAS_CHECK(cond) do {                                           \
        if (!(cond)) {                                                  \
            MGAS_DEBUG_PRINT('E', "check '" #cond "' failed at %s:%d.\n", \
                             __FILE__, __LINE__);                       \
            mgas_abort();                                               \
        }                                                               \
    } while (0)

#define MGAS_NOT_REACHED  do {                          \
        MGAS_DEBUG_PRINT('E', "NOT REACHED AT %s:%d.",  \
                         __FILE__, __LINE__);           \
        mgas_abort();                                   \
    } while (0)

#define MGAS_UNDEFINED do {                                     \
        MGAS_DEBUG_PRINT('E', "NOT IMPLEMENTED AT %s:%d.",      \
                         __FILE__, __LINE__);                   \
        mgas_abort();                                           \
    } while (0)


#if MGAS_COMM_LOG_ENABLED
#define MGAS_COMM_LOG(fmt, ...) \
    MGAS_DEBUG_PRINT('C', "comm: " fmt, ##__VA_ARGS__)
#else
#define MGAS_COMM_LOG(fmt, ...) \
    do {} while (0)
#endif

typedef int64_t tsc_t;

static inline tsc_t rdtsc(void)
{
#if (defined __i386__) || (defined __x86_64__)
  uint32_t hi,lo;
  asm volatile("lfence\nrdtsc" : "=a"(lo),"=d"(hi));
  return (tsc_t)((uint64_t)hi)<<32 | lo;
#elif (defined __sparc__) && (defined __arch64__)
  uint64_t tick;
  asm volatile("rd %%tick, %0" : "=r" (tick));
  return (tsc_t)tick;
#else
#warning "rdtsc() is not implemented."
  return 0;
#endif
}

static inline tsc_t rdtsc_nofence(void)
{
#if (defined __i386__) || (defined __x86_64__)
  uint32_t hi,lo;
  asm volatile("rdtsc" : "=a"(lo),"=d"(hi));
  return (tsc_t)((uint64_t)hi)<<32 | lo;
#elif (defined __sparc__) && (defined __arch64__)
  uint64_t tick;
  asm volatile("rd %%tick, %0" : "=r" (tick));
  return (tsc_t)tick;
#else
#warning "rdtsc() is not implemented."
  return 0;
#endif
}

static inline double now(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec*1e-6;
}

#define DPUTP(p) DPUTS("%s = %p", #p, (p))
#define DPUTSZ(size) DPUTS("%s = %zu", #size, (size))
#define DPUTMP(mp) DPUTS("%s = 0x%"PRIx64, #mp, (mp));
#define DPUTMV(mv) DPUTS("%s = { 0x%"PRIx64", %zu }", #mv, (mv)->mp, (mv)->size)
#define DPUTV(v)   DPUTS("%s = { %p, %zu }", #v, (v)->p, (v)->size)

/*
#define DPUTMVS(mvs, n_mvs)  dput_mvs(#mvs, mvs, n_mvs)
static void dput_mvs(const char *s, const mgas_vector_t mvs[], size_t n_mvs)
{
    size_t n = n_mvs < 3 ? n_mvs : 3;

    DPUTS("%s:", s);
    size_t i;
    for (i = 0; i < n; i++) {
        const mgas_vector_t *mv = &mvs[i];
        DPUTMV(mv);
    }
}
*/

#endif
