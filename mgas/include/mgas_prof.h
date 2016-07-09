#ifndef MGAS_PROFILE_H
#define MGAS_PROFILE_H

#include <inttypes.h>
#include "mgas_config.h"
#include "mgas.h"
#include "mgas_debug.h"


typedef enum mgas_prof_kind {
    MGAS_PROF_TOTAL,

    MGAS_PROF_LOCALIZE,
    MGAS_PROF_COMMIT,
    MGAS_PROF_UNLOCALIZE,

    MGAS_PROF_OWN,
    MGAS_PROF_CACHE,
    MGAS_PROF_CACHE_SEARCH,
    MGAS_PROF_UNIQ_BLOCK_PTRS,
    MGAS_PROF_CACHE_UPDATE,
    MGAS_PROF_COPY_GET,
    MGAS_PROF_COPY_PUT,
    MGAS_PROF_COPY_OWN,

    MGAS_PROF_COMM_SYS,
    MGAS_PROF_COMM_GET,
    MGAS_PROF_COMM_PUT,
    MGAS_PROF_COMM_MEMGET,
    MGAS_PROF_COMM_MEMPUT,
    MGAS_PROF_COMM_WAIT,
    MGAS_PROF_COMM_WAIT_OWNREQ,
    MGAS_PROF_COMM_WAIT_DATAREQ,

    MGAS_PROF_GET_ZEROFILL,
    MGAS_PROF_COMM_HANDLE_PUTV,

    MGAS_PROF_PART_HOME,
    MGAS_PROF_REQ_OWNERS,
    MGAS_PROF_PREPARE_PAGE,
    MGAS_PROF_PART_OWNER,
    MGAS_PROF_REQ_DATA,
    MGAS_PROF_REQ_OWNCHG,
    MGAS_PROF_OVH_PROC_OWR,
    MGAS_PROF_OVH_PROC_DAT,
    MGAS_PROF_OVH_PROC_OWC,

    MGAS_PROF_PAGE_HIT,
    MGAS_PROF_PAGE_MISS,
    MGAS_PROF_PAGE_TOUCH,
    MGAS_PROF_PAGE_GIVEUP,
    MGAS_PROF_PAGE_RETRY,
    MGAS_PROF_CACHE_HIT,
    MGAS_PROF_CACHE_MISS,
    MGAS_PROF_HOME_HIT,
    MGAS_PROF_HOME_MISS,

    // for MassiveThreads/DM
    MGAS_PROF_WORK,
   
    MGAS_N_PROF_KINDS,
} mgas_prof_kind_t;

typedef struct mgas_prof_data {
    size_t count;
    tsc_t tsc;
    tsc_t min;
    tsc_t max;
    tsc_t tmp;
} mgas_prof_data_t;

typedef struct mgas_prof_accum {
    mgas_prof_data_t data[MGAS_N_PROF_KINDS];
} mgas_prof_accum_t;

typedef enum mgas_mem_kind {
    MGAS_MEM_COMM_SYS,
    MGAS_MEM_COMM,
} mgas_mem_kind_t;

typedef enum mgas_mem_subkind {
    MGAS_MEM_ALLOC,
    MGAS_MEM_FREE,
} mgas_mem_subkind_t;

typedef enum mgas_log_kind {
    MGAS_LOG_COMPUTE,
    MGAS_LOG_LOCALIZE,
    MGAS_LOG_COMMIT,
    MGAS_LOG_UNLOCALIZE,
    MGAS_LOG_PUT,
    MGAS_LOG_GET,
    MGAS_LOG_RMW,
    MGAS_LOG_COLLECTIVE,
    MGAS_LOG_COMM_MSG,
    MGAS_LOG_COMM_DATA,
    MGAS_LOG_COMM_OWNER_WAIT,
    MGAS_LOG_COMM_DATA_WAIT,
    MGAS_LOG_HANDLER,

    // for MassiveThreads/DM
    MGAS_LOG_WORK,
    MGAS_LOG_STEAL,
    MGAS_LOG_STEAL_HANDLE,
    MGAS_LOG_EXIT,
    MGAS_LOG_WAIT,
    MGAS_LOG_YIELD,
    MGAS_LOG_DISTRIB,
    MGAS_LOG_DEAD,
    MGAS_LOG_COLL,
    MGAS_LOG_MIGRATE,

    MGAS_N_LOG_KINDS,
} mgas_log_kind_t;

typedef enum mgas_log_subkind {
    MGAS_LOG_BEGIN,
    MGAS_LOG_END,
} mgas_log_subkind_t;

typedef struct mgas_log_entry {
    mgas_log_kind_t kind;
    mgas_log_subkind_t subkind;
    tsc_t time;
} mgas_log_entry_t;

typedef struct mgas_logger {
    tsc_t base_time;
    size_t capacity;
    size_t count;
    mgas_log_entry_t *entries;
    ssize_t entry_counts[MGAS_N_LOG_KINDS];
    mgas_bool_t enabled[MGAS_N_LOG_KINDS];
} mgas_logger_t;

typedef struct profile {
    mgas_bool_t enabled;      // whether profiling is enabled or not.
    tsc_t sys_tsc_start;

    tsc_t tsc_start;          // time profiling starts
    tsc_t tsc_stop;           // time profiling stops

    mgas_logger_t logger;     // event logs

    mgas_prof_accum_t accum;  // accumulated profiling data
} profile_t;

#if MGAS_INLINE_GLOBALS
static profile_t *globals_get_profile(void);
#else
extern profile_t *globals_get_profile(void);
#endif

#include "../src/globals.h"


static void mgas_prof_accum_initialize(mgas_prof_accum_t *accum)
{
    size_t i;

    mgas_prof_data_t initial_value = { 0, 0, INT64_MAX, 0, 0 };
    for (i = 0; i < MGAS_N_PROF_KINDS; i++)
        accum->data[i] = initial_value;
}

static void mgas_prof_accum_finalize(mgas_prof_accum_t *accum)
{
}

static void mgas_logger_initialize(mgas_logger_t *logger)
{
    size_t i;

    tsc_t base_time = rdtsc();

    size_t initial_capacity = 8192;
    size_t size = sizeof(mgas_log_entry_t) * initial_capacity;
    mgas_log_entry_t *entries = mgas_sys_malloc(size);

    logger->base_time = base_time;
    logger->entries = entries;
    logger->count = 0;
    logger->capacity = initial_capacity;
    memset(logger->entry_counts, 0, MGAS_N_LOG_KINDS * sizeof(ssize_t));
    memset(logger->enabled, 0, MGAS_N_LOG_KINDS * sizeof(mgas_bool_t));

    mgas_log_kind_t enabled_kinds[] = {
        MGAS_LOG_COMPUTE,
        MGAS_LOG_LOCALIZE,
        MGAS_LOG_COMMIT,
        MGAS_LOG_UNLOCALIZE,
        MGAS_LOG_PUT,
        MGAS_LOG_GET,
        MGAS_LOG_RMW,
        MGAS_LOG_COLLECTIVE,
        MGAS_LOG_COMM_MSG,
        MGAS_LOG_COMM_DATA,
        MGAS_LOG_COMM_OWNER_WAIT,
        MGAS_LOG_COMM_DATA_WAIT,
        MGAS_LOG_HANDLER,
        MGAS_LOG_WORK,
        MGAS_LOG_STEAL,
        MGAS_LOG_STEAL_HANDLE,
        MGAS_LOG_EXIT,
        MGAS_LOG_WAIT,
        MGAS_LOG_YIELD,
        MGAS_LOG_DISTRIB,
        MGAS_LOG_DEAD,
        MGAS_LOG_COLL,
        MGAS_LOG_MIGRATE,
    };
    size_t n_enabled_kinds = sizeof(enabled_kinds) / sizeof(enabled_kinds[0]);

    for (i = 0; i < n_enabled_kinds; i++)
        logger->enabled[enabled_kinds[i]] = MGAS_TRUE;
}

static void mgas_logger_finalize(mgas_logger_t *logger)
{
    mgas_sys_free(logger->entries);
}

static void mgas_logger_add(mgas_logger_t *logger, mgas_log_kind_t kind,
                            mgas_log_subkind_t subkind)
{
    if (!logger->enabled[kind])
        return;
#if MGAS_ENABLE_NESTED_COMM_PROCESS
    // omit overlapped entries
    if (subkind == MGAS_LOG_BEGIN) {
        logger->entry_counts[kind] += 1;
        if (logger->entry_counts[kind] != 1)
            return;
    } else if (subkind == MGAS_LOG_END) {
        MGAS_ASSERT(logger->entry_counts[kind] >= 1);
        logger->entry_counts[kind] -= 1;
        if (logger->entry_counts[kind] != 0)
            return;
    }
#else
    // does not permit overlapped entries
    if (subkind == MGAS_LOG_BEGIN) {
        MGAS_ASSERT(logger->entry_counts[kind] == 0);
        logger->entry_counts[kind] += 1;
    } else {
        MGAS_ASSERT(logger->entry_counts[kind] == 1);
        logger->entry_counts[kind] -= 1;
    }
#endif

    if (logger->count >= logger->capacity) {
        size_t new_capacity = logger->capacity * 2;

        size_t size = sizeof(mgas_log_entry_t) * new_capacity;
        mgas_log_entry_t *new_entries = mgas_sys_realloc(logger->entries, size);

        MGAS_ASSERT(new_entries != NULL);

        logger->entries = new_entries;
        logger->capacity = new_capacity;
    }

    tsc_t time = rdtsc() - logger->base_time;

    mgas_log_entry_t *entry = &logger->entries[logger->count++];
    entry->kind = kind;
    entry->subkind = subkind;
    entry->time = time;
}

void mpi_barrier(void);
void mpi_broadcast(void *p, size_t size, mgas_proc_t root);

static void mgas_prof_initialize(profile_t *p)
{
    p->enabled = MGAS_FALSE;

    p->tsc_start = 0;
    p->tsc_stop = 0;

    mpi_barrier();
    long long t0 = rdtsc();
    mpi_barrier();
    long long t1 = rdtsc();

    long long diff = t1 - t0;
    long long diff0 = diff;
    mpi_broadcast(&diff0, sizeof(diff0), 0);

    p->sys_tsc_start = t0 + diff - diff0;

    mgas_prof_accum_initialize(&p->accum);
    mgas_logger_initialize(&p->logger);
}

static void mgas_prof_finalize(profile_t *p)
{
    mgas_prof_accum_finalize(&p->accum);
    mgas_logger_finalize(&p->logger);
}

static void mgas_prof_begin(mgas_prof_kind_t kind);
static void mgas_prof_end(mgas_prof_kind_t kind);

#if MGAS_PROFILE

static void mgas_prof_begin(mgas_prof_kind_t kind)
{
    profile_t *p = globals_get_profile();
    mgas_prof_data_t *data = &p->accum.data[kind];

    if (p->enabled)
        data->tmp = rdtsc();
}

static void mgas_prof_end(mgas_prof_kind_t kind)
{
    profile_t *p = globals_get_profile();
    mgas_prof_data_t *data = &p->accum.data[kind];

    if (p->enabled) {
        data->count += 1;

        tsc_t t = rdtsc() - data->tmp;
        data->tsc += t;
        data->min = (data->min <= t) ? data->min : t;
        data->max = (data->max >= t) ? data->max : t;
    }
}

static void mgas_prof_count(mgas_prof_kind_t kind, size_t n)
{
    profile_t *p = globals_get_profile();
    mgas_prof_data_t *data = &p->accum.data[kind];

    if (p->enabled)
        data->count += n;
}

static void mgas_prof_alloc(mgas_mem_kind_t kind, size_t size)
{
    // TODO
}

static void mgas_prof_free(mgas_mem_kind_t kind, void *p)
{
    // TODO
}

static void mgas_prof_log(mgas_log_kind_t kind, mgas_log_subkind_t subkind)
{
#if 1
    profile_t *p = globals_get_profile();
    if (p->enabled)
        mgas_logger_add(&p->logger, kind, subkind);
#endif
}

static void mgas_prof_enable_log(mgas_log_kind_t kind)
{
    profile_t *p = globals_get_profile();
    p->logger.enabled[kind] = MGAS_TRUE;
}

static void mgas_prof_disable_log(mgas_log_kind_t kind)
{
    profile_t *p = globals_get_profile();
    p->logger.enabled[kind] = MGAS_FALSE;
}


static void mgas_prof_data_print(FILE *f, const profile_t *p,
                                 mgas_prof_kind_t kind,
                                 const char *name)
{
    mgas_proc_t me = globals_get_pid();
    size_t n_procs = globals_get_n_procs();

    const mgas_prof_data_t *data = &p->accum.data[kind];

    size_t count = data->count;
    tsc_t tsc = data->tsc;
    tsc_t min = (data->min == INT64_MAX) ? 0 : data->min;
    tsc_t max = data->max;
    double ratio = 100.0 * (double)tsc / (double)(p->tsc_stop - p->tsc_start);

    fprintf(f,
            "%4zu,%4zu, %-16s, %10"PRId64", %6.2f, %8zu, %10.0f, "
            "%10"PRId64", %10"PRId64"\n",
            me, n_procs, name, tsc, ratio, count, (double)tsc / (double)count,
            min, max);
}

#else

static void mgas_prof_begin(mgas_prof_kind_t kind) {}
static void mgas_prof_end(mgas_prof_kind_t kind) {}
static void mgas_prof_count(mgas_prof_kind_t kind, size_t n) {}
static void mgas_prof_alloc(mgas_mem_kind_t kind, size_t size) {}
static void mgas_prof_free(mgas_mem_kind_t kind, void *p) {}
static void mgas_prof_log(mgas_log_kind_t kind, mgas_log_subkind_t subkind) {}
static void mgas_prof_disable_log(mgas_log_kind_t kind) {}
static void mgas_prof_data_print(FILE *f, const profile_t *p,
                                 mgas_prof_kind_t kind,
                                 const char *name) {}

#endif

#define MGAS_PROF_BEGIN(name)       mgas_prof_begin(MGAS_PROF_##name)
#define MGAS_PROF_END(name)         mgas_prof_end(MGAS_PROF_##name)

#define MGAS_PROF_COUNT(name, n)    mgas_prof_count(MGAS_PROF_##name, n)
#define MGAS_PROF_ALLOC(name, size) mgas_prof_alloc(MGAS_MEM_##name, size);
#define MGAS_PROF_FREE(name, p)     mgas_prof_free(MGAS_MEM_##name, p);



#define MGAS_LOG_KIND(kind, subkind)                                    \
    do {                                                                \
        mgas_prof_log((kind), MGAS_LOG_##subkind);                      \
        if (MGAS_LOG_##subkind == MGAS_LOG_BEGIN) {                     \
            mgas_prof_begin((mgas_prof_kind_t)(kind));                  \
        } else if (MGAS_LOG_##subkind == MGAS_LOG_END) {                \
            mgas_prof_end((mgas_prof_kind_t)(kind));                    \
        } else {                                                        \
            MGAS_NOT_REACHED;                                           \
        }                                                               \
    } while (0)

#define MGAS_LOG(name, subkind)     MGAS_LOG_KIND(MGAS_LOG_##name, subkind)

#define MGAS_PROF_DATA_PRINT(f, p, name)                        \
    mgas_prof_data_print(f, (p), (MGAS_PROF_##name), #name)

static void mgas_prof_start(void)
{
    profile_t *p = globals_get_profile();
   
    p->tsc_start = rdtsc();
    p->enabled = MGAS_TRUE;

    MGAS_PROF_BEGIN(TOTAL);
}

static void mgas_prof_stop(void)
{
    profile_t *p = globals_get_profile();

    MGAS_PROF_END(TOTAL);

    p->enabled = MGAS_FALSE;
    p->tsc_stop = rdtsc();
}

static tsc_t mgas_prof_tsc(void)
{
    profile_t *p = globals_get_profile();
    return rdtsc() - p->sys_tsc_start;
}


typedef struct mgas_log_header {
    const char *name;
    uint8_t red;
    uint8_t green;
    uint8_t blue;
} mgas_log_header_t;

#define MGAS_LOG_HEADER_INIT(name, red, green, blue) \
    { #name, red, green, blue }

static void mgas_logger_output(FILE *f, const mgas_logger_t *logger)
{
    size_t i;
    mgas_proc_t me = globals_get_pid();

    // categories definition
    mgas_log_header_t headers[] = {
        { "COMPUTE", 255, 0, 0 },
        { "LOCALIZE", 0, 0, 255 },
        { "COMMIT", 0, 255, 0 },
        { "UNLOCALIZE", 0, 0, 0 },
        { "PUT", 0, 255, 0 },
        { "GET", 0, 0, 255 },
        { "RMW", 0, 0, 0 },
        { "COLLECTIVE", 255, 255, 255 },
        { "COMM_MSG", 255, 0, 255 },
        { "COMM_DATA", 0, 255, 255 },
        { "COMM_OWNER_WAIT", 128, 128, 128 },
        { "COMM_DATA_WAIT", 128, 128, 128 },
        { "HANDLER", 255, 255, 0 },

        { "WORK", 255, 0, 0 },
        { "STEAL", 255, 255, 255 },
        { "STEAL_HANDLE", 128, 128, 128 },
        { "EXIT", 0, 0, 255 },
        { "WAIT", 0, 0, 255 },
        { "YIELD", 0, 0, 255 },
        { "DISTRIB", 0, 0, 255 },
        { "DEAD", 0, 0, 255 },
        { "COLL", 0, 0, 255 },
        { "MIGRATE", 0, 255, 0 },
    };
    size_t n_headers = sizeof(headers) / sizeof(headers[0]);
    MGAS_ASSERT(n_headers == MGAS_N_LOG_KINDS);

    // output header
    for (i = 0; i < n_headers; i++) {
        const mgas_log_header_t *h = &headers[i];
        fprintf(f, "category,%s,state,%u,%u,%u,200,1\n",
                h->name, h->red, h->green, h->blue);
    }

    const mgas_log_entry_t *begin_entries[MGAS_N_LOG_KINDS];
    for (i = 0; i < MGAS_N_LOG_KINDS; i++)
        begin_entries[i] = NULL;

    // output entries
    const mgas_log_entry_t *entries = logger->entries;
    for (i = 0; i < logger->count; i++) {
        const mgas_log_entry_t *entry = &entries[i];
        mgas_log_kind_t kind = entry->kind;
        mgas_log_subkind_t subkind = entry->subkind;

        if (subkind == MGAS_LOG_BEGIN) {
            MGAS_ASSERT(begin_entries[kind] == NULL);
            begin_entries[kind] = entry;
        } else if (subkind == MGAS_LOG_END) {
            MGAS_ASSERT(begin_entries[kind] != NULL);

            double divider = 1000 * 1000 * 1000;
            double begin = (double)begin_entries[kind]->time / divider;
            double end = (double)entry->time / divider;

            fprintf(f, "%s,%.6f,%.6f,%zd,\n",
                    headers[kind].name, begin, end, me);

            begin_entries[kind] = NULL;
        }
    }
}

static void mgas_prof_output(FILE *f)
{
    size_t i;

    mgas_proc_t me = globals_get_pid();
    profile_t *p = globals_get_profile();

    MGAS_PROF_DATA_PRINT(f, p, TOTAL);

    MGAS_PROF_DATA_PRINT(f, p, LOCALIZE);
    MGAS_PROF_DATA_PRINT(f, p, COMMIT);
    MGAS_PROF_DATA_PRINT(f, p, UNLOCALIZE);

    MGAS_PROF_DATA_PRINT(f, p, OWN);
    MGAS_PROF_DATA_PRINT(f, p, CACHE);
    MGAS_PROF_DATA_PRINT(f, p, CACHE_SEARCH);
    MGAS_PROF_DATA_PRINT(f, p, UNIQ_BLOCK_PTRS);
    MGAS_PROF_DATA_PRINT(f, p, CACHE_UPDATE);
    MGAS_PROF_DATA_PRINT(f, p, COPY_GET);
    MGAS_PROF_DATA_PRINT(f, p, COPY_PUT);
    MGAS_PROF_DATA_PRINT(f, p, COPY_OWN);

    MGAS_PROF_DATA_PRINT(f, p, COMM_SYS);
    MGAS_PROF_DATA_PRINT(f, p, COMM_GET);
    MGAS_PROF_DATA_PRINT(f, p, COMM_PUT);
    MGAS_PROF_DATA_PRINT(f, p, COMM_MEMGET);
    MGAS_PROF_DATA_PRINT(f, p, COMM_MEMPUT);
    MGAS_PROF_DATA_PRINT(f, p, COMM_WAIT);
    MGAS_PROF_DATA_PRINT(f, p, COMM_WAIT_OWNREQ);
    MGAS_PROF_DATA_PRINT(f, p, COMM_WAIT_DATAREQ);

    MGAS_PROF_DATA_PRINT(f, p, PART_HOME);
    MGAS_PROF_DATA_PRINT(f, p, REQ_OWNERS);
    MGAS_PROF_DATA_PRINT(f, p, PREPARE_PAGE);
    MGAS_PROF_DATA_PRINT(f, p, PART_OWNER);
    MGAS_PROF_DATA_PRINT(f, p, REQ_DATA);
    MGAS_PROF_DATA_PRINT(f, p, REQ_OWNCHG);
    MGAS_PROF_DATA_PRINT(f, p, OVH_PROC_OWR);
    MGAS_PROF_DATA_PRINT(f, p, OVH_PROC_DAT);
    MGAS_PROF_DATA_PRINT(f, p, OVH_PROC_OWC);

    MGAS_PROF_DATA_PRINT(f, p, GET_ZEROFILL);
    MGAS_PROF_DATA_PRINT(f, p, COMM_HANDLE_PUTV);

 #if 0
    MGAS_PROF_DATA_PRINT(f, p, CACHE_HIT);
    MGAS_PROF_DATA_PRINT(f, p, CACHE_MISS);
    MGAS_PROF_DATA_PRINT(f, p, PAGE_HIT);
    MGAS_PROF_DATA_PRINT(f, p, PAGE_MISS);
    MGAS_PROF_DATA_PRINT(f, p, PAGE_TOUCH);
    MGAS_PROF_DATA_PRINT(f, p, PAGE_RETRY);
    MGAS_PROF_DATA_PRINT(f, p, PAGE_GIVEUP);
    MGAS_PROF_DATA_PRINT(f, p, HOME_HIT);
    MGAS_PROF_DATA_PRINT(f, p, HOME_MISS);
#endif

    MGAS_PROF_DATA_PRINT(f, p, WORK);
}

static void mgas_log_output(FILE *f)
{
    profile_t *p = globals_get_profile();

    mgas_logger_output(f, &p->logger);
}

#undef MGAS_PROF_DATA_PRINT

#endif
