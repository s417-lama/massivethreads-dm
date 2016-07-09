#include "../include/mgas.h"
#include "../include/mgas_debug.h"
#include "comm.h"

#include <string.h>


typedef struct syncvar {
    mgas_bool_t filled;    // protected by page lock
    uint8_t data[1];
} syncvar_t;

static void syncvar_reset(void *p, size_t size,
                          const void *param_in, size_t param_in_size,
                          void *param_out, size_t param_out_size)
{
    syncvar_t *sv = p;

    sv->filled = MGAS_FALSE;
}

static void syncvar_fill(void *p, size_t size,
                         const void *param_in, size_t param_in_size,
                         void *param_out, size_t param_out_size)
{
    syncvar_t *sv = p;

    MGAS_ASSERT(sv->filled == MGAS_FALSE);

    sv->filled = MGAS_TRUE;
}

static size_t mgas_syncvar_size(size_t data_size)
{
    return offsetof(syncvar_t, data) + data_size;
}

mgas_syncvar_t mgas_syncvar_create(size_t size)
{
    size_t sv_size = mgas_syncvar_size(size);
    mgas_syncvar_t sv = mgas_malloc(sv_size);

    mgas_rmw(syncvar_reset, sv, sizeof(syncvar_t), NULL, 0, NULL, 0);

    return sv;
}

void mgas_syncvar_destroy(mgas_syncvar_t sv, size_t data_size)
{
    size_t size = mgas_syncvar_size(data_size);
    mgas_free_small(sv, size);
}

void mgas_syncvar_put(mgas_syncvar_t sv, void *p, size_t size)
{
    mgas_put(sv + offsetof(syncvar_t, data), p, size);

    mgas_rmw(syncvar_fill, sv, sizeof(syncvar_t), NULL, 0, NULL, 0);
}

mgas_bool_t mgas_syncvar_try_get(mgas_syncvar_t sv, void *p, size_t size)
{
    size_t sv_size = mgas_syncvar_size(size);
    syncvar_t *local_sv = mgas_sys_malloc(sv_size);

    mgas_get(local_sv, sv, sv_size);

    mgas_bool_t filled = local_sv->filled;
    if (filled)
        memcpy(p, local_sv->data, size);

    mgas_sys_free(local_sv);

    return filled;
}

void mgas_syncvar_get(mgas_syncvar_t sv, void *p, size_t size)
{
    size_t sv_size = mgas_syncvar_size(size);

    while (!mgas_syncvar_try_get(sv, p, size))
        comm_poll();
}
