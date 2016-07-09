#ifndef MGAS_MEMORY_V_H
#define MGAS_MEMORY_V_H

#include "../include/mgas.h"
#include "../include/mgas_debug.h"
#include "../include/mgas_prof.h"
#include "misc.h"


typedef struct {
    mgasptr_t mp;
    void *p;
    size_t size;
} mem_pair_t;

#if MGAS_PROFILE
static void mgas_prof_countp(mgas_prof_kind_t kind, const mem_pair_t *pairs,
                             size_t n_pairs)
{
    size_t i;
    for (i = 0; i < n_pairs; i++)
        mgas_prof_count(kind, pairs[i].size);
}
#else
static void mgas_prof_countp(mgas_prof_kind_t kind, const mem_pair_t *pairs,
                             size_t n_pairs)
{
}
#endif


typedef enum mgas_access {
    MGAS_ACCESS_PUT,
    MGAS_ACCESS_GET,
    MGAS_ACCESS_OWN,
} mgas_access_t;

static const char *
string_of_access(mgas_access_t access)
{
    switch (access) {
    case MGAS_ACCESS_PUT:
        return "PUT";
    case MGAS_ACCESS_GET:
        return "GET";
    case MGAS_ACCESS_OWN:
        return "OWN";
    default:
        MGAS_NOT_REACHED;
    }
}


void mgas_copy_v(const mem_pair_t pairs[], size_t n_pairs,
                 mgas_access_t access);

void mgas_own_v(const mgas_vector_t *mvs, size_t n_mvs);


static void
print_pairs(const mem_pair_t pairs[], size_t n_pairs)
{
    size_t i;
    for (i = 0; i < n_pairs; i++) {
        DPUTS("pairs[%zu] = { 0x%"PRIx64", %p, %zu }",
              i, pairs[i].mp, pairs[i].p, pairs[i].size);
    }
}


#endif
