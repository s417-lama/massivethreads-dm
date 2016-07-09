#include "ga_alloc.h"
#include "misc.h"


enum ga_alloc_constants {
    GALC_BIN_SIZE = 64,
};


// address allocator
struct ga_alloc {
    ga_range_t *free_bins[GALC_BIN_SIZE];
};


ga_range_t *ga_range_create(mgasptr_t base, size_t size, ga_range_t *next)
{
    ga_range_t *r = (ga_range_t *)mgas_sys_malloc(sizeof(ga_range_t));
    r->base = base;
    r->size = size;
    r->next = next;
    return r;
}

void ga_range_destroy(ga_range_t *r)
{
    mgas_sys_free(r);
}

static void ga_alloc_cut_and_store_range(ga_alloc_t *galc, mgasptr_t rbase,
                                         size_t rsize)
{
    size_t i;

    mgasptr_t mp = rbase;
    size_t size = rsize;
    do {
        size_t sz = 1ULL << __builtin_ctzll(mp);
        while (sz > size)
            sz >>= 1UL;

        ga_range_t *r = ga_range_create(mp, sz, NULL);
        ga_alloc_free_address(galc, r);

        mp += sz;
        size -= sz;
    } while (size > 0);
}

ga_alloc_t *ga_alloc_create(mgasptr_t base, size_t size)
{
    size_t i;

    ga_alloc_t *galc = (ga_alloc_t *)mgas_sys_malloc(sizeof(ga_alloc_t));
    for (i = 0; i < GALC_BIN_SIZE; i++) {
        galc->free_bins[i] = NULL;
    }
    ga_alloc_cut_and_store_range(galc, base, size);

    return galc;
}

void ga_alloc_destroy(ga_alloc_t *galc)
{
    size_t i;

    for (i = 0; i < GALC_BIN_SIZE; i++) {
        ga_range_t *r = galc->free_bins[i];
        while (r != NULL) {
            ga_range_t *next = r->next;
            ga_range_destroy(r);
            r = next;
        }
    }

    mgas_sys_free(galc);
}

static size_t bin_index(size_t size)
{
    return 64 - (size_t)__builtin_clzll(size - 1);
}

ga_range_t *ga_alloc_allocate_address(ga_alloc_t *galc, size_t size)
{
    size_t i;

    size_t bin_idx = bin_index(size);
    size_t real_size = 1ULL << bin_idx;

    // find smallest free range
    ga_range_t *smallest_range = NULL;
    for (i = bin_idx; i < GALC_BIN_SIZE; i++) {
        ga_range_t *r = galc->free_bins[i];
        if (r != NULL) {
            MGAS_ASSERT(real_size <= r->size);

            galc->free_bins[i] = r->next;
            smallest_range = r;
            smallest_range->next = NULL;
            break;
        }
    }

    // if no appropriate chunk is found, return NULL.
    if (smallest_range == NULL)
        return NULL;

    MGAS_ASSERT(smallest_range->size >= real_size);

    // divide a large range into small ranges.
    ga_range_t *r = smallest_range;
    while (r->size >= real_size * 2) {
        // divide a range into 2 ranges
        size_t left_size = r->size / 2;
        size_t right_size = r->size - left_size;
        ga_range_t *left = ga_range_create(r->base, left_size, NULL);
        ga_range_t *right = ga_range_create(r->base + left_size, right_size,
                                            NULL);
        ga_range_destroy(r);

        MGAS_ASSERT(left_size >= real_size);

        r = left;
        ga_alloc_free_address(galc, right);
    }
    smallest_range = r;

    MGAS_ASSERT(smallest_range->size == real_size);
    return smallest_range;
}

void ga_alloc_free_address(ga_alloc_t *galc, ga_range_t *r)
{
    size_t idx = bin_index(r->size);

    r->size = 1ULL << idx;
    r->next = galc->free_bins[idx];
    galc->free_bins[idx] = r;
}
