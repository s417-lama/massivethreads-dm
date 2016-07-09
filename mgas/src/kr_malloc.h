#ifndef MGAS_KR_MALLOC_H
#define MGAS_KR_MALLOC_H

#include "mgas_debug.h"
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

/* memory allocator type */
typedef void *(*mgas_mmap_t)(size_t len);
typedef void *(*mgas_aligned_mmap_t)(size_t len, size_t align);

typedef struct alc_header {
    struct alc_header *next;
    uintptr_t size;
} alc_header_t;

typedef struct {
    alc_header_t *free_list;
    alc_header_t *header;
    mgas_mmap_t mmap;
    mgas_aligned_mmap_t aligned_mmap;
} mgas_kr_malloc_t;

/* memory allocator functions */
static void mgas_kr_malloc_init(mgas_kr_malloc_t *alc,
                                mgas_mmap_t my_mmap,
                                mgas_aligned_mmap_t aligned_mmap)
{
    alc_header_t *header = malloc(sizeof(alc_header_t));
    header->next = header;
    header->size = 0;
    
    alc->free_list = header;
    alc->header = header;
    alc->mmap = my_mmap;
    alc->aligned_mmap = aligned_mmap;
}

static void mgas_kr_malloc_finalize(mgas_kr_malloc_t *alc)
{
    free(alc->header);
}

static void mgas_kr_malloc_deallocate(mgas_kr_malloc_t *alc, void *p);

static void * mgas_kr_malloc_allocate(mgas_kr_malloc_t *alc, size_t size)
{
    const size_t min_size = 2 * 1024 * 1024;

    size_t n_units =
        ((size + sizeof(alc_header_t) - 1)) / sizeof(alc_header_t) + 1;

    alc_header_t *prev = alc->free_list;
    alc_header_t *h = prev->next;
    for (;;) {
        if (h->size >= n_units)
            break;

        if (prev == alc->free_list) {
            size_t actual_size = sizeof(alc_header_t) * n_units;

            if (actual_size < min_size)
                actual_size = min_size;

//             fprintf(stderr, "  req_size = %zu, actual_size = %zu\n",
//                     sizeof(alc_header_t) * n_units, actual_size);

            alc_header_t *new_header = alc->mmap(actual_size);

            if (new_header == NULL)
                return NULL;

            size_t new_n_units = actual_size / sizeof(alc_header_t);

            new_header->next = NULL;
            new_header->size = new_n_units;

            mgas_kr_malloc_deallocate(alc, (void *)(new_header + 1));

            // retry this loop
            prev = alc->free_list;
        }

        prev = h;
        h = h->next;
    }

    if (h->size == n_units) {
        prev->next = h->next;
    } else {
        h->size -= n_units;

        h += h->size;
        h->size = n_units;
    }

    alc->free_list = prev;
    h->next = NULL;

    return (void *)(h + 1);
}

static void mgas_kr_malloc_deallocate(mgas_kr_malloc_t *alc, void *p)
{
    alc_header_t *header = (alc_header_t *)p - 1;

    alc_header_t *h = alc->free_list;
    for (;;) {
        if (h < header && header < h->next)
            break;

        if (h >= h->next && (h < header || header < h->next))
            break;

        h = h->next;
    }

    if (header + header->size == h->next) {
        header->size += h->next->size;
        header->next = h->next->next;
    } else {
        header->next = h->next;
    }

    if (h + h->size == header) {
        h->size += header->size;
        h->next = header->next;
    } else {
        h->next = header;
    }

    alc->free_list = h;
}

#endif
