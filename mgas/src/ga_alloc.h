#ifndef GA_ALLOC_H
#define GA_ALLOC_H

#include "../include/mgas.h"
#include <stddef.h>


typedef struct ga_range {
    mgasptr_t base;
    size_t size;
    struct ga_range *next;
} ga_range_t;

typedef struct ga_alloc ga_alloc_t;

ga_range_t *ga_range_create(mgasptr_t base, size_t size, ga_range_t *next);
void ga_range_destroy(ga_range_t *r);

ga_alloc_t *ga_alloc_create(mgasptr_t base, size_t size);
void ga_alloc_destroy(ga_alloc_t *galc);
ga_range_t *ga_alloc_allocate_address(ga_alloc_t *galc, size_t size);
void ga_alloc_free_address(ga_alloc_t *galc, ga_range_t *r);


#endif
