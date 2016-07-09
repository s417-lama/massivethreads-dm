#ifndef MYTH_ALLOC_H_
#define MYTH_ALLOC_H_

#include <stddef.h>

struct myth_alloc;
typedef struct myth_alloc *myth_alloc_t;

typedef void *(*myth_mmap_t)(size_t len);
typedef void *(*myth_aligned_mmap_t)(size_t len, size_t align);

myth_alloc_t myth_alloc_create(size_t nthreads,myth_mmap_t my_mmap,
                               myth_aligned_mmap_t my_ammap);
void myth_alloc_destroy(myth_alloc_t alc);
void *myth_alloc_malloc(myth_alloc_t alc,size_t rank,size_t size);
void myth_alloc_free(myth_alloc_t alc,size_t rank,size_t size,void *ptr);
void *myth_alloc_realloc(myth_alloc_t alc,size_t rank,size_t oldsize,void *ptr,size_t size);
void *myth_alloc_aligned_malloc(myth_alloc_t alc, size_t rank, size_t size,
                                size_t align);
void myth_alloc_aligned_free(myth_alloc_t alc, size_t rank, size_t size, void *p);

#endif


















