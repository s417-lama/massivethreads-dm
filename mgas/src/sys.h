#ifndef SYS_H
#define SYS_H

#include <stddef.h>

enum {
    OS_PAGE_SIZE = 4096,
};


void mgas_sys_initialize(size_t n_threads);
void mgas_sys_finalize(void);

void mgas_die(const char *s);

void *mgas_sys_malloc(size_t size);
void mgas_sys_free(void *p);
void *mgas_sys_realloc(void *p, size_t size);

void *mgas_sys_memalign(size_t size, size_t alignment);

#endif
