#ifndef MGAS_AM_H
#define MGAS_AM_H

#include "mgas.h"

typedef struct mgas_am mgas_am_t;
typedef void (*mgas_am_func_t)(const mgas_am_t *, const void *, size_t);

void mgas_am_request(mgas_am_func_t f, const void *p, size_t size,
                     mgas_proc_t target);
void mgas_am_reply(mgas_am_func_t f, const void *p, size_t size,
                   const mgas_am_t *am);

mgas_proc_t mgas_am_get_initiator(const mgas_am_t *am);


typedef struct mgas_am_handle mgas_am_handle_t;

mgas_am_handle_t *mgas_am_handle_create(size_t count);
void mgas_am_handle_destroy(mgas_am_handle_t *handle);
void mgas_am_handle_notify(mgas_am_handle_t *handle, size_t count);
void mgas_am_handle_wait(mgas_am_handle_t *handle);


void mgas_rma_get(void *dst, void *src, mgas_proc_t pid, size_t size);

#endif
