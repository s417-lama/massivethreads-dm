#ifndef MGAS_GMT_ENTRY_H
#define MGAS_GMT_ENTRY_H

#include "../include/mgas.h"
#include "ga_alloc.h"
#include "misc.h"
#include <stdint.h>


// cache
typedef struct gmt_cache gmt_cache_t;

gmt_cache_t *gmt_cache_create(size_t size, mgasptr_t mp_base,
                              mgas_vector_t mvs[], size_t n_mvs);
void gmt_cache_destroy(gmt_cache_t *cache);
uint8_t *gmt_cache_get(const gmt_cache_t *cache);
mgasptr_t gmt_cache_get_base_addr(const gmt_cache_t *cache);
mgas_vector_t *gmt_cache_get_mvs(const gmt_cache_t *cache);
size_t gmt_cache_get_n_mvs(const gmt_cache_t *cache);
size_t gmt_cache_get_ref_count(const gmt_cache_t *cache);
size_t gmt_cache_incr(gmt_cache_t *cache);
size_t gmt_cache_decr(gmt_cache_t *cache);


// global memory table (GMT)
typedef struct gmt gmt_t;

// GMT entry
typedef struct gmt_entry gmt_entry_t;

// entry creation/destruction
gmt_entry_t *gmt_entry_create(void);
void gmt_entry_destroy(gmt_entry_t *entry);
void gmt_entry_reset(gmt_entry_t *entry, size_t page_size);
void gmt_entry_reset_and_touch(gmt_entry_t *entry, size_t block_size);

// page lock
void gmt_entry_page_read_lock(gmt_entry_t *entry);
mgas_bool_t gmt_entry_page_try_read_lock(gmt_entry_t *entry);
void gmt_entry_page_read_unlock(gmt_entry_t *entry);
void gmt_entry_page_write_lock(gmt_entry_t *entry);
mgas_bool_t gmt_entry_page_try_write_lock(gmt_entry_t *entry);
void gmt_entry_page_write_unlock(gmt_entry_t *entry);
mgas_bool_t gmt_entry_page_reading(gmt_entry_t *entry);
mgas_bool_t gmt_entry_page_writing(gmt_entry_t *entry);

// page state predicates
mgas_bool_t gmt_entry_page_invalid(const gmt_entry_t *entry);
mgas_bool_t gmt_entry_page_valid(const gmt_entry_t *entry);
mgas_bool_t gmt_entry_page_cached(const gmt_entry_t *entry);

// page state transition
void *gmt_entry_page_invalidate(gmt_entry_t *entry);
void gmt_entry_page_prepare(gmt_entry_t *entry, void *page_addr);
void gmt_entry_page_validate(gmt_entry_t *entry);

// getter/setter of page states
void *gmt_entry_get_block(const gmt_entry_t *entry);
size_t gmt_entry_get_block_size(const gmt_entry_t *entry);

// owner management
void gmt_entry_get_owner(gmt_entry_t *entry, mgas_proc_t initiator,
                         mgas_proc_t *owner, size_t *block_size);
mgas_proc_t gmt_entry_get_raw_owner(gmt_entry_t *entry);
void gmt_entry_begin_migration(gmt_entry_t *entry, mgas_proc_t initiator,
                               mgas_proc_t *owner, size_t *block_size);
void gmt_entry_end_migration(gmt_entry_t *entry, mgas_proc_t owner);


#endif
