#ifndef MGAS_GMT_H
#define MGAS_GMT_H

#include "../include/mgas.h"
#include "gmt_entry.h"
#include "localize.h"
#include "misc.h"
#include <stdint.h>

struct dist;

// initialization/finalization of GMT
gmt_t *gmt_create(void);
void gmt_destroy(gmt_t *gmt);

// search an GMT entry corresponding to mp
gmt_entry_t *gmt_find_entry(gmt_t *gmt, mgasptr_t mp);

// address allocation/deallocation
mgasptr_t gmt_alloc_slocal(gmt_t *gmt, size_t size);
void gmt_free_slocal(gmt_t *gmt, mgasptr_t mp);
mgasptr_t gmt_alloc_dist(gmt_t *gmt, size_t size);
void gmt_validate_dist(gmt_t *gmt, mgasptr_t mp, size_t size,
                       const struct dist *dist);
void gmt_invalidate_dist(gmt_t *gmt, mgasptr_t mp);
void gmt_free_dist(gmt_t *gmt, mgasptr_t mp);

// misc functions
size_t gmt_calc_home(gmt_t *gmt, mgasptr_t mp);
mgasptr_t gmt_calc_block_base(gmt_t *gmt, mgasptr_t mp);
mgasptr_t gmt_calc_block_offset(gmt_t *gmt, mgasptr_t mp);
size_t gmt_calc_block_size(gmt_t *gmt, mgasptr_t mp);
size_t gmt_calc_row_size(gmt_t *gmt, mgasptr_t mp);
const struct dist *gmt_get_dist(gmt_t *gmt, mgasptr_t mp);
cachedir_t *gmt_get_cachedir(gmt_t *gmt, mgasptr_t mp);
mgas_bool_t gmt_owned(gmt_t *gmt, mgasptr_t mp);


#endif
