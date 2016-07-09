#ifndef MGAS_DIST_H
#define MGAS_DIST_H

#include "gmt.h"
#include "globals.h"
#include "memory_v.h"
#include <stddef.h>
#include <stdint.h>
#include <string.h>


enum {    
    MGASPTR_BITS = 64,                  // pointer size
    MGASPTR_DEBUG_BITS = 4,             // debug bits
    MGASPTR_TYPE_BITS = 1,              // shared local: 0, distributed: 1

    // for shared local pointer
    MGASPTR_SLOCAL_PID_BITS = 20,       // home process ID
    MGASPTR_SLOCAL_ID_BITS = 20,        // shared local object ID
    MGASPTR_SLOCAL_OFFSET_BITS = 19,    // offset

    // for distributed pointer
    MGASPTR_DIST_ID_BITS = 10,          // distributed object ID
    MGASPTR_DIST_OFFSET_BITS = 49,      // offset

    // bit base
    MGASPTR_SLOCAL_OFFSET_BASE = 0,
    MGASPTR_SLOCAL_ID_BASE = MGASPTR_SLOCAL_OFFSET_BASE
                           + MGASPTR_SLOCAL_OFFSET_BITS,
    MGASPTR_SLOCAL_PID_BASE = MGASPTR_SLOCAL_ID_BASE + MGASPTR_SLOCAL_ID_BITS,
    MGASPTR_DIST_OFFSET_BASE = 0,
    MGASPTR_DIST_ID_BASE = MGASPTR_DIST_OFFSET_BASE + MGASPTR_DIST_OFFSET_BITS,
    MGASPTR_TYPE_BASE = MGASPTR_DIST_ID_BASE + MGASPTR_DIST_ID_BITS,
    MGASPTR_DEBUG_BASE = MGASPTR_TYPE_BASE + MGASPTR_TYPE_BITS,

    MGASPTR_MIN = 1ul << MGASPTR_SLOCAL_ID_BASE,

    // id = 0 is reserved
    MGASPTR_MAX_SLOCALS = (1ul << MGASPTR_SLOCAL_ID_BITS) - 2, 
};

static uint64_t
MGASPTR_MASK_BITS(uint64_t bits)
{
    return (1ULL << bits) - 1;
}

static uint64_t
MGASPTR_MASK(uint64_t value, size_t base, size_t bits)
{
    return (value >> base) & MGASPTR_MASK_BITS(bits);
}

static mgas_bool_t
mgasptr_is_slocal(mgasptr_t mp)
{
    return MGASPTR_MASK(mp, MGASPTR_TYPE_BASE, MGASPTR_TYPE_BITS) == 0;
}

static mgas_bool_t
mgasptr_is_dist(mgasptr_t mp)
{
    return MGASPTR_MASK(mp, MGASPTR_TYPE_BASE, MGASPTR_TYPE_BITS) == 1;
}

static mgas_proc_t
mgasptr_slocal_home(mgasptr_t mp)
{
    MGAS_ASSERT(mgasptr_is_slocal(mp));
    return MGASPTR_MASK(mp, MGASPTR_SLOCAL_PID_BASE, MGASPTR_SLOCAL_PID_BITS);
}

static size_t
mgasptr_slocal_id(mgasptr_t mp)
{
    MGAS_ASSERT(mgasptr_is_slocal(mp));
    return MGASPTR_MASK(mp, MGASPTR_SLOCAL_ID_BASE, MGASPTR_SLOCAL_ID_BITS);
}

static size_t
mgasptr_slocal_offset(mgasptr_t mp)
{
    MGAS_ASSERT(mgasptr_is_slocal(mp));
    return MGASPTR_MASK(mp, MGASPTR_SLOCAL_OFFSET_BASE,
                        MGASPTR_SLOCAL_OFFSET_BITS);
}

static size_t
mgasptr_dist_id(mgasptr_t mp)
{
    MGAS_ASSERT(mgasptr_is_dist(mp));
    return MGASPTR_MASK(mp, MGASPTR_DIST_ID_BASE, MGASPTR_DIST_ID_BITS);
}

static size_t
mgasptr_dist_offset(mgasptr_t mp)
{
    MGAS_ASSERT(mgasptr_is_dist(mp));
    return MGASPTR_MASK(mp, MGASPTR_DIST_OFFSET_BASE, MGASPTR_DIST_OFFSET_BITS);
}

static mgasptr_t
mgasptr_make_slocalptr(mgas_proc_t home, size_t id, size_t offset)
{
    size_t mp = home; // home bits
    mp = (mp << MGASPTR_SLOCAL_ID_BITS) + id;
    mp = (mp << MGASPTR_SLOCAL_OFFSET_BITS) + offset;

    return mp;
}

static mgasptr_t
mgasptr_slocal_base(mgasptr_t mp)
{
    return mp & ~MGASPTR_MASK_BITS(MGASPTR_SLOCAL_OFFSET_BITS);
}

static mgasptr_t
mgasptr_dist_base(mgasptr_t mp)
{
    return mp & ~MGASPTR_MASK_BITS(MGASPTR_DIST_OFFSET_BITS);
}

static mgasptr_t
mgasptr_make_distptr(size_t id, size_t offset)
{
    size_t mp = 1;  // type bits
    mp = (mp << MGASPTR_DIST_ID_BITS) + id;
    mp = (mp << MGASPTR_DIST_OFFSET_BITS) + offset;

    return mp;
}


enum dist_constants {
    DIST_MAX_DIMS = 8,
};

typedef struct dist {
    size_t n_dims;
    size_t block_size[DIST_MAX_DIMS];
    size_t n_blocks[DIST_MAX_DIMS];
} dist_t;

static void
dist_init(dist_t *dist, size_t n_dims, const size_t block_size[],
          const size_t n_blocks[])
{
    MGAS_ASSERT(n_dims <= DIST_MAX_DIMS);
    
    dist->n_dims = n_dims;
    memcpy(dist->block_size, block_size, sizeof(block_size[0]) * n_dims);
    memcpy(dist->n_blocks, n_blocks, sizeof(n_blocks[0]) * n_dims);
}

static void
dist_destroy(dist_t *dist)
{
}

static size_t
dist_calc_block_size(const dist_t *dist)
{
    size_t i;
    
    size_t block_size = 1;
    for (i = 0; i < dist->n_dims; i++)
        block_size *= dist->block_size[i];

    return block_size;
}

static size_t
dist_calc_row_size(const dist_t *dist)
{
    return dist->block_size[dist->n_dims - 1];
}

static size_t
dist_calc_block_id(const dist_t *dist, size_t offset)
{
    const size_t *block_size = dist->block_size;
    const size_t *n_blocks = dist->n_blocks;

    if (dist->n_dims == 1) {
        return offset / block_size[0];
    } else if (dist->n_dims == 2) {
        size_t whole_block_size = block_size[0] * block_size[1];
        size_t stride = whole_block_size * n_blocks[1];

        size_t block_id0 = offset / stride;
        size_t block_id1 = offset / block_size[1] % n_blocks[1];

        MGAS_ASSERT(block_id0 < n_blocks[0]);
        MGAS_ASSERT(block_id1 < n_blocks[1]);

        return block_id0 * n_blocks[0] + block_id1;
    } else {
        MGAS_UNDEFINED;
    }
}

static mgas_proc_t
dist_calc_home_from_block_id(const dist_t *dist, size_t block_id)
{
    size_t n_procs = globals_get_n_procs();
    return block_id % n_procs;
}

static mgas_proc_t
dist_calc_home(const dist_t *dist, size_t offset)
{
    size_t block_id = dist_calc_block_id(dist, offset);
    return dist_calc_home_from_block_id(dist, block_id);
}

static size_t
dist_calc_block_base_from_block_id(const dist_t *dist, size_t block_id)
{
    if (dist->n_dims == 1) {
        return block_id * dist->block_size[0];
    }

    if (dist->n_dims == 2) {
        size_t block_id0 = block_id / dist->n_blocks[1];
        size_t block_id1 = block_id % dist->n_blocks[1];
        size_t block_size = dist_calc_block_size(dist);
        size_t block_stride = block_size * dist->n_blocks[1];
        return block_id0 * block_stride + block_id1 * dist->block_size[1];
    }

    MGAS_UNDEFINED;
}

static size_t
dist_calc_block_base(const dist_t *dist, size_t offset)
{
    size_t block_id = dist_calc_block_id(dist, offset);
    return dist_calc_block_base_from_block_id(dist, block_id);
}

static size_t
dist_calc_block_offset(const dist_t *dist, size_t offset)
{
    const size_t *block_size = dist->block_size;
    const size_t *n_blocks = dist->n_blocks;
    
    if (dist->n_dims == 1)
        return offset % block_size[0];

    if (dist->n_dims == 2) {
        size_t stride = block_size[1] * n_blocks[1];

        size_t offset0 = offset / stride % block_size[0];
        size_t offset1 = offset % block_size[1];

        MGAS_ASSERT(offset0 < block_size[0]);
        MGAS_ASSERT(offset1 < block_size[1]);

        return offset0 * block_size[1] + offset1;
    }

    MGAS_UNDEFINED;
}

static size_t
dist_calc_n_blocks(const dist_t *dist)
{
    size_t i;
    size_t n_blocks = 1;
    for (i = 0; i < dist->n_dims; i++)
        n_blocks *= dist->n_blocks[i];

    return n_blocks;
}

// size_t dist_n_blocks(dist_t *dist, size_t offset, size_t size)
// {
//     if (dist->n_dims == 1) {
//         size_t block_size = dist_block_size(dist);
//         size_t block_base = dist_block_base(dist, offset);

//         size_t size = offset + size - block_base;
//         size_t n_blocks = size / block_size;
//         if (size % block_size > 0)
//             n_blocks += 1;

//         return n_blocks;
//     } else if (dist->n_dims == 2) {
//         MGAS_UNDEFINED;
//     } else {
//         MGAS_UNDEFINED;
//     }
// }

static size_t
dist_calc_next_row_base(const dist_t *dist, size_t offset)
{
    size_t row_size = dist->block_size[dist->n_dims - 1];
    size_t row_base = offset / row_size * row_size;
    return row_base + row_size;
}

static size_t
dist_calc_next_block_row_base(const dist_t *dist, size_t offset)
{
    size_t stride;
    if (dist->n_dims == 1)
        stride = dist->block_size[0];
    else if (dist->n_dims == 2)
        stride = dist->block_size[1] * dist->n_blocks[1];
    else
        MGAS_UNDEFINED;
    
    return offset + stride;
}

static size_t
dist_calc_next_block_base(const dist_t *dist, size_t offset)
{
    size_t block_id = dist_calc_block_id(dist, offset);
    return dist_calc_block_base_from_block_id(dist, block_id + 1);
}

static mgasptr_t
dist_calc_block_last_mp(const dist_t *dist, mgasptr_t mp)
{
    const size_t *block_size = dist->block_size;
    const size_t *n_blocks = dist->n_blocks;
    
    if (dist->n_dims == 1) {
        return mp + block_size[0];
    }

    if (dist->n_dims == 2) {
       
        return mp + (block_size[0] - 1) * block_size[1] * n_blocks[1]
                  + block_size[1];
    }
    
    MGAS_UNDEFINED;
}


typedef struct {
    const dist_t *dist;
    size_t obj_id;
    size_t offset_end;
    size_t offset_next;
    gmt_t *gmt;
} dist_iter_t;

static void
dist_iter_init(dist_iter_t *iter, mgasptr_t mp, size_t size)
{
    gmt_t *gmt = globals_get_gmt();

    if (mgasptr_is_slocal(mp)) {
        iter->dist = NULL;
        iter->obj_id = mp;
        iter->offset_end = size;
        iter->offset_next = 0;
    } else {
        MGAS_ASSERT(mgasptr_is_dist(mp));

        size_t begin = mgasptr_dist_offset(mp);
        size_t end = begin + size;
        
        iter->dist = gmt_get_dist(gmt, mp);
        iter->obj_id = mgasptr_dist_id(mp);
        iter->offset_next = begin;
        iter->offset_end = end;
        iter->gmt = gmt;
    }
}

static mgas_bool_t
dist_iter_next(dist_iter_t *iter, mgasptr_t *mp, size_t *size)
{
    const dist_t *dist = iter->dist;
    size_t obj_id = iter->obj_id;
    size_t end = iter->offset_end;
    size_t next = iter->offset_next;

    // if shared local
    if (dist == NULL) {
        if (next < end) {
            *mp = obj_id;
            *size = end;
            iter->offset_next = end;
            return MGAS_TRUE;
        } else {
            return MGAS_FALSE;
        }
    }

    // if distributed and next is valid
    if (next < end) {
        size_t next2 = dist_calc_next_row_base(dist, next);
        
        *mp = mgasptr_make_distptr(obj_id, next);
        *size = (next2 < end ? next2 : end) - next;
        iter->offset_next = next2;

        return MGAS_TRUE;
    } else {
        return MGAS_FALSE;
    }
}

static mgas_bool_t
dist_iter_next_entry(dist_iter_t *iter, gmt_entry_t **entry)
{
    mgasptr_t mp;
    size_t size;
    mgas_bool_t result = dist_iter_next(iter, &mp, &size);

    if (result)
        *entry = gmt_find_entry(iter->gmt, mp);

    return result;

    // const dist_t *dist = iter->dist;
    // size_t obj_id = iter->obj_id;
    // size_t end = iter->offset_end;
    // size_t next = iter->offset_next;
    // gmt_t *gmt = iter->gmt;

    // if (next < end) {

    //     mgasptr_t mp = mgasptr_make_distptr(obj_id, next);
    //     *entry = gmt_find_entry(gmt, mp);

    //     iter->offset_next = dist_calc_next_block_base(dist, next);
        
    //     return MGAS_TRUE;
    // } else {
    //     return MGAS_FALSE;
    // }
}

typedef struct {
    const mem_pair_t *pairs;
    size_t n_pairs;
    size_t i;
    dist_iter_t iter;
} dist_iterp_t;

static void
dist_iterp_init(dist_iterp_t *iterp, const mem_pair_t pairs[], size_t n_pairs)
{
    iterp->pairs = pairs;
    iterp->n_pairs = n_pairs;
    iterp->i = 0;

    if (n_pairs > 0)
        dist_iter_init(&iterp->iter, pairs[0].mp, pairs[0].size);
}

static void
make_pair_from_mgasptr(const mem_pair_t *base_pair, mgasptr_t mp, size_t size,
                       mem_pair_t *new_pair)
{
    new_pair->mp = mp;
    new_pair->p = (uint8_t *)base_pair->p + (mp - base_pair->mp);
    new_pair->size = size;
}

static mgas_bool_t
dist_iterp_next(dist_iterp_t *iterp, mem_pair_t *pair)
{
    mgasptr_t mp;
    size_t size;

    const mem_pair_t *pairs = iterp->pairs;
    size_t n_pairs = iterp->n_pairs;
    dist_iter_t *iter = &iterp->iter;
    
    if (dist_iter_next(iter, &mp, &size)) {
        make_pair_from_mgasptr(&pairs[iterp->i], mp, size, pair);
        return MGAS_TRUE;
    }
    
    if (iterp->i + 1 >= n_pairs)
        return MGAS_FALSE;

    iterp->i += 1;

    const mem_pair_t *curr_pair = &pairs[iterp->i];    
    dist_iter_init(iter, curr_pair->mp, curr_pair->size);

    if (dist_iter_next(iter, &mp, &size)) {
        make_pair_from_mgasptr(curr_pair, mp, size, pair);
        return MGAS_TRUE;
    } else {
        return MGAS_FALSE;
    }
}

static mgas_bool_t
dist_iterp_next_entry(dist_iterp_t *iterp, gmt_entry_t **entry)
{
    const mem_pair_t *pairs = iterp->pairs;
    size_t n_pairs = iterp->n_pairs;
    dist_iter_t *iter = &iterp->iter;

    if (dist_iter_next_entry(iter, entry))
        return MGAS_TRUE;

    if (iterp->i + 1 >= n_pairs)
        return MGAS_FALSE;
    
    iterp->i += 1;

    const mem_pair_t *pair = &pairs[iterp->i];
    dist_iter_init(iter, pair->mp, pair->size);
    
    return dist_iter_next_entry(iter, entry);
}

typedef struct {
    const mgas_vector_t *mvs;
    size_t n_mvs;
    size_t i;
    dist_iter_t iter;
} dist_iterv_t;

static void
dist_iterv_init(dist_iterv_t *iterv, const mgas_vector_t mvs[],
                size_t n_mvs)
{
    iterv->mvs = mvs;
    iterv->n_mvs = n_mvs;
    iterv->i = 0;

    mgasptr_t mp = (n_mvs > 0) ? mvs[0].mp : MGAS_NULL;
    size_t size  = (n_mvs > 0) ? mvs[0].size : 0;

    dist_iter_init(&iterv->iter, mp, size);
}

static mgas_bool_t
dist_iterv_next(dist_iterv_t *iterv, mgasptr_t *mp, size_t *size)
{
    if (dist_iter_next(&iterv->iter, mp, size))
        return MGAS_TRUE;
    
    if (iterv->i + 1 >= iterv->n_mvs)
        return MGAS_FALSE;

    iterv->i += 1;

    const mgas_vector_t *mv = &iterv->mvs[iterv->i];
    dist_iter_init(&iterv->iter, mv->mp, mv->size);

    return dist_iter_next(&iterv->iter, mp, size);
}

static mgas_bool_t
dist_iterv_next_entry(dist_iterv_t *iterv, gmt_entry_t **entry)
{
    if (dist_iter_next_entry(&iterv->iter, entry))
        return MGAS_TRUE;

    if (iterv->i + 1 >= iterv->n_mvs)
        return MGAS_FALSE;
    
    iterv->i += 1;

    const mgas_vector_t *mv = &iterv->mvs[iterv->i];
    dist_iter_init(&iterv->iter, mv->mp, mv->size);
    
    return dist_iter_next_entry(&iterv->iter, entry);
}


static void
make_pairs_from_mvs(const mgas_vector_t mvs[], size_t n_mvs, mgasptr_t mp_base,
                    void *cache, size_t cache_size, varray_t *pair_list)
{
    mgasptr_t mp;
    size_t size;
    dist_iterv_t iter;

    uint8_t *cache_begin = cache;
    uint8_t *cache_end = (uint8_t *)cache + cache_size;
    
    dist_iterv_init(&iter, mvs, n_mvs);
    while (dist_iterv_next(&iter, &mp, &size)) {
        mem_pair_t pair = { mp, PTR_ADD(cache, mp - mp_base), size };

        if (cache_size > 0) {
            uint8_t *p_begin = pair.p;
            uint8_t *p_end = (uint8_t *)pair.p + pair.size;
            MGAS_ASSERT(cache_begin <= p_begin && p_begin < cache_end);
            MGAS_ASSERT(cache_begin <= p_end && p_end <= cache_end);
        }
        
        varray_add(pair_list, &pair);
    }
}

static void
make_pairs_from_blocks(const mgasptr_t blocks[], size_t n_blocks,
                       mgasptr_t mp_base, uint8_t *cache, size_t cache_size,
                       varray_t *pair_list)
{
    if (n_blocks == 0)
        return;

    size_t i;
    gmt_t *gmt = globals_get_gmt();
    const dist_t *dist = gmt_get_dist(gmt, blocks[0]);

    mgasptr_t mp0 = mgasptr_dist_base(mp_base);
    uint8_t *cache_begin = cache;
    uint8_t *cache_end = (uint8_t *)cache + cache_size;
    
    for (i = 0; i < n_blocks; i++) {
        mgasptr_t mp = blocks[i];
        mgasptr_t mp_last = dist_calc_block_last_mp(dist, mp);
        size_t row_size = dist_calc_row_size(dist);

        size_t offset = mgasptr_dist_offset(mp);
        size_t offset_last = mgasptr_dist_offset(mp_last);

        while (mp < mp_last) {
            mem_pair_t pair = { mp, cache + (mp - mp_base), row_size };

            if (cache_size > 0) {
                uint8_t *p_begin = pair.p;
                uint8_t *p_end = (uint8_t *)pair.p + pair.size;
                MGAS_ASSERT(cache_begin <= p_begin && p_begin < cache_end);
                MGAS_ASSERT(cache_begin <= p_end && p_end <= cache_end);
            }
            
            varray_add(pair_list, &pair);

            size_t offset = mgasptr_dist_offset(mp);
            size_t new_offset = dist_calc_next_block_row_base(dist, offset);
            mp = mp0 + new_offset;
        }
    }
}


#endif
