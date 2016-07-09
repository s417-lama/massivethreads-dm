#include "localize.h"
#include "../include/mgas.h"
#include "../include/mgas_debug.h"
#include "globals.h"
#include "gmt.h"
#include "dist.h"
#include "memory.h"
#include "misc.h"

#include <string.h>

//-- cache ---------------------------------------------------------------------

/*
  cache information in GMT entry
 */
typedef struct {
    volatile size_t ref_count;
    uint8_t *buffer;
    size_t size;
    size_t all_block_size;
    mgasptr_t mp_base;
    varray_t *block_list;
    cachedir_t *cachedir;
} cache_t;

static cache_t *
cache_create(size_t size, mgasptr_t mp_base, varray_t *block_list,
             cachedir_t *dir)
{
#if DEBUG
    gmt_t *gmt = globals_get_gmt();
    size_t row_size = gmt_calc_row_size(gmt, mp_base);
    size_t size_mod_row_size = size % row_size;
    MGAS_ASSERT(size_mod_row_size == 0);
#endif

    uint8_t *buf = (uint8_t *)comm_aligned_malloc(size, OS_PAGE_SIZE);

    cache_t *cache = (cache_t *)mgas_sys_malloc(sizeof(cache_t));
    cache->ref_count = 1;
    cache->buffer = buf;
    cache->size = size;
    cache->mp_base = mp_base;
    cache->block_list = block_list;
    cache->cachedir = dir;

#if 0
    DPUTSR("cache 0x%"PRIx64" [%p, %p) (size = %zu) is created.",
          cache->mp_base, cache->buffer, cache->buffer + cache->size,
          cache->size);
#endif
    
    return cache;
}

static void
cache_destroy(cache_t *cache)
{
#if 0
    DPUTSR("cache 0x%"PRIx64" [%p, %p) (size = %zu) is destroyed.",
          cache->mp_base, cache->buffer, cache->buffer + cache->size,
          cache->size);
#endif

    comm_aligned_free(cache->buffer);
    varray_destroy(cache->block_list);
    mgas_sys_free(cache);
}

static uint8_t *
cache_get(const cache_t *cache)
{
    return cache->buffer;
}

static size_t
cache_get_size(const cache_t *cache)
{
    return cache->size;
}

static size_t
cache_calc_all_block_size(const cache_t *cache)
{
    gmt_t *gmt = globals_get_gmt();

    size_t n_blocks = varray_size(cache->block_list);
    mgasptr_t mp = *(mgasptr_t *)varray_raw(cache->block_list, 0);

    size_t block_size = gmt_calc_block_size(gmt, mp);

    return block_size * n_blocks;
}

static mgasptr_t
cache_get_base_addr(const cache_t *cache)
{
    return cache->mp_base;
}

static mgas_bool_t
cache_try_incr(cache_t *cache, size_t *result)
{
    for (;;) {
        size_t count = cache->ref_count;

        if (count <= 0)
            return MGAS_FALSE;   // abort

        size_t incr_value = count + 1;
        int success = bool_compare_and_swap(&cache->ref_count,
                                            count, incr_value);
        if (success) {
            *result = incr_value;
            return MGAS_TRUE;
        }
    }
}

static void cachedir_unregister(cachedir_t *dir, const cache_t *cache);

static void
cache_decr(cache_t *cache)
{
    size_t count = fetch_and_sub(&cache->ref_count, 1) - 1;

    if (count == 0) {
        cachedir_unregister(cache->cachedir, cache);
        cache_destroy(cache);
    }
}


struct cachedir {
    rwspinlock_t lock;
    varray_t *cache_list;
};

cachedir_t *
cachedir_create(void)
{
    cachedir_t *dir = mgas_sys_malloc(sizeof(cachedir_t));

    rwspinlock_init(&dir->lock);
    dir->cache_list = varray_create(sizeof(cache_t *), 16);

    return dir;
}

void
cachedir_destroy(cachedir_t *dir)
{
    varray_destroy(dir->cache_list);
    mgas_sys_free(dir);
}

static void
cachedir_register(cachedir_t *dir, cache_t *cache)
{
    rwspinlock_write_lock(&dir->lock);
    varray_add(dir->cache_list, &cache);
    rwspinlock_write_unlock(&dir->lock);
}

static void cachedir_unregister(cachedir_t *dir, const cache_t *cache)
{
    size_t i;
    rwspinlock_write_lock(&dir->lock);

    const cache_t **caches = varray_raw(dir->cache_list, 0);
    size_t n_caches = varray_size(dir->cache_list);

    for (i = 0; i < n_caches; i++)
        if (caches[i] == cache)
            break;

    MGAS_ASSERT(i < n_caches);

    varray_remove(dir->cache_list, i);

    varray_t *cache_list = dir->cache_list;
    memset(cache_list->buf + sizeof(cache_t *) * cache_list->n_elems,
        0, sizeof(cache_t *) * (cache_list->capacity - cache_list->n_elems));
        
    rwspinlock_write_unlock(&dir->lock);
}

static mgas_bool_t
cache_contains(const cache_t *cache, const mgasptr_t blocks[], size_t n_blocks)
{
    MGAS_ASSERT(n_blocks > 0);

    size_t *cache_blocks = varray_raw(cache->block_list, 0);
    size_t n_cache_blocks = varray_size(cache->block_list);
    
    size_t i;
    size_t j = 0;
    for (i = 0; i < n_cache_blocks; i++) {
        if (cache_blocks[i] == blocks[j]) {
            j += 1;

            if (j == n_blocks)
                return MGAS_TRUE;
        }
    }

    return MGAS_FALSE;
}

static cache_t *
cachedir_find(cachedir_t *dir, mgasptr_t blocks[], size_t n_blocks)
{
    size_t i;
    rwspinlock_read_lock(&dir->lock);

    cache_t **caches = varray_raw(dir->cache_list, 0);
    size_t n_caches = varray_size(dir->cache_list);

    cache_t *result = NULL;
    for (i = 0; i < n_caches; i++) {
        cache_t *cache = caches[i];

        if (cache_contains(cache, blocks, n_blocks)) {
            result = cache;
            break;
        }
    }
    
    rwspinlock_read_unlock(&dir->lock);

    return result;
}


//-- localize handle operations ------------------------------------------------

typedef struct mgas_local {
    struct mgas_local *next;
    cache_t *cache;
} mgas_local_t;

void
mgas_handle_init(mgas_handle_t *handle)
{
    mgas_handle_t h = MGAS_HANDLE_INIT;
    *handle = h;
}

static void
mgas_handle_push_cache(mgas_handle_t *handle, cache_t *cache)
{
    mgas_local_t *local = mgas_sys_malloc(sizeof(*local));
    local->next = handle->locals;
    local->cache = cache;

    handle->locals = local;
}

static cache_t *
mgas_handle_pop_cache(mgas_handle_t *handle)
{
    cache_t *cache;

    mgas_local_t *local = handle->locals;
    if (local != NULL) {
        handle->locals = local->next;

        cache = local->cache;

        mgas_sys_free(local);
    } else {
        cache = NULL;
    }

    return cache;
}


//-- localize ------------------------------------------------------------------

static int
compare_mp(const void *lhs, const void *rhs)
{
    mgasptr_t mp0 = *(const mgasptr_t *)lhs;
    mgasptr_t mp1 = *(const mgasptr_t *)rhs;
    if (mp0 < mp1)
        return -1;
    else if (mp0 == mp1)
        return 0;
    else
        return 1;
}

static void
make_unique_block_ptrs(gmt_t *gmt, const mgas_vector_t mvs[], size_t n_mvs,
                      varray_t *block_list)
{
    mgasptr_t mp;
    size_t size;
    dist_iterv_t iter;

    dist_iterv_init(&iter, mvs, n_mvs);
    while (dist_iterv_next(&iter, &mp, &size)) {
        mgasptr_t mp_base = gmt_calc_block_base(gmt, mp);
        varray_add(block_list, &mp_base);
    }

    varray_sort(block_list, compare_mp);
    varray_unique(block_list);
}

static void
mgas_copy_mvs(const mgas_vector_t mvs[], size_t n_mvs, mgasptr_t mp_base,
              void *cache_base, size_t cache_size, mgas_access_t access)
{
    varray_t *pair_list = varray_create(sizeof(mem_pair_t), 1024);
        
    // create cache vector corresponding to mvs
    make_pairs_from_mvs(mvs, n_mvs, mp_base, cache_base, cache_size, pair_list);

    // copy remote data into local cache
    mem_pair_t *pairs = varray_raw(pair_list, 0);
    size_t n_pairs = varray_size(pair_list);
    mgas_copy_v(pairs, n_pairs, access);

    // finalize
    varray_destroy(pair_list);
}

static void
mgas_copy_blocks(const mgasptr_t blocks[], size_t n_blocks, mgasptr_t mp_base,
                 void *cache_base, size_t cache_size, mgas_access_t access)
{
    varray_t *pair_list = varray_create(sizeof(mem_pair_t), 1024);
        
    // create cache vector corresponding to blocks
    make_pairs_from_blocks(blocks, n_blocks, mp_base, cache_base, cache_size,
                           pair_list);

    // copy remote data into local cache
    mem_pair_t *pairs = varray_raw(pair_list, 0);
    size_t n_pairs = varray_size(pair_list);
    mgas_copy_v(pairs, n_pairs, access);

    // finalize
    varray_destroy(pair_list);
}

static cache_t *
mgas_cache_v(const mgas_vector_t mvs[], size_t n_mvs, mgas_bool_t update)
{
    MGAS_PROF_BEGIN(CACHE);

    if (n_mvs == 0)
        return NULL;

    MGAS_PROF_BEGIN(CACHE_SEARCH);

    globals_giant_lock();

    varray_t *block_list = varray_create(sizeof(mgasptr_t), 1024);
    
    gmt_t *gmt = globals_get_gmt();
    cachedir_t *dir = gmt_get_cachedir(gmt, mvs[0].mp);

    MGAS_PROF_BEGIN(UNIQ_BLOCK_PTRS);

    // mgas_vector list -> sorted, unique block id list
    make_unique_block_ptrs(gmt, mvs, n_mvs, block_list);

    MGAS_PROF_END(UNIQ_BLOCK_PTRS);

    mgasptr_t *blocks = varray_raw(block_list, 0);
    size_t n_blocks = varray_size(block_list);

    // find specified cache
    cache_t *cache = cachedir_find(dir, blocks, n_blocks);
    mgas_bool_t cached = (cache != NULL);

    if (cached) {
        // increment refcount before data copy
        // because refcount may be decremented at comm handler
        // (e.g. task stealing using active messages).
        size_t count;
        mgas_bool_t success = cache_try_incr(cache, &count);

        if (!success)
            cached = MGAS_FALSE;

        MGAS_ASSERT(success && count >= 2);
    }

    MGAS_PROF_END(CACHE_SEARCH);

    if (cached && !update) {
        // if already cached and not requring cache update
    } else {
        // if not cached or requring cache update
        //
        MGAS_PROF_BEGIN(CACHE_UPDATE);
        
        mgasptr_t mp_base = blocks[0];
        const dist_t *dist = gmt_get_dist(gmt, mp_base);
        mgasptr_t mp_last = dist_calc_block_last_mp(dist, blocks[n_blocks - 1]);

        if (!cached) {
            // if not already cached, allocate a new cache
            size_t cache_size = mp_last - mp_base;
            cache = cache_create(cache_size, mp_base, block_list, dir);
        }

        uint8_t *cache_base = cache_get(cache);
        size_t cache_size = cache_get_size(cache);

        mgasptr_t cached_mp_base = cache_get_base_addr(cache);
        MGAS_ASSERT(mp_base - cached_mp_base < cache_size);
        MGAS_ASSERT(mp_last - cached_mp_base <= cache_size);
        MGAS_ASSERT(mp_last - mp_base <= cache_size);
        
        // copy data from distributed blocks to the cache
        mgas_copy_blocks(blocks, n_blocks, cached_mp_base, cache_base,
                         cache_size,
                         MGAS_ACCESS_GET);
        //mgas_copy_mvs(mvs, n_mvs, cached_mp_base, cache_base, cache_size,
        //              MGAS_ACCESS_GET);
    
        // register cache
        if (!cached)
            cachedir_register(dir, cache);

        MGAS_PROF_END(CACHE_UPDATE);
    }

    // finalize
    // (if the cache is newly allocated, it is deallocated at mgas_unlocalize)
    if (cached)
        varray_destroy(block_list);
   
    globals_giant_unlock();

    MGAS_PROF_END(CACHE);

    return cache;
}

void *mgas_localize_v(mgasptr_t mp, const mgas_vector_t mvs[], size_t n_mvs,
                      mgas_flag_t flags, mgas_handle_t *handle)
{
    MGAS_PROF_BEGIN(LOCALIZE);

    gmt_t *gmt = globals_get_gmt();

    if (n_mvs == 0)
        return NULL;

    if (flags & MGAS_OWN)
        mgas_own_v(mvs, n_mvs);

    mgas_bool_t update;
    if (flags & (MGAS_RO | MGAS_RWE))
        update = MGAS_FALSE;
    else if (flags & MGAS_RWS)
        update = MGAS_TRUE;
    else
        MGAS_CHECK(0);

    cache_t *cache = mgas_cache_v(mvs, n_mvs, update);
    
    mgas_handle_push_cache(handle, cache);

    uint8_t *p = cache_get(cache);
    mgasptr_t mp_base = cache_get_base_addr(cache);
    MGAS_ASSERT(mp_base <= mp);
    
    size_t offset = mp - mp_base;

    size_t cache_size = cache_get_size(cache);
    MGAS_ASSERT(offset < cache_size);
    
    comm_poll();
    
    MGAS_PROF_END(LOCALIZE);
    
    return p + offset;
}

void mgas_commit_v(mgasptr_t mp, void *p, const mgas_vector_t mvs[],
                   size_t n_mvs)
{
    gmt_t *gmt = globals_get_gmt();

    if (n_mvs == 0)
        return;

    MGAS_PROF_BEGIN(COMMIT);
    globals_giant_lock();

    mgas_copy_mvs(mvs, n_mvs, mp, p, 0, MGAS_ACCESS_PUT);

    globals_giant_unlock();

    comm_poll();
    
    MGAS_PROF_END(COMMIT);
}

void
mgas_unlocalize(mgas_handle_t *handle)
{
    MGAS_PROF_BEGIN(UNLOCALIZE);

    globals_giant_lock();

    MGAS_CHECK(handle != NULL);

    for (;;) {
        cache_t *cache = mgas_handle_pop_cache(handle);

        if (cache == NULL)
            break;

        cache_decr(cache);
    }

    globals_giant_unlock();

    MGAS_PROF_END(UNLOCALIZE);
}
