
#include "../include/mgas.h"
#include "gmt_entry.h"
#include "dist.h"


enum mgasptr_constants {
    MAX_DIMS = 2,
};


/*
  shared local object directory
 */
typedef struct {
    idpool_t *free_pool;
    varray_t *objects[];          // gmt_entry_t *objects[n_procs][n_objects]
} gmt_slocaldir_t;


static gmt_slocaldir_t *
gmt_slocaldir_create(void)
{
    size_t i, j;
    size_t n_procs = globals_get_n_procs();

    size_t dir_header_size = offsetof(gmt_slocaldir_t, objects);
    size_t dir_body_size = sizeof(varray_t *) * n_procs;
    gmt_slocaldir_t *dir = mgas_sys_malloc(dir_header_size + dir_body_size);

    dir->free_pool = idpool_create();
    idpool_get(dir->free_pool); // skip id = 0

    for (i = 0; i < n_procs; i++) {
        size_t n_elems = 1024;
        gmt_entry_t *value = NULL;
        dir->objects[i] = varray_create(sizeof(gmt_entry_t *), n_elems);
        varray_extend(dir->objects[i], n_elems);

        gmt_entry_t **objects = varray_raw(dir->objects[i], 0);
        for (j = 0; j < n_elems; j++)
            objects[j] = NULL;
    }

    return dir;
}

static void
gmt_slocaldir_destroy(gmt_slocaldir_t *dir)
{
    size_t i, j;
    size_t n_procs = globals_get_n_procs();
    
    for (i = 0; i < n_procs; i++) {
        varray_t *obj_list = dir->objects[i];

        gmt_entry_t **objs = varray_raw(obj_list, 0);
        size_t n_objs = varray_size(obj_list);

        for (j = 0; j < n_objs; j++)
            if (objs[j] != NULL)
                gmt_entry_destroy(objs[j]);
    }

    idpool_destroy(dir->free_pool);
    mgas_sys_free(dir);
}

static size_t
ceil_with_pow2(size_t size)
{
    //__builtin_clzl(0) is undefined,
    // so ceil_with_pow2(1) == 2 in gcc-x86-64.
    return 1ul << (64 - __builtin_clzl(size - 1));
}

static gmt_entry_t *
gmt_slocaldir_find_entry(gmt_slocaldir_t *dir, size_t pid, size_t obj_id)
{
    size_t i;
    MGAS_ASSERT(pid < globals_get_n_procs());
    
    varray_t *objects = dir->objects[pid];

    // extend the slocal table when first touch
    size_t n_elems = varray_size(objects);
    if (obj_id >= n_elems) {
        size_t new_elems = ceil_with_pow2(obj_id + 1);
        varray_extend(objects, new_elems);

        gmt_entry_t **new_objs = varray_raw(objects, 0);
        for (i = n_elems; i < new_elems; i++)
            new_objs[i] = NULL;
    }
    
    gmt_entry_t **entry = varray_raw(objects, obj_id);
    if (*entry == NULL)
        *entry = gmt_entry_create();

    return *entry;
}

static size_t 
gmt_slocaldir_alloc(gmt_slocaldir_t *dir, size_t size)
{
    size_t me = globals_get_pid();
    size_t id = idpool_get(dir->free_pool);

    MGAS_ASSERT(id <= MGASPTR_MAX_SLOCALS);

    gmt_entry_t *entry = gmt_slocaldir_find_entry(dir, me, id);
    gmt_entry_reset_and_touch(entry, size);
    
    return id;
}

static void
gmt_slocaldir_free(gmt_slocaldir_t *dir, size_t id)
{
    idpool_put(dir->free_pool, id);
}


/*
  distributed object information
 */   
typedef struct {
    size_t n_blocks;
    gmt_entry_t **blocks;
    dist_t dist;
    cachedir_t *cachedir;
} gmt_distobj_t;

static void
gmt_distobj_init(gmt_distobj_t *obj)
{
    obj->n_blocks = 0;
    obj->blocks = NULL;
}

static void
gmt_distobj_finalize(gmt_distobj_t *obj)
{
    MGAS_ASSERT(obj->n_blocks == 0);
    MGAS_ASSERT(obj->blocks == NULL);
}

static void
gmt_distobj_validate(gmt_distobj_t *obj, const dist_t *dist)
{
    size_t i;
    mgas_proc_t me = globals_get_pid();
    
    size_t n_blocks = dist_calc_n_blocks(dist);
    gmt_entry_t **blocks = array_create(gmt_entry_t *, n_blocks);

    // initialize the entries whose home is this process
    size_t page_size = dist_calc_block_size(dist);
    for (i = 0; i < n_blocks; i++) {
        mgas_proc_t home = dist_calc_home_from_block_id(dist, i);
        if (home == me) {
            gmt_entry_t *entry = gmt_entry_create();
            gmt_entry_reset(entry, page_size);

            blocks[i] = entry;
        } else {
            blocks[i] = NULL;
        }
    }

    cachedir_t *dir = cachedir_create();

    obj->n_blocks = n_blocks;
    obj->blocks = blocks;
    obj->dist = *dist;
    obj->cachedir = dir;
}

static void
gmt_distobj_invalidate(gmt_distobj_t *obj)
{
    MGAS_ASSERT(obj->n_blocks != 0);
    MGAS_ASSERT(obj->blocks != NULL);

    size_t i;
    for (i = 0; i < obj->n_blocks; i++) {
        gmt_entry_t *entry = obj->blocks[i];
        if (entry != NULL)
            gmt_entry_destroy(entry);
    }
    
    array_destroy(obj->blocks);
    dist_destroy(&obj->dist);
    cachedir_destroy(obj->cachedir);

    obj->n_blocks = 0;
    obj->blocks = NULL;
}

static gmt_entry_t *
gmt_distobj_find_entry(gmt_distobj_t *obj, size_t offset)
{   
    size_t block_id = dist_calc_block_id(&obj->dist, offset);

    MGAS_ASSERT(block_id < obj->n_blocks);
    
    if (obj->blocks[block_id] == NULL)
        obj->blocks[block_id] = gmt_entry_create();
       
    return obj->blocks[block_id];
}

static const dist_t *gmt_distobj_get_dist(const gmt_distobj_t *obj)
{
    return &obj->dist;
}

static cachedir_t *gmt_distobj_get_cachedir(const gmt_distobj_t *obj)
{
    return obj->cachedir;
}


/*
  distributed object directory
 */
typedef struct {
    varray_t *objects;        // gmt_distobj_t objects[n_objects]
    idpool_t *free_pool;
} gmt_distdir_t;

static void
gmt_distdir_init(gmt_distdir_t *dir)
{
    size_t i;
    size_t initial_capacity = 16;
    
    varray_t *obj_list = varray_create(sizeof(gmt_distobj_t),
                                       initial_capacity);
    varray_extend(obj_list, initial_capacity);

    gmt_distobj_t *objs = varray_raw(obj_list, 0);
    size_t n_objs = varray_size(obj_list);
    
    for (i = 0; i < n_objs; i++)
        gmt_distobj_init(&objs[i]);

    dir->objects = obj_list;
    dir->free_pool = idpool_create();
}

static void
gmt_distdir_finalize(gmt_distdir_t *dir)
{
    size_t i;
    
    gmt_distobj_t *objs = varray_raw(dir->objects, 0);
    size_t n_objs = varray_size(dir->objects);
    
    for (i = 0; i < n_objs; i++)
        gmt_distobj_finalize(&objs[i]);

    varray_destroy(dir->objects);
    idpool_destroy(dir->free_pool);
}

static gmt_distobj_t *
gmt_distdir_get_obj(gmt_distdir_t *dir, size_t id)
{
    MGAS_ASSERT(id < varray_size(dir->objects));  // TODO: extend varray
    
    gmt_distobj_t *objects = varray_raw(dir->objects, 0);
    return &objects[id];
}

static size_t 
gmt_distdir_alloc(gmt_distdir_t *dir, size_t size)
{
    return idpool_get(dir->free_pool);
}

static void
gmt_distdir_validate(gmt_distdir_t *dir, size_t id, size_t size,
                     const dist_t *dist)
{
    gmt_distobj_t *obj = gmt_distdir_get_obj(dir, id);
    gmt_distobj_validate(obj, dist);
}

static void
gmt_distdir_invalidate(gmt_distdir_t *dir, size_t id)
{
    gmt_distobj_t *obj = gmt_distdir_get_obj(dir, id);
    gmt_distobj_invalidate(obj);
}

static void
gmt_distdir_free(gmt_distdir_t *dir, size_t id)
{
    idpool_put(dir->free_pool, id);
}

static gmt_entry_t *
gmt_distdir_find_entry(gmt_distdir_t *dir, size_t obj_id, size_t offset)
{
    gmt_distobj_t *dobj = varray_raw(dir->objects, obj_id);
    return gmt_distobj_find_entry(dobj, offset);
}

/*
  global memory table
*/
struct gmt {
    rwspinlock_t localdir_lock;
    gmt_slocaldir_t *slocaldir;

    rwspinlock_t distdir_lock;
    gmt_distdir_t distdir;

    // TODO: mutual exclusion
};

gmt_t *
gmt_create(void)
{
    gmt_t *gmt = mgas_sys_malloc(sizeof(*gmt));

    gmt->slocaldir = gmt_slocaldir_create();
    gmt_distdir_init(&gmt->distdir);

    rwspinlock_init(&gmt->localdir_lock);
    rwspinlock_init(&gmt->distdir_lock);

    return gmt;
}

void
gmt_destroy(gmt_t *gmt)
{
    gmt_slocaldir_destroy(gmt->slocaldir);
    gmt_distdir_finalize(&gmt->distdir);

    rwspinlock_destroy(&gmt->localdir_lock);
    rwspinlock_destroy(&gmt->distdir_lock);
}

gmt_entry_t *
gmt_find_entry(gmt_t *gmt, mgasptr_t mp)
{
    MGAS_ASSERT(mp >= MGASPTR_MIN);
    
    if (mgasptr_is_slocal(mp)) {
        size_t pid = mgasptr_slocal_home(mp);
        size_t id = mgasptr_slocal_id(mp);
        return gmt_slocaldir_find_entry(gmt->slocaldir, pid, id);
    } else {
        MGAS_ASSERT(mgasptr_is_dist(mp));

        size_t id = mgasptr_dist_id(mp);
        size_t offset = mgasptr_dist_offset(mp);
        return gmt_distdir_find_entry(&gmt->distdir, id, offset);
    }
}

mgasptr_t
gmt_alloc_slocal(gmt_t *gmt, size_t size)
{
    mgas_proc_t me = globals_get_pid();
    size_t id = gmt_slocaldir_alloc(gmt->slocaldir, size);

    return mgasptr_make_slocalptr(me, id, 0);
}

void
gmt_free_slocal(gmt_t *gmt, mgasptr_t mp)
{
    MGAS_ASSERT(mgasptr_is_slocal(mp));
    MGAS_ASSERT(mgasptr_slocal_home(mp) == globals_get_pid());
    
    size_t id = mgasptr_slocal_id(mp);
    gmt_slocaldir_free(gmt->slocaldir, id);
}

mgasptr_t
gmt_alloc_dist(gmt_t *gmt, size_t size)
{
    MGAS_ASSERT(globals_get_pid() == 0);
    
    size_t id = gmt_distdir_alloc(&gmt->distdir, size);
    return mgasptr_make_distptr(id, 0);
}

void
gmt_validate_dist(gmt_t *gmt, mgasptr_t mp, size_t size, const dist_t *dist)
{
    MGAS_ASSERT(mgasptr_is_dist(mp));
    
    size_t id = mgasptr_dist_id(mp);
    gmt_distdir_validate(&gmt->distdir, id, size, dist);
}

void
gmt_invalidate_dist(gmt_t *gmt, mgasptr_t mp)
{
    size_t id = mgasptr_dist_id(mp);
    gmt_distdir_invalidate(&gmt->distdir, id);
}

void
gmt_free_dist(gmt_t *gmt, mgasptr_t mp)
{
    MGAS_ASSERT(mgasptr_is_dist(mp));
    MGAS_ASSERT(globals_get_pid() == 0);
    
    size_t id = mgasptr_dist_id(mp);
    gmt_distdir_free(&gmt->distdir, id);
}

size_t
gmt_calc_home(gmt_t *gmt, mgasptr_t mp)
{
    if (mgasptr_is_slocal(mp))
        return mgasptr_slocal_home(mp);

    MGAS_ASSERT(mgasptr_is_dist(mp));

    const dist_t *dist = gmt_get_dist(gmt, mp);
    size_t offset = mgasptr_dist_offset(mp);
    
    return dist_calc_home(dist, offset);
}

mgasptr_t
gmt_calc_block_base(gmt_t *gmt, mgasptr_t mp)
{
    if (mgasptr_is_slocal(mp))
        return mgasptr_slocal_base(mp);

    MGAS_ASSERT(mgasptr_is_dist(mp));

    const dist_t *dist = gmt_get_dist(gmt, mp);
    size_t id = mgasptr_dist_id(mp);
    size_t offset = mgasptr_dist_offset(mp);

    size_t new_offset = dist_calc_block_base(dist, offset);
    return mgasptr_make_distptr(id, new_offset);
}

mgasptr_t gmt_calc_block_offset(gmt_t *gmt, mgasptr_t mp)
{
    if (mgasptr_is_slocal(mp))
        return mgasptr_slocal_offset(mp);

    MGAS_ASSERT(mgasptr_is_dist(mp));

    const dist_t *dist = gmt_get_dist(gmt, mp);
    size_t offset = mgasptr_dist_offset(mp);

    return dist_calc_block_offset(dist, offset);
}

size_t
gmt_calc_block_size(gmt_t *gmt, mgasptr_t mp)
{
    if (mgasptr_is_slocal(mp))
        MGAS_UNDEFINED;
    
    const dist_t *dist = gmt_get_dist(gmt, mp);
    return dist_calc_block_size(dist);
}

size_t
gmt_calc_row_size(gmt_t *gmt, mgasptr_t mp)
{
    if (mgasptr_is_slocal(mp))
        return gmt_calc_block_size(gmt, mp);
    
    const dist_t *dist = gmt_get_dist(gmt, mp);
    return dist_calc_row_size(dist);
}

const dist_t *
gmt_get_dist(gmt_t *gmt, mgasptr_t mp)
{
    MGAS_ASSERT(mgasptr_is_dist(mp));

    size_t id = mgasptr_dist_id(mp);
    gmt_distobj_t *obj = gmt_distdir_get_obj(&gmt->distdir, id);
    return gmt_distobj_get_dist(obj);
}

cachedir_t *
gmt_get_cachedir(gmt_t *gmt, mgasptr_t mp)
{
    MGAS_ASSERT(mgasptr_is_dist(mp));
    
    size_t id = mgasptr_dist_id(mp);
    gmt_distobj_t *obj = gmt_distdir_get_obj(&gmt->distdir, id);
    return gmt_distobj_get_cachedir(obj);
}
    
mgas_bool_t
gmt_owned(gmt_t *gmt, mgasptr_t mp)
{
    gmt_entry_t *entry = gmt_find_entry(gmt, mp);
    return gmt_entry_page_valid(entry);
}
