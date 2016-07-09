#include "mgas_alloc.h"
#include "handlers.h"
#include "../include/mgas.h"
#include "dist.h"
#include "gmt.h"
#include "comm.h"
#include "globals.h"
#include "ga_alloc.h"


// enum mgas_alloc_constants {
//     GLOBAL_MMAP_UNIT_SIZE = 4 * GM_PAGE_SIZE,
// };

// struct mgas_alloc {
//     ga_alloc_t *galc;
// };


// static mgasptr_t mgas_alloc_mmap(size_t size)
// {
//     MGAS_ASSERT(size >= GM_PAGE_SIZE);

//     gmt_t *gmt = globals_get_gmt();
//     mgasptr_t mp = gmt_alloc_slocal(gmt, size);

//     MGAS_CHECK(mp != MGAS_NULL);
//     MGAS_ASSERT(mp >= SMALL_SPACE_BASE);
//     MGAS_ASSERT(sizeof(mp) <= sizeof(void *));

//     return mp;
// }

mgas_alloc_t *mgas_alloc_create(void)
{
    // size_t i;

    // gmt_t *gmt = globals_get_gmt();

    // size_t size = GLOBAL_MMAP_UNIT_SIZE;
    // mgasptr_t mp = mgas_alloc_mmap(GLOBAL_MMAP_UNIT_SIZE);

    // mgas_alloc_t *alloc = mgas_sys_malloc(sizeof(*alloc));
    // alloc->galc = ga_alloc_create(mp, size);

    // return alloc;

    return NULL;
}

void mgas_alloc_destroy(mgas_alloc_t *alloc)
{
    // size_t i;

    // ga_alloc_destroy(alloc->galc);
    // mgas_sys_free(alloc);
}

// static mgasptr_t mgas_alloc_malloc(mgas_alloc_t *alloc, size_t size)
// {
//     ga_alloc_t *galc = alloc->galc;
//     ga_range_t *r = ga_alloc_allocate_address(galc, size);

//     if (r == NULL) {
//         // add a new chunk.
//         size_t aligned_size = align_page_size(size);
//         size_t new_size = max_size(aligned_size, GLOBAL_MMAP_UNIT_SIZE);
//         mgasptr_t mp = mgas_alloc_mmap(new_size);
//         ga_range_t *new_r = ga_range_create(mp, new_size, NULL);
//         ga_alloc_free_address(galc, new_r);

//         r = ga_alloc_allocate_address(galc, size);
//     }
//     MGAS_ASSERT(r != NULL);

//     mgasptr_t mp = r->base;
//     ga_range_destroy(r);

//     MGAS_ASSERT(gmt_calc_home(globals_get_gmt(), mp) == globals_get_pid());
// //    DPUTS("alloc %p", (void *)mp);
//     return mp;
// }

// static void mgas_alloc_free(mgas_alloc_t *alloc, mgasptr_t mp, size_t size)
// {
//     MGAS_ASSERT(gmt_calc_home(globals_get_gmt(), mp) == globals_get_pid());

//     ga_alloc_t *galc = alloc->galc;
//     ga_range_t *r = ga_range_create(mp, size, NULL);
//     ga_alloc_free_address(galc, r);
// }


//-- allocation/deallocation of global memory ----------------------------------

mgasptr_t mgas_malloc(size_t size)
{
    gmt_t *gmt = globals_get_gmt();
    return gmt_alloc_slocal(gmt, size);

//    mgas_alloc_t *alloc = globals_get_alloc();
//    return mgas_alloc_malloc(alloc, size);
}

void mgas_process_alloc(size_t size, mgasptr_t *mp_ptr,
                       const comm_handle_t *handle,
                       mgas_proc_t initiator)
{
    gmt_t *gmt = globals_get_gmt();

    mgasptr_t mp = gmt_alloc_dist(gmt, size);

    comm_put_u64(mp_ptr, initiator, mp);
    comm_handle_notify(handle, 1);
}

mgasptr_t mgas_dmalloc(size_t size)
{
    MGAS_UNDEFINED;
    
    // gmt_t *gmt = globals_get_gmt();
    // return comm_request_alloc(size);
}

void mgas_process_free(mgasptr_t mp)
{
    gmt_t *gmt = globals_get_gmt();
    gmt_free_dist(gmt, mp);
}

void mgas_free_small(mgasptr_t mp, size_t size)
{
    mgas_proc_t me = globals_get_pid();
    gmt_t *gmt = globals_get_gmt();

    if (gmt_calc_home(gmt, mp) == me)
        gmt_free_slocal(gmt, mp);
    else
        {} // MGAS_UNDEFINED;
    
    // mgas_proc_t me = globals_get_pid();
    // gmt_t *gmt = globals_get_gmt();
    // mgas_alloc_t *alloc = globals_get_alloc();

    // if (gmt_calc_home(gmt, mp) == me)
    //     mgas_alloc_free(alloc, mp, size);
    // else
    //     //{} // MGAS_UNDEFINED;
    //     DPUTS("not freed [0x%"PRIx64", 0x%"PRIx64"]", mp, mp + size);
}

static void mgas_free_large(mgasptr_t mp)
{
//    comm_request_free(mp);
    // TODO: free page buffers on all nodes
    MGAS_UNDEFINED;
}

void mgas_free(mgasptr_t mp)
{
    if (mgasptr_is_slocal(mp))
        mgas_free_small(mp, 0);
    else
        mgas_free_large(mp);
}


//-- collectives ---------------------------------------------------------------

mgasptr_t mgas_all_alloc(size_t size)
{
    DPUTS("This function is deprecated. Use mgas_all_dmalloc().");

    size_t n_dims = 1;
    size_t block_size[] = { 4096 };
    size_t n_blocks[] = { (size + block_size[0] - 1) / block_size[0] };
    
    return mgas_all_dmalloc(size, n_dims, block_size, n_blocks);
}

mgasptr_t mgas_all_dmalloc(size_t size, size_t n_dims,
                           const size_t block_size[],
                           const size_t n_blocks[])
{
    mgas_barrier();

    size_t i;
    size_t whole_size = 1;
    for (i = 0; i < n_dims; i++) {
        whole_size *= block_size[i];
        whole_size *= n_blocks[i];
    }
    MGAS_CHECK(size == whole_size);
    
    mgas_proc_t me = globals_get_pid();
    gmt_t *gmt = globals_get_gmt();

    // allocate addresses
    mgasptr_t mp = MGAS_NULL;
    if (me == 0) {
        mp = gmt_alloc_dist(gmt, size);

        MGAS_CHECK(mp != MGAS_NULL);
        MGAS_ASSERT(mp >= MGASPTR_MIN);
    }

    comm_broadcast(&mp, sizeof(mgasptr_t), 0);

    dist_t dist;
    dist_init(&dist, n_dims, block_size, n_blocks);
    
    gmt_validate_dist(gmt, mp, size, &dist);

    mgas_barrier();
    return mp;
}

void mgas_all_free(mgasptr_t mp)
{
    mgas_barrier();

    mgas_proc_t me = globals_get_pid();
    gmt_t *gmt = globals_get_gmt();

    gmt_invalidate_dist(gmt, mp);
    
    // deallocate addresses
    if (me == 0)
        gmt_free_dist(gmt, mp);

    // process all system messages
    comm_poll();

    mgas_barrier();
}
