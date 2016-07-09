#include "memory_v.h"
#include "memory.h"
#include "handlers.h"
#include "globals.h"
#include "dist.h"
#include "comm.h"


static int compare_pairs(const void *lhs, const void *rhs)
{
    const mem_pair_t *p0 = lhs;
    const mem_pair_t *p1 = rhs;

    if (p0->mp < p1->mp)
        return -1;
    else if (p0->mp == p1->mp)
        return 0;
    else
        return 1;
}

static void
assert_pairs_is_sorted(const mem_pair_t pairs[], size_t n_pairs)
{
#ifdef DEBUG
    size_t i;
    
    mem_pair_t *sorted = array_copy(const mem_pair_t, pairs, n_pairs);
    qsort(sorted, n_pairs, sizeof(const mem_pair_t), compare_pairs);

    for (i = 0; i < n_pairs; i++) {
        const mem_pair_t *pair = &pairs[i];
        const mem_pair_t *sorted_pair = &sorted[i];
        MGAS_ASSERT(pair->mp == sorted_pair->mp);
    }
#endif
}

static void
assert_pairs_is_page_separeted(const mem_pair_t pairs[], size_t n_pairs)
{
#ifdef DEBUG
    size_t i;
    gmt_t *gmt = globals_get_gmt();

    for (i = 0; i < n_pairs; i++) {
        const mem_pair_t *pair = &pairs[i];

        if (pair->size > 0) {
            mgasptr_t mp = pair->mp;
            mgasptr_t mp_last = pair->mp + pair->size - 1;
        
            mgasptr_t mp_base = gmt_calc_block_base(gmt, mp);
            mgasptr_t mp_last_base = gmt_calc_block_base(gmt, mp_last);

            MGAS_ASSERT(mp_base == mp_last_base);
        }
    }
#endif
}

static void
assert_pairs(const mem_pair_t pairs[], size_t n_pairs)
{
//    assert_pairs_is_sorted(pairs, n_pairs);
//    assert_pairs_is_page_separeted(pairs, n_pairs);
}

static void assert_pages_are_unlocked(const mem_pair_t pairs[], size_t n_pairs)
{
//    gmt_entry_t *entry;
//    dist_iterp_t iter;

//    dist_iterp_init(&iter, pairs, n_pairs);
//    while (dist_iterp_next_entry(&iter, &entry)) {
    size_t i;
    gmt_t *gmt = globals_get_gmt();
    for (i = 0; i < n_pairs; i++) {
        gmt_entry_t *entry = gmt_find_entry(gmt, pairs[i].mp);
        
        MGAS_ASSERT(!gmt_entry_page_reading(entry));
        MGAS_ASSERT(!gmt_entry_page_writing(entry));
    }
}


//-- resolving owner -----------------------------------------------------------

void
mgas_process_owner_request(const msg_t *msg, const mgasptr_t mps[],
                           size_t n_mps, mgas_access_t access,
                           mgas_proc_t initiator)
{
    MGAS_PROF_BEGIN(OVH_PROC_OWR);
    
    size_t i;

    mgas_proc_t me = globals_get_pid();
    size_t n_procs = globals_get_n_procs();
    gmt_t *gmt = globals_get_gmt();

    size_t n_results = n_mps;
    owner_result_t *results = array_create(owner_result_t, n_results);

    // make a set of owners
    for (i = 0; i < n_mps; i++) {
        mgasptr_t mp = mps[i];
        owner_result_t *result = &results[i];
        MGAS_ASSERT(gmt_calc_home(gmt, mp) == me);

        gmt_entry_t *entry = gmt_find_entry(gmt, mp);

        if (access == MGAS_ACCESS_OWN) {
            mgas_proc_t old_owner = gmt_entry_get_raw_owner(entry);
            
            gmt_entry_begin_migration(entry, initiator,
                                      &result->owner, &result->block_size);
        } else {
            // owner assignment strategy: first touch
            gmt_entry_get_owner(entry, initiator,
                                &result->owner, &result->block_size);
        }

        MGAS_ASSERT(result->block_size > 0);
    }

    // handle multiple mps in one page
    if (access == MGAS_ACCESS_OWN) {
        mgasptr_t prev_mp_base = MGAS_NULL;
        mgas_proc_t prev_mp_owner = MGAS_INVALID_PID;
        size_t i;
        for (i = 0; i < n_mps; i++) {
            mgasptr_t mp_base = gmt_calc_block_base(gmt, mps[i]);

            if (mp_base != prev_mp_base) {
                prev_mp_base = mp_base;
                prev_mp_owner = results[i].owner;
            } else {
                MGAS_ASSERT(prev_mp_owner != MGAS_INVALID_PID);
                MGAS_ASSERT(results[i].owner == MGAS_PID_MIGRATING);
                results[i].owner = prev_mp_owner;
            }
        }
    }

    MGAS_PROF_END(OVH_PROC_OWR);

    // reply owners of mvs
    MGAS_PROF_BEGIN(COMM_SYS);
    comm_reply_owners(msg, results, n_results);
    MGAS_PROF_END(COMM_SYS);

    array_destroy(results);
}

void
mgas_process_owner_reply(const owner_result_t results[], size_t n_results,
                         join_counter_t *jc, owner_result_t *result_buf)
{
    MGAS_PROF_BEGIN(OVH_PROC_OWR);
    memcpy(result_buf, results, sizeof(results[0]) * n_results);
    MGAS_PROF_END(OVH_PROC_OWR);

    join_counter_notify(jc, 1);
}

static mgasptr_t *
create_array_mps_from_pairs(const mem_pair_t *pairs, size_t n_pairs)
{
    size_t i;

    mgasptr_t *mps = array_create(mgasptr_t, n_pairs);
    for (i = 0; i < n_pairs; i++)
        mps[i] = pairs[i].mp;

    return mps;
}

static void
request_owners(const partitions_t *home_parts, mgas_access_t access,
               owner_result_t results[])
{
    MGAS_PROF_BEGIN(REQ_OWNERS);

    mgas_proc_t proc;
    size_t n_pairs;
    partitions_iter_t iter;

    mgas_proc_t me = globals_get_pid();
    size_t n_parts = partitions_size(home_parts);

    // initialize join counter
    join_counter_t jc;
    join_counter_init(&jc, n_parts);

    size_t idx = 0;
    partitions_iter_init(&iter, home_parts);
    for (;;) {
        const mem_pair_t *pairs = partitions_iter_next(&iter, &proc, &n_pairs);

        if (pairs == NULL)
            break;

        mgasptr_t *mps = create_array_mps_from_pairs(pairs, n_pairs);
        
        // send request resolving owners of blocks
        MGAS_PROF_BEGIN(COMM_SYS);
        comm_request_owners(mps, n_pairs, access, &jc, &results[idx], proc);
        MGAS_PROF_END(COMM_SYS);

        idx += n_pairs;

        array_destroy(mps);
    }

    // wait reply messages
    MGAS_PROF_BEGIN(COMM_WAIT);
    MGAS_PROF_BEGIN(COMM_WAIT_OWNREQ);
    join_counter_wait(&jc);
    MGAS_PROF_END(COMM_WAIT_OWNREQ);
    MGAS_PROF_END(COMM_WAIT);

    MGAS_PROF_END(REQ_OWNERS);
}

//-- data transfer -------------------------------------------------------------
static void
copy_owned_pages_v(const mem_pair_t pairs[], size_t n_pairs,
                   mgas_access_t access, varray_t *remote_pairs)
{
    size_t i;
    gmt_t *gmt = globals_get_gmt();
    
    for (i = 0; i < n_pairs; i++) {
        const mem_pair_t *pair = &pairs[i];

        gmt_entry_t *entry = gmt_find_entry(gmt, pair->mp);

        // in the case of OWN, it has already been locked.
        if (access != MGAS_ACCESS_OWN)
            gmt_entry_page_read_lock(entry);

        if (gmt_entry_page_valid(entry)) {
            uint8_t *page_base = gmt_entry_get_block(entry);
            size_t offset = gmt_calc_block_offset(gmt, pair->mp);
            uint8_t *page_buf = page_base + offset;

//            const dist_t *dist = gmt_get_dist(gmt, pair->mp);
//            MGAS_ASSERT(offset + pair->size <= dist_calc_block_size(dist));


            // copy one page
            if (access == MGAS_ACCESS_GET) {
                MGAS_PROF_BEGIN(COMM_MEMGET);
                memcpy(pair->p, page_buf, pair->size);
                MGAS_PROF_END(COMM_MEMGET);
            } else if (access == MGAS_ACCESS_PUT) {
                MGAS_PROF_BEGIN(COMM_MEMPUT);
                memcpy(page_buf, pair->p, pair->size);
                MGAS_PROF_END(COMM_MEMPUT);
            }
        } else {
            // add to remote pointer lists
            varray_add(remote_pairs, pair);
        }
        
        if (access != MGAS_ACCESS_OWN)
            gmt_entry_page_read_unlock(entry);
    }
}

static void
partition_pairs_by_home(const mem_pair_t pairs[], size_t n_pairs,
                        partitions_t *ps)
{
    MGAS_PROF_BEGIN(PART_HOME);

    size_t i;
    gmt_t *gmt = globals_get_gmt();

    for (i = 0; i < n_pairs; i++) {
        const mem_pair_t *pair = &pairs[i];
        mgas_proc_t home = gmt_calc_home(gmt, pair->mp);
        partitions_add(ps, home, pair);
    }

    MGAS_PROF_END(PART_HOME);
}

static void
prepare_pages_v(const partitions_t *home_parts, mgas_access_t access,
                const owner_result_t *owner_results)
{
    // prepare page buffers for OWN or first-touch operations

    MGAS_PROF_BEGIN(PREPARE_PAGE);

    size_t i;
    mgas_proc_t proc;
    mem_pair_t *pairs;
    size_t n_pairs;
    partitions_iter_t iter;

    gmt_t *gmt = globals_get_gmt();

    size_t idx = 0;
    partitions_iter_init(&iter, home_parts);
    for (;;) {
        pairs = partitions_iter_next(&iter, &proc, &n_pairs);

        if (pairs == NULL)
            break;

        for (i = 0; i < n_pairs; i++) {
            const owner_result_t *result = &owner_results[idx];

            // if not (first touch or OWN)
            if (result->owner != MGAS_INVALID_PID &&
                access != MGAS_ACCESS_OWN) {
                MGAS_ASSERT(pairs[i].p != NULL);
                MGAS_ASSERT(pairs[i].size != 0);
                idx += 1;
                continue;
            }

            if (result->owner == MGAS_PID_MIGRATING) {
                MGAS_ASSERT(access == MGAS_ACCESS_OWN);
                // give up migration
                idx += 1;
                continue;
            }
            
            gmt_entry_t *entry = gmt_find_entry(gmt, pairs[i].mp);
                
            // lock
            if (access != MGAS_ACCESS_OWN) {
                gmt_entry_page_write_lock(entry);
            } else {
                // already locked at make_nowned_pair_list_with_lock
            }

            MGAS_ASSERT(gmt_entry_page_invalid(entry));

            uint8_t *page = allocate_page(result->block_size);
//            DPUTS("page allocated: [%p, %p) (size = %zu)",
//                  page, page + result->block_size, result->block_size);

#ifdef DEBUG
//memset(page, 0, result->block_size);
#endif

            gmt_entry_page_prepare(entry, page);
                
            // do validation if first touch PUT/GET
            if (access != MGAS_ACCESS_OWN) {
                
                // first touch
                gmt_entry_page_validate(entry);

                // unlock
                gmt_entry_page_write_unlock(entry);
            }
            
            // fill pair.{p, size}
            if (access == MGAS_ACCESS_OWN) {
                MGAS_ASSERT(pairs[i].p == NULL);
                MGAS_ASSERT(pairs[i].size == 0);
                pairs[i].p = page;
                pairs[i].size = result->block_size;
            }

            idx += 1;
        }
    }

    MGAS_PROF_END(PREPARE_PAGE);
}

static void
partition_pairs_by_owner(const partitions_t *home_parts,
                         const owner_result_t *owner_results,
                         mgas_access_t access,
                         partitions_t *owner_parts)
{
    MGAS_PROF_BEGIN(PART_OWNER);
    
    size_t i;
    mgas_proc_t proc;
    size_t n_pairs;
    partitions_iter_t iter;

    size_t idx = 0;
    partitions_iter_init(&iter, home_parts);
    for (;;) {
        const mem_pair_t *pairs = partitions_iter_next(&iter, &proc, &n_pairs);

        if (pairs == NULL)
            break;

        for (i = 0; i < n_pairs; i++) {
            const owner_result_t *result = &owner_results[idx];

            if (result->owner != MGAS_INVALID_PID &&
                result->owner != MGAS_PID_MIGRATING) {
                MGAS_ASSERT(pairs[i].p != NULL);
                MGAS_ASSERT(pairs[i].size != 0);
            }
            
            partitions_add(owner_parts, result->owner, &pairs[i]);
            idx += 1;
        }
    }

    MGAS_PROF_END(PART_OWNER);
}

static void
make_transfer_lists_with_lock(const mem_pair_t *pairs, size_t n_pairs,
                              mgas_access_t access, mgas_proc_t initiator,
                              varray_t *local_list, varray_t *remote_list,
                              varray_t *retry_list, varray_t *free_entry_list)
{
    size_t i;
    gmt_t *gmt = globals_get_gmt();
    mgas_proc_t me = globals_get_pid();

    for (i = 0; i < n_pairs; i++) {
        mgasptr_t mp = pairs[i].mp;
        void *p = pairs[i].p;
        size_t size = pairs[i].size;

        gmt_entry_t *entry = gmt_find_entry(gmt, mp);

        // lock the page
        if (access == MGAS_ACCESS_OWN && initiator != me) {
            if (!gmt_entry_page_try_write_lock(entry)) {
                // this page is now migrating
                continue;
            }               
        } else if (access == MGAS_ACCESS_OWN && initiator == me) {
            // do nothing
            continue;
        } else {
            // PUT/GET
            if (!gmt_entry_page_try_read_lock(entry)) {
                varray_add(retry_list, &i);
                continue;
            }
        }

        if (gmt_entry_page_valid(entry)) {
            uint8_t *page = gmt_entry_get_block(entry);
            size_t offset = gmt_calc_block_offset(gmt, mp);

//            const dist_t *dist = gmt_get_dist(gmt, mp);
//            MGAS_ASSERT(offset + size <= dist_calc_block_size(dist));
            
            // add memvec to vector list
            mgas_memvec_t local_v = { page + offset, size };
            varray_add(local_list, &local_v);
            mgas_memvec_t remote_v = { p, size };
            varray_add(remote_list, &remote_v);
            
            // add to processing entry list
            varray_add(free_entry_list, &entry);
        } else {
            if (access == MGAS_ACCESS_OWN)
                gmt_entry_page_write_unlock(entry);
            else
                gmt_entry_page_read_unlock(entry);
            
            varray_add(retry_list, &i);
        }
    }
}

static void
unlock_and_free_pages(varray_t *free_entry_list, mgas_access_t access)
{
    size_t i;

    size_t n_entries = varray_size(free_entry_list);
    for (i = 0; i < n_entries; i++) {
        gmt_entry_t *entry = *(void **)varray_raw(free_entry_list, i);
        
        if (access == MGAS_ACCESS_OWN) {
            void *page = gmt_entry_page_invalidate(entry);
            free_page(page);

            gmt_entry_page_write_unlock(entry);
        } else {
            gmt_entry_page_read_unlock(entry);
        }
    }
}

static void
transfer_data(const varray_t *local_list, const varray_t *remote_list,
              mgas_access_t access, mgas_proc_t target)
{
    const mgas_memvec_t *local_vs = varray_raw(local_list, 0);
    size_t n_local_vs = varray_size(local_list);
    const mgas_memvec_t *remote_vs = varray_raw(remote_list, 0);
    size_t n_remote_vs = varray_size(remote_list);

    if (n_local_vs > 0) {
        if (access == MGAS_ACCESS_PUT) {
            MGAS_PROF_BEGIN(COMM_PUT);
            comm_get_v(local_vs, n_local_vs, remote_vs, n_remote_vs, target);
            MGAS_PROF_END(COMM_PUT);
        } else if (access == MGAS_ACCESS_GET) {
            MGAS_PROF_BEGIN(COMM_GET);
            comm_put_v(remote_vs, n_remote_vs, target, local_vs, n_local_vs);
            MGAS_PROF_END(COMM_GET);
        } else if (access == MGAS_ACCESS_OWN) {
            MGAS_PROF_BEGIN(COMM_GET);
            comm_put_v(remote_vs, n_remote_vs, target, local_vs, n_local_vs);
            MGAS_PROF_END(COMM_GET);
        }
    }
}

static void
reply_data_transfer(const msg_t *msg, const varray_t *retry_list)
{
    const size_t *retry_indices = varray_raw(retry_list, 0);
    size_t n_retry_indices = varray_size(retry_list);

    MGAS_PROF_BEGIN(COMM_SYS);
    comm_reply_data_transfer(msg, retry_indices, n_retry_indices);
    MGAS_PROF_END(COMM_SYS);
}

void
mgas_process_data_transfer_request(const msg_t *msg, const mem_pair_t pairs[],
                                   size_t n_pairs, mgas_access_t access,
                                   mgas_proc_t initiator)
{
//    DPUTS("PROCESS_DATA(%s, %"PRIx64")", string_of_access(access), pairs->mp);
    MGAS_PROF_BEGIN(OVH_PROC_DAT); 

    size_t i;
    mgas_proc_t me = globals_get_pid();
    gmt_t *gmt = globals_get_gmt();

    MGAS_ASSERT(n_pairs != 0);

    varray_t *local_list = varray_create(sizeof(mgas_memvec_t), 1024);
    varray_t *remote_list = varray_create(sizeof(mgas_memvec_t), 1024);
    varray_t *retry_list = varray_create(sizeof(size_t), 1024);
    varray_t *free_entry_list = varray_create(sizeof(void *), 1024);
    
    // lock & make local memory vectors and not owned vectors
    make_transfer_lists_with_lock(pairs, n_pairs, access, initiator,
                                  local_list, remote_list,
                                  retry_list, free_entry_list);

    MGAS_PROF_END(OVH_PROC_DAT); 

    // data transfer
    transfer_data(local_list, remote_list, access, initiator);

    // send a reply message
    reply_data_transfer(msg, retry_list);

    MGAS_PROF_BEGIN(OVH_PROC_DAT); 

    // invalidate pages if OWN, and unlock
    unlock_and_free_pages(free_entry_list, access);
    
    // finalize
    varray_destroy(local_list);
    varray_destroy(remote_list);
    varray_destroy(retry_list);
    varray_destroy(free_entry_list);

    MGAS_PROF_END(OVH_PROC_DAT); 
}

static void
validate_pages_if_not_retry(const mem_pair_t *pairs, size_t n_pairs,
                            mgas_access_t access, const size_t retry_indices[],
                            size_t n_retry_indices, varray_t *done_list,
                            varray_t *retry_list)

{
    size_t i;
    gmt_t *gmt = globals_get_gmt();

    size_t ri_idx = 0;
    for (i = 0; i < n_pairs; i++) {
        const mem_pair_t *pair = &pairs[i];
        mgasptr_t mp_base = gmt_calc_block_base(gmt, pair->mp);

        if (ri_idx < n_retry_indices && i == retry_indices[ri_idx]) {
            // when data transfer failed
            varray_add(retry_list, pair);
            ri_idx += 1;
        } else {
            // when data transfer succeeded
            varray_add(done_list, pair);

            // validate transferred page
            // (in the case of own, the pair indicates a page,
            //  so double-validation does not occur.)
            if (access == MGAS_ACCESS_OWN) {
                gmt_entry_t *entry = gmt_find_entry(gmt, pair->mp);

                // validate the prepared page
                gmt_entry_page_validate(entry);

                // unlock (locked at make_nowned_pair_list_with_lock
                gmt_entry_page_write_unlock(entry);
            }
        }
    }
}

void
mgas_process_data_transfer_reply(const size_t retry_indices[],
                                 size_t n_retry_indices,
                                 const data_rep_arg_t *arg)
{
    MGAS_PROF_BEGIN(OVH_PROC_DAT);

    spinlock_lock(arg->lock);

    // validate and unlock succeeded pages
    validate_pages_if_not_retry(arg->pairs, arg->n_pairs, arg->access,
                                retry_indices, n_retry_indices,
                                arg->done_list, arg->retry_list);

    // notify end of data transfer
    join_counter_notify(arg->jc, 1);

    spinlock_unlock(arg->lock);

    MGAS_PROF_END(OVH_PROC_DAT);
}

static void
request_data_transfers(const partitions_t *owner_parts, mgas_access_t access,
                       varray_t *done_list, varray_t *retry_list)
{
    MGAS_PROF_BEGIN(REQ_DATA);

    size_t i;
    mgas_proc_t proc;
    size_t n_pairs;
    partitions_iter_t iter;
    size_t me = globals_get_pid();
    gmt_t *gmt = globals_get_gmt();

    size_t n_owners = partitions_size(owner_parts);

    // done/retry_list lock
    spinlock_t lock;
    spinlock_init(&lock);

    // handler arguments
    data_rep_arg_t *rep_args = array_create(data_rep_arg_t, n_owners);
    
    // prepare synchronization
    join_counter_t jc;
    join_counter_init(&jc, n_owners);

    size_t idx = 0;
    partitions_iter_init(&iter, owner_parts);
    for (;;) {
        const mem_pair_t *pairs = partitions_iter_next(&iter, &proc, &n_pairs);

        if (pairs == NULL)
            break;

        // if this access is first touch, only put operation is performed
        if (proc == MGAS_INVALID_PID && access == MGAS_ACCESS_PUT)
            proc = me;

        if (proc == MGAS_INVALID_PID) {

            // if the access is first touch or
            //    the page is migrating, do nothing
            join_counter_notify(&jc, 1);

        } else if (proc == MGAS_PID_MIGRATING) {

            for (i = 0; i < n_pairs; i++) {
                if (access == MGAS_ACCESS_OWN) {
                    // give up
                    gmt_entry_t *entry = gmt_find_entry(gmt, pairs[i].mp);
                    gmt_entry_page_write_unlock(entry);
                } else {
                    // retry
                    varray_add(retry_list, &pairs[i]);
                }
            }

            join_counter_notify(&jc, 1);

        } else {
            data_rep_arg_t v = { pairs, n_pairs, access, &lock, done_list,
                                 retry_list, &jc };
            rep_args[idx] = v;
#if 0
            MGAS_PROF_BEGIN(GET_ZEROFILL);

            if (access == MGAS_ACCESS_GET)
                for (i = 0; i < n_pairs; i++)
                    memset(pairs[i].p, 0, pairs[i].size);

            MGAS_PROF_END(GET_ZEROFILL);
#endif
            // send data transfer request to owner
            MGAS_PROF_BEGIN(COMM_SYS);
            comm_request_data_transfer(pairs, n_pairs, access, &rep_args[idx],
                                       proc);
            MGAS_PROF_END(COMM_SYS);
        }

        idx += 1;
    }

    // wait
    MGAS_PROF_BEGIN(COMM_WAIT);
    MGAS_PROF_BEGIN(COMM_WAIT_DATAREQ);
    join_counter_wait(&jc);
    MGAS_PROF_END(COMM_WAIT_DATAREQ);
    MGAS_PROF_END(COMM_WAIT);

    // finalize
    spinlock_destroy(&lock);
    array_destroy(rep_args);

    MGAS_PROF_END(REQ_DATA);
}

static void request_owner_change(const mem_pair_t pairs[], size_t n_pairs);

static void
mgas_try_copy_v(const mem_pair_t pairs[], size_t n_pairs, mgas_access_t access,
                varray_t *retry_list)
{
    size_t n_procs = globals_get_n_procs();

    partitions_t *home_parts = partitions_create(sizeof(mem_pair_t), n_procs);
    partitions_t *owner_parts = partitions_create(sizeof(mem_pair_t), n_procs);
    varray_t *done_list = varray_create(sizeof(mem_pair_t), n_pairs);
    owner_result_t *owner_results = array_create(owner_result_t, n_pairs);

    // partition mgasptrs by home process
    partition_pairs_by_home(pairs, n_pairs, home_parts);

    // resolve owner of each page
    request_owners(home_parts, access, owner_results);

    // prepare page buffers
    prepare_pages_v(home_parts, access, owner_results);

    // partition pairs by owner process
    partition_pairs_by_owner(home_parts, owner_results, access, owner_parts);
    
    // request data transfer to each owner process
    request_data_transfers(owner_parts, access, done_list, retry_list);

    if (access == MGAS_ACCESS_OWN) {
        // array of pair must be sorted.
        varray_sort(done_list, compare_pairs);

        mem_pair_t *done_pairs = varray_raw(done_list, 0);
        size_t n_done_pairs = varray_size(done_list);

        // request changing owner of pages to home
        request_owner_change(done_pairs, n_done_pairs);
    }

    // array pairs must be sorted
    varray_sort(retry_list, compare_pairs);
    
    // finalize
    partitions_destroy(home_parts);
    partitions_destroy(owner_parts);
    varray_destroy(done_list);
    array_destroy(owner_results);
}

void
mgas_copy_v(const mem_pair_t pairs[], size_t n_pairs, mgas_access_t access)
{
    assert_pairs(pairs, n_pairs);

    if (access == MGAS_ACCESS_GET)
        MGAS_PROF_BEGIN(COPY_GET);
    else if (access == MGAS_ACCESS_PUT)
        MGAS_PROF_BEGIN(COPY_PUT);
    else if (access == MGAS_ACCESS_OWN)
        MGAS_PROF_BEGIN(COPY_OWN);

#if 1
    // allocation-avoiding version

    // this function is not called recursively, so we can use global variable.
    static __thread mgas_bool_t init = MGAS_FALSE;
    static __thread varray_t *remote_pair_list = NULL;
    static __thread varray_t *retry_pair_list = NULL;

    if (!init) {
        remote_pair_list = varray_create(sizeof(pairs[0]), 1024);
        retry_pair_list = varray_create(sizeof(pairs[0]), 1024);
        init = MGAS_TRUE;
    }
    varray_clear(remote_pair_list);
    varray_clear(retry_pair_list);
#else
    varray_t *remote_pair_list = varray_create(sizeof(pairs[0]), 1024);
    varray_t *retry_pair_list = varray_create(sizeof(pairs[0]), 1024);
#endif
    
    // copy owned pages without resolving their owners
    copy_owned_pages_v(pairs, n_pairs, access, remote_pair_list);

    size_t retry_count = 0;
    
    varray_t *try_pair_list = remote_pair_list;
    for (;;) {
        const mem_pair_t *try_pairs = varray_raw(try_pair_list, 0);
        size_t n_try_pairs = varray_size(try_pair_list);

        if (n_try_pairs == 0)
            break;

        // copy pages
        mgas_try_copy_v(try_pairs, n_try_pairs, access, retry_pair_list);
        
        // prepare to retry
        varray_t *tmp = try_pair_list;
        try_pair_list = retry_pair_list;
        retry_pair_list = tmp;
        varray_clear(retry_pair_list);
        
        comm_poll();

        retry_count += 1;
        MGAS_ASSERT(retry_count < 100000);
    }
    if (retry_count > 10)
        DPUTS("N_RETIRES = %zu", retry_count);

#if 0
    varray_destroy(try_pair_list);
    varray_destroy(retry_pair_list);
#endif

    if (access == MGAS_ACCESS_GET)
        MGAS_PROF_END(COPY_GET);
    else if (access == MGAS_ACCESS_PUT)
        MGAS_PROF_END(COPY_PUT);
    else if (access == MGAS_ACCESS_OWN)
        MGAS_PROF_END(COPY_OWN);
}

//-- own -----------------------------------------------------------------------

void
mgas_process_owner_change(const mgasptr_t mps[], size_t n_mps, mgas_proc_t proc)
{
    MGAS_PROF_BEGIN(OVH_PROC_OWC);

    size_t i;
    mgas_proc_t me = globals_get_pid();
    gmt_t *gmt = globals_get_gmt();

    for (i = 0; i < n_mps; i++) {
        mgasptr_t mp = mps[i];
        MGAS_ASSERT(gmt_calc_home(gmt, mp) == me);

        gmt_entry_t *entry = gmt_find_entry(gmt, mp);

        mgas_proc_t old_owner;
        size_t block_size;
        gmt_entry_get_owner(entry, proc, &old_owner, &block_size);

        MGAS_ASSERT(old_owner != MGAS_INVALID_PID);
        MGAS_ASSERT(old_owner != proc);
        
        gmt_entry_end_migration(entry, proc);
    }

    // do not reply

    MGAS_PROF_END(OVH_PROC_OWC);
}

static void
request_owner_change(const mem_pair_t pairs[], size_t n_pairs)
{
    MGAS_PROF_BEGIN(REQ_OWNCHG);

    mgas_proc_t proc;
    size_t n_ps;
    partitions_iter_t iter;
    size_t n_procs = globals_get_n_procs();

    partitions_t *home_parts = partitions_create(sizeof(mem_pair_t), n_procs);

    // partition mvs by home process
    partition_pairs_by_home(pairs, n_pairs, home_parts);

    // send owner change requests to home processes
    partitions_iter_init(&iter, home_parts);
    for (;;) {
        mem_pair_t *ps = partitions_iter_next(&iter, &proc, &n_ps);

        if (ps == NULL)
            break;

        mgasptr_t *mps = create_array_mps_from_pairs(ps, n_ps);
        MGAS_PROF_BEGIN(COMM_SYS);
        comm_request_owner_change(mps, n_ps, proc);
        MGAS_PROF_END(COMM_SYS);

        array_destroy(mps);
    }

    // do not wait for completion of owner change

    // finalize
    partitions_destroy(home_parts);

    MGAS_PROF_END(REQ_OWNCHG);
}


static void
make_nowned_pair_list_with_lock(const mgas_vector_t *mvs, size_t n_mvs,
                                varray_t *pair_list)
{
    mgasptr_t mp;
    size_t size;
    dist_iterv_t iter;
    gmt_t *gmt = globals_get_gmt();

    size_t i = 0;

    // lock pages, and make vectors/memvecs representing whole pages
    dist_iterv_init(&iter, mvs, n_mvs);
    while (dist_iterv_next(&iter, &mp, &size)) {
        gmt_entry_t *entry = gmt_find_entry(gmt, mp);

        // acquire page lock
        if (!gmt_entry_page_try_write_lock(entry)) {
            // this page is now migrating
            continue;
        }
        
        if (gmt_entry_page_valid(entry)) {
            // do not copy a page which this process owns.
            gmt_entry_page_write_unlock(entry);
        } else {
            mgasptr_t mp_base = gmt_calc_block_base(gmt, mp);

            // add vector/memvec to copy list.
            mem_pair_t pair = { mp_base, NULL, 0 };
            varray_add(pair_list, &pair);
        }
    }
}

void mgas_own_v(const mgas_vector_t *mvs, size_t n_mvs)
{
    MGAS_PROF_BEGIN(OWN);

    globals_giant_lock();

    varray_t *pair_list = varray_create(sizeof(mem_pair_t), 1024);

    // lock pages if not owned (unlocked in request_data_transfers),
    // and make mem_pairs representing whole pages
    make_nowned_pair_list_with_lock(mvs, n_mvs, pair_list);

    const mem_pair_t *pairs = varray_raw(pair_list, 0);
    size_t n_pairs = varray_size(pair_list);
    assert_pairs(pairs, n_pairs);
    
    MGAS_PROF_BEGIN(COPY_OWN);

    // data transfer (including owner change)
    mgas_copy_v(pairs, n_pairs, MGAS_ACCESS_OWN);

    MGAS_PROF_END(COPY_OWN);

    assert_pages_are_unlocked(pairs, n_pairs);
    
    // finalization
    varray_destroy(pair_list);

    globals_giant_unlock();

    MGAS_PROF_END(OWN);
}
