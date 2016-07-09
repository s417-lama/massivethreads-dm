#include "memory.h"

#include "handlers.h"
#include "memory_v.h"
#include "dist.h"


//---- contiguous access operations --------------------------------------------

void *mgas_localize(mgasptr_t mp, size_t size, mgas_flag_t flags,
                    mgas_handle_t *handle)
{
    MGAS_CHECK(mp != MGAS_NULL);
    MGAS_ASSERT(mp >= MGASPTR_MIN);

    const mgas_vector_t mvs[] = { { mp, size } };
    return mgas_localize_v(mp, mvs, 1, flags, handle);
}

void mgas_commit(mgasptr_t mp, void *p, size_t size)
{
    MGAS_CHECK(mp != MGAS_NULL);
    MGAS_ASSERT(mp >= MGASPTR_MIN);

    const mgas_vector_t mvs[] = { { mp, size } };
    mgas_commit_v(mp, p, mvs, 1);
}

//---- stride access operations ------------------------------------------------

static mgas_vector_t *create_array_mvs_from_stride(mgasptr_t mp, size_t stride,
                                                   const size_t count[2])
{
    size_t i;
    size_t n_mvs = count[0];
 
    mgas_vector_t *mvs = array_create(mgas_vector_t, n_mvs);
    for (i = 0; i < n_mvs; i++) {
        mvs[i].mp = mp + stride * i;
        mvs[i].size = count[1];
    }

    return mvs;
}
void *mgas_localize_s(mgasptr_t mp, size_t stride, const size_t count[2],
                      mgas_flag_t flags, mgas_handle_t *handle)
{
    size_t i;

    mgas_vector_t *mvs = create_array_mvs_from_stride(mp, stride, count);
    size_t n_mvs = count[0];
    void *p = mgas_localize_v(mp, mvs, n_mvs, flags, handle);

    array_destroy(mvs);
    return p;
}

void mgas_commit_s(mgasptr_t mp, void *p, size_t stride, const size_t count[2])

{
    size_t n_mvs = count[0];
    mgas_vector_t *mvs = create_array_mvs_from_stride(mp, stride, count);
    mgas_commit_v(mp, p, mvs, n_mvs);
    array_destroy(mvs);
}

//---- misc functions ----------------------------------------------------------

void mgas_put(mgasptr_t mp, void *p, size_t size)
{
    globals_giant_lock();

    mem_pair_t pairs[] = { { mp, p, size } };
    mgas_copy_v(pairs, 1, MGAS_ACCESS_PUT);
    
    /*
    varray_t *pair_list = varray_create(sizeof(mem_pair_t), 1024);

    mgas_vector_t mvs[] = { { mp, size } };
    make_pairs_from_mvs(mvs, 1, mp, p, size, pair_list);
    
    const mem_pair_t *pairs = varray_raw(pair_list, 0);
    size_t n_pairs = varray_size(pair_list);
    mgas_copy_v(pairs, n_pairs, MGAS_ACCESS_PUT);

    varray_destroy(pair_list);
    */
    
    globals_giant_unlock();
}

void mgas_get(void *p, mgasptr_t mp, size_t size)
{
    globals_giant_lock();

    mem_pair_t pairs[] = { { mp, p, size } };
    mgas_copy_v(pairs, 1, MGAS_ACCESS_GET);

    globals_giant_unlock();
}

void mgas_set(mgasptr_t mp, int value, size_t size)
{
    // reference implementation

    mgas_handle_t handle = MGAS_HANDLE_INIT;

    void *p = mgas_sys_malloc(size);
    memset(p, value, size);

    mgas_put(mp, p, size);

    mgas_sys_free(p);
}

//---- atomic operations -------------------------------------------------------

void mgas_process_rmw_reply(mgas_bool_t success, void *param_out,
                            const void *param_out_data, size_t param_out_size,
                            mgas_bool_t *result_buf)
{
    if (success)
        memcpy(param_out, param_out_data, param_out_size);
    
    *result_buf = success;
}

void mgas_process_rmw_request(const msg_t *msg, mgas_rmw_func_t f,
                              mgasptr_t mp, size_t size,
                              const void *param_in, size_t param_in_size,
                              void *param_out, size_t param_out_size)
{
    gmt_t *gmt = globals_get_gmt();
    size_t offset = gmt_calc_block_offset(gmt, mp);
    
//    MGAS_ASSERT(offset + size <= gmt_calc_row_size(gmt, mp));

    gmt_entry_t *entry = gmt_find_entry(gmt, mp);

    gmt_entry_page_read_lock(entry);

    void *param_out_buf;
    mgas_bool_t success = gmt_entry_page_valid(entry);

    if (success) {
        uint8_t *p = gmt_entry_get_block(entry);
        param_out_buf = mgas_sys_malloc(param_out_size);
        
        f(p + offset, size,
          param_in, param_in_size, param_out_buf, param_out_size);
    } else {
        param_out_buf = NULL;
    }

    gmt_entry_page_read_unlock(entry);

    comm_reply_rmw(msg, success, param_out_buf);

    if (param_out_buf != NULL)
        mgas_sys_free(param_out_buf);
}

void mgas_rmw(mgas_rmw_func_t f, mgasptr_t mp, size_t size,
              const void *param_in, size_t param_in_size,
              void *param_out, size_t param_out_size)
{
    mgas_proc_t me = globals_get_pid();
    size_t n_procs = globals_get_n_procs();
    gmt_t *gmt = globals_get_gmt();

    MGAS_CHECK(mp != MGAS_NULL);
    MGAS_CHECK(mp >= MGASPTR_MIN);
//    MGAS_CHECK(gmt_calc_block_offset(gmt, mp) + size
//               <= gmt_calc_row_size(gmt, mp));

    // if owned
    gmt_entry_t *entry = gmt_find_entry(gmt, mp);
    gmt_entry_page_read_lock(entry);
    if (gmt_entry_page_valid(entry)) {
        size_t offset = gmt_calc_block_offset(gmt, mp);
        uint8_t *p = gmt_entry_get_block(entry);

        f(p + offset, size,
          param_in, param_in_size, param_out, param_out_size);

        gmt_entry_page_read_unlock(entry);
        return;
    }
    gmt_entry_page_read_unlock(entry);

    mgas_proc_t home = gmt_calc_home(gmt, mp);

    for (;;) {
        // prepare synchronization
        join_counter_t jc;
        join_counter_init(&jc, 1);

        // request owner of the page
        owner_result_t owner_result;
        comm_request_owners(&mp, 1, MGAS_ACCESS_PUT, &jc, &owner_result, home);

        // wait
        join_counter_wait(&jc);

        mgas_proc_t owner = owner_result.owner;
        size_t block_size = owner_result.block_size;
        
        // first touch
        if (owner == MGAS_INVALID_PID) {
            gmt_entry_t *entry = gmt_find_entry(gmt, mp);

            gmt_entry_page_write_lock(entry);
            MGAS_ASSERT(!gmt_entry_page_valid(entry));
            validate_first_touch_page(entry, block_size);
            gmt_entry_page_write_unlock(entry);

            owner = me;
        }

        MGAS_ASSERT(owner < n_procs);

        mgas_bool_t success = comm_request_rmw(f, mp, size,
                                               param_in, param_in_size,
                                               param_out, param_out_size,
                                               owner);
        if (success)
            break;
    }

}
