#include "madm.h"
#include "madm-cxx.h"

#include <mgas.h>

using namespace madi;
namespace uth = madm::uth;

extern "C" {

    void madm_start(void (*f)(int, char **, void *),
                    int *argc, char ***argv, void *p)
    {
        madm::start(f, *argc, *argv, p);
    }

    /*
    madm_bool_t madm_initialized(void)
    {
    }
    */

    madm_pid_t madm_get_pid(void)
    {
        return madm::get_pid();
    }

    size_t madm_get_n_procs(void)
    {
        return madm::get_n_procs();
    }

    /*
    madm_pid_t madm_home(madm_t th);
    madm_bool_t madm_stolen(void);
    */

    madm_t madm_create(madm_func_t f, madm_ptr_t p)
    {
        madm::thread<madm_ptr_t> th([=] {
            // call the specified function
            return f(p);
        });

        // cast madm::thread to madm_t
        madm_t md;
        auto * md_ptr = reinterpret_cast<decltype(th) *>(&md);
        memcpy(md_ptr, &th, sizeof(madm_t));

        return md;
    }

    /*
    madm_t madm_create__(madm_func_local_t f, void *p, size_t size, 
                         size_t result_size);
    */

    madm_ptr_t madm_join(madm_t th)
    {
        madm::thread<madm_ptr_t> thr;
        memcpy(&thr, &th, sizeof(thr));

        return thr.join();
    }

    void madm_poll()
    {
        madm::poll();
    }

    void madm_barrier()
    {
        madm::barrier();
    }

    madm_ptr_t madm_malloc(size_t size)
    {
        return mgas_malloc(size);
    }

    void madm_free(madm_ptr_t mp)
    {
        return mgas_free(mp);
    }

    void madm_free_small(madm_ptr_t mp, size_t size)
    {
        return mgas_free_small(mp, size);
    }

    madm_ptr_t madm_all_dmalloc(size_t size, size_t n_dims, size_t block_size[],
                                size_t n_blocks[])
    {
        return mgas_all_dmalloc(size, n_dims, block_size, n_blocks);
    }

    void madm_all_free(madm_ptr_t mp)
    {
        madm::all_dealloc_darray<char>(mp);
    }

    void *madm_localize(madm_ptr_t mp, size_t size, madm_flag_t flags)
    {
        auto& handle = madi::worker_storage::instance().current_handle();

        MADI_LOCALIZE_DPUTS("localize %p", &handle);

        return mgas_localize(mp, size, (mgas_flag_t)flags, &handle);
    }

    void madm_commit(madm_ptr_t mp, void *p, size_t size)
    {
        mgas_commit(mp, p, size);
    }

    void madm_unlocalize()
    {
        madm::unlocalize();
    }

    void *madm_localize_s(madm_ptr_t mp, size_t stride, const size_t count[2],
                          madm_flag_t flags)
    {
        auto& handle = madi::worker_storage::instance().current_handle();

        MADI_LOCALIZE_DPUTS("localize %p", &handle);

        return mgas_localize_s(mp, stride, count, (mgas_flag_t)flags,
                               &handle);
    }

    void madm_commit_s(madm_ptr_t mp, void *p, size_t stride,
                       const size_t count[2])
    {
        mgas_commit_s(mp, p, stride, count);
    }

    void madm_get(void *p, madm_ptr_t mp, size_t size)
    {
        mgas_get(p, mp, size);
    }

    void madm_put(madm_ptr_t mp, void *p, size_t size)
    {
        mgas_put(mp, p, size);
    }

    void madm_set(madm_ptr_t mp, int value, size_t size)
    {
        mgas_set(mp, value, size);
    }

    /*
    void madm_broadcast(void *p, size_t size, madm_pid_t root);
    void madm_gather(void *dst, void *src, size_t size, madm_pid_t root);
    void madm_reduce_sum_long(void *dst, void *src, size_t size,
                              madm_pid_t root);
    */

    double madm_time()
    {
        return madm::time();
    }
}

