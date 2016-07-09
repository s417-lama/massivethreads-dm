
#include "../../include/mgas_debug.h"

#include <mgas.h>
#include <mgas_prof.h>
#include <math.h>

enum {
    PAGE_SIZE = 4096
};

typedef enum owner {
    OWNER_ME,
    OWNER_HOME,
    OWNER_OTHER,
} owner_t;

// home == owner にからデータを localize したときの通信時間
static void measure_localize(mgas_proc_t me, size_t n_procs,
                             size_t max_size, owner_t owner)
{
    size_t i;

    size_t mod = max_size % PAGE_SIZE;
    MGAS_ASSERT(mod == 0);

    size_t real_size = max_size * n_procs;
    size_t n_dims = 1;
    size_t block_size[1] = { PAGE_SIZE };
    size_t n_blocks[1] = { real_size / block_size[0] };
    mgasptr_t mp_base = mgas_all_dmalloc(real_size, n_dims,
                                         block_size, n_blocks);

    // initial acccess
    // - this enables us to omit initial cost
    {
        mgas_handle_t h = MGAS_HANDLE_INIT;

        // cyclic distribution using first touch
        {
            mgasptr_t mp = mp_base + PAGE_SIZE * me;
            size_t stride = PAGE_SIZE * n_procs;
            size_t count[] = { real_size / PAGE_SIZE, PAGE_SIZE };
            mgas_localize_s(mp, stride, count, MGAS_REUSE, &h);
        }

        // make global memory table entry
        if (me == 0) {
            mgasptr_t mp = mp_base;
            size_t stride = real_size;
            size_t count[] = { 1, real_size };
            mgas_localize_s(mp, stride, count, MGAS_REUSE, &h);
        }

        mgas_unlocalize(&h);
    }

//    mgas_prof_start();

    if (me == 0) {
        // measure performance
        printf("# function\towner\tdata size (bytes)\ttime (sec)\n");

        const char *owner_str;
        mgasptr_t mp;
        if (owner == OWNER_ME) {
            owner_str = "me";
            mp = mp_base;
        } else if (owner == OWNER_HOME) {
            owner_str = "home";
            mp = mp_base + PAGE_SIZE;
        } else {
            MGAS_UNDEFINED;
        }

        size_t stride = PAGE_SIZE * n_procs;

        // small size (< PAGE_SIZE)
        size_t data_size;
        for (data_size = 1; data_size < PAGE_SIZE; data_size *= 2) {

            enum { trials = 64 };

            size_t size = PAGE_SIZE;
            size_t count[] = { 1, data_size };

            for (i = 0; i < trials; i++) {
                size_t idx = (stride * i) % real_size;

                mgas_handle_t h = MGAS_HANDLE_INIT;
                mgas_localize_s(mp + idx, stride, count, MGAS_REUSE, &h);
                mgas_unlocalize(&h);
            }

            mgas_handle_t h = MGAS_HANDLE_INIT;

            double t0 = now();
            for (i = 0; i < trials; i++) {
                size_t idx = (stride * i) % real_size;
                mgas_localize_s(mp + idx, stride, count, MGAS_REUSE, &h);
            }
            double t1 = now();

            mgas_unlocalize(&h);

            printf("localize\t%s\t%8zd\t%.9f\n",
                   owner_str, data_size, (t1 - t0) / trials);
        }

        // large size (>= PAGE_SIZE)
        size_t n_pages;
        for (n_pages = 1; n_pages * PAGE_SIZE <= max_size; n_pages *= 2) {

            size_t trials = 32;

            size_t size = PAGE_SIZE * n_procs * n_pages;
            size_t count[] = { n_pages, PAGE_SIZE };

            mgas_handle_t h = MGAS_HANDLE_INIT;
            mgas_localize_s(mp, stride, count, MGAS_REUSE, &h);
            mgas_unlocalize(&h);

            double t0 = now();
            mgas_localize_s(mp, stride, count, MGAS_REUSE, &h);
            double t1 = now();

            mgas_unlocalize(&h);

            printf("localize\t%s\t%8zd\t%.9f\n",
                   owner_str, n_pages * PAGE_SIZE, t1 - t0);

        }
    }

    // mgas_all_free(mp);

    mgas_barrier();

//    mgas_prof_stop();
//    mgas_prof_output(stderr);
}

static void measure_cache_hit(mgas_proc_t me, size_t n_procs)
{
    size_t i;

    size_t n_dims = 1;
    size_t block_size[1] = { PAGE_SIZE };
    size_t n_blocks[1] = { 8192 / block_size[0] };
    mgasptr_t mp = mgas_all_dmalloc(8192, n_dims, block_size, n_blocks);

    if (me == 0) {
        mgas_handle_t handle = MGAS_HANDLE_INIT;

        // cache data
        size_t count[] = { 1, 8192 };
        mgas_localize_s(mp, 0, count, MGAS_REUSE, &handle);

        // first access time is not included.
        size_t count_[] = { 1, 1024 };
        mgas_localize_s(mp + 4096, 0, count_, MGAS_REUSE, &handle);

        // measure
        size_t n_trials = 4096;
        double t0 = now();
        for (i = 0; i < n_trials; i++) {
            mgas_handle_t h = MGAS_HANDLE_INIT;
            mgas_localize_s(mp + 4096, 0, count_, MGAS_REUSE, &h);
            mgas_unlocalize(&h);
        }
        double t1 = now();

        printf("localize\tcached\t%8zu\t%.9f\n",
               1024UL, (t1 - t0) / (double)n_trials);

        mgas_unlocalize(&handle);
    }

    // mgas_all_free(mp);
    mgas_barrier();
}

int main(int argc, char **argv)
{
    mgas_initialize(&argc, &argv);

    mgas_proc_t me = mgas_get_pid();
    size_t n_procs = mgas_get_n_procs();

    DPUTS("me = %zd, n_procs = %zd", me, n_procs);

    // FIXME: if the data size is more than 4MB, it does not works
    //        because of a limitation of AMMPI.
    measure_localize(me, n_procs, 4 * 1024 * 1024, OWNER_ME);
    measure_localize(me, n_procs, 4 * 1024 * 1024, OWNER_HOME);

    measure_cache_hit(me, n_procs);

    mgas_finalize();

    return 0;
}
