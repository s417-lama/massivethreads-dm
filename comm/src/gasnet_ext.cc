#include "gasnet_ext.h"
#include "options.h"
#include <madm_debug.h>

extern "C" {

    size_t __attribute__((weak))
    mgas_init_gasnet_handlers(gasnet_handlerentry_t *handlers,
                              size_t start_idx)
    {
        // this function does nothing unless MGAS is linked
        return start_idx;
    }

}

namespace madi {
namespace comm {

    static void reduce_long_fn(void *results, size_t result_count,
                               const void *left_operands, size_t left_count,
                               const void *right_operands,
                               size_t elem_size, int flags, int arg)
    {
        size_t i;

        long *res = (long *)results;
        long *src1 = (long *)left_operands;
        long *src2 = (long *)right_operands;
        MADI_ASSERT(elem_size == sizeof(long));
        MADI_ASSERT(result_count==left_count);

        switch(arg) {
            case 0:
                for(i = 0; i < result_count; i++) {
                    res[i] = src1[i] + src2[i];
                }
                break;
            case 1:
                for(i = 0; i < result_count; i++) {
                    res[i] = (src1[i] < src2[i]) ? src1[i] : src2[i];
                }
                break;
            case 2:
                for(i = 0; i < result_count; i++) {
                    res[i] = (src1[i] > src2[i]) ? src1[i] : src2[i];
                }
                break;
            default:
                MADI_UNDEFINED;
        }
    }

    void initialize_gasnet(int& argc, char **& argv,
                           gasnet_handlerentry_t amentries[],
                           size_t n_amentries)
    {
        gasnet_init(&argc, &argv);

        // register handlers needed by MGAS if MGAS is linked
        n_amentries = mgas_init_gasnet_handlers(amentries, n_amentries);

        // determin GASNet segment size
        uintptr_t max_segsize = gasnet_getMaxLocalSegmentSize();
        uintptr_t minheapoffset = 0;
        uintptr_t segsize = options.gasnet_segment_size;

        if (segsize == 0 || segsize >= max_segsize) {
            segsize = max_segsize;

            if (gasnet_mynode() == 0)
                MADI_DPUTS1("gasnet segment size is fixed to %zu",
                           (size_t)segsize);
        } else {
            if (gasnet_mynode() == 0)
                MADI_DPUTS1("gasnet_segment_size = %zu", (size_t)segsize);
        }

        // allocate GASNet memory segment
        gasnet_attach(amentries, n_amentries, segsize, minheapoffset);

        gasnet_node_t me = gasnet_mynode();

        // initailize GASNet collectives
        // - images = NULL: 1 image per node
        gasnet_coll_fn_entry_t entries[1] = {
            { reduce_long_fn, 0 },
        };
        size_t n_entries = sizeof(entries) / sizeof(entries[0]);
        gasnet_coll_init(NULL, (gasnet_image_t)me, entries, n_entries, 0);

        setenv("MADI_GASNET_INITIALIZED", "1", 1);

        if (gasnet_mynode() == 0)
            MADI_DPUTS("set MADI_GASNET_INITIALIZED");

        gasnet_barrier_notify(0, GASNET_BARRIERFLAG_ANONYMOUS);
        gasnet_barrier_wait(0, GASNET_BARRIERFLAG_ANONYMOUS);
    }

}
}
