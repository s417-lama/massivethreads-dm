#include "mpi3/comm_base.h"
#include "mpi3/comm_memory.h"
#include "ampeer.h"
#include "options.h"

#include <mpi.h>
#include <mpi-ext.h>

#define MADI_CB_DEBUG  0

#if MADI_CB_DEBUG
#define MADI_CB_DPUTS(s, ...)  MADI_DPUTS(s, ##__VA_ARGS__)
#else
#define MADI_CB_DPUTS(s, ...)
#endif

namespace madi {
namespace comm {

    comm_base::comm_base(int& argc, char **& argv)
        : cmr_(NULL)
        , comm_alc_(NULL)
        , value_buf_(NULL)
        , native_config_()
    {
        cmr_ = new comm_memory(native_config_);

        // initialize basic RDMA features (malloc/free/put/get)
        comm_alc_ = new allocator<comm_memory>(cmr_);

        value_buf_ = (long *)comm_alc_->allocate(sizeof(long), native_config_);
    }

    comm_base::~comm_base()
    {
        comm_alc_->deallocate((void *)value_buf_);
        delete comm_alc_;
        delete cmr_;
    }

    void ** comm_base::coll_malloc(size_t size, process_config& config)
    {
        int n_procs = config.get_n_procs();
        MPI_Comm comm = config.comm();
        comm_allocator *alc = comm_alc_;

        void **ptrs = new void *[n_procs];

        void *p = alc->allocate(size, config);

        MADI_ASSERT(p != NULL);

        MPI_Allgather(&p, sizeof(p), MPI_BYTE, 
                      ptrs, sizeof(p), MPI_BYTE,
                      comm);

        int me = config.get_pid();
        MADI_ASSERT(ptrs[me] != NULL);

        return ptrs;
    }

    void comm_base::coll_free(void **ptrs, process_config& config)
    {
        int me = config.get_pid();
        comm_allocator *alc = comm_alc_;

        alc->deallocate(ptrs[me]);

        delete [] ptrs;
    }

    void * comm_base::malloc(size_t size, process_config& config)
    {
        MPI_Comm comm = config.comm();

        // FIXME: non-collective implementation
        // currently, the comm_allocater::allocate function may call
        // collective function `extend_to'.
        MPI_Barrier(comm);

        comm_allocator *alc = comm_alc_;
        return alc->allocate(size, config);
    }

    void comm_base::free(void *p, process_config& config)
    {
        MPI_Comm comm = config.comm();

        // FIXME: non-collective implementation
        MPI_Barrier(comm);

        comm_allocator *alc = comm_alc_;
        alc->deallocate(p);
    }

    int comm_base::coll_mmap(uint8_t *addr, size_t size, process_config& config)
    {
        return cmr_->coll_mmap(addr, size, config);
    }

    void comm_base::coll_munmap(int memid, process_config& config)
    {
        cmr_->coll_munmap(memid, config);
    }

    void comm_base::put_nbi(void *dst, void *src, size_t size, int target,
                            process_config& config)
    {
        reg_put_nbi(-1, dst, src, size, target, config);
    }

    void comm_base::reg_put_nbi(int memid, void *dst, void *src, size_t size,
                                int target, process_config& config)
    {
        int pid = config.native_pid(target);
        int me = config.get_native_pid();
        raw_put__(memid, dst, src, size, pid, 0, me);
    }

    void comm_base::get_nbi(void *dst, void *src, size_t size, int target,
                            process_config& config)
    {
        reg_get_nbi(-1, dst, src, size, target, config);
    }

    void comm_base::reg_get_nbi(int memid, void *dst, void *src, size_t size,
                                int target, process_config& config)
    {
        int pid = config.native_pid(target);
        int me = config.get_native_pid();
        raw_get(memid, dst, src, size, pid, 0, me);
    }

//     void comm_base::raw_put(int tag, void *dst, void *src, size_t size,
//                             int target, int me)
//     {
//         raw_put__(-1, dst, src, size, target, tag, 0, me);
//         /* no handle */
//     }

//     void comm_base::raw_put_ordered(int tag, void *dst, void *src, size_t size,
//                                     int target, int me)
//     {
//         raw_put__(-1, dst, src, size, target, FJMPI_RDMA_STRONG_ORDER,
//                 me);
//         /* no handle */
//     }

//     void comm_base::raw_put_with_notice(int tag, void *dst, void *src,
//                                         size_t size, int target, int me)
//     {
//         raw_put__(-1, dst, src, size, target, tag, FJMPI_RDMA_REMOTE_NOTICE,
//                   me);
//         /* no handle */
//     }
//
    void comm_base::raw_put__(int memid, void *dst, void *src, size_t size,
                              int target, int flags, int me)
    {
        if (target == me) {
            MADI_DPUTS3("memcpy(%p, %p, %zu)", dst, src, size);
            memcpy(dst, src, size);
            return;
        }

        MADI_ASSERT(0 <= target && target < native_config_.get_n_procs());

        comm_memory& cmr = *cmr_;

        // calculate local/remote buffer address
        MPI_Win win;
        size_t target_disp;
        cmr.translate(memid, dst, size, target, &target_disp, &win);

        // issue
        MPI_Put(src, size, MPI_BYTE, target, target_disp, size, MPI_BYTE, win);
    }

    void comm_base::raw_get(int memid, void *dst, void *src, size_t size,
                            int target, int flags, int me)
    {
        if (target == me) {
            MADI_DPUTS3("memcpy(%p, %p, %zu)", dst, src, size);
            memcpy(dst, src, size);
            return;
        }

        MADI_ASSERT(0 <= target && target < native_config_.get_n_procs());

        comm_memory& cmr = *cmr_;

        // calculate local/remote buffer address
        MPI_Win win;
        size_t target_disp;
        cmr.translate(memid, src, size, target, &target_disp, &win);

        // issue
        MPI_Get(dst, size, MPI_BYTE, target, target_disp, size, MPI_BYTE, win);
    }

    int comm_base::poll(int *tag_out, int *pid_out, process_config& config)
    {
        sync();
        return 0;
    }

    void comm_base::fence()
    {
        for (auto& win : cmr_->windows())
            if (win != MPI_WIN_NULL)
                MPI_Win_flush_all(win);
    }

    void comm_base::sync()
    {
        for (auto& win : cmr_->windows())
            if (win != MPI_WIN_NULL)
                MPI_Win_sync(win);
    }

    void comm_base::native_barrier(process_config& config)
    {
        MPI_Comm comm = config.comm();
        MPI_Barrier(comm);
    }

    template <class T> inline MPI_Datatype mpi_type();
    template <> inline MPI_Datatype mpi_type<int>() { return MPI_INT; }
    template <> inline MPI_Datatype mpi_type<long>() { return MPI_LONG; }


    template <class T>
    T comm_base::fetch_and_add(T *dst, T value, int target,
                               process_config& config)
    {
        // calculate local/remote buffer address
        MPI_Win win;
        size_t target_disp;
        cmr_->translate(-1, dst, sizeof(T), target, &target_disp, &win);

        MPI_Datatype type = mpi_type<T>();

        // issue
        T result;
        MPI_Fetch_and_op(&value, &result, type, target, target_disp,
                         MPI_SUM, win);
        MPI_Win_flush(target, win);

        return result;
    }

    template int comm_base::fetch_and_add<int>(int *, int, int,
                                               process_config&);
    template long comm_base::fetch_and_add<long>(long *, long, int,
                                                 process_config&);
}
}

