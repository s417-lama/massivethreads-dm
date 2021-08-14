#ifndef MADI_FUTURE_INL_H
#define MADI_FUTURE_INL_H

#include "madi.h"
#include "future.h"
#include "uth_comm-inl.h"
#include "process.h"
#include "prof.h"
#include <madm/threadsafe.h>

namespace madi {

    constexpr uth_pid_t PID_INVALID = static_cast<uth_pid_t>(-1);

}

namespace madm {
namespace uth {

    template <class T>
    inline future<T>::future()
        : id_(-1)
        , pid_(madi::PID_INVALID)
    {
    }

    template <class T>
    inline future<T>::future(int id, madi::uth_pid_t pid)
        : id_(id)
        , pid_(pid)
    {
    }

    template <class T>
    inline future<T> future<T>::make()
    {
        madi::worker& w = madi::current_worker();
        return make(w);
    }

    template <class T>
    inline future<T> future<T>::make(madi::worker& w)
    {
        return w.fpool().get<T>();
    }

    template <class T>
    inline void future<T>::set(T& value)
    {
        if (id_ < 0 || pid_ == madi::PID_INVALID)
            MADI_DIE("invalid future");

        madi::worker& w = madi::current_worker();
        w.fpool().fill(*this, value);
    }

    template <class T>
    inline bool future<T>::try_get(T *value)
    {
        madi::worker& w = madi::current_worker();
        return try_get(w, value);
    }

    template <class T>
    inline bool future<T>::try_get(madi::worker& w, T *value)
    {
        if (id_ < 0 || pid_ == madi::PID_INVALID)
            MADI_DIE("invalid future");

        madi::future_pool& pool = w.fpool();

        return pool.synchronize(*this, value);
    }

    template <class T>
    inline T future<T>::get()
    {
        madi::logger::checkpoint<madi::logger::kind::THREAD>();

        madi::worker& w = madi::current_worker();

        T value;
        while (!try_get(w, &value))
            w.do_scheduler_work();

        madi::logger::checkpoint<madi::logger::kind::SCHED>();

        return value;
    }

}
}

namespace madi {

    inline dist_spinlock::dist_spinlock(uth_comm& c) :
        c_(c),
        locks_(NULL)
    {
        uth_pid_t me = c.get_pid();

        locks_ = (uint64_t **)c_.malloc_shared(sizeof(uint64_t));
        c.lock_init(locks_[me]);
    }

    inline dist_spinlock::~dist_spinlock()
    {
        c_.free_shared((void **)locks_);
    }

    inline bool dist_spinlock::trylock(uth_pid_t target)
    {
        MADI_DPUTSP2("DIST SPIN LOCK");

        uint64_t *lock = locks_[target];
        return c_.trylock(lock, target);
    }

    inline double random_double()
    {
        return (double)random_int(INT_MAX) / (double)INT_MAX;
    }

    inline void dist_spinlock::lock(uth_pid_t target)
    {
        logger::begin_data bd = logger::begin_event<logger::kind::DIST_SPINLOCK_LOCK>();

        const long backoff_base = 4 * 1e6;
        const long backoff_max  = 32 * 1e6;

        long t_backoff = backoff_base;

        long t0 = rdtsc();
        long t_start = t0;

        while (!trylock(target)) {

#if 0
            long rand_t_backoff = (long)((double)t_backoff * random_double());

            while (rdtsc() - t_start <= rand_t_backoff)
                MADI_UTH_COMM_POLL();

            if (t_backoff * 2 <= backoff_max)
                t_backoff *= 2;

            t_start = rdtsc();
#else
            MADI_UTH_COMM_POLL();
#endif

#if 0
            if (rdtsc() - t0 >= 1e9)
                MADI_DIE("BUG: cannot acquire remote spinlock");
#endif
        }

        long t1 = rdtsc();
        g_prof->t_dist_lock += t1 - t0;

        logger::end_event<logger::kind::DIST_SPINLOCK_LOCK>(bd);
    }

    inline void dist_spinlock::unlock(uth_pid_t target)
    {
        logger::begin_data bd = logger::begin_event<logger::kind::DIST_SPINLOCK_UNLOCK>();

        MADI_DPUTSP2("DIST SPIN UNLOCK");

        uth_comm::lock_t *lock = locks_[target];
        c_.unlock(lock, target);

        MADI_DPUTSP2("DIST SPIN UNLOCK DONE");

        logger::end_event<logger::kind::DIST_SPINLOCK_UNLOCK>(bd);
    }

    template <class T>
    inline dist_pool<T>::dist_pool(uth_comm& c, int size) :
        c_(c),
        size_(size),
        locks_(c),
        idxes_(NULL),
        data_(NULL)
    {
        uth_pid_t me = c.get_pid();

        idxes_ = (uth_comm::lock_t **)c_.malloc_shared(sizeof(uth_comm::lock_t));
        data_ = (T **)c_.malloc_shared(sizeof(T) * size);

        *idxes_[me] = 0UL;
    }

    template <class T>
    inline dist_pool<T>::~dist_pool()
    {
        c_.free_shared((void **)idxes_);
        c_.free_shared((void **)data_);
    }

    template <class T>
    inline bool dist_pool<T>::empty(uth_pid_t target)
    {
        uth_pid_t me = c_.get_pid();

        uint64_t *idx_ptr = idxes_[target];

        uint64_t idx;
        if (target == me) {
            idx = *idx_ptr;
        } else {
            idx = c_.get_value(idx_ptr, target);
        }

        MADI_ASSERT(0 <= idx && idx < size_);

        return idx == 0;
    }

    template <class T>
    inline bool dist_pool<T>::push_remote(T& v, uth_pid_t target)
    {
        logger::begin_data bd = logger::begin_event<logger::kind::DIST_POOL_PUSH>();

        locks_.lock(target);
        
        MADI_DPUTSP2("PUSH REMOTE IDX INCR");

        uint64_t *idx_ptr = idxes_[target];
        uint64_t idx = c_.fetch_and_add(idx_ptr, 1UL, target);

        MADI_ASSERT(0 <= idx && idx < size_);

        bool success;
        if (idx < size_) {
            T *buf = data_[target] + idx;

            // v is on a stack registered for RDMA
            c_.put_buffered(buf, &v, sizeof(T), target);

            success = true;
        } else {
            c_.put_value(idx_ptr, idx, target);
            success = false;
        }

        locks_.unlock(target);

        logger::end_event<logger::kind::DIST_POOL_PUSH>(bd);

        return success;
    }

    template <class T>
    inline void dist_pool<T>::begin_pop_local()
    {
        log_bd_ = logger::begin_event<logger::kind::DIST_POOL_POP>();

        uth_pid_t target = c_.get_pid();
        locks_.lock(target);
    }

    template <class T>
    inline void dist_pool<T>::end_pop_local()
    {
        uth_pid_t target = c_.get_pid();
        locks_.unlock(target);

        logger::end_event<logger::kind::DIST_POOL_POP>(log_bd_);
    }

    template <class T>
    inline bool dist_pool<T>::pop_local(T *buf)
    {
        uth_pid_t target = c_.get_pid();

        uint64_t current_idx = *idxes_[target];

        MADI_ASSERT(0 <= current_idx && current_idx < size_);

        if (current_idx == 0)
            return false;

        uint64_t idx = current_idx - 1;
        T *src = data_[target] + idx;
        memcpy(buf, src, sizeof(T));

        *idxes_[target] -= 1;

        return true;
    }

    inline size_t index_of_size(size_t size)
    {
        return 64UL - static_cast<size_t>(__builtin_clzl(size - 1));
    }

    inline future_pool::future_pool() :
        ptr_(0), buf_size_(0), remote_bufs_(NULL), retpools_(NULL)
    {
    }
    inline future_pool::~future_pool()
    {
    }

    inline void future_pool::initialize(uth_comm& c, size_t buf_size)
    {
        int retpool_size = 16 * 1024;

        ptr_ = 0;
        buf_size_ = (int)buf_size;

        remote_bufs_ = (uint8_t **)c.malloc_shared(buf_size);
        retpools_ = new dist_pool<retpool_entry>(c, retpool_size);
    }

    inline void future_pool::finalize(uth_comm& c)
    {
        c.free_shared((void **)remote_bufs_);

        for (size_t i = 0; i < MAX_ENTRY_BITS; i++)
            id_pools_[i].clear();

        delete retpools_;

        ptr_ = 0;
        buf_size_ = 0;
        remote_bufs_ = NULL;
        retpools_ = NULL;
    }

    template <class T>
    inline void future_pool::reset(int id)
    {
        uth_comm& c = madi::proc().com();
        uth_pid_t me = c.get_pid();

        entry<T> *e = (entry<T> *)(remote_bufs_[me] + id);

        e->done = 0;
    }

    inline void future_pool::move_back_returned_ids()
    {
        retpools_->begin_pop_local();

        size_t count = 0;
        retpool_entry entry;
        while (retpools_->pop_local(&entry)) {
            size_t idx = index_of_size(entry.size);
            id_pools_[idx].push_back(entry.id);

            count += 1;
        }

        retpools_->end_pop_local();

        MADI_DPUTSB1("move back returned future ids: %zu", count);
    }

    template <class T>
    inline madm::uth::future<T> future_pool::get()
    {
        uth_pid_t me = madi::proc().com().get_pid();

        size_t entry_size = sizeof(entry<T>);
        size_t idx = index_of_size(entry_size);

        int real_size = 1 << idx;

        // move future ids from the return pool to the local pool
        if (!retpools_->empty(me)) {
            move_back_returned_ids();
        }

        // pop a future id from the local pool
        if (!id_pools_[idx].empty()) {
            int id = id_pools_[idx].back();
            id_pools_[idx].pop_back();

            reset<T>(id);

            return madm::uth::future<T>(id, me);
        }

        // if pool is empty, allocate a future id from ptr_
        if (ptr_ + real_size < buf_size_) {
            int id = ptr_;
            ptr_ += real_size;

            return madm::uth::future<T>(id, me);
        }

        madi::die("future pool overflow");
    }

    template <class T>
    inline void future_pool::fill(madm::uth::future<T> f, T& value)
    {
        uth_comm& c = madi::proc().com();
        uth_pid_t me = c.get_pid();
        int fid = f.id_;
        uth_pid_t pid = f.pid_;

        entry<T> *e = (entry<T> *)(remote_bufs_[pid] + fid);

        if (pid == me) {
            e->value = value;
            comm::threadsafe::wbarrier();
            e->done = 1;
        } else {
            // value is on a stack registered for RDMA
            c.put_buffered(&e->value, &value, sizeof(value), pid);
            c.put_value(&e->done, 1, pid);
        }
    }

    template <class T>
    inline bool future_pool::synchronize(madm::uth::future<T> f, T *value)
    {
        uth_comm& c = madi::proc().com();
        uth_pid_t me = c.get_pid();
        int fid = f.id_;
        uth_pid_t pid = f.pid_;

        entry<T> *e = (entry<T> *)(remote_bufs_[pid] + fid);

        entry<T> entry_buf;
        if (pid != me) {
#if 0
            // Is this safe in terms of memory ordering???
            c.get(&entry_buf, e, sizeof(e), pid);
#else
            entry_buf.done = c.get_value(&e->done, pid);

            if (entry_buf.done == 1) {
                // entry_buf is on a stack registered for RDMA
                c.get_buffered(&entry_buf.value, &e->value,
                               sizeof(e->value), pid);
            }
#endif
            e = &entry_buf;
        }

        MADI_ASSERT(0 <= fid && fid < buf_size_);

        if (e->done) {
            if (pid == me) {
                size_t idx = index_of_size(sizeof(entry<T>));
                id_pools_[idx].push_back(fid);
            } else {
                // return fork-join descriptor to processor pid.
                retpool_entry rpentry = { fid, (int)sizeof(entry<T>) };
                bool success = retpools_->push_remote(rpentry, pid);

                if (success) {
                    MADI_DPUTSR1("push future %d to return pool(%zu)",
                                fid, pid);
                } else {
                    madi::die("future return pool becomes full");
                }
            }
 
            comm::threadsafe::rbarrier();
            *value = e->value;
        }

        return e->done;
    }
}

#endif
