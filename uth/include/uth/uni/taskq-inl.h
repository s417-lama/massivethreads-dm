#ifndef MADI_TASKQUE_INL_H
#define MADI_TASKQUE_INL_H

#include "taskq.h"
#include "../uth_comm.h"
#include <madm/threadsafe.h>

namespace madi {

    typedef comm::threadsafe threadsafe;

    inline void local_taskque::push(const taskq_entry& th) {
        deque_.push_back(th);
    }

    inline bool local_taskque::pop(taskq_entry *th) {
        if (deque_.empty()) {
            return false;
        } else {
            *th = deque_.back();
            deque_.pop_back();
            return true;
        }
    }

    inline bool local_taskque::steal(taskq_entry *th) {
        if (deque_.empty()) {
            return false;
        } else {
            *th = deque_.front();
            deque_.pop_front();
            return true;
        }
    }
    
    inline bool global_taskque::local_trylock()
    {
        uint64_t one = 1;
        uint64_t zero = 0;
        return threadsafe::fetch_and_add(&lock_, one) == zero;
    }

    inline void global_taskque::local_lock()
    {
        while (!local_trylock())
            MADI_UTH_COMM_POLL();
    }

    inline void global_taskque::local_unlock()
    {
        comm::threadsafe::wbarrier();
        lock_ = 0UL;
    }

    inline bool global_taskque::remote_trylock(uth_comm& c, uth_pid_t target)
    {
        MADI_DPUTSY2("TASKQ REMOTE TRYLOCK");

        return c.fetch_and_add((uint64_t *)&lock_, 1UL, target) == 0UL;
    }

    inline void global_taskque::remote_unlock(uth_comm& c, uth_pid_t target)
    {
        c.put_value((uint64_t *)&lock_, 0UL, target);
    }

    inline void global_taskque::push(const taskq_entry& entry)
    {
        int t = top_;

        comm::threadsafe::rbarrier();

        if (t == n_entries_) {
            local_lock();

            if (base_ == 0)
                madi::die("task queue overflow");

            int offset_x2 = n_entries_ - (base_ + top_);
            int offset = offset_x2 / 2;

            if (offset_x2 % 2)
                offset -= 1;

            if (top_ - base_) {
                int dst = base_ + offset;
                int src = base_;
                size_t size = sizeof(taskq_entry) * (top_ - base_);
                memmove(&entries_[dst], &entries_[src], size);
            }

            top_ += offset;
            base_ += offset;

            t = top_;

            local_unlock();
        }

        entries_[t] = entry;

        comm::threadsafe::wbarrier();

        top_ = t + 1;

        MADI_DPUTS3("top = %d", top_);
    }

    inline taskq_entry * global_taskque::pop()
    {
// pop operation must block until a thief is acquiring lock_.
//         // quick check
//         if (top_ <= base_)
//             return NULL;

        int t = top_ - 1;
        top_ = t;

        comm::threadsafe::rwbarrier();

        int b = base_;

        if (b + 1 < t) {
            return &entries_[t];
        }

        local_lock();

        b = base_;

        taskq_entry *result;
        if (b <= t) {
            result = &entries_[t];
        } else {
            top_ = n_entries_ / 2;
            base_ = top_;

            result = NULL;
        }

        local_unlock();

        return result;
    }

#if 1
    inline bool global_taskque::local_steal(taskq_entry *entry)
    {
        // quick check
        if (top_ - base_ <= 0)
            return false;

//        if (!local_trylock())
//            return false;

        int b = base_;
        base_ = b + 1;

        comm::threadsafe::rwbarrier();

        int t = top_;

        bool result;
        if (b < t) {
            *entry = entries_[b];
            result = true;
        } else {
            base_ = b;
            result = false;
        }

//        local_unlock();
        
        return result;
    }
#endif

    inline bool global_taskque::empty(uth_comm& c, uth_pid_t target,
                                      global_taskque *taskq_buf)
    {
        global_taskque& self = *taskq_buf; // RMA buffer
        c.get(&self, this, sizeof(self), target);

        return self.base_ >= self.top_;
    }

    inline bool global_taskque::steal_trylock(uth_comm& c, uth_pid_t target)
    {
        return remote_trylock(c, target);
    }

    inline void global_taskque::steal_unlock(uth_comm& c, uth_pid_t target)
    {
        return remote_unlock(c, target);
    }

    inline bool global_taskque::steal(uth_comm& c,
                                      uth_pid_t target,
                                      taskq_entry *entries,
                                      taskq_entry *entry,
                                      global_taskque *taskq_buf)
    {
        // assume that `this' pointer is remote.
        // assume that this function is protected by
        // steal_trylock and steal_unlock.

        global_taskque& self = *taskq_buf; // RMA buffer
        c.get(&self, this, sizeof(self), target);
        int b = self.base_;
        int t = self.top_;

//        MADI_DPUTS3("t (top) = %d", t);

        bool result;
        if (b < t) {
            c.put_value((int *)&base_, self.base_ + 1, target);

            MADI_DPUTS3("RDMA_GET(%p, %p, %zu) rma_entries[%d] = %p",
                        entry, &entries[b], sizeof(*entry), 
                        target, entries);

            MADI_CHECK(entries != NULL);

            c.get(entry, &entries[b], sizeof(*entry), target);

            MADI_DPUTS3("RDMA_GET done");

            result = true;
        } else {
            result = false;
        }

        return result;
    }

}

#endif
