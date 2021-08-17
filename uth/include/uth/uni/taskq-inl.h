#ifndef MADI_TASKQUE_INL_H
#define MADI_TASKQUE_INL_H

#include "taskq.h"
#include "../uth_comm.h"
#include <madm/threadsafe.h>

namespace madi {

    typedef comm::threadsafe threadsafe;

    inline void global_taskque::push(uth_comm& c, const taskq_entry& entry)
    {
        logger::begin_data bd = logger::begin_event<logger::kind::TASKQ_PUSH>();

        int t = top_;

        comm::threadsafe::rbarrier();

        if (t == n_entries_) {
            pid_t me = c.get_pid();
            c.lock(&lock_, me);

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

            c.unlock(&lock_, me);
        }

        entries_[t] = entry;

        comm::threadsafe::wbarrier();

        top_ = t + 1;

        MADI_DPUTS3("top = %d", top_);

        logger::end_event<logger::kind::TASKQ_PUSH>(bd);
    }

    inline taskq_entry * global_taskque::pop(uth_comm& c)
    {
        logger::begin_data bd = logger::begin_event<logger::kind::TASKQ_POP>();

// pop operation must block until a thief is acquiring lock_.
//         // quick check
//         if (top_ <= base_)
//             return NULL;
        taskq_entry *result;

        int t = top_ - 1;
        top_ = t;

        comm::threadsafe::rwbarrier();

        int b = base_;

        if (b + 1 < t) {
            result = &entries_[t];
        } else {
            pid_t me = c.get_pid();
            c.lock(&lock_, me);

            b = base_;

            if (b <= t) {
                result = &entries_[t];
            } else {
                top_ = n_entries_ / 2;
                base_ = top_;

                result = NULL;
            }

            c.unlock(&lock_, me);
        }

        logger::end_event<logger::kind::TASKQ_POP>(bd);

        return result;
    }

    inline bool global_taskque::local_steal(taskq_entry *entry)
    {
        // quick check
        if (top_ - base_ <= 0)
            return false;

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

        return result;
    }

    inline bool global_taskque::empty(uth_comm& c, uth_pid_t target,
                                      global_taskque *taskq_buf)
    {
        global_taskque& self = *taskq_buf; // RMA buffer
        c.get(&self, this, sizeof(self), target);

        return self.base_ >= self.top_;
    }

    inline bool global_taskque::trylock(uth_comm& c, uth_pid_t target)
    {
        return c.trylock(&lock_, target);
    }

    inline void global_taskque::unlock(uth_comm& c, uth_pid_t target)
    {
        c.unlock(&lock_, target);
    }

    inline bool global_taskque::steal(uth_comm& c,
                                      uth_pid_t target,
                                      taskq_entry *entries,
                                      taskq_entry *entry,
                                      global_taskque *taskq_buf)
    {
        logger::begin_data bd = logger::begin_event<logger::kind::TASKQ_STEAL>();

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

        logger::end_event<logger::kind::TASKQ_STEAL>(bd, target);

        return result;
    }

}

#endif
