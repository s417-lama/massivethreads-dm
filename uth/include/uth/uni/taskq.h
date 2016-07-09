#ifndef MADI_TASKQUE_H
#define MADI_TASKQUE_H

#include "../madi.h"
#include "context.h"
#include "../misc.h"
#include "../debug.h"
#include <deque>
#include <cstring>
#include <climits>

namespace madi {

    class uth_comm;

    struct taskq_entry {
        uint8_t *frame_base;
        size_t frame_size;
        context *ctx;
    };

#define MADI_TENTRY_PRINT(level, entry_ptr) \
    do { \
        MADI_UNUSED madi::taskq_entry *e__ = (entry_ptr); \
        MADI_DPUTS##level("(" #entry_ptr ")->frame_base = %p", \
                          e__->frame_base); \
        MADI_DPUTS##level("(" #entry_ptr ")->frame_size = %zu", \
                          e__->frame_size); \
        MADI_DPUTS##level("(" #entry_ptr ")->ctx        = %p", \
                          e__->ctx); \
    } while (false)

#define MADI_TENTRY_ASSERT(entry_ptr) \
    do { \
        MADI_UNUSED madi::taskq_entry *e__ = (entry_ptr); \
        MADI_ASSERT(e__->frame_base <= (uint8_t *)e__->ctx); \
        MADI_ASSERT((uint8_t *)e__->ctx < e__->frame_base + e__->frame_size); \
        MADI_ASSERT(e__->ctx->parent != NULL); \
        MADI_ASSERT(e__->frame_base <= (uint8_t *)e__->ctx->parent); \
        MADI_ASSERT((uint8_t *)e__->ctx->parent \
                    < e__->frame_base + e__->frame_size); \
    } while (false)

    class local_taskque {
        std::deque<taskq_entry> deque_;
    public:
        local_taskque() : deque_() {}
        ~local_taskque() {}

        void initialize(size_t capacity);
        void finalize();

        void push(const taskq_entry& th);
        bool pop(taskq_entry *th);
        bool steal(taskq_entry *th);
    };

    class global_taskque {
        MADI_NONCOPYABLE(global_taskque);

        volatile int top_;
        volatile int base_;

        int n_entries_;
        taskq_entry *entries_;

        volatile uint64_t lock_;
        
    public:
        global_taskque();
        ~global_taskque();

        void initialize(uth_comm& c, taskq_entry *entries, size_t n_entries);
        void finalize(uth_comm& c);

        void push(const taskq_entry& entry);
        taskq_entry * pop();

        bool empty(uth_comm& c, uth_pid_t target, global_taskque *taskq_buf);
        bool steal(uth_comm& c, uth_pid_t target, taskq_entry *entries,
                   taskq_entry *entry, global_taskque *taskq_buf);
        bool steal_trylock(uth_comm& c, uth_pid_t target);
        void steal_unlock(uth_comm& c, uth_pid_t target);

        bool local_trylock();
        bool local_steal(taskq_entry *entry);
        void local_unlock();

    private:
        void local_lock();

        bool remote_trylock(uth_comm& c, uth_pid_t target);
        void remote_unlock(uth_comm& c, uth_pid_t target);
    };

    typedef global_taskque taskque;
}


#endif
