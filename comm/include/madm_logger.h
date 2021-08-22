#ifndef MADM_LOGGER_H
#define MADM_LOGGER_H

#include <cstdio>
#include <sstream>

#include "madm_global_clock.h"

/* #define MLOG_DISABLE_CHECK_BUFFER_SIZE 1 */
/* #define MLOG_DISABLE_REALLOC_BUFFER    1 */
#include "mlog/mlog.h"

namespace madi {

    class logger {
    public:
        using begin_data = void*;
        enum class kind {
            INIT,
            TEST,
            SCHED,
            THREAD,

            TASKQ_PUSH,
            TASKQ_POP,
            TASKQ_STEAL,
            TASKQ_EMPTY,

            FUTURE_POOL_SYNC,
            FUTURE_POOL_FILL,
            FUTURE_POOL_GET,

            DIST_POOL_PUSH,
            DIST_POOL_POP,

            DIST_SPINLOCK_LOCK,
            DIST_SPINLOCK_UNLOCK,

            WORKER_RESUME_LWT,
            WORKER_RESUME_HWT,
            WORKER_RESUME_REMOTE,
            WORKER_TRY_STEAL,

            COMM_PUT,
            COMM_GET,
            COMM_FENCE,
            COMM_FETCH_AND_ADD,
            COMM_TRYLOCK,
            COMM_LOCK,
            COMM_UNLOCK,
            COMM_POLL,

            OTHER
        };

    private:
#ifndef MADM_LOGGER_DEFAULT_SIZE
#define MADM_LOGGER_DEFAULT_SIZE (1 << 20)
#endif
        static constexpr size_t default_size_ = MADM_LOGGER_DEFAULT_SIZE;
#undef MADM_LOGGER_DEFAULT_SIZE

        mlog_data_t md_;
        int rank_;
        begin_data bp_ = nullptr;
        FILE* stream_;

        static inline logger& get_instance_() {
            static logger my_instance;
            return my_instance;
        }

        static constexpr bool kind_included_(kind k, kind kinds[], int n) {
            return n > 0 && (k == *kinds || kind_included_(k, kinds + 1, n - 1));
        }

        static constexpr bool is_valid_kind_(kind k) {
#if 0
            kind disabled_kinds[] = {
                /* kind::TEST, */
                /* kind::SCHED, */
                /* kind::THREAD, */

                /* kind::TASKQ_PUSH, */
                /* kind::TASKQ_POP, */
                /* kind::TASKQ_STEAL, */
                /* kind::TASKQ_EMPTY, */

                /* kind::FUTURE_POOL_SYNC, */
                /* kind::FUTURE_POOL_FILL, */
                /* kind::FUTURE_POOL_GET, */

                /* kind::DIST_POOL_PUSH, */
                /* kind::DIST_POOL_POP, */

                /* kind::DIST_SPINLOCK_LOCK, */
                /* kind::DIST_SPINLOCK_UNLOCK, */

                /* kind::WORKER_RESUME_LWT, */
                /* kind::WORKER_RESUME_HWT, */
                /* kind::WORKER_RESUME_REMOTE, */
                /* kind::WORKER_TRY_STEAL, */

                /* kind::COMM_PUT, */
                /* kind::COMM_GET, */
                /* kind::COMM_FENCE, */
                /* kind::COMM_FETCH_AND_ADD, */
                /* kind::COMM_TRYLOCK, */
                /* kind::COMM_LOCK, */
                /* kind::COMM_UNLOCK, */
                /* kind::COMM_POLL, */
            };
#else
#ifndef MADM_LOGGER_DISABLED_KINDS
#define MADM_LOGGER_DISABLED_KINDS {}
#endif
            kind disabled_kinds[] = MADM_LOGGER_DISABLED_KINDS;
#undef MADM_LOGGER_DISABLED_KINDS
#endif
            return !kind_included_(k, disabled_kinds, sizeof(disabled_kinds) / sizeof(*disabled_kinds));
        }

        static constexpr const char* kind_name(kind k) {
            switch (k) {
                case kind::INIT:                 return "";
                case kind::TEST:                 return "test";
                case kind::SCHED:                return "sched";
                case kind::THREAD:               return "thread";

                case kind::TASKQ_PUSH:           return "taskq_push";
                case kind::TASKQ_POP:            return "taskq_pop";
                case kind::TASKQ_STEAL:          return "taskq_steal";
                case kind::TASKQ_EMPTY:          return "taskq_empty";

                case kind::FUTURE_POOL_SYNC:     return "future_pool_sync";
                case kind::FUTURE_POOL_FILL:     return "future_pool_fill";
                case kind::FUTURE_POOL_GET:      return "future_pool_get";

                case kind::DIST_POOL_PUSH:       return "dist_pool_push";
                case kind::DIST_POOL_POP:        return "dist_pool_pop";

                case kind::DIST_SPINLOCK_LOCK:   return "dist_spinlock_lock";
                case kind::DIST_SPINLOCK_UNLOCK: return "dist_spinlock_unlock";

                case kind::WORKER_RESUME_LWT:    return "worker_resume_lwt";
                case kind::WORKER_RESUME_HWT:    return "worker_resume_hwt";
                case kind::WORKER_RESUME_REMOTE: return "worker_resume_remote";
                case kind::WORKER_TRY_STEAL:     return "worker_try_steal";

                case kind::COMM_PUT:             return "comm_put";
                case kind::COMM_GET:             return "comm_get";
                case kind::COMM_FENCE:           return "comm_fence";
                case kind::COMM_FETCH_AND_ADD:   return "comm_fetch_and_add";
                case kind::COMM_TRYLOCK:         return "comm_trylock";
                case kind::COMM_LOCK:            return "comm_lock";
                case kind::COMM_UNLOCK:          return "comm_unlock";
                case kind::COMM_POLL:            return "comm_poll";

                default:                         return "other";
            }
        }

        template <kind k>
        static void* logger_decoder_tl_(FILE* stream, int _rank0, int _rank1, void* buf0, void* buf1) {
            uint64_t t0 = MLOG_READ_ARG(&buf0, uint64_t);
            uint64_t t1 = MLOG_READ_ARG(&buf1, uint64_t);

            logger& lgr = get_instance_();
            fprintf(stream, "%d,%lu,%d,%lu,%s\n", lgr.rank_, t0, lgr.rank_, t1, kind_name(k));
            return buf1;
        }

        template <kind k, typename MISC>
        static void* logger_decoder_tl_w_misc_(FILE* stream, int _rank0, int _rank1, void* buf0, void* buf1) {
            uint64_t t0 = MLOG_READ_ARG(&buf0, uint64_t);
            uint64_t t1 = MLOG_READ_ARG(&buf1, uint64_t);
            MISC     m  = MLOG_READ_ARG(&buf1, MISC);

            logger& lgr = get_instance_();
            std::stringstream ss;
            ss << m;
            fprintf(stream, "%d,%lu,%d,%lu,%s,%s\n", lgr.rank_, t0, lgr.rank_, t1, kind_name(k), ss.str().c_str());
            return buf1;
        }

    public:
#ifndef MADM_LOGGER_ENABLE
#define MADM_LOGGER_ENABLE 0
#endif
#if MADM_LOGGER_ENABLE
        static void init(int rank, size_t size = default_size_) {
            logger& lgr = get_instance_();
            lgr.rank_ = rank;
            mlog_init(&lgr.md_, 1, size);

            char filename[128];
            sprintf(filename, "madm_log_%d.ignore", rank);
            lgr.stream_ = fopen(filename, "w+");

            global_clock::init();
        }

        static void flush() {
            logger& lgr = get_instance_();
            mlog_flush_all(&lgr.md_, lgr.stream_);
        }

        static void warmup() {
            logger& lgr = get_instance_();
            mlog_warmup(&lgr.md_, 0);
        }

        static void clear() {
            logger& lgr = get_instance_();
            mlog_clear_all(&lgr.md_);
        }

        template <kind k>
        static inline void checkpoint() {
            if (is_valid_kind_(k)) {
                logger& lgr = get_instance_();
                uint64_t t = global_clock::get_time();
                if (lgr.bp_) {
                    auto fn = &logger_decoder_tl_<k>;
                    MLOG_END(&lgr.md_, 0, lgr.bp_, fn, t);
                }
                lgr.bp_ = MLOG_BEGIN(&lgr.md_, 0, t);
            }
        }

        template <kind k>
        static inline begin_data begin_event() {
            if (is_valid_kind_(k)) {
                logger& lgr = get_instance_();
                uint64_t t = global_clock::get_time();
                begin_data bp = MLOG_BEGIN(&lgr.md_, 0, t);
                return bp;
            } else {
                return nullptr;
            }
        }

        template <kind k>
        static inline void end_event(begin_data bp) {
            if (is_valid_kind_(k)) {
                logger& lgr = get_instance_();
                uint64_t t = global_clock::get_time();
                auto fn = &logger_decoder_tl_<k>;
                MLOG_END(&lgr.md_, 0, bp, fn, t);
            }
        }

        template <kind k, typename MISC>
        static inline void end_event(begin_data bp, MISC m) {
            if (is_valid_kind_(k)) {
                logger& lgr = get_instance_();
                uint64_t t = global_clock::get_time();
                auto fn = &logger_decoder_tl_w_misc_<k, MISC>;
                MLOG_END(&lgr.md_, 0, bp, fn, t, m);
            }
        }
#else
        static void init(int rank, size_t size = 0) {}
        static void flush() {}
        static void warmup() {}
        static void clear() {}
        template <kind k>
        static inline void checkpoint() {}
        template <kind k>
        static inline begin_data begin_event() { return NULL; }
        template <kind k>
        static inline void end_event(begin_data bp) {}
        template <kind k, typename MISC>
        static inline void end_event(begin_data bp, MISC m) {}
#endif
#undef MADM_LOGGER_ENABLE
    };

}

#endif
