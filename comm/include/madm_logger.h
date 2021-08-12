#ifndef MADM_LOGGER_H
#define MADM_LOGGER_H

#include <cstdio>

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
#ifndef MADM_LOGGER_DISABLED_KINDS
#define MADM_LOGGER_DISABLED_KINDS {}
#endif
            kind disabled_kinds[] = MADM_LOGGER_DISABLED_KINDS;
#undef MADM_LOGGER_DISABLED_KINDS
            return !kind_included_(k, disabled_kinds, sizeof(disabled_kinds) / sizeof(*disabled_kinds));
        }

        static constexpr const char* kind_name(kind k) {
            switch (k) {
                case kind::INIT:   return "";
                case kind::TEST:   return "test";
                case kind::SCHED:  return "sched";
                case kind::THREAD: return "thread";
                case kind::OTHER:  return "other";
            }
            return "error";
        }

        template <kind k>
        static void* logger_decoder_tl_(FILE* stream, int _rank0, int _rank1, void* buf0, void* buf1) {
            uint64_t t0 = MLOG_READ_ARG(&buf0, uint64_t);
            uint64_t t1 = MLOG_READ_ARG(&buf1, uint64_t);

            logger& lgr = get_instance_();
            fprintf(stream, "%d,%lu,%d,%lu,%s\n", lgr.rank_, t0, lgr.rank_, t1, kind_name(k));
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
#else
        static void init(int rank, size_t size = 0) {}
        static void flush() {}
        static void warmup() {}
        static void clear() {}
        template <kind k>
        static inline void checkpoint() {}
        template <kind k>
        static inline begin_data begin_event() {}
        template <kind k>
        static inline void end_event(begin_data bp) {}
#endif
#undef MADM_LOGGER_ENABLE
    };

}

#endif
