#ifndef MADM_MISC_H
#define MADM_MISC_H

#include <memory>
#include <tuple>
#include <utility>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cassert>

#include <sys/time.h>

#define MADI_NORETURN    __attribute__((noreturn))
#define MADI_UNUSED      __attribute__((unused))
#define MADI_NOINLINE    __attribute__((noinline))

// noncopyable attribute suppressing -Weff++
#define MADI_NONCOPYABLE(name) \
    name(const name&); \
    name& operator=(const name&)


extern "C" {
    void madi_exit(int) MADI_NORETURN;
    size_t madi_get_debug_pid();
}

namespace madi {

    // inline std::unique_ptr.
    // this class can be use without std:: prefix.
#if 0
    template <class T, class D = std::default_delete<T>>
    using unique_ptr = std::unique_ptr<T, D>;
#else
    // g++ 4.6.1 does not support template aliases
    template <class T, class D = std::default_delete<T>>
    struct unique_ptr : std::unique_ptr<T, D> {
        typedef unique_ptr<T, D> this_type;
        typedef std::unique_ptr<T, D> base_type;
        typedef typename std::unique_ptr<T, D>::pointer pointer;

        constexpr unique_ptr() : base_type() {}

        explicit unique_ptr(pointer p) : base_type(p) {}

        unique_ptr(pointer p, D d) : base_type(p, d) {}

        unique_ptr(this_type&& u) : base_type(std::forward<this_type>(u)) {}

        constexpr unique_ptr(std::nullptr_t p) : base_type(p) {}

        template <class U, class E>
        unique_ptr(unique_ptr<U, E>&& u)
            : std::unique_ptr<U, E>(std::forward<unique_ptr<U, E>>(u)) {}

        template <class U>
        unique_ptr(std::auto_ptr<U>&& u)
            : std::unique_ptr<U>(std::forward<std::auto_ptr<U>>(u)) {}

        unique_ptr& operator=(unique_ptr&& u)
        {
            base_type& lhs = *static_cast<base_type *>(this);
            base_type&& rhs = static_cast<base_type&&>(u);
            base_type& result = lhs.operator=(std::forward<base_type>(rhs));
            return static_cast<unique_ptr&>(result);
        }
    };
#endif

    // C++14 make_unique function
    template<class T, class... Args>
    unique_ptr<T> make_unique(Args&&... args)
    {
        return unique_ptr<T>(new T(std::forward<Args>(args)...));
    }
   
    // utility class similar to boost::noncopyable
    struct noncopyable {
    protected:
        noncopyable() = default;
        ~noncopyable() = default;
        noncopyable(const noncopyable&) = delete;
        noncopyable& operator=(const noncopyable&) = delete;
    };

    // apply tuple elements to a function
    template <size_t N, class R>
    struct tapply {
        template <class F, class... Args, class... FArgs>
        static R f(F f, std::tuple<Args...> t, FArgs... args) {
            return tapply<N-1, R>::f(f, t, std::get<N-1>(t), args...);
        }
    };
    template <>
    struct tapply<0, void> {
        template <class F, class... Args, class... FArgs>
        static void f(F f, MADI_UNUSED std::tuple<Args...> t, FArgs... args) {
            f(args...);
        }
    };
    template <class R>
    struct tapply<0, R> {
       template <class F, class... Args, class... FArgs>
       static R f(F f, MADI_UNUSED std::tuple<Args...> t, FArgs... args) {
           return f(args...);
       }
    };
    template <size_t N>
    struct tapply<N, void> {
        template <class F, class... Args, class... FArgs>
        static void f(F f, std::tuple<Args...> t, FArgs... args) {
            tapply<N-1, void>::f(f, t, std::get<N-1>(t), args...);
        }
    };
    template <class R>
    struct tuple_apply {
        template <class F, class... Args>
        static R f(F f, std::tuple<Args...> t) {
            return tapply<sizeof...(Args), R>::f(f, t);
        }
    };
    template <>
    struct tuple_apply<void> {
        template <class F, class... Args>
        static void f(F f, std::tuple<Args...> t) {
            tapply<sizeof...(Args), void>::f(f, t);
        }
    };

    typedef long tsc_t;

    static inline tsc_t rdtsc(void)
    {
#if (defined __i386__) || (defined __x86_64__)
        uint32_t hi,lo;
        asm volatile("lfence\nrdtsc" : "=a"(lo),"=d"(hi));
        return (tsc_t)((uint64_t)hi)<<32 | lo;
#elif (defined __sparc__) && (defined __arch64__)
        uint64_t tick;
        asm volatile("rd %%tick, %0" : "=r" (tick));
        return (tsc_t)tick;
#else
#warning "rdtsc() is not implemented."
        return 0;
#endif
    }

    static inline tsc_t rdtsc_nofence(void)
    {
#if (defined __i386__) || (defined __x86_64__)
        uint32_t hi,lo;
        asm volatile("rdtsc" : "=a"(lo),"=d"(hi));
        return (tsc_t)((uint64_t)hi)<<32 | lo;
#elif (defined __sparc__) && (defined __arch64__)
        uint64_t tick;
        asm volatile("rd %%tick, %0" : "=r" (tick));
        return (tsc_t)tick;
#else
#warning "rdtsc() is not implemented."
        return 0;
#endif
    }

    static inline double now(void)
    {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return (double)tv.tv_sec + (double)tv.tv_usec*1e-6;
    }

    inline int random_int(int n)
    {
        assert(n > 0);

        size_t rand_max =
            ((size_t)RAND_MAX + 1) - ((size_t)RAND_MAX + 1) % (size_t)n;
        int r;
        do {
            r = rand();
        } while ((size_t)r >= rand_max);

        return (int)((double)n * (double)r / (double)rand_max);
    }

    void die(const char *s) MADI_NORETURN;

    inline void die(const char *s)
    {
        fprintf(stderr, "MassiveThreads/DM fatal error: %s\n", s);
        madi_exit(1);
    }

#define MADI_DIE(fmt, ...)                                              \
    do {                                                                \
        fprintf(stderr,                                                 \
                "MassiveThreads/DM fatal error: " fmt " (%s:%d)\n",     \
                ##__VA_ARGS__, __FILE__, __LINE__);                     \
        fflush(stderr);                                                 \
        madi_exit(1);                                                   \
    } while (false)

#define MADI_SPMD_DIE(fmt, ...)                         \
    do {                                                \
        int me = madi_get_debug_pid();                  \
        if (me == 0)                                    \
            MADI_DIE(fmt, ##__VA_ARGS__);               \
        /*else                                          \
            madi_exit(1);*/                             \
    } while (false)

#define MADI_PERR_DIE(str) \
    MADI_DIE("`%s' failed with error `%s'", (str), strerror(errno))

#define MADI_SPMD_PERR_DIE(str) \
    MADI_SPMD_DIE("`%s' failed with error `%s'", (str), strerror(errno))

}

#endif
