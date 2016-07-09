#ifndef MADM_CXX_H
#define MADM_CXX_H

#include <uth.h>
#include <mgas.h>
#include <cstddef>

namespace madm {

    using madm::uth::time;
    using madm::uth::tick;

    //
    // multi-threading operations
    //

    typedef size_t pid_t;

#if 0
    template <class F, class... Args>
    void start(F f, int& argc, char **& argv, Args... args);
#else
    // gcc-4.6.3 has a bug on variadic templates...
    template <class F>
    void start(F f, int& argc, char **& argv);
    template <class F, class T0>
    void start(F f, int& argc, char **& argv, T0& arg0);
    template <class F, class T0, class T1>
    void start(F f, int& argc, char **& argv, T0& arg0, T1& arg1);
 #endif

    size_t get_pid();
    size_t get_n_procs();

    void poll();

    void barrier();

    template <class T>
    class thread {
        uth::thread<T> impl_;
    public:
        explicit thread() = default;

#if 0
        template <class F, class... Args>
        explicit thread(F f, Args... args);
#else
// gcc-4.6.3 has a bug on variadic arguments,
// so we cannot use it.
#include "madm/threadT-constr-decls.h"
#endif

        ~thread() = default;

        T join();
    };


    //
    // global address space operations
    //

    template <class T>
    class ptr {
        mgasptr_t mp_;
    public:
        ptr(mgasptr_t mp) : mp_(mp) {}
        ~ptr() = default;
    
        mgasptr_t raw() { return mp_; }

        ptr<T> operator+(size_t n) const
        { return ptr<T>(mp_ + sizeof(T) * n); }
    };

    enum cache_flags : int {
        reuse   = MGAS_REUSE,
        update  = MGAS_UPDATE,
        reuse_own  = MGAS_REUSE | MGAS_OWN,
        update_own = MGAS_UPDATE | MGAS_OWN,
    };

    template <class T>
    ptr<T> all_alloc_darray(size_t size, size_t block_size);
    
    template <class T>
    void all_dealloc_darray(ptr<T> ptr);

    template <class T>
    T * localize(ptr<T> mp, size_t size, cache_flags flags);

    template <class T>
    void commit(ptr<T> mp, T *p, size_t size);

    void unlocalize();

    template <class T>
    T * localize_s(ptr<T> mp, size_t stride, size_t count[2],
                   cache_flags flags);
    template <class T>
    void commit_s(ptr<T> mp, T *p, size_t stride, const size_t count[2]);
}

#include "madm/madm-cxx-inl.h"

#endif
