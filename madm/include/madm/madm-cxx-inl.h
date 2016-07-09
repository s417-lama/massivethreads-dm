#ifndef MADM_CXX_INL_H
#define MADM_CXX_INL_H

#include <uth.h>
#include <mgas.h>
#include <madm_debug.h>

#if 0
#define MADI_LOCALIZE_DPUTS  MADI_DPUTS
#else
#define MADI_LOCALIZE_DPUTS(...)
#endif

namespace madi {

    class mgas_handle_holder {
        friend class worker_storage;

        mgas_handle_t value;
        mgas_handle_holder *parent;
    public:
        mgas_handle_holder()
            : value MGAS_HANDLE_INIT
            , parent(nullptr)
        {
        }

        bool is_main_task()
        {
            return parent == nullptr;
        }
    };
    
    class thread_storage {
        mgas_handle_holder gas_handle_holder_;

    public:
        thread_storage()
            : gas_handle_holder_()
        {
            madi::set_thread_local<thread_storage>(this);
        }

        static thread_storage& instance()
        {
            return *madi::get_thread_local<thread_storage>();
        }

        mgas_handle_holder& gas_handle_holder()
        {
            return gas_handle_holder_;
        }
    };

    class worker_storage {
        mgas_handle_holder *current_handle_holder_;

        worker_storage()
            : current_handle_holder_(nullptr)
        {
            //madi::set_worker_local<worker_storage>(this);
        }

    public:
        class init {
        public:
            init()
            {
                auto wls = new worker_storage;
                madi::set_worker_local<worker_storage>(wls);
            }

            ~init()
            {
                auto wls = madi::get_worker_local<worker_storage>();
                delete wls;
            }
        };

        static worker_storage& instance()
        {
            return *madi::get_worker_local<worker_storage>();
        }

        void register_gas_handle(mgas_handle_holder *holder)
        {
            holder->parent = current_handle_holder_;
            current_handle_holder_ = holder;

            if (holder->parent == nullptr)
                MADI_LOCALIZE_DPUTS("root: %p",
                                    &current_handle_holder_->value);
            else
                MADI_LOCALIZE_DPUTS("parent: %p -> current: %p",
                                    &current_handle_holder_->parent->value,
                                    &current_handle_holder_->value);
        }

        void unlocalize_gas_handle()
        {
            auto& holder = *current_handle_holder_;

            MADI_LOCALIZE_DPUTS("unlocalize %p", &holder.value);

            mgas_unlocalize(&holder.value);
        }

        void deregister_and_unlocalize_gas_handle()
        {
            unlocalize_gas_handle();

            auto& holder = *current_handle_holder_;

            MADI_LOCALIZE_DPUTS("current: %p -> parent: %p",
                                &holder.value, &holder.parent->value);

            current_handle_holder_ = holder.parent;
        }

        void unlocalize_all_gas_handles()
        {
            // do nothing when a stolen thread exits
            if (current_handle_holder_ == nullptr)
                return;

            // FIXME: what is the precise semantics of localize?
            if (current_handle_holder_->is_main_task()) {
                unlocalize_gas_handle();
            } else {
                while (!current_handle_holder_->is_main_task())
                    deregister_and_unlocalize_gas_handle();
            }
        }

        void reset_gas_handle_links()
        {
            auto& holder = thread_storage::instance().gas_handle_holder();

            holder.parent = nullptr;
            mgas_handle_init(&holder.value);

            current_handle_holder_ = &holder;

            MADI_LOCALIZE_DPUTS("current: %p", current_handle_holder_);
        }

        mgas_handle_t& current_handle()
        {
            return current_handle_holder_->value;
        }
    };
  
    static void run_at_parent_is_stolen()
    {
        // called when a victim of work stealing observes a parent thread 
        // is stolen
        
        MADI_LOCALIZE_DPUTS("the parent thread is stolen");

        // unlocalize MGAS caches
        worker_storage::instance().unlocalize_all_gas_handles();
    }

    static void run_at_thread_resuming()
    {
        // called when a thief resumes a stolen thread

        auto& wls = worker_storage::instance();
        wls.reset_gas_handle_links();
    }
  
    template <class F, class... Args>
    void start_func(int argc, char **argv, F f, Args... args)
    {
        // init MGAS
        mgas_initialize(&argc, &argv);

        // init worker local storage
        worker_storage::init wls_init;
        auto& wls = worker_storage::instance();

        // allocate thread local storage
        madi::thread_storage tls;

        // register mgas handle to worker
        auto& handle_holder = tls.gas_handle_holder();
        wls.register_gas_handle(&handle_holder);

        // set a polling function for communication progress
        madm::uth::set_user_poll(mgas_poll);

        // set a function called at exit/migration.
        madi::at_parent_is_stolen(run_at_parent_is_stolen);
        madi::at_thread_resuming(run_at_thread_resuming);

        // call thread start function
        f(argc, argv, args...);

        wls.deregister_and_unlocalize_gas_handle();

        // finalize MGAS
        mgas_finalize();
    }
}

namespace madm {
#if 0
    template <class F, class... Args>
    void start(F f, int& argc, char **& argv, Args... args)
    {
        uth::start(madi::start_func<F, Args...>, argc, argv, f, args...);
    }
#else
    template <class F>
    void start(F f, int& argc, char **& argv)
    { uth::start(madi::start_func<F>, argc, argv, f); }
    template <class F, class T0>
    void start(F f, int& argc, char **& argv, T0& arg0)
    { uth::start(madi::start_func<F, T0>, argc, argv, f, arg0); }
    template <class F, class T0, class T1>
    void start(F f, int& argc, char **& argv, T0& arg0, T1& arg1)
    { uth::start(madi::start_func<F, T0, T1>, argc, argv, f, arg0, arg1); }
#endif

    inline size_t get_pid()
    {
        return uth::get_pid();
    }

    inline size_t get_n_procs()
    {
        return uth::get_n_procs();
    }

    inline void poll()
    {
        uth::poll();
    }

    inline void barrier()
    {
        uth::barrier();
    }
#if 0
    template <class T>
    template <class F, class... Args>
    inline thread<T>::thread(F f, Args... args)
        : impl_([=] {
              // init thread local storage
              madi::thread_storage tls;

              // register mgas handle to worker
              auto& wls0 = madi::worker_storage::instance();
              wls0.register_gas_handle(&handle_holder);

              // call the specified function
              T value = f(args...);

              // unlocalize at thread exit
              auto& wls1 = madi::worker_storage::instance();
              wls1.deregister_and_unlocalize_gas_handle();

              return value;
          })
    {
    }
#else
#include "threadT-constr-impl.h"
#endif

    template <class T>
    inline T thread<T>::join()
    {
        return impl_.join();
    }

    template <>
    class thread<void> {
        uth::thread<void> impl_;
    public:
#if 0
        template <class F, class... Args>
        thread(F f, Args... args)
            : impl_([=] {
              // init thread local storage
              madi::thread_storage tls;

              // register mgas handle to worker
              auto& wls0 = madi::worker_storage::instance();
              wls0.register_gas_handle(&handle_holder);

              // call the specified function
              f(args...);

              // unlocalize at thread exit
              auto& wls1 = madi::worker_storage::instance();
              wls1.deregister_and_unlocalize_gas_handle();
          })
        {
        }
#else
#include "threadvoid-constr-inline.h"
#endif

        ~thread() = default;

        void join() { impl_.join(); }
    };

    template <class T>
    inline ptr<T> all_alloc_darray(size_t size, size_t block_size)
    {
        auto real_size = sizeof(T) * size;
        auto real_block_size = sizeof(T) * block_size;
        auto n_blocks = (real_size + real_block_size - 1) / real_block_size;

        size_t n_dims = 1;
        size_t block_size_arr[] = { real_block_size };
        size_t n_blocks_arr[] = { n_blocks };

        mgasptr_t mp = mgas_all_dmalloc(real_size, n_dims,
                                        block_size_arr, n_blocks_arr);

        return ptr<T>(mp);
    }
    
    template <class T>
    inline void all_dealloc_darray(ptr<T> ptr)
    {
        // FIXME: unlocalize only the array specified by ptr
        unlocalize();

        mgasptr_t mp = ptr.raw();
        mgas_all_free(mp);
    }

    template <class T>
    inline T * localize(ptr<T> mp, size_t size, cache_flags flags)
    {
        auto& handle = madi::worker_storage::instance().current_handle();

        MADI_LOCALIZE_DPUTS("localize   %p", &handle);

        auto real_size = sizeof(T) * size;
        void *p = mgas_localize(mp.raw(), real_size, 
                                static_cast<mgas_flag_t>(flags),
                                &handle);

        return reinterpret_cast<T *>(p);
    }

    template <class T>
    inline void commit(ptr<T> mp, T *p, size_t size)
    {
        auto real_size = sizeof(T) * size;
        mgas_commit(mp.raw(), p, real_size);
    }

    inline void unlocalize()
    {
        madi::worker_storage::instance().unlocalize_gas_handle();
    }

    template <class T>
    inline T * localize_s(ptr<T> mp, size_t stride, size_t count[2],
                          cache_flags flags)
    {
        auto& handle = madi::worker_storage::instance().current_handle();

        MADI_LOCALIZE_DPUTS("localize %p", &handle);

        auto stride_raw = stride * sizeof(T);
        size_t count_raw[] = { count[0], count[1] * sizeof(T) };

        auto p = mgas_localize_s(mp.raw(), stride_raw, count_raw,
                                 static_cast<mgas_flag_t>(flags),
                                 &handle);

        return reinterpret_cast<T *>(p);
    }

    template <class T>
    inline void commit_s(ptr<T> mp, T *p, size_t stride, const size_t count[2])
    {
        auto stride_raw = stride * sizeof(T);
        size_t count_raw[] = { count[0], count[1] * sizeof(T) };

        mgas_commit_s(mp.raw(), p, stride_raw, count_raw);
    }
    
}

#endif
