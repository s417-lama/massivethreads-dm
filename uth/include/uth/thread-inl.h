#ifndef MADM_UTH_THREAD_INL_H
#define MADM_UTH_THREAD_INL_H

#include "future.h"
#include "future-inl.h"
#include "uni/worker-inl.h"
#include <tuple>

namespace madm {
namespace uth {

    template <class T, int NDEPS>
    thread<T, NDEPS>::thread() : future_() {}

    template <class T, int NDEPS>
    template <class F, class... Args>
    thread<T, NDEPS>::thread(const F& f, Args... args)
        : future_()
    {
        spawn(f, args...);
    }

    template <class T, int NDEPS>
    template <class F, class... Args>
    bool thread<T, NDEPS>::spawn(const F& f, Args... args)
    {
        return spawn_aux(f, std::make_tuple(args...), []{});
    }

    template <class T, int NDEPS>
    template <class F, class ArgsTuple, class Callback>
    bool thread<T, NDEPS>::spawn_aux(const F& f, ArgsTuple args, Callback c)
    {
        madi::logger::checkpoint<madi::logger::kind::WORKER_BUSY>();

        madi::worker& w = madi::current_worker();
        future_ = future<T, NDEPS>::make(w);

        return w.fork(start<F, ArgsTuple>, future_, f, args);
    }

    template <class T, int NDEPS>
    T thread<T, NDEPS>::join(int dep_id)
    {
        T ret = future_.get(dep_id);
        return ret;
    }

    template <class T, int NDEPS>
    void thread<T, NDEPS>::discard(int dep_id)
    {
        return future_.discard(dep_id);
    }

    template <class T, int NDEPS>
    template <class F, class ArgsTuple>
    void thread<T, NDEPS>::start(future<T, NDEPS> fut, F f, ArgsTuple args)
    {
        madi::logger::checkpoint<madi::logger::kind::WORKER_THREAD_FORK>();

        T value = std::apply(f, args);

        madi::logger::checkpoint<madi::logger::kind::WORKER_BUSY>();

        fut.set(value);
    }

    template <int NDEPS>
    class thread<void, NDEPS> {
    private:
        future<long, NDEPS> future_;

    public:
        // constr/destr with no thread
        thread()  = default;
        ~thread() = default;

        // constr create a thread
        template <class F, class... Args>
        explicit thread(const F& f, Args... args)
            : future_()
        {
            spawn(f, args...);
        }

        template <class F, class... Args>
        bool spawn(const F& f, Args... args)
        {
            return spawn_aux(f, std::make_tuple(args...), []{});
        }

        template <class F, class ArgsTuple, class Callback>
        bool spawn_aux(const F& f, ArgsTuple args, Callback c)
        {
            madi::logger::checkpoint<madi::logger::kind::WORKER_BUSY>();

            madi::worker& w = madi::current_worker();
            future_ = future<long, NDEPS>::make(w);

            return w.fork(start<F, ArgsTuple>, future_, f, args);
        }

        // copy and move constrs
        thread& operator=(const thread&) = delete;
        thread(thread&& other);  // TODO: implement

        void join(int dep_id = 0) { future_.get(dep_id); }
        void discard(int dep_id) { return future_.discard(dep_id); }

    private:
        template <class F, class ArgsTuple>
        static void start(future<long, NDEPS> fut, F f, ArgsTuple args)
        {
            madi::logger::checkpoint<madi::logger::kind::WORKER_THREAD_FORK>();

            std::apply(f, args);

            madi::logger::checkpoint<madi::logger::kind::WORKER_BUSY>();

            long value = 0;
            fut.set(value);
        }
    };

    template <class F, class... Args>
    static void fork(F&& f, Args... args)
    {
        madi::logger::checkpoint<madi::logger::kind::WORKER_BUSY>();

        madi::worker& w = madi::current_worker();
        w.fork(f, args...);
    }

    template <class F, class... Args>
    static void suspend(F&& f, Args... args)
    {
        madi::worker& w = madi::current_worker();
        w.suspend(f, args...);
    }

    static void resume(saved_context *sctx)
    {
        madi::worker& w = madi::current_worker();
        w.resume(sctx);
    }

}
}

#endif
