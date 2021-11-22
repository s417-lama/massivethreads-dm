#ifndef MADM_UTH_THREAD_INL_H
#define MADM_UTH_THREAD_INL_H

#include "future.h"
#include "future-inl.h"
#include "uni/worker-inl.h"

namespace madm {
namespace uth {

    template <class T, int NDEPS>
    thread<T, NDEPS>::thread() : future_() {}

    template <class T, int NDEPS>
    template <class F, class... Args>
    thread<T, NDEPS>::thread(const F& f, Args... args)
        : future_()
    {
        madi::logger::checkpoint<madi::logger::kind::THREAD>();

        madi::worker& w = madi::current_worker();
        future_ = future<T, NDEPS>::make(w);

        w.fork(start<F, Args...>, future_, f, args...);

        madi::logger::checkpoint<madi::logger::kind::SCHED>();
    }

    template <class T, int NDEPS>
    T thread<T, NDEPS>::join(int dep_id)
    {
        return future_.get(dep_id);
    }

    template <class T, int NDEPS>
    template <class F, class... Args>
    void thread<T, NDEPS>::start(future<T, NDEPS> fut, F f, Args... args)
    {
        madi::logger::checkpoint<madi::logger::kind::SCHED>();

        T value = f(args...);

        madi::logger::checkpoint<madi::logger::kind::THREAD>();

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
            madi::logger::checkpoint<madi::logger::kind::THREAD>();

            madi::worker& w = madi::current_worker();
            future_ = future<long, NDEPS>::make(w);

            w.fork(start<F, Args...>, future_, f, args...);

            madi::logger::checkpoint<madi::logger::kind::SCHED>();
        }

        // copy and move constrs
        thread& operator=(const thread&) = delete;
        thread(thread&& other);  // TODO: implement

        void join(int dep_id = 0) { future_.get(dep_id); }

    private:
        template <class F, class... Args>
        static void start(future<long, NDEPS> fut, F f, Args... args)
        {
            madi::logger::checkpoint<madi::logger::kind::SCHED>();

            f(args...);

            madi::logger::checkpoint<madi::logger::kind::THREAD>();

            long value = 0;
            fut.set(value);
        }
    };

    template <class F, class... Args>
    static void fork(F&& f, Args... args)
    {
        madi::logger::checkpoint<madi::logger::kind::THREAD>();

        madi::worker& w = madi::current_worker();
        w.fork(f, args...);

        madi::logger::checkpoint<madi::logger::kind::SCHED>();
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
