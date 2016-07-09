    template <class T>
    template <class F>
    inline thread<T>::thread(F f)
        : impl_([=] {
              // init thread local storage
              madi::thread_storage tls;

              // register mgas handle to worker
              auto& wls0 = madi::worker_storage::instance();
              auto& handle_holder = tls.gas_handle_holder();
              wls0.register_gas_handle(&handle_holder);

              // call the specified function
              T value = f();

              // unlocalize at thread exit
              auto& wls1 = madi::worker_storage::instance();
              wls1.deregister_and_unlocalize_gas_handle();

              return value;
          })
    {
    }
    template <class T>
    template <class F, class T0>
    inline thread<T>::thread(F f, T0& arg0)
        : impl_([=] {
              // init thread local storage
              madi::thread_storage tls;

              // register mgas handle to worker
              auto& wls0 = madi::worker_storage::instance();
              auto& handle_holder = tls.gas_handle_holder();
              wls0.register_gas_handle(&handle_holder);

              // call the specified function
              T value = f(arg0);

              // unlocalize at thread exit
              auto& wls1 = madi::worker_storage::instance();
              wls1.deregister_and_unlocalize_gas_handle();

              return value;
          })
    {
    }
    template <class T>
    template <class F, class T0, class T1>
    inline thread<T>::thread(F f, T0& arg0, T1& arg1)
        : impl_([=] {
              // init thread local storage
              madi::thread_storage tls;

              // register mgas handle to worker
              auto& wls0 = madi::worker_storage::instance();
              auto& handle_holder = tls.gas_handle_holder();
              wls0.register_gas_handle(&handle_holder);

              // call the specified function
              T value = f(arg0, arg1);

              // unlocalize at thread exit
              auto& wls1 = madi::worker_storage::instance();
              wls1.deregister_and_unlocalize_gas_handle();

              return value;
          })
    {
    }
    template <class T>
    template <class F, class T0, class T1, class T2>
    inline thread<T>::thread(F f, T0& arg0, T1& arg1, T2& arg2)
        : impl_([=] {
              // init thread local storage
              madi::thread_storage tls;

              // register mgas handle to worker
              auto& wls0 = madi::worker_storage::instance();
              auto& handle_holder = tls.gas_handle_holder();
              wls0.register_gas_handle(&handle_holder);

              // call the specified function
              T value = f(arg0, arg1, arg2);

              // unlocalize at thread exit
              auto& wls1 = madi::worker_storage::instance();
              wls1.deregister_and_unlocalize_gas_handle();

              return value;
          })
    {
    }
    template <class T>
    template <class F, class T0, class T1, class T2, class T3>
    inline thread<T>::thread(F f, T0& arg0, T1& arg1, T2& arg2, T3& arg3)
        : impl_([=] {
              // init thread local storage
              madi::thread_storage tls;

              // register mgas handle to worker
              auto& wls0 = madi::worker_storage::instance();
              auto& handle_holder = tls.gas_handle_holder();
              wls0.register_gas_handle(&handle_holder);

              // call the specified function
              T value = f(arg0, arg1, arg2, arg3);

              // unlocalize at thread exit
              auto& wls1 = madi::worker_storage::instance();
              wls1.deregister_and_unlocalize_gas_handle();

              return value;
          })
    {
    }
    template <class T>
    template <class F, class T0, class T1, class T2, class T3, class T4>
    inline thread<T>::thread(F f, T0& arg0, T1& arg1, T2& arg2, T3& arg3, T4& arg4)
        : impl_([=] {
              // init thread local storage
              madi::thread_storage tls;

              // register mgas handle to worker
              auto& wls0 = madi::worker_storage::instance();
              auto& handle_holder = tls.gas_handle_holder();
              wls0.register_gas_handle(&handle_holder);

              // call the specified function
              T value = f(arg0, arg1, arg2, arg3, arg4);

              // unlocalize at thread exit
              auto& wls1 = madi::worker_storage::instance();
              wls1.deregister_and_unlocalize_gas_handle();

              return value;
          })
    {
    }
    template <class T>
    template <class F, class T0, class T1, class T2, class T3, class T4, class T5>
    inline thread<T>::thread(F f, T0& arg0, T1& arg1, T2& arg2, T3& arg3, T4& arg4, T5& arg5)
        : impl_([=] {
              // init thread local storage
              madi::thread_storage tls;

              // register mgas handle to worker
              auto& wls0 = madi::worker_storage::instance();
              auto& handle_holder = tls.gas_handle_holder();
              wls0.register_gas_handle(&handle_holder);

              // call the specified function
              T value = f(arg0, arg1, arg2, arg3, arg4, arg5);

              // unlocalize at thread exit
              auto& wls1 = madi::worker_storage::instance();
              wls1.deregister_and_unlocalize_gas_handle();

              return value;
          })
    {
    }
    template <class T>
    template <class F, class T0, class T1, class T2, class T3, class T4, class T5, class T6>
    inline thread<T>::thread(F f, T0& arg0, T1& arg1, T2& arg2, T3& arg3, T4& arg4, T5& arg5, T6& arg6)
        : impl_([=] {
              // init thread local storage
              madi::thread_storage tls;

              // register mgas handle to worker
              auto& wls0 = madi::worker_storage::instance();
              auto& handle_holder = tls.gas_handle_holder();
              wls0.register_gas_handle(&handle_holder);

              // call the specified function
              T value = f(arg0, arg1, arg2, arg3, arg4, arg5, arg6);

              // unlocalize at thread exit
              auto& wls1 = madi::worker_storage::instance();
              wls1.deregister_and_unlocalize_gas_handle();

              return value;
          })
    {
    }
