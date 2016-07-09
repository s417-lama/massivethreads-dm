
open("threadT-constr-impl.h", "w") do |f|
  8.times do |i|
    f.puts <<EOS
    template <class T>
    template <class F#{ (0...i).map{|x| ", class T#{x}"}.join }>
    inline thread<T>::thread(F f#{ (0...i).map{|x| ", T#{x}& arg#{x}"}.join })
        : impl_([=] {
              // init thread local storage
              madi::thread_storage tls;

              // register mgas handle to worker
              auto& wls0 = madi::worker_storage::instance();
              auto& handle_holder = tls.gas_handle_holder();
              wls0.register_gas_handle(&handle_holder);

              // call the specified function
              T value = f(#{ (0...i).map{|x| "arg#{x}"}.join ", "});

              // unlocalize at thread exit
              auto& wls1 = madi::worker_storage::instance();
              wls1.deregister_and_unlocalize_gas_handle();

              return value;
          })
    {
    }
EOS
  end
end

