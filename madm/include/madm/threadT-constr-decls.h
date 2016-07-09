        template <class F>
        explicit thread(F f);
        template <class F, class T0>
        explicit thread(F f, T0& arg0);
        template <class F, class T0, class T1>
        explicit thread(F f, T0& arg0, T1& arg1);
        template <class F, class T0, class T1, class T2>
        explicit thread(F f, T0& arg0, T1& arg1, T2& arg2);
        template <class F, class T0, class T1, class T2, class T3>
        explicit thread(F f, T0& arg0, T1& arg1, T2& arg2, T3& arg3);
        template <class F, class T0, class T1, class T2, class T3, class T4>
        explicit thread(F f, T0& arg0, T1& arg1, T2& arg2, T3& arg3, T4& arg4);
        template <class F, class T0, class T1, class T2, class T3, class T4, class T5>
        explicit thread(F f, T0& arg0, T1& arg1, T2& arg2, T3& arg3, T4& arg4, T5& arg5);
        template <class F, class T0, class T1, class T2, class T3, class T4, class T5, class T6>
        explicit thread(F f, T0& arg0, T1& arg1, T2& arg2, T3& arg3, T4& arg4, T5& arg5, T6& arg6);
