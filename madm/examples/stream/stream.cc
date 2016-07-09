
#include <madm.h>
#include <madm-dtbb.h>
#include <stdio.h>
#include <madm_debug.h>

typedef double elem_t;

#if 0
template <class T>
T * LOCALIZE__(madm::ptr<T> mp, size_t size, madm::cache_flags flags)
{
    MADI_DPUTS("localize(%p, %zu, %d)",
               mp, size, (int)flags);

    return madm::localize(mp, size, flags);
}

template <class T>
void COMMIT__(madm::ptr<T> mp, T *p, size_t size)
{
    MADI_DPUTS("commit(%p, %p, %zu)", mp, p, size);

    madm::commit(mp, p, size);
}
#else
#define LOCALIZE__ madm::localize
#define COMMIT__   madm::commit
#endif

#define check_array(x, size, value, str) \
    do { \
        for (auto i = 0UL; i < size; i++) { \
            if (x[i] != value) { \
                MADI_DPUTS("%s[%zu] = %f (correct: %f)", \
                           str, i, x[i], value); \
                exit(1); \
            } \
        } \
    } while (0)

#define check_darray(X, size, value, str) \
    do { \
        madm::pid_t me = madm::get_pid(); \
        size_t n_procs = madm::get_n_procs(); \
        \
        assert(size % n_procs == 0); \
        \
        auto begin = size / n_procs * me; \
        auto end = size / n_procs * (me + 1); \
        \
        auto *x = LOCALIZE__(X + begin, end - begin, madm::update); \
        \
        check_array(x, end - begin, value, str); \
        \
        /* madm::unlocalize(); */ \
    } while (0)

static void stream_seq(elem_t *x, elem_t *y, elem_t alpha, size_t size)
{
    for (auto i = 0UL; i < size; i++)
        y[i] = alpha * x[i] + y[i];
}

static void stream(madm::ptr<elem_t> X, madm::ptr<elem_t> Y, size_t alpha,
                   size_t size, size_t block_size, size_t local_size)
{
//     MADI_DPUTS("stream(X = 0x%015lx, Y = 0x%015lx, size = %zu)",
//                X.raw(), Y.raw(), size);

    elem_t *x = nullptr, *y = nullptr;

    if (size <= local_size) {
        x = LOCALIZE__(X, size, madm::reuse);
        y = LOCALIZE__(Y, size, madm::update);  // reuse is OK

        check_array(x, size, 10.0, "stream_before_x");
        check_array(y, size, 20.0, "stream_before_y");
    }

    if (size <= block_size) {

        stream_seq(x, y, alpha, size);

        check_array(x, size, 10.0, "stream_after_x");
        check_array(y, size, 30.0, "stream_after_y");

        COMMIT__(Y, y, size);
    } else {
        size_t half = size / 2;
        madm::dtbb::parallel_invoke(
            [=] { stream(X, Y, alpha, half,
                         block_size, local_size); },
            [=] { stream(X + half, Y + half, alpha, size - half,
                         block_size, local_size); }
        );
    }
}

void init_arrays(madm::ptr<elem_t> X, madm::ptr<elem_t> Y, size_t size)
{
    madm::pid_t me = madm::get_pid();
    size_t n_procs = madm::get_n_procs();

    assert(size % n_procs == 0);

    size_t begin = size / n_procs * me;
    size_t end = size / n_procs * (me + 1);

    elem_t *x = LOCALIZE__(X + begin, end - begin, madm::reuse);
    elem_t *y = LOCALIZE__(Y + begin, end - begin, madm::reuse);

    MADI_DP(begin);
    MADI_DP(end);

    for (auto i = 0UL; i < end - begin; i++) {
        x[i] = 10.0;
        y[i] = 20.0;
    }

    COMMIT__(X + begin, x, end - begin);
    COMMIT__(Y + begin, y, end - begin);

    madm::unlocalize();
}

void real_main(int argc, char **argv)
{
    madm::pid_t me = madm::get_pid();
    size_t n_procs = madm::get_n_procs();

    printf("pid = %zu, n_procs = %zu\n", me, n_procs);
    printf("argc = %d\n", argc);
    if (argc < 4) {
        if (me == 0)
            fprintf(stderr, "Usage: %s size block_size local_size\n",
                    argv[0]);
        exit(1);
    }

    size_t size = atol(argv[1]);
    size_t block_size = atol(argv[2]);
    size_t local_size = atol(argv[3]);
    double alpha = 1.0;

    printf("size = %zu, block_size = %zu, local_size = %zu\n",
           size, block_size, local_size);

    if (size < block_size || size < local_size || local_size < block_size) {
        fprintf(stderr, "argument error\n");
        exit(1);
    }

    auto X = madm::all_alloc_darray<elem_t>(size, block_size);
    auto Y = madm::all_alloc_darray<elem_t>(size, block_size);

    init_arrays(X, Y, size);
    
    check_darray(X, size, 10.0, "x");
    check_darray(Y, size, 20.0, "y");
    madm::unlocalize();

    madm::barrier();

    auto t0 = madm::time();

    if (me == 0) {
        madm::thread<void> th(stream, X, Y, alpha, size,
                              block_size, local_size);
        th.join();
    }

    auto t1 = madm::time();

    madm::barrier();

    check_darray(X, size, 10.0, "x");
    check_darray(Y, size, 30.0, "y");

    if (me == 0)
        printf("n_procs = %zu, time = %2.6f\n", n_procs, t1 - t0);

    madm::all_dealloc_darray(X);
    madm::all_dealloc_darray(Y);
}

int main(int argc, char **argv)
{
    madm::start(real_main, argc, argv);
    return 0;
}

