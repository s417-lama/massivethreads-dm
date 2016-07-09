
#include <mgas.h>
#include <mgas_prof.h>
#include <stdio.h>
#include <stdlib.h>


int main(int argc, char **argv)
{
    mgas_initialize(&argc, &argv);

    mgas_proc_t me = mgas_get_pid();
    size_t n_procs = mgas_get_n_procs();
//    printf("hello %05ld/%05zu\n", me, n_procs);

    if (me == 0) {
        printf("start barrier\n");
    }
    
    mgas_barrier();

    if (me == 0) {
        printf("end barrier\n");
    }

    mgas_finalize();
    return 0;
}
