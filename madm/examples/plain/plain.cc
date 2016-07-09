
#include <madm.h>
#include <stddef.h>
#include <stdio.h>

void real_main(int argc, char **argv)
{
    madm::pid_t me = madm::get_pid();
    size_t n_procs = madm::get_n_procs();

    printf("pid = %zu, n_procs = %zu\n", me, n_procs);
}

int main(int argc, char **argv)
{
    madm::start(real_main, argc, argv);
    return 0;
}

