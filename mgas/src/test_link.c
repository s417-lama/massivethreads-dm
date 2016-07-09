
#include "../include/mgas.h"

int main(int argc, char **argv)
{
    mgas_initialize(&argc, &argv);
    mgas_finalize();
    return 0;
}
