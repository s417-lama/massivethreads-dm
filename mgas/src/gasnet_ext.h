#ifndef MGAS_GASNET_H
#define MGAS_GASNET_H

#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wstrict-prototypes"
#pragma GCC diagnostic ignored "-Wredundant-decls"
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Wsign-conversion"
#pragma GCC diagnostic ignored "-Wfloat-equal"
#pragma GCC diagnostic ignored "-Wdeprecated-register"
#pragma GCC diagnostic ignored "-Wreserved-user-defined-literal"
#include <gasnet.h>
#include <gasnet_vis.h>
#pragma GCC diagnostic warning "-Wstrict-prototypes"
#pragma GCC diagnostic warning "-Wredundant-decls"
#pragma GCC diagnostic warning "-Wconversion"
#pragma GCC diagnostic warning "-Wsign-conversion"
#pragma GCC diagnostic warning "-Wfloat-equal"
#pragma GCC diagnostic warning "-Wdeprecated-register"
#pragma GCC diagnostic warning "-Wreserved-user-defined-literal"

// suppress warning in mpi.h
#undef __PLATFORM_COMPILER_GNU_VERSION_STR

#include <mpi.h>


/* Macro to check return codes and terminate with useful message. */
#define GASNET_SAFE(fncall) do {                                     \
    int _retval;                                                     \
    if ((_retval = fncall) != GASNET_OK) {                           \
      fprintf(stderr, "ERROR calling: %s\n"                          \
                      " at: %s:%i\n"                                 \
                      " error: %s (%s)\n",                           \
              #fncall, __FILE__, __LINE__,                           \
              gasnet_ErrorName(_retval),                             \
              gasnet_ErrorDesc(_retval));                            \
      fflush(stderr);                                                \
      abort();                                                       \
      /*gasnet_exit(_retval);*/                                      \
    }                                                                \
  } while(0)

#define MAKEWORD(hi,lo)                                             \
    ((((uint64_t)(hi)) << 32) | (((uint64_t)(lo)) & 0xFFFFFFFF))
#define HIWORD(arg)     ((gasnet_handlerarg_t)(((uint64_t)(arg)) >> 32))
#define LOWORD(arg)     ((gasnet_handlerarg_t)((uint64_t)(arg)))


static gasnet_node_t gasnet_node_of_proc(mgas_proc_t proc)
{
    return (gasnet_node_t)proc;
}

#endif
