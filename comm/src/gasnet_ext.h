#ifndef MADI_COMM_GASNET_EXT_H
#define MADI_COMM_GASNET_EXT_H

// for suppressing warnings
#define GASNETT_USE_GCC_ATTRIBUTE_MAYALIAS 1
#undef PLATFORM_COMPILER_FAMILYNAME
#undef PLATFORM_COMPILER_FAMILYID
#undef PLATFORM_COMPILER_VERSION
#undef PLATFORM_COMPILER_VERSION_STR
#undef __PLATFORM_COMPILER_GNU_VERSION_STR

#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wdeprecated-register"
#pragma GCC diagnostic ignored "-Wreserved-user-defined-literal"

#include <gasnet.h>

#pragma GCC diagnostic warning "-Wunused-variable"
#pragma GCC diagnostic warning "-Wdeprecated-register"
#pragma GCC diagnostic warning "-Wreserved-user-defined-literal"

#undef PLATFORM_COMPILER_FAMILYNAME
#undef PLATFORM_COMPILER_FAMILYID
#undef PLATFORM_COMPILER_VERSION
#undef PLATFORM_COMPILER_VERSION_STR
#undef __PLATFORM_COMPILER_GNU_VERSION_STR

#include "madm/madm_comm-decls.h"
#include <stdlib.h>

namespace madi {
namespace comm {

    static gasnet_node_t gasnet_node_of_proc(madi::comm::pid_t proc)
    {
        return static_cast<gasnet_node_t>(proc);
    }

    void initialize_gasnet(int& argc, char **& argv,
                           gasnet_handlerentry_t amentries[],
                           size_t n_amentries);
}
}

#endif
