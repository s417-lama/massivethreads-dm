#ifndef CONVERSION_H
#define CONVERSION_H

#include "../include/mgas.h"
#include "../include/mgas_debug.h"

#include <stdint.h>
#include <limits.h>

static int int_of_size(size_t size)
{
    MGAS_ASSERT(size < INT_MAX);
    return (int)size;
}

static int64_t int64_of_size(size_t size)
{
    MGAS_ASSERT(size < INT64_MAX);
    return (int64_t)size;
}

#define SSIZE_OF(i)  (MGAS_ASSERT((i) < SSIZE_MAX), (ssize_t)(i))

#define uintptr_of_ptr(p)          ((uintptr_t)(p))
#define ptr_of_uintptr(type, i)    ((type)(i))

static void *PTR_ADD(void *p, size_t size)
{
    return (uint8_t *)p + size;
}

static void *PTR_SUB(void *p, size_t size)
{
    return (uint8_t *)p - size;
}

#endif
