#ifndef MGAS_CONFIG_H
#define MGAS_CONFIG_H


#define MGAS_NO_LOOPBACK 1
#define MGAS_INLINE_GLOBALS 1
#define MGAS_MYTH_MALLOC 0
#define MGAS_PROFILE 1
#define MGAS_WARMUP 1

#define MGAS_COMM_NONCONTIG_PACKED 1

#ifndef MGAS_COMM_LOG_ENABLED
#define MGAS_COMM_LOG_ENABLED 0
#endif

#define MGAS_ENABLE_NESTED_COMM_PROCESS    0

#define MGAS_ENABLE_GIANT_LOCK 0

#define MGAS_ALWAYS_INLINE __attribute__((always_inline))
#define MGAS_PURE __attribute__((pure))

#endif
