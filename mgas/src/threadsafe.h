#ifndef MGAS_THREADSAFE_H
#define MGAS_THREADSAFE_H

#include "../include/mgas.h"
#include "../include/mgas_config.h"
#include "type_conversion.h"


#pragma GCC diagnostic ignored "-Wredundant-decls"
#if MGAS_INLINE_GLOBALS
static mgas_thread_t globals_get_tid(void);
#else
extern mgas_thread_t globals_get_tid(void);
#endif

void comm_poll(void);
#pragma GCC diagnostic warning "-Wredundant-decls"


//-- atomic operations ---------------------------------------------------------

#define bool_compare_and_swap(p, oldv, newv) \
    __sync_bool_compare_and_swap((p), (oldv), (newv))

#define fetch_and_add(p, value) \
    __sync_fetch_and_add((p), (value))

#define fetch_and_sub(p, value) \
    __sync_fetch_and_sub((p), (value))

#define add_and_fetch(p, value) \
    __sync_add_and_fetch((p), (value))

#define sub_and_fetch(p, value) \
    __sync_sub_and_fetch((p), (value))


//-- memory barriers -----------------------------------------------------------
static void read_barrier(void)
{
    __sync_synchronize();
//    asm volatile("lfence\n":::"memory");
}

static void write_barrier(void)
{
    __sync_synchronize();
//    asm volatile("sfence\n":::"memory");
}

static void memory_barrier(void)
{
    __sync_synchronize();
}

//-- spin locks ----------------------------------------------------------------

#define MGAS_SPIN_FAD       0
#define MGAS_SPIN_CAS       1
#define MGAS_SPIN_STRATEGY  MGAS_SPIN_FAD

typedef struct spinlock {
    volatile int value;
    mgas_thread_t tid;
} spinlock_t;

static void spinlock_init(spinlock_t *lock)
{
    lock->value = 1;
    lock->tid = 0;
}

static void spinlock_destroy(spinlock_t *lock)
{
}

static void spinlock_lock(spinlock_t *lock)
{
#if MGAS_SPIN_STRATEGY == MGAS_SPIN_FAD

    int value;
    while (fetch_and_sub(&lock->value, 1) != 1) {
        while (lock->value <= 0)
            comm_poll();
    }
    
#elif MGAS_SPIN_STRATEGY == MGAS_SPIN_CAS
    
    while (!bool_compare_and_swap(&lock->value, 1, 0)) {
        // TODO: exponential backoff
        comm_poll();
    }
#else
#error
#endif
}

static void spinlock_lock_pure(spinlock_t *lock)
{
#if MGAS_SPIN_STRATEGY == MGAS_SPIN_FAD

    int value;
    while (fetch_and_sub(&lock->value, 1) != 1)
        while (lock->value <= 0)
            ;
    
#elif MGAS_SPIN_STRATEGY == MGAS_SPIN_CAS
    
    while (!bool_compare_and_swap(&lock->value, 1, 0))
        ;

#else
#error
#endif
}

static void spinlock_unlock(spinlock_t *lock)
{
#if MGAS_SPIN_STRATEGY == MGAS_SPIN_CAS
    MGAS_ASSERT(lock->value == 0);
#endif

    // FIXME: thread unsafe
//    write_barrier();
    lock->value = 1;
}

static void spinlock_rec_lock(spinlock_t *lock)
{
    mgas_thread_t me = globals_get_tid();
    
    if (lock->value < 1 && lock->tid == me) {
        lock->value -= 1;
        return;
    }

    while (!bool_compare_and_swap(&lock->value, 1, 0)) {
        // TODO: exponential backoff
        comm_poll();
    }

    lock->tid = me;
}

static void spinlock_rec_lock_pure(spinlock_t *lock)
{
    mgas_thread_t me = globals_get_tid();

    if (lock->value < 1 && lock->tid == me) {
        lock->value -= 1;
        return;
    }
    
    while (!bool_compare_and_swap(&lock->value, 1, 0))
        ;

    lock->tid = me;
}

static void spinlock_rec_unlock(spinlock_t *lock)
{
    if (lock->value < 0) {
        lock->value += 1;
        return;
    }

    MGAS_ASSERT(lock->value == 0);
    
    write_barrier();
    lock->value = 1;
}

static mgas_bool_t spinlock_locked(spinlock_t *lock)
{
    return lock->value <= 0;
}

//-- readers-writer spin locks -------------------------------------------------

typedef struct rwspinlock {
    spinlock_t lock;
    uint16_t writing;
    uint16_t n_readers;
} rwspinlock_t;

static void rwspinlock_init(rwspinlock_t *lock)
{
    spinlock_init(&lock->lock);
    lock->n_readers = 0;
    lock->writing = 0;
}

static void rwspinlock_destroy(rwspinlock_t *lock)
{
    spinlock_destroy(&lock->lock);
}

static mgas_bool_t rwspinlock_read_locked(const rwspinlock_t *lock)
{
    return lock->n_readers != 0;
}

static mgas_bool_t rwspinlock_write_locked(const rwspinlock_t *lock)
{
    return lock->writing == 1;
}

static mgas_bool_t rwspinlock_try_read_lock(rwspinlock_t *lock)
{
    if (lock->writing)
        return MGAS_FALSE;

    spinlock_lock(&lock->lock);

    mgas_bool_t result = (lock->writing == 0);
    if (result)
        lock->n_readers++;

    spinlock_unlock(&lock->lock);

    return result;
}

static void rwspinlock_read_lock(rwspinlock_t *lock)
{
    while (!rwspinlock_try_read_lock(lock))
        comm_poll();
}

static void rwspinlock_read_unlock(rwspinlock_t *lock)
{
    spinlock_lock(&lock->lock);

    MGAS_ASSERT(lock->n_readers != 0);
    
    lock->n_readers--;
    spinlock_unlock(&lock->lock);
}

static mgas_bool_t rwspinlock_try_write_lock(rwspinlock_t *lock)
{
    if (lock->n_readers != 0 || lock->writing != 0)
        return MGAS_FALSE;
    
    spinlock_lock(&lock->lock);

    mgas_bool_t result = (lock->n_readers == 0 && lock->writing == 0);
    if (result)
        lock->writing = 1;

    spinlock_unlock(&lock->lock);

    return result;
}

static void rwspinlock_write_lock(rwspinlock_t *lock)
{
    while (!rwspinlock_try_write_lock(lock))
        comm_poll();
}

static void rwspinlock_write_unlock(rwspinlock_t *lock)
{
#ifdef DEBUG
    spinlock_lock(&lock->lock);
#endif
    MGAS_ASSERT(rwspinlock_write_locked(lock));
    
    write_barrier();
    lock->writing = 0;

#ifdef DEBUG
    spinlock_unlock(&lock->lock);
#endif
}

static void rwspinlock_upgrade(rwspinlock_t *lock)
{
    spinlock_lock(&lock->lock);

    while (lock->n_readers > 1) {
        spinlock_unlock(&lock->lock);
        comm_poll();
        spinlock_lock(&lock->lock);
    }

    lock->writing = 1;

    spinlock_unlock(&lock->lock);
}

static void rwspinlock_downgrade(rwspinlock_t *lock)
{
#ifdef DEBUG
    spinlock_lock(&lock->lock);
#endif

    write_barrier();
    lock->writing = 0;
    
#ifdef DEBUG
    spinlock_unlock(&lock->lock);
#endif
}


#pragma GCC diagnostic ignored "-Wredundant-decls"
#include "globals.h"
#pragma GCC diagnostic warning "-Wredundant-decls"


#endif /* MGAS_THREADSAFE_H */
