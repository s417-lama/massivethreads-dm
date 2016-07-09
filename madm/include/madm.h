#ifndef MADM_H
#define MADM_H

#include <mgas.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum madm_bool {
    MADM_FALSE,
    MADM_TRUE,
} madm_bool_t;

/*
 * Basic Types
 * ===========
 */

/*
 * Process ID.
 */
typedef size_t madm_pid_t;

/*
 * Global pointer: a pointer which points to global memory.
 */
typedef mgasptr_t madm_ptr_t;

enum { MADM_NULL = 0UL };

/*
 * Task descriptor corresponding to a task.
 */
//typedef madm_ptr_t madm_t;
typedef struct {
    madm_pid_t pid;
    void *desc;
} madm_t;

/*
 * Startpoint function for task.
 */
typedef madm_ptr_t (*madm_func_t)(madm_ptr_t);
typedef void (*madm_func_local_t)(void *p, size_t size, void *parent);


/*
 * Initialization/Finalization
 * ===========================
 */

/*
 * Setup MassiveThreads/DM.
 */
// void madm_initialize(int *argc, char ***argv);

void madm_start(void (*f)(int, char **, void *),
                int *argc, char ***argv, void *p);

/*
 * Release resources for MassiveThreads/DM, and exits current program.
 */
// void madm_finalize(void);


/*
 * Return whether MassiveThreads/DM is initialized or not.
 */
madm_bool_t madm_initialized(void);

/*
 * Process Configurations
 * ======================
 */

/*
 * Return the process ID of the current process.
 */
madm_pid_t madm_get_pid(void);

/*
 * Return the number of processes.
 */
size_t madm_get_n_procs(void);

/*
 * Return the home process of the given task.
 */
madm_pid_t madm_home(madm_t th);

/*
 * Return whether the current task is stolen or not.
 */
madm_bool_t madm_stolen(void);


/*
 * Task Management
 * ===============
 */

/*
 * Create a task which executes given function.
 */
madm_t madm_create(madm_func_t f, madm_ptr_t p);
madm_t madm_create__(madm_func_local_t f, void *p, size_t size, 
                     size_t result_size);

void madm_exit(madm_ptr_t result);
void madm_exit__(void *p, size_t size);

/*
 * Wait for the given task to exit, and then returns the result pointer.
 */
madm_ptr_t madm_join(madm_t th);
void madm_join__(madm_t th, void *p, size_t size);

/*
 * Migrate the given task to the given process.
 */
void madm_migrate_to(madm_pid_t target);

/*
 * communication polling and scheduler switching
 */
void madm_poll(void);

/*
 * Suspend the executing task, and then transfer control to the other task.
 */
void madm_yield(void);

/*
 * task pools (independent tasks)
 */

typedef void (*madm_void_func_t)(madm_ptr_t);

void madm_spawn(madm_void_func_t f, madm_ptr_t p);
void madm_process(void);

typedef void (*madm_void_func_local_t)(void *p, size_t size, void *parent);
    
void madm_spawn__(madm_void_func_local_t f, void *p, size_t size);
void madm_spawn_with_params__(madm_void_func_local_t f, void *p, size_t size,
                              size_t stack_size);
    
void madm_push_parent(void *parent);


/*
 * Global Address Space Access
 * ===========================
 */

/*
 * global address space allocation
 */
madm_ptr_t madm_malloc(size_t size);
//madm_ptr_t madm_dmalloc(size_t size);
void madm_free(madm_ptr_t mp);
void madm_free_small(madm_ptr_t mp, size_t size);

madm_ptr_t madm_all_dmalloc(size_t size, size_t n_dims, size_t block_size[],
                            size_t n_blocks[]);
void madm_all_free(madm_ptr_t mp);

/*
 * global address space access
 */
typedef enum {
    MADM_REUSE = MGAS_RO,    // use cached data
    MADM_UPDATE = MGAS_RWS,  // update cache
    MADM_OWN = MGAS_OWN,     // page migration
} madm_flag_t;

void *madm_localize(madm_ptr_t mp, size_t size, madm_flag_t flags);
void madm_commit(madm_ptr_t mp, void *p, size_t size);
void madm_unlocalize(void);

void madm_get(void *p, madm_ptr_t mp, size_t size);
void madm_put(madm_ptr_t mp, void *p, size_t size);
void madm_set(madm_ptr_t mp, int value, size_t size);

/* vector operations */
typedef mgas_vector_t madm_vector_t;
void *madm_localize_v(madm_ptr_t mp, const madm_vector_t mvs[], size_t n_mvs,
                      madm_flag_t flags);
void madm_commit_v(madm_ptr_t mp, void *p, const madm_vector_t mvs[],
                   size_t n_mvs);

/* strided operations */
void *madm_localize_s(madm_ptr_t mp, size_t stride, const size_t count[2],
                      madm_flag_t flags);
void madm_commit_s(madm_ptr_t mp, void *p, size_t stride,
                   const size_t count[2]);

/*
 * collectives
 */
void madm_barrier(void);
void madm_broadcast(void *p, size_t size, madm_pid_t root);
void madm_gather(void *dst, void *src, size_t size, madm_pid_t root);
void madm_reduce_sum_long(void *dst, void *src, size_t size, madm_pid_t root);


/*
 * profiling and configurations
 */
void madm_prof_start(void);
void madm_prof_stop(void);
void madm_prof_dump(void);
void madm_log_dump(void);

void madm_prof_dump_to_file(FILE *f);
void madm_log_dump_to_file(FILE *f);
void madm_conf_dump_to_file(FILE *f);

double madm_time(void);
long madm_tsc(void);

/* #include "madm_profile.h" */

/*
 * debug
 */

#ifdef __x86_64__
    
#define madm_get_current_sp(ptr_sp) do {                        \
        uint8_t *sp__ = NULL;                                   \
        __asm__ volatile("mov %%rsp,%0\n" : "=r"(sp__));        \
        *ptr_sp = sp__;                                         \
    } while (0)

#elif (defined __sparc__) && (defined __arch64__)

#define madm_get_current_sp(ptr_sp) do {                        \
        uint8_t *sp__ = NULL;                                   \
        __asm__ volatile("mov %%sp,%0\n" : "=r"(sp__));        \
        *ptr_sp = sp__;                                         \
    } while (0)
    
#else
#error "MassiveThreads/DM does not support this archtecture."
#endif
    
void *madm_get_current_stack(void);

#define madm_print_stack_usage() do {                                   \
        uint8_t *sp;                                                    \
        madm_get_current_sp(&sp);                                       \
        uint8_t *stack_end = (uint8_t *)madm_get_current_stack();       \
        printf("%s: sp = %p, stack usage = %zu\n",                      \
               __FUNCTION__, sp, stack_end - sp);                       \
    } while (0)

    

#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
#include "madm-cxx.h"
#endif

#endif
