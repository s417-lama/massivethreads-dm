#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <assert.h>

#include <sys/time.h>

#include <mgas_am.h>
#include "topo.h"


typedef uint64_t tsc_t;

static inline tsc_t rdtsc(void)
{
#if (defined __i386__) || (defined __x86_64__)
    uint32_t hi,lo;
    asm volatile("lfence\nrdtsc" : "=a"(lo),"=d"(hi));
    return (tsc_t)((uint64_t)hi)<<32 | lo;
#elif (defined __sparc__) && (defined __arch64__)
    uint64_t tick;
    asm volatile("rd %%tick, %0" : "=r" (tick));
    return (tsc_t)tick;
#else
#warning "rdtsc() is not implemented."
        return 0;
#endif
}

static inline double now(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec*1e-6;
}

static inline void die(const char *s)
{
    fprintf(stderr, "die: %s\n", s);
    exit(1);
}


typedef struct msg {
    int *done;
} msg_t;

static void handler_rep(const mgas_am_t *am, const void *buf, size_t nbytes)
{
    const msg_t *msg = buf;
    *msg->done = 1;
}

static void handler_req(const mgas_am_t *am, const void *buf, size_t nbytes)
{
    mgas_am_reply(handler_rep, buf, nbytes, am);
}

static int random_int(int n)
{
    if (n == 0)
        return 0;

    size_t rand_max =
        ((size_t)RAND_MAX + 1) - ((size_t)RAND_MAX + 1) % (size_t)n;
    int r;
    do {
       r = rand();
    } while ((size_t)r >= rand_max);

    return (int)((double)n * (double)r / (double)rand_max);
}

typedef struct {
    int target;
    tsc_t comm;
    tsc_t wait;
} timebuf_t;

static void real_main(int me, int n_procs, 
                      size_t n_msgs, size_t wait, size_t data_size, 
                      const char *type)
{
    size_t i, j;

    srand((unsigned)rdtsc());

    int target_type = 0;
    if (strcmp(type, "rand") == 0)
        target_type = 0;
    else if (strcmp(type, "neighb") == 0)
        target_type = 1;
    else if (strcmp(type, "me") == 0)
        target_type = 2;

    timebuf_t *timebuf = calloc(sizeof(*timebuf), n_msgs);

    msg_t *msg = malloc(sizeof(msg_t) + data_size);
    memset(msg, 0, sizeof(msg_t) + data_size);

    int neighbs[6];
    if (target_type == 1)
        topo_neighbs(me, neighbs);

    for (i = 0; i < n_msgs; i++) {
        int target;
        if (target_type == 0) {
            do {
                target = random_int(n_procs);
            } while (target == me);
        } else if (target_type == 1) {
            target = topo_rand_neighb(neighbs, me);
        } else if (target_type == 2) {
            int node = topo_node_rank((int)me);
            do {
                target = node + random_int(n_procs <= 16 ? n_procs : 16);
            } while (target == me);
        }

        int done = 0;
        msg->done = &done;

        tsc_t t0 = rdtsc();

        mgas_am_request(handler_req, msg, sizeof(msg_t) + data_size, 
                        (mgas_proc_t)target);

        tsc_t t1 = rdtsc();
        
        while (!done)
            mgas_poll();

        tsc_t t2 = rdtsc();

        timebuf[i].target = target;
        timebuf[i].comm = t1 - t0;
        timebuf[i].wait = t2 - t1;

        for (j = 0; j < wait; j++)
            mgas_poll();
    }

    mgas_barrier();

    char filename[1024];
    sprintf(filename, "p2p_rand.%05d.out", me);

    FILE *fp = fopen(filename, "w");

    for (i = 0; i < n_msgs; i++) {
        fprintf(fp, 
                "pid = %6d, id = %9zu, target = %9d, "
                "comm = %9lu, wait = %9lu\n",
                me, i, 
                timebuf[i].target, 
                (unsigned long)timebuf[i].comm, 
                (unsigned long)timebuf[i].wait);
    }

    fclose(fp);
}

int main(int argc, char **argv)
{
    mgas_initialize(&argc, &argv);

    if (argc != 5) {
        fprintf(stderr, "Usage: %s n_msgs wait data_size [ rand | neighb | me ]\n", argv[0]);
        exit(1);
    }
    long n_msgs = atol(argv[1]);
    long wait = atol(argv[2]);
    long data_size = atol(argv[3]);
    const char *type = argv[4];

    int me = (int)mgas_get_pid();
    int n_procs = (int)mgas_get_n_procs();

    mgas_barrier();

    real_main(me, n_procs, (size_t)n_msgs, (size_t)wait, 
              (size_t)data_size, type);

    mgas_barrier();

    mgas_finalize();
    return 0;
}
