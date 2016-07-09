#include "comm.h"

#include "../include/mgas.h"
#include "../include/mgas_config.h"
#include "../include/mgas_debug.h"
#include "handlers.h"
#include "globals.h"
#include "sys.h"
#include "allocator.h"
#include "misc.h"
#include "type_conversion.h"

#include "gasnet_ext.h"
#include "mcomm_compat.h"

#include <stddef.h>
#include <sys/mman.h>

#define MGAS_COMM_THREAD 0

#if MGAS_COMM_THREAD
// FIXME: temporal
static __thread comm_handler_t *mgas_g_handler = NULL;
#endif

typedef struct msgbuf_pool msgbuf_pool_t;

static msgbuf_pool_t *comm_get_msgbuf_pool(comm_t *comm);

static void *comm_sys_malloc(size_t size)
{
    MGAS_PROF_ALLOC(COMM_SYS, size);

    return mgas_sys_malloc(size);
}

static void comm_sys_free(void *p)
{
    MGAS_PROF_FREE(COMM_SYS, p);

    mgas_sys_free(p);
}


enum comm_constants {
    MGAS_MAX_MSG_SIZE = 16 * 1024 * 1024,
};

struct comm_handle {
    int64_t count;       // valid data if it is on proc
    int64_t *count_ptr;  // a pointer to 'count' field on proc
    mgas_proc_t proc;
};


typedef enum msg_tag {
    MSG_ALLOC,
    MSG_FREE,
//    MSG_TRANSFER,
//    MSG_OWNER_CHANGE,

    MSG_OWNER_REQ,
    MSG_OWNER_RES,
    MSG_OWNER_CHANGE,
    MSG_DATA_REQ,
    MSG_DATA_RES,
    MSG_AM_REQ,
    MSG_RMW_REQ,
    MSG_RMW_RES,
} msg_tag_t;

typedef struct msg_alloc {
    size_t size;
    mgasptr_t *mp_ptr;
    comm_handle_t handle;
} msg_alloc_t;

typedef struct msg_free {
    mgasptr_t mp;
} msg_free_t;

typedef struct msg_owner_req {
    mgas_access_t access;
    join_counter_t *jc;
    owner_result_t *result_buf;
    size_t n_mps;
    // va_body: const mgasptr_t mps[]
} msg_owner_req_t;

typedef struct msg_owner_res {
    join_counter_t *jc;
    owner_result_t *result_buf;
    size_t n_results;
    // va_body: const mgas_proc_t owners[]
} msg_owner_res_t;

typedef struct msg_owner_change_t {
    size_t n_mps;
} msg_owner_change_t;

typedef struct msg_data_req {
    const struct data_rep_arg *rep_arg;
    mgas_access_t access;
    size_t n_pairs;
    // va_body: const mem_pair_t pairs[]
} msg_data_req_t;

typedef struct msg_data_res {
    const struct data_rep_arg *rep_arg;
    size_t n_retry_indices;
    // va_body: const size_t retry_indices[]
} msg_data_res_t;

typedef struct msg_rmw_req {
    mgas_rmw_func_t f;
    mgasptr_t mp;
    size_t size;
    mgas_bool_t *result_buf;
    size_t param_in_size;
    void *param_out;
    size_t param_out_size;
    comm_handle_t *handle;
    // va_body: none
} msg_rmw_req_t;

typedef struct msg_rmw_res {
    mgas_bool_t success;
    mgas_bool_t *result_buf;
    void *param_out;
    size_t param_out_size;
    comm_handle_t *handle;
    // va_body: none
} msg_rmw_res_t;

typedef struct msg_am_req {
    mgas_am_func_t f;
    size_t size;
    // va_body: uint8_t data[]
} msg_am_req_t;


struct msg {
    uint8_t tag;
    mgas_proc_t initiator;
    mgas_proc_t target;
    union {
        msg_alloc_t alloc;
        msg_free_t free;

        msg_owner_req_t vo_req;
        msg_owner_res_t vo_res;
        msg_owner_change_t vo_change;
        msg_data_req_t va_req;
        msg_data_res_t va_res;

        msg_rmw_req_t rmw_req;
        msg_rmw_res_t rmw_res;
        msg_am_req_t am_req;
    } body;
    uint8_t va_body[1];
};


struct comm_handler {
    spinlock_t lock;
    
    mgas_bool_t finishing;
    mgas_bool_t processing;

    spinlock_t msg_id_lock;
    int32_t next_msg_id;

    spinlock_t msg_q_lock;
    queue_t *msg_q;
};

typedef struct msgbuf {
    struct msgbuf *next;
    mgas_proc_t proc;
    int32_t msg_id;
    size_t received;
    uint8_t *msg;
} msgbuf_t;

struct msgbuf_pool {
    spinlock_t lock;
    msgbuf_t *first;
};

struct comm {
    uint8_t *gheap_base;
    size_t gheap_size;
    uint8_t *gheap_ptr;

    mgas_alc_t comm_alc;
    void **msg_buf;

    msgbuf_pool_t msgbuf_pool;
};


static const char *string_of_tag(msg_tag_t tag)
{
    const char *s;

#define CASE(tag) case tag: { s = #tag; break; }

    switch (tag) {
        CASE(MSG_ALLOC);
        CASE(MSG_FREE);
        CASE(MSG_OWNER_REQ);
        CASE(MSG_OWNER_RES);
        CASE(MSG_OWNER_CHANGE);
        CASE(MSG_DATA_REQ);
        CASE(MSG_DATA_RES);
        CASE(MSG_AM_REQ);
        CASE(MSG_RMW_REQ);
        CASE(MSG_RMW_RES);
    default:
        s = "UNKNOWN";
    }

#undef CASE

    return s;
}

static void handler_process(comm_handler_t *handler);

comm_handler_t *comm_handler_create(void)
{
    comm_handler_t *handler =
        (comm_handler_t *)comm_sys_malloc(sizeof(comm_handler_t));
    handler->finishing = MGAS_FALSE;
    handler->processing = MGAS_FALSE;
    handler->msg_q = queue_create();

    spinlock_init(&handler->lock);
    spinlock_init(&handler->msg_id_lock);
    spinlock_init(&handler->msg_q_lock);

    return handler;
}

void comm_handler_destroy(comm_handler_t *handler)
{
    spinlock_destroy(&handler->lock);
    spinlock_destroy(&handler->msg_id_lock);
    spinlock_destroy(&handler->msg_q_lock);
    queue_destroy(handler->msg_q);
    comm_sys_free(handler);
}

static void handler_giant_lock(comm_handler_t *handler)
{
//    spinlock_lock(&handler->lock);
}

static void handler_giant_unlock(comm_handler_t *handler)
{
//    spinlock_unlock(&handler->lock);
}

static void msg_set_header(msg_t *msg, msg_tag_t tag, mgas_proc_t target)
{
    mgas_proc_t me = globals_get_pid();

    msg->tag = tag;
    msg->initiator = me;
    msg->target = target;
}

static msg_t *msg_create(msg_tag_t tag, mgas_proc_t target, size_t va_body_size)
{
    size_t msg_size = offsetof(msg_t, va_body) + va_body_size;
    msg_t *msg = comm_sys_malloc(msg_size);

    msg_set_header(msg, tag, target);

    return msg;
}

static void msg_destroy(msg_t *msg, size_t va_body_size)
{
    comm_sys_free(msg);
}

static size_t msg_get_size(size_t va_body_size)
{
    return offsetof(msg_t, va_body) + va_body_size;
}

static int32_t handler_new_msg_id(comm_handler_t *handler);
static void handler_queue_msg_body(comm_handler_t *handler,
                                   void *buf, size_t nbytes);

long mgas_g_send_time;
long mgas_g_send_msg_size;

static void comm_send_msg(msg_t *msg, size_t msg_size, mgas_proc_t target)
{
    mgas_proc_t me = globals_get_pid();

#if MGAS_COMM_THREAD
    comm_handler_t *handler = mgas_g_handler;
    if (handler == NULL) {
        MGAS_COMM_LOG("this is a worker thread.");
        handler = globals_get_handler();
    } else {
        MGAS_COMM_LOG("this is a comm thread.");
    }
#else
    comm_handler_t *handler = globals_get_handler();
#endif
    
#if MGAS_NO_LOOPBACK
    if (target == me) {
        handler_giant_lock(handler);

        handler_queue_msg_body(handler, msg, msg_size);

        handler_giant_unlock(handler);        
    } else {
#endif
        MGAS_COMM_LOG("send %s(init=%zu, target=%zu, msg_size=%zu)",
                      string_of_tag(msg->tag), msg->initiator, msg->target,
                      msg_size);

        if (msg_size <= mgas_gasnet_AMMaxMedium()) {
            MGAS_COMM_LOG("this message is small.");
            long t0 = rdtsc();
            GASNET_SAFE(gasnet_AMRequestMedium0(gasnet_node_of_proc(target),
                                                HANDLER_QUEUE_MSG,
                                                msg, msg_size));
            long t1 = rdtsc();
            mgas_g_send_time = t1 - t0;
            mgas_g_send_msg_size = (long)msg_size;

#if 0
            MGAS_DEBUG_PRINT('.', "size = %9ld, time = %9ld cycles",
                             mgas_g_send_msg_size, mgas_g_send_time);
#endif

#if 0
        } else if (msg_size <= mgas_gasnet_AMMaxLongRequest()) {

            MGAS_COMM_LOG("this message is medium.");
            MGAS_CHECK(!"FIXME: msg_buf[target] is not safe in concurrent accesses");
            
            comm_t *comm = globals_get_comm();
            void *target_msg_buf = comm->msg_buf[target];

            GASNET_SAFE(gasnet_AMRequestLong0(gasnet_node_of_proc(target),
                                              HANDLER_QUEUE_MSG,
                                              msg, msg_size, target_msg_buf));
#endif
        } else {
            MGAS_COMM_LOG("this message is large.");

            MGAS_CHECK(msg_size <= MGAS_MAX_MSG_SIZE);
            MGAS_ASSERT(msg_size <= INT32_MAX);
            MGAS_ASSERT(me <= INT32_MAX);

            handler_giant_lock(handler);
            
            size_t max_long_size = mgas_gasnet_AMMaxLongRequest();
            size_t msg_id = (size_t)handler_new_msg_id(handler);

            handler_giant_unlock(handler);

            size_t n_sub_msgs = msg_size / max_long_size;
            if (msg_size % max_long_size > 0)
                n_sub_msgs += 1;

            size_t offset = 0;
            size_t i;
            for (i = 0; i < n_sub_msgs; i++) {
                uint8_t *msg_base = (void *)msg;
                size_t rest_size = msg_size - offset;
                size_t size = MIN(rest_size, max_long_size);

                mgas_gasnet_AMRequestMedium4(gasnet_node_of_proc(target),
                                        HANDLER_QUEUE_LARGE_MSG,
                                        msg_base + offset, size,
                                        (int32_t)me, (int32_t)msg_id,
                                        (int32_t)offset, (int32_t)msg_size);
                offset += size;
            }
            
        }

        MGAS_COMM_LOG("send done");

#if MGAS_NO_LOOPBACK
    }
#endif

#if !MGAS_COMM_THREAD
    handler_process(handler);
#endif
}

mgasptr_t comm_request_alloc(size_t size)
{
    comm_handle_t *handle = comm_handle_create(1);

    mgasptr_t *mp_buf = comm_malloc(sizeof(mgasptr_t));

    msg_t msg;
    msg_set_header(&msg, MSG_ALLOC, 0); // GA_ALLOC is only sent to process 0.

    msg_alloc_t *alloc = &msg.body.alloc;
    alloc->size = size;
    alloc->mp_ptr = mp_buf;
    alloc->handle = *handle;

    comm_send_msg(&msg, msg.target, sizeof(msg_t));
    comm_handle_wait(handle);
    comm_handle_destroy(handle);

    mgasptr_t mp = *mp_buf;
    comm_free(mp_buf);

    return mp;
}

void comm_request_free(mgasptr_t mp)
{
    MGAS_UNDEFINED;
}

void comm_request_owners(const mgasptr_t mps[], size_t n_mps,
                         mgas_access_t access, join_counter_t *jc,
                         owner_result_t *result_buf, mgas_proc_t target)
{
    mgas_proc_t me = globals_get_pid();

    if (target == me) {
        msg_t msg;
        msg_set_header(&msg, MSG_OWNER_REQ, target);
        msg_owner_req_t *req = &msg.body.vo_req;
        req->access = access;
        req->jc = jc;
        req->result_buf = result_buf;
        req->n_mps = n_mps;
        
        mgas_process_owner_request(&msg, mps, req->n_mps, req->access,
                                   msg.initiator);
    } else {
        size_t mps_size = sizeof(mgasptr_t) * n_mps;
        size_t msg_size = msg_get_size(mps_size);

        msg_t *msg = msg_create(MSG_OWNER_REQ, target, mps_size);
        msg_owner_req_t *req = &msg->body.vo_req;
        req->access = access;
        req->jc = jc;
        req->result_buf = result_buf;
        req->n_mps = n_mps;

        memcpy(msg->va_body, mps, mps_size);

        comm_send_msg(msg, msg_size, target);

        msg_destroy(msg, mps_size);
    }
}

void comm_reply_owners(const msg_t *req, const owner_result_t results[],
                       size_t n_results)
{
    mgas_proc_t me = globals_get_pid();

    if (req->initiator == me) {
        const msg_owner_req_t *vo_req = &req->body.vo_req;
        mgas_process_owner_reply(results, n_results,
                                 vo_req->jc, vo_req->result_buf);
    } else {
        size_t results_size = sizeof(results[0]) * n_results;
        size_t msg_size = msg_get_size(results_size);

        msg_t *msg = msg_create(MSG_OWNER_RES, req->initiator, results_size);
        msg_owner_res_t *res = &msg->body.vo_res;
        res->jc = req->body.vo_req.jc;
        res->result_buf = req->body.vo_req.result_buf;
        res->n_results = n_results;

        memcpy(msg->va_body, results, results_size);

        comm_send_msg(msg, msg_size, msg->target);

        msg_destroy(msg, results_size);
    }
}

void comm_request_owner_change(const mgasptr_t mps[], size_t n_mps,
                               mgas_proc_t target)
{
    mgas_proc_t me = globals_get_pid();

    size_t mps_size = sizeof(mgasptr_t) * n_mps;
    size_t msg_size = msg_get_size(mps_size);

    msg_t *msg = msg_create(MSG_OWNER_CHANGE, target, mps_size);
    msg_owner_change_t *vo_change = &msg->body.vo_change;
    vo_change->n_mps = n_mps;

    memcpy(msg->va_body, mps, mps_size);

    comm_send_msg(msg, msg_size, target);

    msg_destroy(msg, mps_size);
}

void comm_request_data_transfer(const mem_pair_t pairs[], size_t n_pairs,
                                mgas_access_t access,
                                const struct data_rep_arg *rep_arg,
                                mgas_proc_t target)
{   
    mgas_proc_t me = globals_get_pid();

//    DPUTS("REQ_DATA(%s, %"PRIx64"): %zu -> %zu",
//          string_of_access(access), pairs[0].mp, me, target);

    size_t pairs_size = sizeof(pairs[0]) * n_pairs;
    size_t msg_size = msg_get_size(pairs_size);

    msg_t *msg = msg_create(MSG_DATA_REQ, target, pairs_size);
    msg_data_req_t *req = &msg->body.va_req;
    req->rep_arg = rep_arg;
    req->access = access;
    req->n_pairs = n_pairs;

    memcpy(msg->va_body, pairs, pairs_size);

    comm_send_msg(msg, msg_size, target);

    msg_destroy(msg, pairs_size);
}

void comm_reply_data_transfer(const msg_t *req_msg,
                              const size_t retry_indices[],
                              size_t n_retry_indices)
{
    const msg_data_req_t *req = &req_msg->body.va_req;
//    DPUTS("REP_DATA(%s, %"PRIx64"): %zu -> %zu",
//          string_of_access(req->access), ((mem_pair_t *)req_msg->va_body)->mp,
//          globals_get_pid(), req_msg->initiator);
    
    size_t retry_indices_size = sizeof(size_t) * n_retry_indices;
    size_t msg_size = msg_get_size(retry_indices_size);

    msg_t *msg = msg_create(MSG_DATA_RES, req_msg->initiator,
                            retry_indices_size);
    msg_data_res_t *res = &msg->body.va_res;
    res->rep_arg = req_msg->body.va_req.rep_arg;
    res->n_retry_indices = n_retry_indices;

    memcpy(msg->va_body, retry_indices, retry_indices_size);

    comm_send_msg(msg, msg_size, msg->target);

    msg_destroy(msg, retry_indices_size);
}

mgas_bool_t comm_request_rmw(mgas_rmw_func_t f, mgasptr_t mp, size_t size,
                             const void *param_in, size_t param_in_size,
                             void *param_out, size_t param_out_size,
                             mgas_proc_t target)
{
    mgas_proc_t me = globals_get_pid();

    mgas_bool_t success;
    comm_handle_t *handle = comm_handle_create(1);

    if (me == target) {
        msg_t msg;
        msg_set_header(&msg, MSG_RMW_REQ, target);

        msg_rmw_req_t *req = &msg.body.rmw_req;
        req->f = f;
        req->mp = mp;
        req->size = size;
        req->result_buf = &success;
        req->param_in_size = param_in_size;
        req->param_out = param_out;
        req->param_out_size = param_out_size;
        req->handle = handle;

        mgas_process_rmw_request(&msg, f, mp, size, param_in, param_in_size,
                                 param_out, param_out_size);
    } else {
        size_t msg_size = msg_get_size(param_in_size);
        msg_t *msg = msg_create(MSG_RMW_REQ, target, param_in_size);

        msg_rmw_req_t *req = &msg->body.rmw_req;
        req->f = f;
        req->mp = mp;
        req->size = size;
        req->result_buf = &success;
        req->param_in_size = param_in_size;
        req->param_out = param_out;
        req->param_out_size = param_out_size;
        req->handle = handle;
        memcpy(msg->va_body, param_in, param_in_size);

        comm_send_msg(msg, msg_size, msg->target);

        msg_destroy(msg, param_in_size);
    }
    
    comm_handle_wait(handle);
    comm_handle_destroy(handle);

    return success;
}

static void comm_process_rmw_reply(mgas_bool_t success,
                                   mgas_bool_t *result_buf,
                                   void *param_out,
                                   const void *param_out_data,
                                   size_t param_out_size,
                                   comm_handle_t *handle)
{
    mgas_process_rmw_reply(success, param_out, param_out_data, param_out_size,
                           result_buf);
    comm_handle_notify(handle, 1);
}

void comm_reply_rmw(const msg_t *req_msg, mgas_bool_t success,
                    const void *param_out_buf)
{
    mgas_proc_t me = globals_get_pid();

    mgas_proc_t target = req_msg->initiator;
    const msg_rmw_req_t *req = &req_msg->body.rmw_req;

    if (me == target) {
        comm_process_rmw_reply(success, req->result_buf,
                               req->param_out, param_out_buf,
                               req->param_out_size,
                               req->handle);
    } else {
        size_t param_out_size = req->param_out_size;
        size_t msg_size = msg_get_size(param_out_size);

        msg_t *msg = msg_create(MSG_RMW_RES, target, param_out_size);

        msg_rmw_res_t *res = &msg->body.rmw_res;
        res->success = success;
        res->result_buf = req->result_buf;
        res->param_out = req->param_out;
        res->param_out_size = req->param_out_size;
        res->handle = req->handle;
        memcpy(msg->va_body, param_out_buf, param_out_size);

        comm_send_msg(msg, msg_size, msg->target);

        msg_destroy(msg, param_out_size);
    }
}

#define MGAS_AM_MY_HANDLER  0

#if MGAS_AM_MY_HANDLER

void comm_request_active_message(mgas_am_func_t f, const void *p, size_t size,
                                 mgas_proc_t target)
{
    size_t msg_size = msg_get_size(size);

    msg_t *msg = msg_create(MSG_AM_REQ, target, size);
    msg_am_req_t *req = &msg->body.am_req;
    req->f = f;
    req->size = size;

    memcpy(msg->va_body, p, size);

    comm_send_msg(msg, msg_size, msg->target);

    msg_destroy(msg, size);
}
void comm_reply_active_message(const mgas_am_t *am, mgas_am_func_t f,
                               const void *p, size_t size)
{
    comm_request_active_message(f, p, size, am->msg->initiator);
}

#else

void comm_request_active_message(mgas_am_func_t f, const void *p, size_t size,
                                 mgas_proc_t target)
{
    mgas_gasnet_AMRequestMedium2(
            gasnet_node_of_proc(target),
            HANDLER_MGAS_AM,
            (void *)p, size,
            HIWORD(f), LOWORD(f));
}
void comm_reply_active_message(const mgas_am_t *am, mgas_am_func_t f,
                               const void *p, size_t size)
{
    mgas_gasnet_AMReplyMedium2(
            *(gasnet_token_t *)am->token, 
            HANDLER_MGAS_AM, 
            (void *)p, size,
            HIWORD(f), LOWORD(f));
}

#endif

static void comm_process_active_message(gasnet_token_t token,
                                        void *buf, size_t nbytes,
                                        gasnet_handlerarg_t arg0,
                                        gasnet_handlerarg_t arg1)
{
    mgas_am_func_t f = (mgas_am_func_t)MAKEWORD(arg0, arg1);

    mgas_am_t am = { NULL, &token };
    f(&am, buf, nbytes);
}

//-- msgbuf pool ---------------------------------------------------------------

static void msgbuf_pool_init(msgbuf_pool_t *pool)
{
    spinlock_init(&pool->lock);
    pool->first = NULL;
}

static void msgbuf_pool_destroy(msgbuf_pool_t *pool)
{
    spinlock_destroy(&pool->lock);
    MGAS_ASSERT(pool->first == NULL);
}

static void msgbuf_pool_lock(msgbuf_pool_t *pool)
{
    spinlock_lock(&pool->lock);
}

static void msgbuf_pool_unlock(msgbuf_pool_t *pool)
{
    spinlock_unlock(&pool->lock);
}

static msgbuf_t *msgbuf_pool_lookup(msgbuf_pool_t *pool, mgas_proc_t proc,
                                    int32_t msg_id)
{
    msgbuf_t *msgbuf = pool->first;

    while (msgbuf != NULL) {
        if (msgbuf->proc == proc && msgbuf->msg_id == msg_id)
            break;

        msgbuf = msgbuf->next;
    }

    return msgbuf;
}

static msgbuf_t *msgbuf_pool_lookup_msgbuf(msgbuf_pool_t *pool,
                                           mgas_proc_t proc,
                                           int32_t msg_id)
{
    msgbuf_t *msgbuf = msgbuf_pool_lookup(pool, proc, msg_id);

    if (msgbuf == NULL) {
        msgbuf = mgas_sys_malloc(sizeof(*msgbuf));
        msgbuf->next = pool->first;
        msgbuf->proc = proc;
        msgbuf->msg_id = msg_id;
        msgbuf->received = 0;
        msgbuf->msg = NULL;

        pool->first = msgbuf;
    }

    return msgbuf;
}

static void msgbuf_pool_destroy_msgbuf(msgbuf_pool_t *pool, mgas_proc_t proc,
                                       int32_t msg_id)
{
    msgbuf_t *msgbuf = pool->first;

    msgbuf_t *prev = NULL;
    while (msgbuf != NULL) {
        if (msgbuf->proc == proc && msgbuf->msg_id == msg_id)
            break;

        prev = msgbuf;
        msgbuf = msgbuf->next;
    }

    if (prev != NULL)
        prev->next = msgbuf->next;
    else if (msgbuf != NULL)
        pool->first = msgbuf->next;
    else
        MGAS_NOT_REACHED;
}

//-- basic messaging mechanisms ------------------------------------------------

static void handler_queue_push(comm_handler_t *handler, msg_t *msg)
{
#if MGAS_COMM_THREAD
    spinlock_lock(&handler->msg_q_lock);
#endif
    
    queue_t *msg_q = handler->msg_q;
    queue_push(msg_q, msg);

#if MGAS_COMM_THREAD
    spinlock_unlock(&handler->msg_q_lock);
#endif
}

static mgas_bool_t handler_queue_pop(comm_handler_t *handler, msg_t **msg)
{
#if MGAS_COMM_THREAD
    spinlock_lock(&handler->msg_q_lock);
#endif
    
    queue_t *msg_q = handler->msg_q;
    mgas_bool_t result = queue_pop(handler->msg_q, (void **)msg);

#if MGAS_COMM_THREAD
    spinlock_unlock(&handler->msg_q_lock);
#endif
    return result;
}

static int32_t handler_new_msg_id(comm_handler_t *handler)
{
#if MGAS_COMM_THREAD
    spinlock_lock(&handler->msg_id_lock);
#endif
    
    int32_t id = handler->next_msg_id++;

#if MGAS_COMM_THREAD
    spinlock_unlock(&handler->msg_id_lock);
#endif
    return id;
}

static void handler_queue_msg_body(comm_handler_t *handler,
                                   void *buf, size_t nbytes)
{
    handler_giant_lock(handler);
    
    msg_t *msg = mgas_sys_malloc(nbytes);
    memcpy(msg, buf, nbytes);

    MGAS_COMM_LOG("recv %s(init=%zu, target=%zu, msg_size=%zu)",
                  string_of_tag(msg->tag), msg->initiator, msg->target,
                  nbytes);

    handler_queue_push(handler, msg);

    handler_giant_unlock(handler);
}

static void handler_queue_msg(gasnet_token_t token, void *buf, size_t nbytes)
{
    mgas_gasnet_hold_interrupts();

#if MGAS_COMM_THREAD
    comm_handler_t *handler = mgas_g_handler; // FIXME: temporal
    if (handler == NULL)
        handler = globals_get_handler();
#else
    comm_handler_t *handler = globals_get_handler();
#endif
    
    handler_queue_msg_body(handler, buf, nbytes);

    mgas_gasnet_resume_interrupts();
}

static void handler_queue_large_msg_body(comm_handler_t *handler,
                                         void *buf, size_t nbytes,
                                         mgas_proc_t proc, int32_t msg_id,
                                         size_t offset, size_t msg_size)
{
    handler_giant_lock(handler);

    comm_t *comm = globals_get_comm();
    msgbuf_pool_t *pool = comm_get_msgbuf_pool(comm);

    msgbuf_pool_lock(pool);
    
    msgbuf_t *msg_buf = msgbuf_pool_lookup_msgbuf(pool, proc, msg_id);

    if (msg_buf->msg == NULL)
        msg_buf->msg = mgas_sys_malloc(msg_size);

    // copy data to msg_buf
    memcpy(msg_buf->msg + offset, buf, nbytes);
    msg_buf->received += nbytes;

    MGAS_COMM_LOG("id = %4d, offset = %8zu, size = %8zu, received = %8zu, "
                  "msg_size = %8zu",
                  msg_id, offset, nbytes, msg_buf->received, msg_size);
    MGAS_ASSERT(msg_buf->received <= msg_size);
    
    // if done, push the msg_buf to message queue
    if (msg_buf->received == msg_size) {
        handler_queue_push(handler, (msg_t *)msg_buf->msg);

        msg_buf->msg = NULL;
        msgbuf_pool_destroy_msgbuf(pool, proc, msg_id);
    }

    msgbuf_pool_unlock(pool);

    handler_giant_unlock(handler);
}

static void handler_queue_large_msg(gasnet_token_t token,
                                    void *buf, size_t nbytes,
                                    gasnet_handlerarg_t arg0,
                                    gasnet_handlerarg_t arg1,
                                    gasnet_handlerarg_t arg2,
                                    gasnet_handlerarg_t arg3)
{
    int32_t proc = arg0;
    int32_t msg_id = arg1;
    int32_t offset = arg2;
    int32_t msg_size = arg3;
    
    mgas_gasnet_hold_interrupts();

#if MGAS_COMM_THREAD
    comm_handler_t *handler = mgas_g_handler; // FIXME: temporal
    if (handler == NULL)
        handler = globals_get_handler();
#else
    comm_handler_t *handler = globals_get_handler();
#endif

    handler_queue_large_msg_body(handler, buf, nbytes, (mgas_proc_t)proc,
                                 msg_id, (size_t)offset, (size_t)msg_size);

    mgas_gasnet_resume_interrupts();
}

static void handler_process_msg(const msg_t *msg)
{
    switch (msg->tag) {
    case MSG_ALLOC: {
        const msg_alloc_t *alloc = &msg->body.alloc;
        mgas_process_alloc(alloc->size, alloc->mp_ptr, &alloc->handle,
                           msg->initiator);
        break;
    }
    case MSG_FREE: {
        const msg_free_t *free = &msg->body.free;
        mgas_process_free(free->mp);
        break;
    }
    case MSG_OWNER_REQ: {
        const msg_owner_req_t *req = &msg->body.vo_req;
        const mgasptr_t *mps = (mgasptr_t *)msg->va_body;
        mgas_process_owner_request(msg, mps, req->n_mps, req->access,
                                   msg->initiator);
        break;
    }
    case MSG_OWNER_RES: {
        const msg_owner_res_t *res = &msg->body.vo_res;
        const owner_result_t *results = (owner_result_t *)msg->va_body;
        mgas_process_owner_reply(results, res->n_results, res->jc,
                                 res->result_buf);
        break;
    }
    case MSG_OWNER_CHANGE: {
        const msg_owner_change_t *ochange = &msg->body.vo_change;
        const mgasptr_t *mps = (mgasptr_t *)msg->va_body;
        mgas_process_owner_change(mps, ochange->n_mps, msg->initiator);
        break;
    }

    case MSG_DATA_REQ: {
        const msg_data_req_t *req = &msg->body.va_req;
        const mem_pair_t *pairs = (mem_pair_t *)msg->va_body;
        mgas_process_data_transfer_request(msg, pairs, req->n_pairs,
                                           req->access, msg->initiator);
        break;
    }
    case MSG_DATA_RES: {
        const msg_data_res_t *res = &msg->body.va_res;
        const size_t *retry_indices = (size_t *)msg->va_body;
        mgas_process_data_transfer_reply(retry_indices, res->n_retry_indices,
                                         res->rep_arg);
        break;
    }
    case MSG_RMW_REQ: {
        const msg_rmw_req_t *req = &msg->body.rmw_req;
        const void *param_in = (const void *)msg->va_body;
        mgas_process_rmw_request(msg, req->f, req->mp, req->size,
                                 param_in, req->param_in_size,
                                 req->param_out, req->param_out_size);
        break;
    }
    case MSG_RMW_RES: {
        const msg_rmw_res_t *res = &msg->body.rmw_res;
        const void *param_out_data = (const void *)msg->va_body;
        comm_process_rmw_reply(res->success, res->result_buf, res->param_out,
                               param_out_data, res->param_out_size,
                               res->handle);
        break;
    }
    case MSG_AM_REQ: {
        const msg_am_req_t *req = &msg->body.am_req;
        void *p = (void *)msg->va_body;
        mgas_am_process_request(msg, req->f, p, req->size);
        break;
    }
    default:
        MGAS_NOT_REACHED;
    }
}

int mgas_g_poll_processed;
long mgas_g_poll_tsc;

static void handler_process(comm_handler_t *handler)
{
#if !MGAS_ENABLE_NESTED_COMM_PROCESS
    if (handler->processing)
        return;
    handler->processing = MGAS_TRUE;
#endif

    handler_giant_lock(handler);
    
    mgas_g_poll_processed = 0;

    msg_t *msg;
    while (handler_queue_pop(handler, &msg)) {
        tsc_t t0 = rdtsc();
        
        handler_giant_unlock(handler);
        handler_process_msg(msg);
        handler_giant_lock(handler);
        
        comm_sys_free(msg);

        tsc_t t1 = rdtsc();

        mgas_g_poll_tsc = t1 - t0;
        mgas_g_poll_processed = 1;
    }

    handler_giant_unlock(handler);
    
#if !MGAS_ENABLE_NESTED_COMM_PROCESS
    handler->processing = MGAS_FALSE;
#endif
}

long mgas_g_working = 0;
long mgas_g_n_gasnet_poll = 0;
long mgas_g_tsc_gasnet_poll = 0;

long mgas_g_poll_check = 0;
long mgas_g_prev_poll_time = 0;
long mgas_g_n_large_poll_intervals = 0;

static void warn_large_poll_interval(long t)
{
    MGAS_DEBUG_PRINT('D', 
                     "poll() has not been called "
                     "for 1M cycles (%ld) (%zu times)",
                     t, mgas_g_n_large_poll_intervals);
}


static void comm_internal_poll(comm_handler_t *handler)
{
    if (mgas_g_working == 1) {
        mgas_g_tsc_gasnet_poll -= rdtsc_nofence();
    }

    mgas_gasnet_AMPoll();

    if (mgas_g_working == 1) {
        mgas_g_tsc_gasnet_poll += rdtsc_nofence();
        mgas_g_n_gasnet_poll += 1;
    }
    
    handler_process(handler);
}

void comm_poll(void)
{
#if !MGAS_COMM_THREAD
    comm_handler_t *handler = globals_get_handler();
    comm_internal_poll(handler);
#endif
}

//-- vector put/get opeartions -------------------------------------------------

static void handler_put_v_packed_body(comm_handler_t *handler,
                                      void *buf, size_t nbytes,
                                      size_t n_dsts)
{
    size_t i;

    handler_giant_lock(handler);

    mgas_memvec_t *dst = (mgas_memvec_t *)buf;
    uint8_t *data = (void *)&dst[n_dsts];

    size_t data_idx = 0;
    for (i = 0; i < n_dsts; i++) {
        mgas_memvec_t *v = &dst[i];

        memcpy(v->p, data + data_idx, v->size);

        data_idx += v->size;
    }

    handler_giant_unlock(handler);
}

static void handler_put_v_packed(gasnet_token_t token,
                                 void *buf, size_t nbytes,
                                 gasnet_handlerarg_t arg0)
{
    MGAS_PROF_BEGIN(COMM_HANDLE_PUTV);

    int32_t n_dsts = arg0;
   
    mgas_gasnet_hold_interrupts();

#if MGAS_COMM_THREAD
    comm_handler_t *handler = mgas_g_handler; // FIXME: temporal
    if (handler == NULL)
        handler = globals_get_handler();
#else
    comm_handler_t *handler = globals_get_handler();
#endif

    handler_put_v_packed_body(handler, buf, nbytes, (size_t)n_dsts);

    mgas_gasnet_resume_interrupts();

    MGAS_PROF_END(COMM_HANDLE_PUTV);
}

static size_t calculate_n_send_dsts(size_t max_size,
                                    const mgas_memvec_t dst[], size_t n_dst)
{
    size_t i;

    if (n_dst == 0)
        return 0;

    // calculate
    size_t n_send_dsts = 0;
    size_t total_size = 0;
    for (i = 0; i < n_dst; i++) {
        total_size += sizeof(mgas_memvec_t) + dst[i].size;
        n_send_dsts += 1;

        if (total_size >= max_size)
            break;
    }

    return n_send_dsts;
}

static void comm_put_v_packed(const mgas_memvec_t dst[], size_t n_dst,
                              mgas_proc_t dstproc,
                              const mgas_memvec_t src[], size_t n_src)
{
    size_t i;

    size_t max_packet_size = mgas_gasnet_AMMaxMedium();
    MGAS_ASSERT(max_packet_size <= 16 * 1024 * 1024);

    if (n_dst == 0 || n_src == 0) {
        MGAS_ASSERT(n_dst == 0 && n_src == 0);
        return;
    }

    size_t buf_idx = 0;
    uint8_t *buf = (uint8_t *)mgas_sys_malloc(max_packet_size);

    mgas_memvec_t *buf_vecs = (mgas_memvec_t *)buf;

    size_t dstv_idx = 0;
    size_t srcv_idx = 0;
    size_t dst_idx = 0;
    size_t src_idx = 0;
    while (dst_idx < n_dst && src_idx < n_src) {
        // temporarily rewrite dst
        mgas_memvec_t *first_dst = (mgas_memvec_t *)&dst[dst_idx];
        first_dst->p = (uint8_t *)first_dst->p + dstv_idx;
        first_dst->size = first_dst->size - dstv_idx;

        // calculate # of sending dsts
        size_t n_send_dsts = calculate_n_send_dsts(max_packet_size,
                                                   dst + dst_idx,
                                                   n_dst - dst_idx);
        MGAS_ASSERT(n_send_dsts > 0);

        size_t send_dsts_size = sizeof(mgas_memvec_t) * n_send_dsts;
        uint8_t *buf_data = buf + buf_idx + send_dsts_size;

        size_t buf_vec_idx = 0;
        size_t buf_data_idx = 0;

        // pack dst vector and data for each dsts
        size_t next_dstv_idx = dstv_idx;
        size_t packet_size = send_dsts_size;
        for (i = 0; i < n_send_dsts; i++) {
            MGAS_ASSERT(dst_idx < n_dst);
            const mgas_memvec_t *dstv = &dst[dst_idx];

            ssize_t packet_overrun_size =
                (ssize_t)(packet_size + dstv->size) - (ssize_t)max_packet_size;

            size_t dstv_size;
            if (packet_overrun_size > 0) {
                MGAS_ASSERT(i == n_send_dsts - 1);
                dstv_size = dstv->size - (size_t)packet_overrun_size;
                next_dstv_idx += dstv_size;
            } else {
                dstv_size = dstv->size;
                dst_idx += 1;
                next_dstv_idx = 0;
            }

            // temporarily rewrite src
            mgas_memvec_t *first_src = (mgas_memvec_t *)&src[src_idx];
            first_src->p = (uint8_t *)first_src->p + srcv_idx;
            first_src->size = first_src->size - srcv_idx;

            // src data copy
            size_t next_srcv_idx = srcv_idx;
            size_t copy_size = 0;
            while (copy_size < dstv_size) {
                MGAS_ASSERT(src_idx < n_src);
                const mgas_memvec_t *srcv = &src[src_idx];

                ssize_t dstv_overrun_size = 
                    (ssize_t)(copy_size + srcv->size) - (ssize_t)dstv_size;

                size_t size;
                if (dstv_overrun_size > 0) {
                    size = srcv->size - (size_t)dstv_overrun_size;
                    next_srcv_idx += size;
                } else {
                    size = srcv->size;
                    src_idx += 1;
                    next_srcv_idx = 0;
                }

                MGAS_ASSERT(buf <= buf_data + buf_data_idx);
                MGAS_ASSERT(buf_data + buf_data_idx + size <= buf + max_packet_size);

                memcpy(buf_data + buf_data_idx, srcv->p, size);
                buf_data_idx += size;

                copy_size += size;
            }
            MGAS_ASSERT(copy_size == dstv_size);

            packet_size += copy_size;

            // rollback src
            first_src->p = (uint8_t *)first_src->p - srcv_idx;
            first_src->size = first_src->size + srcv_idx;

            srcv_idx = next_srcv_idx;

            // dst vector copy
            mgas_memvec_t *bufv = &buf_vecs[buf_vec_idx++];
            bufv->p = dstv->p;
            bufv->size = dstv_size;
        }

        // rollback first_dst
        first_dst->p = (uint8_t *)first_dst->p - dstv_idx;
        first_dst->size = first_dst->size + dstv_idx;

        MGAS_ASSERT(n_send_dsts <= UINT32_MAX);

        dstv_idx = next_dstv_idx;

        MGAS_ASSERT(packet_size <= max_packet_size);

        // data transfer
        mgas_gasnet_AMRequestMedium1((gasnet_node_t)dstproc,
                                     HANDLER_PUT_V_PACKED,
                                     buf, packet_size,
                                     (gasnet_handlerarg_t)n_send_dsts);
    }

    MGAS_ASSERT(dst_idx == n_dst);
    MGAS_ASSERT(src_idx == n_src);
    MGAS_ASSERT(dstv_idx == 0);
    MGAS_ASSERT(srcv_idx == 0);

    mgas_sys_free(buf);
}
static void handler_reply_get_v_packed(const mgas_am_t *am,
                                       const void *buf, size_t buf_size)
{
    int *done = *(int **)buf;
    *done = 1;
}

static void handler_request_get_v_packed(const mgas_am_t *am,
                                         const void *buf, size_t buf_size)
{
    mgas_proc_t initiator = mgas_am_get_initiator(am);
    const uint8_t *p = buf;

    int *done = *(int * const *)p;
    p += sizeof(done);

    size_t n_dst = *(const size_t *)p;
    p += sizeof(n_dst);

    size_t n_src = *(const size_t *)p;
    p += sizeof(n_src);

    const mgas_memvec_t *dst = (const mgas_memvec_t *)p;
    p += sizeof(*dst) * n_dst;

    const mgas_memvec_t *src = (const mgas_memvec_t *)p;
    p += sizeof(*src) * n_src;

    comm_put_v(dst, n_dst, initiator, src, n_src);

    mgas_am_reply(handler_reply_get_v_packed, &done, sizeof(done), am);
}

static void comm_get_v_packed(const mgas_memvec_t dst[], size_t n_dst,
                              const mgas_memvec_t src[], size_t n_src,
                              mgas_proc_t srcproc)
{
    volatile int done = 0;

    size_t dst_size = sizeof(*dst) * n_dst;
    size_t src_size = sizeof(*src) * n_src;
    size_t buf_size = sizeof(&done) + sizeof(n_dst) + sizeof(n_src)
                      + dst_size + src_size;
    uint8_t *buf = (uint8_t *)mgas_sys_malloc(buf_size);

    size_t idx = 0;

    *(volatile int **)(buf + idx) = &done;

    *(size_t *)(buf + idx) = n_dst;
    idx += sizeof(n_dst);

    *(size_t *)(buf + idx) = n_src;
    idx += sizeof(n_src);

    memcpy(buf + idx, dst, dst_size);
    idx += dst_size;

    memcpy(buf + idx, src, src_size);
    idx += src_size;

    mgas_am_request(handler_request_get_v_packed, buf, buf_size, srcproc);

    while (done == 0)
        mgas_poll();
}

void comm_put_v(const mgas_memvec_t dst[], size_t n_dst, mgas_proc_t dstproc,
                const mgas_memvec_t src[], size_t n_src)
{
    size_t me = globals_get_pid();

#if MGAS_COMM_NONCONTIG_PACKED
    comm_put_v_packed(dst, n_dst, dstproc, src, n_src);
#else
    mgas_gasnet_putv_bulk(gasnet_node_of_proc(dstproc),
                          n_dst, (gasnet_memvec_t *)dst,
                          n_src, (gasnet_memvec_t *)src);
#endif

#if !MGAS_COMM_THREAD
    if (dstproc != me) {
        comm_handler_t *handler = globals_get_handler();
        handler_process(handler);
    }
#endif
}

void comm_get_v(const mgas_memvec_t dst[], size_t n_dst,
                const mgas_memvec_t src[], size_t n_src, mgas_proc_t srcproc)
{
    size_t me = globals_get_pid();

#if MGAS_COMM_VECTOR_PACKED
    comm_get_packed(dst, n_dst, src, n_src, srcproc);
#else
    mgas_gasnet_getv_bulk(n_dst, (gasnet_memvec_t *)dst,
                          gasnet_node_of_proc(srcproc),
                          n_src, (gasnet_memvec_t *)src);
#endif

#if !MGAS_COMM_THREAD
    if (srcproc != me) {
        comm_handler_t *handler = globals_get_handler();
        handler_process(handler);
    }
#endif
}

//-- misc communication functions ----------------------------------------------

void comm_put_u64(uint64_t *p, mgas_proc_t proc, uint64_t value)
{
    size_t me = globals_get_pid();

#if MGAS_NO_LOOPBACK
    if (proc == me) {
        *p = value;
    } else {
#endif

        mgas_gasnet_put_val(gasnet_node_of_proc(proc), p, value,
                            sizeof(uint64_t));

#if !MGAS_COMM_THREAD
        comm_handler_t *handler = globals_get_handler();
        handler_process(handler);
#endif

#if MGAS_NO_LOOPBACK
    }
#endif
}

static void handler_atomic_add_i64(gasnet_token_t token,
                                   gasnet_handlerarg_t p_high,
                                   gasnet_handlerarg_t p_low,
                                   gasnet_handlerarg_t value_high,
                                   gasnet_handlerarg_t value_low)
{
    int64_t *p = (int64_t *)MAKEWORD(p_high, p_low);
    int64_t value = (int64_t)MAKEWORD(value_high, value_low);

    __sync_fetch_and_add(p, value);
}

void comm_atomic_add_i64(int64_t *p, mgas_proc_t target,
                         int64_t value)
{
    mgas_proc_t me = globals_get_pid();
#if MGAS_NO_LOOPBACK
    if (target == me) {
        __sync_fetch_and_add(p, value);
    } else {
#endif

        GASNET_SAFE(gasnet_AMRequestShort4(gasnet_node_of_proc(target),
                                           HANDLER_ATOMIC_ADD_I64,
                                           HIWORD(p), LOWORD(p),
                                           HIWORD(value), LOWORD(value)));

#if !MGAS_COMM_THREAD
        comm_handler_t *handler = globals_get_handler();
        handler_process(handler);
#endif
        
#if MGAS_NO_LOOPBACK
    }
#endif
}

//-- intialization/finalization of communication layer -------------------------

static void *comm_mmap(size_t len)
{
    void *p = mmap(NULL, len, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANON, -1, 0);
    MGAS_CHECK(p != MAP_FAILED);
    return p;
}

static void *comm_aligned_mmap(size_t len, size_t align)
{
    void *p = comm_mmap(len);

#ifdef DEBUG
    // FIXME
    uintptr_t aligned = (uintptr_t)p % align == 0;
    MGAS_CHECK(aligned);
#endif

    return p;
}

static void *comm_gheap_mmap(size_t size)
{
    comm_t *comm = globals_get_comm();

    uint8_t *p = comm->gheap_ptr;
    if (p + size < comm->gheap_base + comm->gheap_size) {
        comm->gheap_ptr += size;
        return p;
    } else {
        return NULL;
    }
}

static void *comm_gheap_aligned_mmap(size_t size, size_t align)
{
    // FIXME: it is desirable to reuse leftover memory chunk.
    comm_t *comm = globals_get_comm();

    uint8_t *ptr = comm->gheap_ptr;
    uint8_t *p = (uint8_t *)(((uintptr_t)ptr + align - 1) / align * align);

    if (p + size < comm->gheap_base + comm->gheap_size) {
        comm->gheap_ptr = p + size;
        return p;
    } else {
        return NULL;
    }
}

static void *comm_thread_start(void *p)
{
#if MGAS_COMM_THREAD
    comm_handler_t *handler = p;

    mgas_g_handler = handler; // FIXME: temporal

    while (handler->finishing == 1)
        comm_internal_poll(handler);
#endif
    return NULL;
}

pthread_t comm_spawn_helper_thread(void)
{
    pthread_t th = NULL;

#if MGAS_COMM_THREAD
    comm_handler_t *handler = globals_get_handler();
    handler->finishing = 1;

    int result = pthread_create(&th, NULL, comm_thread_start, handler);

    MGAS_CHECK(result == 0);
#endif

    return th;
}

void comm_join_helper_thread(pthread_t th)
{
#if MGAS_COMM_THREAD
    // notify finishing to the comm thread
    comm_handler_t *handler = globals_get_handler();
    handler->finishing = 2;

    // join the thread
    int result = pthread_join(th, NULL);

    MGAS_CHECK(result == 0);
#endif
}

comm_t *comm_initialize(int *argc, char ***argv, size_t n_threads)
{
    mgas_proc_t proc;

    mgas_initialize_gasnet(argc, argv);

    mgas_proc_t me = comm_get_pid();
    size_t n_procs = comm_get_n_procs();

#if !GASNET_SEGMENT_EVERYTHING
    gasnet_seginfo_t *table = array_create(gasnet_seginfo_t, n_procs);
    GASNET_SAFE(mgas_gasnet_getSegmentInfo(table, (int)n_procs));

    // buffer for a long system message
    void **msg_buf = array_create(void *, n_procs);
    for (proc = 0; proc < n_procs; proc++)
        msg_buf[proc] = table[proc].addr;

    // setup global heap
    void *gheap_base = PTR_ADD(table[me].addr, MGAS_MAX_MSG_SIZE);
    size_t gheap_size = table[me].size - MGAS_MAX_MSG_SIZE;
    void *gheap_ptr = gheap_base;
    MGAS_ASSERT(gheap_size > 0);

    // if GASNET_SEGMENT is FAST or LARGE, use gasnet segment memory mapper
    mgas_alc_t comm_alc = mgas_alc_create(n_threads,
                                          comm_gheap_mmap,
                                          comm_gheap_aligned_mmap);

    array_destroy(table);
#else
    // buffer for a long system message
    void **msg_buf = NULL;
// FIXME: does not scalable and this impl is not safe for concurrency.
//    void **msg_buf = array_create(void *, n_procs);
//    for (proc = 0; proc < n_procs; proc++)
//        msg_buf[proc] = comm_sys_malloc(MGAS_MAX_MSG_SIZE);

    // setup global heap
    void *gheap_base = NULL;
    size_t gheap_size = 0;
    void *gheap_ptr = NULL;
    // if GASNET_SEGMENT is EVERYTHING, use system mmap
    mgas_alc_t comm_alc = mgas_alc_create(n_threads,
                                          comm_mmap, comm_aligned_mmap);
#endif

    comm_t *comm = (comm_t *)comm_sys_malloc(sizeof(comm_t));
    comm->gheap_base = gheap_base;
    comm->gheap_size = gheap_size;
    comm->gheap_ptr = gheap_ptr;
    comm->comm_alc = comm_alc;
    comm->msg_buf = msg_buf;

    msgbuf_pool_init(&comm->msgbuf_pool);
    
    return comm;
}

void comm_finalize(comm_t *comm)
{
    mgas_proc_t proc;
    size_t n_procs = comm_get_n_procs();

#if GASNET_SEGMENT_EVERYTHING
// FIXME:
//    for (proc = 0; proc < n_procs; proc++)
//        comm_sys_free(comm->msg_buf[proc]);
#endif

// FIXME:
//    array_destroy(comm->msg_buf);
    mgas_alc_destroy(comm->comm_alc);
    comm_sys_free(comm);

    mgas_gasnet_exit(0);
}

void comm_exit(int status)
{
    mgas_gasnet_exit(status);
}

const char *comm_get_segment_name(void)
{
#if GASNET_SEGMENT_FAST
    return "GASNET_SEGMENT_FAST";
#elif GASNET_SEGMENT_LARGE
    return "GASNET_SEGMENT_LARGE";
#elif GASNET_SEGMENT_EVERYTHING
    return "GASNET_SEGMENT_EVERYTHING";
#endif
}

mgas_proc_t comm_get_pid(void)
{
    return mgas_gasnet_mynode();
}

size_t comm_get_n_procs(void)
{
    return mgas_gasnet_nodes();
}

static msgbuf_pool_t *comm_get_msgbuf_pool(comm_t *comm)
{
    return &comm->msgbuf_pool;
}

void *comm_malloc(size_t size)
{
    mgas_alc_t alc = globals_get_comm()->comm_alc;
    void *p = mgas_alc_malloc(alc, size);

    if (p == NULL)
        mgas_die("fatal error: GASNet global memory segment is full");

    return p;
}

void comm_free(void *p)
{
    mgas_alc_t alc = globals_get_comm()->comm_alc;
    mgas_alc_free(alc, p);
}

void *comm_aligned_malloc(size_t size, size_t align)
{
#if GASNET_SEGMENT_EVERYTHING
    void *p = mgas_sys_memalign(size, align);
#else
    mgas_alc_t alc = globals_get_comm()->comm_alc;
    void *p = mgas_alc_aligned_malloc(alc, size, align);

    if (p == NULL)
        mgas_die("fatal error: GASNet global memory segment is full");
#endif

    return p;
}

void comm_aligned_free(void *p)
{
#if GASNET_SEGMENT_EVERYTHING
    mgas_sys_free(p);
#else
    mgas_alc_t alc = globals_get_comm()->comm_alc;
    mgas_alc_aligned_free(alc, p);
#endif
}

void comm_barrier_with_poll(mgas_poll_t poll)
{
    static long count = 0;
    static long count2 = 0;
    
    // sometimes gasnet_barrier_notify does not progress
    // in PSHM configuration (?).

    mgas_gasnet_barrier_notify(0, GASNET_BARRIERFLAG_ANONYMOUS);
    for (;;) {
        tsc_t t0 = rdtsc();
        
        int result = mgas_gasnet_barrier_try(0, GASNET_BARRIERFLAG_ANONYMOUS);

        tsc_t t1 = rdtsc();

        count += 1;

        if (result == GASNET_OK)
            break;
        else if (result == GASNET_ERR_NOT_READY)
            ;
        else
            MGAS_NOT_REACHED;

        comm_poll();
        
        if (poll != NULL)
            poll();
    }
}

void mpi_barrier(void)
{
    mgas_native_barrier();
}

void mpi_broadcast(void *p, size_t size, mgas_proc_t root)
{
    mgas_native_broadcast(p, size, root);
}

void comm_barrier(void)
{
    comm_barrier_with_poll(NULL);
}

void comm_broadcast_with_poll(void *p, size_t size, mgas_proc_t root,
                              mgas_poll_t poll)
{
    int flags =
        GASNET_COLL_IN_NOSYNC | GASNET_COLL_OUT_NOSYNC | GASNET_COLL_LOCAL;
    gasnet_coll_handle_t handle =
        mgas_gasnet_coll_broadcast_nb(MGAS_GASNET_TEAM_ALL,
                                      p, (gasnet_image_t)root,
                                      p, size, flags);

    for (;;) {
        int result = mgas_gasnet_coll_try_sync(handle);

        if (result == GASNET_OK)
            break;
        else if (result == GASNET_ERR_NOT_READY)
            ;
        else
            MGAS_NOT_REACHED;

        comm_poll();

        if (poll != NULL)
            poll();
    }
}

void comm_broadcast(void *p, size_t size, mgas_proc_t root)
{
    comm_broadcast_with_poll(p, size, root, NULL);
}

void comm_gather_with_poll(void *dst, void *src, size_t size, mgas_proc_t root,
                           mgas_poll_t poll)
{
    int flags =
        GASNET_COLL_IN_NOSYNC | GASNET_COLL_OUT_NOSYNC | GASNET_COLL_LOCAL;
    gasnet_coll_handle_t handle =
        mgas_gasnet_coll_gather_nb(MGAS_GASNET_TEAM_ALL, (gasnet_image_t)root,
                                   dst, src, size, flags);

    for (;;) {
        int result = mgas_gasnet_coll_try_sync(handle);

        if (result == GASNET_OK)
            break;
        else if (result == GASNET_ERR_NOT_READY)
            ;
        else
            MGAS_NOT_REACHED;

        comm_poll();

        if (poll != NULL)
            poll();
    }
}

void comm_gather(void *dst, void *src, size_t size, mgas_proc_t root)
{
    comm_gather_with_poll(dst, src, size, root, NULL);
}

void comm_reduce_sum_long_with_poll(long *dst, long *src, size_t size,
                                   mgas_proc_t root, mgas_poll_t poll)
{
    int flags =
        GASNET_COLL_IN_NOSYNC | GASNET_COLL_OUT_NOSYNC | GASNET_COLL_LOCAL;
    gasnet_coll_handle_t handle =
        mgas_gasnet_coll_reduce_nb(MGAS_GASNET_TEAM_ALL, (gasnet_image_t)root,
                                   dst, src, 0, 0, sizeof(long), size,
                                   0, 0, flags);

    for (;;) {
        int result = mgas_gasnet_coll_try_sync(handle);

        if (result == GASNET_OK)
            break;
        else if (result == GASNET_ERR_NOT_READY)
            ;
        else
            MGAS_NOT_REACHED;

        comm_poll();

        if (poll != NULL)
            poll();
    }
}

void comm_reduce_sum_long(long *dst, long *src, size_t size, mgas_proc_t root)
{
    comm_reduce_sum_long_with_poll(dst, src, size, root, NULL);    
}

static void
reduce_long_fn(void *results, size_t result_count,
              const void *left_operands, size_t left_count,
              const void *right_operands,
              size_t elem_size, int flags, int arg)
{
    size_t i;

    long *res = (long *)results;
    long *src1 = (long *)left_operands;
    long *src2 = (long *)right_operands;
    MGAS_ASSERT(elem_size == sizeof(long));
    MGAS_ASSERT(result_count==left_count);

    switch(arg) {
    case 0:
        for(i = 0; i < result_count; i++) {
            res[i] = src1[i] + src2[i];
        }
        break;
    case 1:
        for(i = 0; i < result_count; i++) {
            res[i] = (src1[i] < src2[i]) ? src1[i] : src2[i];
        }
        break;
    case 2:
        for(i = 0; i < result_count; i++) {
            res[i] = (src1[i] > src2[i]) ? src1[i] : src2[i];
        }
        break;
    default:
        MGAS_UNDEFINED;
    }
}


//-- communication handle (a synchronization primitive) ------------------------

comm_handle_t *comm_handle_create(size_t count)
{
    mgas_proc_t me = globals_get_pid();

    comm_handle_t *handle = comm_malloc(sizeof(comm_handle_t));
    handle->count = int64_of_size(count);
    handle->count_ptr = &handle->count;
    handle->proc = me;

    return handle;
}

void comm_handle_destroy(comm_handle_t *handle)
{
    comm_free(handle);
}

void comm_handle_notify(const comm_handle_t *handle, size_t count)
{
    comm_atomic_add_i64(handle->count_ptr, handle->proc, -int64_of_size(count));
}

void comm_handle_wait(const comm_handle_t *handle)
{
    while (handle->count > 0)
        comm_poll();
}

mgas_proc_t comm_get_initiator(const mgas_am_t *am)
{
    gasnet_node_t pid;

#if MGAS_AM_MY_HANDLER
    pid = am->msg->initiator;
#else
    mgas_gasnet_AMGetMsgSource(*(gasnet_token_t *)am->token,
                               &pid);
#endif

    return (mgas_proc_t)pid;
}

mgas_bool_t comm_handler_processing(comm_handler_t *handler)
{
    return handler->processing;
}

void mgas_rma_get(void *dst, void *src, mgas_proc_t pid, size_t size)
{
    gasnet_get(dst, (gasnet_node_t)pid, src, size);
}
