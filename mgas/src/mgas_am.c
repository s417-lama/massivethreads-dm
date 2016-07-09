#include "../include/mgas_am.h"
#include "../include/mgas_debug.h"
#include "handlers.h"
#include "comm.h"

#include "gasnet_ext.h"

void mgas_am_process_request(const msg_t *msg, mgas_am_func_t f,
                             const void *p, size_t size)
{
    mgas_am_t am = { msg, NULL };
    f(&am, p, size);
}

void mgas_am_request(mgas_am_func_t f, const void *p, size_t size,
                     mgas_proc_t target)
{
    comm_request_active_message(f, p, size, target);
}

void mgas_am_reply(mgas_am_func_t f, const void *p, size_t size,
                   const mgas_am_t *am)
{
    comm_reply_active_message(am, f, p, size);
}

mgas_proc_t mgas_am_get_initiator(const mgas_am_t *am)
{
    return comm_get_initiator(am);
}

mgas_am_handle_t *mgas_am_handle_create(size_t count)
{
    return (mgas_am_handle_t *)comm_handle_create(count);
}

void mgas_am_handle_destroy(mgas_am_handle_t *handle)
{
    comm_handle_destroy((comm_handle_t *)handle);
}

void mgas_am_handle_notify(mgas_am_handle_t *handle, size_t count)
{
    comm_handle_notify((comm_handle_t *)handle, count);
}

void mgas_am_handle_wait(mgas_am_handle_t *handle)
{
    comm_handle_wait((comm_handle_t *)handle);
}
