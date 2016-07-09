#include "gmt.h"

#include "../include/mgas_config.h"
#include "../include/mgas_debug.h"
#include "dist.h"
#include "sys.h"
#include "comm.h"
#include "misc.h"
#include "threadsafe.h"


typedef enum {
    PAGE_INVALID,
    PAGE_OWNED,
    PAGE_CACHED,
} page_state_t;

/*
  global memory table entry
 */
struct gmt_entry {
    // page info
    rwspinlock_t page_lock;
    void *page_addr;
    page_state_t state;

    // home info
    spinlock_t home_lock;
    mgas_proc_t owner;
    size_t block_size;
};


//-- global memory table entry -------------------------------------------------

gmt_entry_t *gmt_entry_create(void)
{
    gmt_entry_t *entry = (gmt_entry_t *)mgas_sys_malloc(sizeof(gmt_entry_t));

    entry->page_addr = NULL;
    entry->state = PAGE_INVALID;

    entry->owner = MGAS_INVALID_PID;
    entry->block_size = 0;

    rwspinlock_init(&entry->page_lock);
    spinlock_init(&entry->home_lock);
    
    return entry;
}

void gmt_entry_destroy(gmt_entry_t *entry)
{
    rwspinlock_destroy(&entry->page_lock);
    spinlock_destroy(&entry->home_lock);
    mgas_sys_free(entry);
}

void gmt_entry_reset(gmt_entry_t *entry, size_t block_size)
{
    MGAS_ASSERT(!gmt_entry_page_reading(entry));
    MGAS_ASSERT(!gmt_entry_page_writing(entry));
    MGAS_ASSERT(!spinlock_locked(&entry->home_lock));

    if (gmt_entry_page_valid(entry)) {
        void *page = gmt_entry_page_invalidate(entry);
        free_page(page);
    }
    
    MGAS_ASSERT(block_size > 0);
    entry->owner = MGAS_INVALID_PID;
    entry->block_size = block_size;
}

void gmt_entry_reset_and_touch(gmt_entry_t *entry, size_t block_size)
{
    mgas_proc_t me = globals_get_pid();

    if (entry->block_size != block_size)
        gmt_entry_reset(entry, block_size);

    if (!gmt_entry_page_valid(entry)) {
        void *page = allocate_page(block_size);
        gmt_entry_page_prepare(entry, page);
        gmt_entry_page_validate(entry);
    }
    
    entry->owner = me;
}

void gmt_entry_page_read_lock(gmt_entry_t *entry)
{
    rwspinlock_read_lock(&entry->page_lock);
}

mgas_bool_t gmt_entry_page_try_read_lock(gmt_entry_t *entry)
{
    return rwspinlock_try_read_lock(&entry->page_lock);
}

void gmt_entry_page_read_unlock(gmt_entry_t *entry)
{
    rwspinlock_read_unlock(&entry->page_lock);
}

void gmt_entry_page_write_lock(gmt_entry_t *entry)
{
    rwspinlock_write_lock(&entry->page_lock);
}

mgas_bool_t gmt_entry_page_try_write_lock(gmt_entry_t *entry)
{
    return rwspinlock_try_write_lock(&entry->page_lock);
}

void gmt_entry_page_write_unlock(gmt_entry_t *entry)
{
    rwspinlock_write_unlock(&entry->page_lock);
}

mgas_bool_t gmt_entry_page_reading(gmt_entry_t *entry)
{
    return rwspinlock_read_locked(&entry->page_lock);
}

mgas_bool_t gmt_entry_page_writing(gmt_entry_t *entry)
{
    return rwspinlock_write_locked(&entry->page_lock);
}

mgas_bool_t gmt_entry_page_valid(const gmt_entry_t *entry)
{
    return entry->state == PAGE_OWNED;
}

mgas_bool_t gmt_entry_page_invalid(const gmt_entry_t *entry)
{
    return entry->state == PAGE_INVALID;
}

mgas_bool_t gmt_entry_page_cached(const gmt_entry_t *entry)
{
    return entry->state == PAGE_CACHED;
}

void *gmt_entry_page_invalidate(gmt_entry_t *entry)
{
    MGAS_ASSERT(gmt_entry_page_valid(entry));

    void *page_addr = entry->page_addr;
    entry->page_addr = NULL;
    entry->state = PAGE_INVALID;

    return page_addr;
}

void gmt_entry_page_prepare(gmt_entry_t *entry, void *page_addr)
{
    MGAS_ASSERT(gmt_entry_page_invalid(entry));
    MGAS_ASSERT(entry->page_addr == NULL);

    entry->page_addr = page_addr;
}

void gmt_entry_page_validate(gmt_entry_t *entry)
{
    MGAS_ASSERT(gmt_entry_page_invalid(entry));
    MGAS_ASSERT(entry->page_addr != NULL);

    entry->state = PAGE_OWNED;
}

void *gmt_entry_get_block(const gmt_entry_t *entry)
{
    MGAS_ASSERT(gmt_entry_page_valid(entry));
    return entry->page_addr;
}

size_t gmt_entry_get_block_size(const gmt_entry_t *entry)
{
    MGAS_ASSERT(entry->block_size > 0);
    return entry->block_size;
}

void gmt_entry_get_owner(gmt_entry_t *entry, mgas_proc_t initiator,
                         mgas_proc_t *owner, size_t *block_size)
{
    spinlock_lock(&entry->home_lock);
    
    mgas_proc_t entry_owner = entry->owner;
    if (entry_owner == MGAS_INVALID_PID) {
        // first touch
        entry->owner = initiator;
    }

    *owner = entry_owner;
    *block_size = entry->block_size;

    spinlock_unlock(&entry->home_lock);
}

mgas_proc_t gmt_entry_get_raw_owner(gmt_entry_t *entry)
{
    return entry->owner;
}

void gmt_entry_begin_migration(gmt_entry_t *entry, mgas_proc_t initiator,
                               mgas_proc_t *owner, size_t *block_size)
{
    spinlock_lock(&entry->home_lock);

    mgas_proc_t entry_owner = entry->owner;
    if (entry_owner == MGAS_INVALID_PID) {
        // first touch
        entry->owner = initiator;
    } else if (entry_owner == MGAS_PID_MIGRATING) {
        // do nothing when migrating
    } else {
        // when not migrating
        entry->owner = MGAS_PID_MIGRATING;
    }

    *owner = entry_owner;
    *block_size = entry->block_size;

    spinlock_unlock(&entry->home_lock);
}

void gmt_entry_end_migration(gmt_entry_t *entry, mgas_proc_t owner)
{
    spinlock_lock(&entry->home_lock);

    MGAS_ASSERT(entry->owner == MGAS_PID_MIGRATING);

    entry->owner = owner;

    spinlock_unlock(&entry->home_lock);
}

