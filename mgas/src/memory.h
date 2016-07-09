#ifndef MGAS_MEMORY_H
#define MGAS_MEMORY_H

#include "gmt.h"
#include "gmt_entry.h"
#include "comm.h"
#include "misc.h"

#include "dist.h" // FIXME


static void validate_first_touch_page(gmt_entry_t *entry, size_t block_size)
{
    uint8_t *page = allocate_page(block_size);
    gmt_entry_page_prepare(entry, page);
    gmt_entry_page_validate(entry);
}


#endif
