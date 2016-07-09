#ifndef MISC_H
#define MISC_H

#include "sys.h"
#include "../include/mgas_debug.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#pragma GCC diagnostic ignored "-Wredundant-decls"
void comm_poll(void);
#pragma GCC diagnostic warning "-Wredundant-decls"


//-- array ---------------------------------------------------------------------

typedef struct arrray_pair {
    void *array;
    size_t size;
} array_pair_t;

static void *array_create__(size_t elem_size, size_t n_elems)
{
    return mgas_sys_malloc(elem_size * n_elems);
}

static void array_destroy__(void *p)
{
    mgas_sys_free(p);
}

static void *array_copy__(size_t elem_size, const void *array, size_t n_elems)
{
    size_t size = elem_size * n_elems;
    void *new_array = mgas_sys_malloc(size);
    memcpy(new_array, array, size);
    return new_array;
}

#define array_create(type, size)                        \
    ((type *)array_create__(sizeof(type), (size)))
#define array_destroy(p) \
    array_destroy__(p)
#define array_copy(type, array, n_elems) \
    array_copy__(sizeof(type), (array), (n_elems))


//-- variable array ------------------------------------------------------------
typedef struct varray {
    size_t capacity;
    size_t elem_size;
    size_t n_elems;
    uint8_t *buf;
} varray_t;


static varray_t *varray_create(size_t elem_size, size_t capacity)
{
    uint8_t *buf = (uint8_t *)mgas_sys_malloc(elem_size * capacity);

    varray_t *va = (varray_t *)mgas_sys_malloc(sizeof(varray_t));
    va->capacity = capacity;
    va->elem_size = elem_size;
    va->n_elems = 0;
    va->buf = buf;

    return va;
}

static void varray_destroy(varray_t *va)
{
    mgas_sys_free(va->buf);
    mgas_sys_free(va);
}

static void varray_clear(varray_t *va)
{
    va->n_elems = 0;
}

static size_t varray_size(const varray_t *va)
{
    return va->n_elems;
}

static void varray_at(const varray_t *va, size_t i, void *buf)
{
    memcpy(buf, va->buf + va->elem_size * i, va->elem_size);
}

static void *varray_raw(const varray_t *va, size_t i)
{
    return (void *)(va->buf + va->elem_size * i);
}

static void varray_reserve(varray_t *va, size_t new_capacity)
{
    size_t elem_size = va->elem_size;
    size_t n_elems = va->n_elems;
    
    if (new_capacity >= va->capacity) {
        uint8_t *new_buf = mgas_sys_malloc(elem_size * new_capacity);
        memcpy(new_buf, va->buf, elem_size * n_elems);

        mgas_sys_free(va->buf);

        va->capacity = new_capacity;
        va->buf = new_buf;
    }
}

static void varray_extend(varray_t *va, size_t new_elems)
{
    if (new_elems >= va->capacity)
        varray_reserve(va, new_elems);

    va->n_elems = new_elems;
}

static void varray_add(varray_t *va, const void *value)
{
    size_t capacity = va->capacity;
    size_t elem_size = va->elem_size;
    size_t n_elems = va->n_elems;

    if (n_elems >= capacity)
        varray_reserve(va, capacity * 2);

    memcpy(va->buf + elem_size * n_elems, value, elem_size);
    va->n_elems += 1;
}

static void varray_remove(varray_t *va, size_t idx)
{
    size_t i;
    size_t elem_size = va->elem_size;
    size_t n_elems = va->n_elems;

    size_t n_rests = n_elems - idx - 1;
    if (n_rests > 0) {
        void *dst = va->buf + elem_size * idx;
        void *src = va->buf + elem_size * (idx + 1);
        memmove(dst, src, elem_size * n_rests);
    }

    va->n_elems -= 1;
}

static void varray_copy(varray_t *dst, varray_t *src, size_t n_elems)
{
    MGAS_ASSERT(dst->elem_size  == src->elem_size);
    MGAS_ASSERT(src->n_elems >= n_elems);

    // if (dst->capacity < dst->n_elems + n_elems) {
    //     MGAS_UNDEFINED; // extend dst
    // }

    // memcpy(dst->buf, src->buf, dst->elem_size * n_elems);

    size_t i;
    for (i = 0; i < n_elems; i++) {
        void *value = varray_raw(src, i);
        varray_add(dst, value);
    }
}

static varray_t *varray_duplicate(varray_t *src)
{
    varray_t *va = varray_create(src->elem_size, src->n_elems);
    varray_copy(va, src, src->n_elems);

    return va;
}

static void varray_sort(varray_t *va,
                        int (*compare)(const void *, const void *))
{
    void *array = va->buf;
    size_t n_elems = va->n_elems;
    size_t elem_size = va->elem_size;
    
    qsort(array, n_elems, elem_size, compare);
}

static void varray_unique(varray_t *va)
{
    size_t i;
    uint8_t *buf = va->buf;
    size_t elem_size = va->elem_size;
    size_t n_elems = va->n_elems;

    if (n_elems == 0)
        return;

    size_t count = 1;
    const uint8_t *prev = buf;
    for (i = 1; i < n_elems; i++) {
        const uint8_t *elem = buf + elem_size * i;
        if (memcmp(elem, prev, elem_size) != 0) {
            memcpy(buf + elem_size * count, elem, elem_size);
            prev = buf + elem_size * count;
            count += 1;
        }
    }

    va->n_elems = count;
}

static void test_varray(void)
{
    size_t i;

    size_t init_capacity = 32;
    size_t n_elems = 33;

    varray_t *va = varray_create(sizeof(int), init_capacity);
    for (i = 0; i < n_elems; i++) {
        int value = 1;
        varray_add(va, &value);
    }
    varray_destroy(va);

    DPUTS("OK");
}


//-- queue ---------------------------------------------------------------------

typedef struct queue_cell {
    struct queue_cell *next;
    void *value;
} queue_cell_t;

typedef struct queue {
    queue_cell_t *first;
    queue_cell_t *last;
} queue_t;


static queue_t *queue_create(void)
{
    queue_t *q = mgas_sys_malloc(sizeof(queue_t));
    q->first = NULL;
    q->last = NULL;
    return q;
}

static void queue_destroy(queue_t *q)
{
    queue_cell_t *cell = q->first;
    while (cell != NULL) {
        queue_cell_t *next = cell->next;
        mgas_sys_free(cell);
        cell = next;
    }
    mgas_sys_free(q);
}

static void queue_push(queue_t *q, void *value)
{
    queue_cell_t *cell = (queue_cell_t *)mgas_sys_malloc(sizeof(queue_cell_t));
    cell->next = NULL;
    cell->value = value;

    if (q->first == NULL)
        q->first = cell;

    if (q->last != NULL)
        q->last->next = cell;

    q->last = cell;
}

static mgas_bool_t queue_pop(queue_t *q, void **value)
{
    if (q->first == NULL) {
        return MGAS_FALSE;
    } else {
        MGAS_CHECK(q->last != NULL);

        queue_cell_t *first = q->first;
        q->first = first->next;
        if (first == q->last)
            q->last = NULL;

        *value = first->value;
        mgas_sys_free(first);

        return MGAS_TRUE;
    }
}

//-- singly linked list --------------------------------------------------------

typedef struct linkedlist_cell {
    struct linkedlist_cell *next;
    uint8_t data[1];
} linkedlist_cell_t;

typedef struct linkedlist {
    linkedlist_cell_t *head;
    size_t elem_size;
    size_t size;
} linkedlist_t;


static linkedlist_t *linkedlist_create(size_t elem_size)
{
    linkedlist_t *list = (linkedlist_t *)mgas_sys_malloc(sizeof(linkedlist_t));
    list->head = NULL;
    list->elem_size = elem_size;
    list->size = 0;

    return list;
}

static void linkedlist_destroy(linkedlist_t *list)
{
    linkedlist_cell_t *cell = list->head;
    while (cell != NULL) {
        linkedlist_cell_t *next = cell->next;
        mgas_sys_free(cell);
        cell = next;
    }

    mgas_sys_free(list);
}

static mgas_bool_t linkedlist_empty(const linkedlist_t *list)
{
    return list->size == 0;
}

static const linkedlist_cell_t *linkedlist_head(const linkedlist_t *list)
{
    return list->head;
}

static linkedlist_cell_t * linkedlist_cell_create(const void *data,
                                                  size_t elem_size,
                                                  linkedlist_cell_t *next)
{
    size_t cell_size = offsetof(linkedlist_cell_t, data) + elem_size;

    linkedlist_cell_t *cell = (linkedlist_cell_t *)mgas_sys_malloc(cell_size);
    cell->next = next;
    memcpy(cell->data, data, elem_size);

    return cell;
}

static void linkedlist_cell_destroy(linkedlist_cell_t *cell)
{
    mgas_sys_free(cell);
}

static void linkedlist_get(linkedlist_t *list, linkedlist_cell_t *cell,
                           void *data)
{
    memcpy(data, cell->data, list->elem_size);
}

static void linkedlist_push(linkedlist_t *list, const void *data)
{
    linkedlist_cell_t *cell =
        linkedlist_cell_create(data, list->elem_size, list->head);
    list->head = cell;
    list->size += 1;
}

static void linkedlist_pop(linkedlist_t *list, void *data)
{
    linkedlist_cell_t *cell = list->head;
    list->head = cell->next;
    list->size -= 1;
    memcpy(data, cell->data, list->elem_size);
}

static void linkedlist_insert(linkedlist_t *list, linkedlist_cell_t *prev,
                              void *data)
{
    MGAS_ASSERT(prev != NULL);

    linkedlist_cell_t *cell =
        linkedlist_cell_create(data, list->elem_size, prev->next);
    prev->next = cell;

    list->size += 1;
}

//-- ID pool -------------------------------------------------------------------

typedef struct idpool {
    size_t next_id;
    linkedlist_t *list;
} idpool_t;

static idpool_t *idpool_create(void)
{
    idpool_t *pool = (idpool_t *)mgas_sys_malloc(sizeof(idpool_t));
    pool->next_id = 0;
    pool->list = linkedlist_create(sizeof(size_t));

    return pool;
}

static void idpool_destroy(idpool_t *pool)
{
    linkedlist_destroy(pool->list);
    mgas_sys_free(pool);
}

static size_t idpool_get(idpool_t *pool)
{
    size_t id;
    if (linkedlist_empty(pool->list)) {
        id = pool->next_id;
        pool->next_id += 1;
    } else {
        linkedlist_pop(pool->list, &id);
    }

    return id;
}

static void idpool_put(idpool_t *pool, size_t id)
{
    if (id == pool->next_id - 1) {
        pool->next_id -= 1;
    } else {
        linkedlist_push(pool->list, &id);
    }
}

//-- integer set ---------------------------------------------------------------

typedef struct uint_set {
    mgas_bool_t *map;
    size_t max_value;
    size_t n_elems;
} uint_set_t;

static uint_set_t *uint_set_create(size_t max_value)
{
    size_t i;

    mgas_bool_t *map = array_create(mgas_bool_t, max_value + 1);
    for (i = 0; i < max_value; i++)
        map[i] = MGAS_FALSE;

    uint_set_t *set = (uint_set_t *)mgas_sys_malloc(sizeof(uint_set_t));
    set->map = map;
    set->max_value = max_value;
    set->n_elems = 0;

    return set;
}

static void uint_set_destroy(uint_set_t *set)
{
    array_destroy(set->map);
    mgas_sys_free(set);
}

static size_t uint_set_size(uint_set_t *set)
{
    return set->n_elems;
}

static mgas_bool_t uint_set_contains(uint_set_t *set, size_t i)
{
    MGAS_CHECK(i <= set->max_value);

    return set->map[i];
}

static void uint_set_add(uint_set_t *set, size_t i)
{
    MGAS_CHECK(i < set->max_value);

    if (!set->map[i]) {
        set->map[i] = MGAS_TRUE;
        set->n_elems += 1;
    }
}

static void uint_set_to_array(uint_set_t *set, size_t *array)
{
    size_t idx = 0;
    size_t i;
    for (i = 0; i <= set->max_value; i++) {
        if (uint_set_contains(set, i)) {
            array[idx] = i;
            idx += 1;
        }
    }
}

//-- data structure for data partitioning --------------------------------------

#if 1

typedef struct {
    size_t key;
    varray_t *values;
} part_pair_t;

typedef struct {
    varray_t *parts;
    size_t elem_size;
} partitions_t;

static partitions_t *partitions_create(size_t elem_size, size_t n_max_parts)
{
    partitions_t *ps = mgas_sys_malloc(sizeof(*ps));

    ps->parts = varray_create(sizeof(part_pair_t), 1024);
    ps->elem_size = elem_size;

    return ps;
}

static void partitions_destroy(partitions_t *ps)
{
    size_t i;

    const part_pair_t *pairs = varray_raw(ps->parts, 0);
    size_t n_elems = varray_size(ps->parts);

    for (i = 0; i < n_elems; i++)
        varray_destroy(pairs[i].values);

    varray_destroy(ps->parts);
}

static varray_t *partitions_find_values(const partitions_t *ps, size_t key)
{
    size_t i;

    part_pair_t *pairs = varray_raw(ps->parts, 0);
    size_t n_pairs = varray_size(ps->parts);

    for (i = 0; i < n_pairs; i++)
        if (pairs[i].key == key)
            return pairs[i].values;

    return NULL;
}

static void partitions_add(partitions_t *ps, size_t key, const void *value)
{
    varray_t *values = partitions_find_values(ps, key);

    if (values == NULL) {
        values = varray_create(ps->elem_size, 1024);

        part_pair_t pair = { key, values };
        varray_add(ps->parts, &pair);
    }

    varray_add(values, value);
}

static void *partitions_get(const partitions_t *ps, size_t key,
                            size_t *n_values)
{
    varray_t *values = partitions_find_values(ps, key);

    if (values == NULL) {
        *n_values = 0;
        return NULL;
    } else {
        *n_values = varray_size(values);
        return varray_raw(values, 0);
    }
}

static mgas_bool_t partitions_contains(const partitions_t *ps, size_t key)
{
    return partitions_find_values(ps, key) != NULL;
}

static size_t partitions_size(const partitions_t *ps)
{
    return varray_size(ps->parts);
}

static size_t partitions_max_size(const partitions_t *ps)
{
    return varray_size(ps->parts);
}

typedef struct partitions_iter {
    const partitions_t *ps;
    size_t i;
} partitions_iter_t;

static void partitions_iter_init(partitions_iter_t *iter,
                                 const partitions_t *ps)
{
    iter->ps = ps;
    iter->i = 0;
}

static void *partitions_iter_next(partitions_iter_t *iter, size_t *key,
                                  size_t *n_values)
{
    const partitions_t *ps = iter->ps;
    part_pair_t *parts = varray_raw(ps->parts,0);
    size_t n_parts = partitions_size(ps);

    size_t i = iter->i;
    if (i < n_parts) {
        iter->i = i + 1;

        *key = parts[i].key;
        *n_values = varray_size(parts[i].values);
        return varray_raw(parts[i].values, 0);
    } else {
        *n_values = 0;
        return NULL;
    }
}

#else

typedef struct partitions {
    varray_t **parts;
    size_t elem_size;
    size_t n_max_parts;
    size_t n_parts;
} partitions_t;

static partitions_t *partitions_create(size_t elem_size, size_t n_max_parts)
{
    size_t i;

    varray_t **parts = array_create(varray_t *, n_max_parts);
    for (i = 0; i < n_max_parts; i++)
        parts[i] = NULL;

    partitions_t *ps = mgas_sys_malloc(sizeof(partitions_t));
    ps->parts = parts;
    ps->elem_size = elem_size;
    ps->n_max_parts = n_max_parts;
    ps->n_parts = 0;

    return ps;
}

static void partitions_destroy(partitions_t *ps)
{
    size_t i;

    varray_t **parts = ps->parts;
    size_t n_max_parts = ps->n_max_parts;

    for (i = 0; i < n_max_parts; i++)
        if (parts[i] != NULL)
            varray_destroy(parts[i]);
    array_destroy(parts);
    mgas_sys_free(ps);
}

static void partitions_add(partitions_t *ps, size_t key, const void *value)
{
    MGAS_ASSERT(key < ps->n_max_parts);
    varray_t **parts = ps->parts;
    size_t elem_size = ps->elem_size;

    if (parts[key] == NULL) {
        parts[key] = varray_create(elem_size, 1024); // FIME: 1024
        ps->n_parts += 1;
    }

    varray_add(parts[key], value);
}

static void *partitions_get(const partitions_t *ps, size_t key,
                            size_t *n_values)
{
    varray_t **parts = ps->parts;
    size_t n_parts = ps->n_parts;

    if (parts[key]) {
        *n_values = varray_size(parts[key]);
        return varray_raw(parts[key], 0);
    } else {
        *n_values = 0;
        return NULL;
    }
}

static mgas_bool_t partitions_contains(const partitions_t *ps, size_t key)
{
    return ps->parts[key] != NULL;
}

static size_t partitions_size(const partitions_t *ps)
{
    return ps->n_parts;
}

static size_t partitions_max_size(const partitions_t *ps)
{
    return ps->n_max_parts;
}

typedef struct partitions_iter {
    const partitions_t *ps;
    size_t i;
} partitions_iter_t;

static void partitions_iter_init(partitions_iter_t *iter,
                                 const partitions_t *ps)
{
    iter->ps = ps;
    iter->i = 0;
}

static void *partitions_iter_next(partitions_iter_t *iter, size_t *key,
                                  size_t *n_values)
{
    size_t i;

    const partitions_t *ps = iter->ps;
    size_t n_max_parts = partitions_max_size(ps);

    for (i = iter->i; i < n_max_parts; i++) {
        void *values = partitions_get(ps, i, n_values);
        if (n_values > 0) {
            *key = i;
            iter->i = i + 1;
            return values;
        }
    }

    *n_values = 0;
    return NULL;
}

#endif

//-- join counter --------------------------------------------------------------

typedef struct join_counter {
    volatile size_t count;
} join_counter_t;

static void join_counter_init(join_counter_t *jc, size_t count)
{
    jc->count = count;
}

static void join_counter_notify(join_counter_t *jc, size_t count)
{
    __sync_fetch_and_add(&jc->count, -count);
}

static void join_counter_wait(const join_counter_t *jc)
{
    while (jc->count > 0)
        comm_poll();
    // FIXME: insert mgas_read_barrier();
}

//-- future --------------------------------------------------------------------

typedef struct future {
    volatile mgas_bool_t filled;
    void *value;
} future_t;

static void future_init(future_t *f)
{
    f->filled = MGAS_FALSE;
    f->value = NULL;
}

static void future_set(future_t *f, void *value)
{
    f->value = value;
    // FIXME: insert mgas_write_barrier();
    f->filled = MGAS_TRUE;
}

static void *future_get(const future_t *f, void *value)
{
    while (!f->filled)
        comm_poll();
    // FIXME: insert mgas_read_barrier();
    return f->value;
}

//-- future list ---------------------------------------------------------------

typedef struct future_list {
    uint8_t *values;
    size_t elem_size;
    join_counter_t jc;
} future_list_t;

static future_list_t *future_list_create(size_t elem_size, size_t n_elems)
{
    uint8_t *values = (uint8_t *)mgas_sys_malloc(elem_size * n_elems);

    future_list_t *f_list =
        (future_list_t *)mgas_sys_malloc(sizeof(future_list_t));
    f_list->values = values;
    f_list->elem_size = elem_size;
    join_counter_init(&f_list->jc, n_elems);

    return f_list;
}

static void future_list_destroy(future_list_t *f_list)
{
    mgas_sys_free(f_list->values);
    mgas_sys_free(f_list);
}

static void future_list_set(future_list_t *f_list, size_t idx, void *value)
{
    uint8_t *values = f_list->values;
    size_t elem_size = f_list->elem_size;
    join_counter_t *jc = &f_list->jc;

    memcpy(values + elem_size * idx, value, elem_size);
    join_counter_notify(jc, 1);
}

static void future_list_wait_all(const future_list_t *f_list)
{
    join_counter_wait(&f_list->jc);
}

static void *future_list_raw(const future_list_t *f_list)
{
    future_list_wait_all(f_list);

    return f_list->values;
}

//-- misc functions ------------------------------------------------------------

static size_t min_size(size_t x, size_t y)
{
    return x <= y ? x : y;
}

static size_t max_size(size_t x, size_t y)
{
    return x >= y ? x : y;
}

#endif
