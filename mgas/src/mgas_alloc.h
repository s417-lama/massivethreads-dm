#ifndef MGAS_ALLOC_H
#define MGAS_ALLOC_H

typedef struct mgas_alloc mgas_alloc_t;

mgas_alloc_t *mgas_alloc_create(void);
void mgas_alloc_destroy(mgas_alloc_t *alloc);


#endif
