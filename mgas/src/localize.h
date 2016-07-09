#ifndef LOCALIZE_H
#define LOCALIZE_H


typedef struct cachedir cachedir_t;

cachedir_t *cachedir_create(void);
void cachedir_destroy(cachedir_t *dir);


#endif
