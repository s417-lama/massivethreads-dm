#include "myth_alloc.h"

#include "../include/mgas_debug.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <assert.h>
#include <sys/mman.h>

#define PAGE_SIZE 4096


/** myth malloc ***************************************************************/
//一般化したfreelistの実装
//突っ込むデータはポインタの大きさより大きい必要あり

static void *myth_default_mmap(size_t len)
{
    void *ptr;
    ptr=mmap(NULL,PAGE_SIZE,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANON,-1,0);
    if (ptr==MAP_FAILED){
        fprintf(stderr,"mmap failed size:%lu\n",(unsigned long)len);
    }
    return ptr;
}


//freelistの実装
//flはvoid**
typedef void** myth_freelist_t;
#define myth_freelist_init(fl) \
{fl=NULL;}
#define myth_freelist_push(fl,data) \
{\
	void **tmp_ptr=(void**)data;\
	*tmp_ptr=(void*)fl;\
	fl=tmp_ptr;\
}
#define myth_freelist_pop(fl,ret) \
{\
	if (fl){\
		void **tmp;\
		tmp=fl;\
		ret=(void*)fl;\
		fl=*tmp;\
	}\
	else{ret=NULL;}\
}

//フリーリストは1,2,4,8,...2^(FREE_LIST_NUM-1)バイトの分のみ用意し、
//メモリ確保/解放は必ずこの粒度で行う
#define FREE_LIST_NUM 31

#if defined __i386__ || defined __arch32__
//size_tは32bit
#define MYTH_MALLOC_SIZE_TO_INDEX(s) (32-__builtin_clzl((s)-1))
#elif defined __x86_64__ || defined __arch64__
//size_tは64bit
#define MYTH_MALLOC_SIZE_TO_INDEX(s) (64-__builtin_clzl((s)-1))
#else
#error
#endif
#define MYTH_MALLOC_INDEX_TO_RSIZE(s) (1ULL<<(s))
#define MYTH_MALLOC_SIZE_TO_RSIZE(s) (MYTH_MALLOC_INDEX_TO_RSIZE(MYTH_MALLOC_SIZE_TO_INDEX(s)))


typedef struct myth_alloc {
    myth_freelist_t **freelist;
    size_t nthreads;
    myth_mmap_t mmap;
    myth_aligned_mmap_t aligned_mmap;
} myth_alloc;

static void myth_alloc_init_worker(myth_alloc_t alc,size_t rank)
{
	size_t i;
	//フリーリストを確保
//	assert(real_malloc);
	alc->freelist[rank]=malloc(sizeof(myth_freelist_t)*FREE_LIST_NUM);
	//初期化
	for (i=0;i<FREE_LIST_NUM;i++){myth_freelist_init(alc->freelist[rank][i]);}
}

myth_alloc_t myth_alloc_create(size_t nthreads,myth_mmap_t my_mmap,
                               myth_aligned_mmap_t aligned_mmap)
{
    size_t i;
    myth_alloc_t alc;
//	assert(real_malloc);
    alc=malloc(sizeof(myth_alloc));
	alc->freelist=malloc(sizeof(myth_freelist_t*)*nthreads);
    for (i=0;i<nthreads;i++){
        myth_alloc_init_worker(alc,i);
    }
    alc->nthreads=nthreads;
    alc->mmap=my_mmap;
    alc->aligned_mmap=aligned_mmap;

    MGAS_ASSERT((uint8_t *)alc->freelist >= (uint8_t *)0x4096);
    return alc;
}

static inline void myth_alloc_fini_worker(myth_alloc_t alc,size_t rank)
{
	//フリーリストの中身を解放
	/*for (i=0;i<FREE_LIST_NUM;i++){
		void **ptr=g_myth_freelist[i];

	}*/
	//配列を解放

	free(alc->freelist[rank]);
}

void myth_alloc_destroy(myth_alloc_t alc)
{
    size_t i;
    for (i=0;i<alc->nthreads;i++){
        myth_alloc_fini_worker(alc,i);
    }
    free(alc->freelist);
    free(alc);
}

void *myth_alloc_malloc(myth_alloc_t alc,size_t rank,size_t size)
{
	//内部アロケータ
	size_t realsize;
	int idx;
	void **ptr=NULL;

        MGAS_ASSERT((uint8_t *)alc->freelist >= (uint8_t *)0x4096);

	if (size<8)size=8;
	idx=MYTH_MALLOC_SIZE_TO_INDEX(size);
	myth_freelist_pop(alc->freelist[rank][idx],ptr);

	if (!ptr){
		//フリーリストにない。新しく確保
		realsize=MYTH_MALLOC_INDEX_TO_RSIZE(idx);
		if (realsize<PAGE_SIZE){
			assert(PAGE_SIZE%realsize==0);
			char *p,*p2;
			ptr=(void**)alc->mmap(PAGE_SIZE);

                        if (!ptr) fprintf(stderr, "global memory is full.\n");
                        MGAS_CHECK(ptr);

			p=(char*)ptr;
			p2=p+PAGE_SIZE;p+=realsize;
			while (p<p2){
				myth_freelist_push(alc->freelist[rank][idx],(void**)p);
				p+=realsize;
			}
		}
		else{
			//mmapで確保してそのまま返す
			assert(realsize%4096==0);

                        // FIX: aligned_malloc のために、
                        // size >= PAGE_SIZE のときのみ PAGE_SIZE でアライン
                        // (akiyama)
			ptr=(void**)alc->aligned_mmap(realsize, PAGE_SIZE);
                        MGAS_CHECK(ptr);
		}

	}
	return ptr;
}

static void myth_alloc_sanity_check_at_free(myth_alloc_t alc,size_t rank,size_t size,void *ptr)
{
	//ポインタが既にfreelist内に存在していたらエラー
	int idx;
	if (size<8)size=8;
	idx=MYTH_MALLOC_SIZE_TO_INDEX(size);
	myth_freelist_t tmp_freelist;
	myth_freelist_init(tmp_freelist);
	void *lptr;
	while (1){
		myth_freelist_pop(alc->freelist[rank][idx],lptr);
		if (lptr==NULL)break;
		assert(lptr!=ptr);
		myth_freelist_push(tmp_freelist,lptr);
	}
	while (1){
		myth_freelist_pop(tmp_freelist,lptr);
		if (lptr==NULL)break;
		assert(lptr!=ptr);
		myth_freelist_push(alc->freelist[rank][idx],lptr);
	}
}

void myth_alloc_free(myth_alloc_t alc,size_t rank,size_t size,void *ptr)
{
	//フリーリストに入れる
	int idx;

        MGAS_ASSERT((uint8_t *)alc->freelist >= (uint8_t *)0x4096);
        MGAS_ASSERT(ptr != NULL);

        if (size<8)size=8;
	idx=MYTH_MALLOC_SIZE_TO_INDEX(size);
	//fprintf(stderr,"F%d %d %p\n",rank,idx,ptr);
	myth_freelist_push(alc->freelist[rank][idx],ptr);
}

void *myth_alloc_realloc(myth_alloc_t alc,size_t rank,size_t oldsize,void *ptr,size_t size)
{
	void *ret;
	size_t cp_size;
	ret=myth_alloc_malloc(alc,rank,size);
	assert(ret);
	cp_size=(size<oldsize)?size:oldsize;
	memcpy(ret,ptr,cp_size);
	myth_alloc_free(alc,rank,oldsize,ptr);
	return ret;
}


void *myth_alloc_aligned_malloc(myth_alloc_t alc, size_t rank, size_t size,
                                size_t align)
{
    // FIXME: this is an instant implementation
    if (size < PAGE_SIZE)
        return alc->aligned_mmap(size, align);
    else
        return myth_alloc_malloc(alc, rank, size);
}

void myth_alloc_aligned_free(myth_alloc_t alc, size_t rank, size_t size, void *p)
{
    // FIXME: this is an instant implementation
    myth_alloc_free(alc, rank, size, p);
}
