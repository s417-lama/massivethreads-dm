#ifndef MADI_CONTEXT_X86_64_H
#define MADI_CONTEXT_X86_64_H

#include "../uth_config.h"
#include "../debug.h"
#include <cstddef>
#include <cstdint>
#include <cstdio>

namespace madi {

#if   MADI_OS_TYPE == MADI_OS_LINUX

#define MADI_IP_MIN             ((uint8_t *)0x400000)
#define MADI_IP_MAX             ((uint8_t *)0x1000000)

#ifdef PIC
#define MADI_FUNC(name)         #name "@PLT"
#else
#define MADI_FUNC(name)         #name
#endif

#elif MADI_OS_TYPE == MADI_OS_DARWIN

#define MADI_IP_MIN             ((uint8_t *)0x100000000)
#define MADI_IP_MAX             ((uint8_t *)0x200000000)
#define MADI_FUNC(name)         "_" #name

#else
#error ""
#endif


struct context {
    void *rip;
    void *rsp;
    void *rbp;
    void *rbx;
    void *r12;
    void *r13;
    void *r14;
    void *r15;
    context *parent;

    uint8_t *instr_ptr() const {
        return reinterpret_cast<uint8_t *>(rip);
    }
    
    uint8_t *stack_ptr() const {
        return reinterpret_cast<uint8_t *>(rsp);
    }
    
    uint8_t *top_ptr() const {
        return reinterpret_cast<uint8_t *>(rsp); // - 128; // red zone
    }

    size_t stack_size() const {
        size_t size = (uint8_t *)parent + sizeof(context) - top_ptr();

        MADI_ASSERTP1(0 < size && size < 1024 * 1024, size);

        return size;
    }

    void print() const {
        MADI_DPUTS("rip = %p", rip);
        MADI_DPUTS("rsp = %p", rsp);
        MADI_DPUTS("rbp = %p", rbp);
        MADI_DPUTS("rbx = %p", rbx);
        MADI_DPUTS("r12 = %p", r12);
        MADI_DPUTS("r13 = %p", r13);
        MADI_DPUTS("r14 = %p", r14);
        MADI_DPUTS("r15 = %p", r15);
        MADI_DPUTS("parent = %p", parent);
    }
};

#define MADI_CONTEXT_PRINT(level, ctx) \
    do { \
        MADI_DPUTS##level("(" #ctx ")         = %p", (ctx)); \
        MADI_DPUTS##level("(" #ctx ")->rip    = %p", (ctx)->rip); \
        MADI_DPUTS##level("(" #ctx ")->rsp    = %p", (ctx)->rsp); \
        MADI_DPUTS##level("(" #ctx ")->rbp    = %p", (ctx)->rbp); \
        MADI_DPUTS##level("(" #ctx ")->rbx    = %p", (ctx)->rbx); \
        MADI_DPUTS##level("(" #ctx ")->r12    = %p", (ctx)->r12); \
        MADI_DPUTS##level("(" #ctx ")->r13    = %p", (ctx)->r13); \
        MADI_DPUTS##level("(" #ctx ")->r14    = %p", (ctx)->r14); \
        MADI_DPUTS##level("(" #ctx ")->r15    = %p", (ctx)->r15); \
        MADI_DPUTS##level("(" #ctx ")->parent = %p", (ctx)->parent); \
    } while (false)


struct saved_context {
    bool is_main_task;
    void *ip;
    void *sp;
    context *ctx;
    uint8_t *stack_top;
    size_t stack_size;
    uint8_t partial_stack[1];
};

#define MADI_SCONTEXT_ASSERT(sctx_ptr) \
    do { \
        MADI_UNUSED saved_context *sctx__ = (sctx_ptr); \
        MADI_IP_ASSERT(sctx__->ip); \
        if (!sctx__->is_main_task) { \
            MADI_SP_ASSERT(sctx__->sp); \
            MADI_SP_ASSERT(sctx__->ctx); \
            MADI_SP_ASSERT(sctx__->stack_top); \
            MADI_SP_ASSERT(sctx__->stack_top + sctx__->stack_size); \
        } \
    } while (false)

#define MADI_SCONTEXT_PRINT(level, sctx_ptr) \
    do { \
        MADI_UNUSED saved_context *sctx__ = (sctx_ptr); \
        MADI_DPUTS##level("(" #sctx_ptr ")->is_main_task = %d", \
                          sctx__->is_main_task); \
        MADI_DPUTS##level("(" #sctx_ptr ")->ip           = %p", \
                          sctx__->ip); \
        MADI_DPUTS##level("(" #sctx_ptr ")->sp           = %p", \
                          sctx__->sp); \
        MADI_DPUTS##level("(" #sctx_ptr ")->ctx          = %p", \
                          sctx__->ctx); \
        MADI_DPUTS##level("(" #sctx_ptr ")->stack_top    = %p", \
                          sctx__->stack_top); \
        MADI_DPUTS##level("(" #sctx_ptr ")->stack_size   = %zu", \
                          sctx__->stack_size); \
    } while (false)

#define MADI_GET_CURRENT_SP(ptr_sp)                             \
    do {                                                        \
        uint8_t *sp__ = NULL;                                   \
        __asm__ volatile("mov %%rsp,%0\n" : "=r"(sp__));        \
        *(ptr_sp) = sp__;                                       \
    } while (false)

#define MADI_GET_CURRENT_STACK_TOP(ptr_sp)                      \
    do {                                                        \
        MADI_GET_CURRENT_SP(ptr_sp);                            \
        *(uint8_t **)ptr_sp += 128;                             \
    } while (false)

#if 1

#define MADI_CLOBBERS \
    "%rax", "%rdx", "%rsi", "%rdi", "%r8", "%r9", "%r10", "%r11", \
    "cc", "memory"

#if 0
/*
 * FIXME: first pass で red zone を書きつぶした後に resume し得るが問題ないか？
 *        (setjmp/longjmp では setjmp が関数呼び出しとして実装されているため、
 *        この問題は発生しない)
 */
#define MADI_SAVE_CONTEXT(ctx_ptr, parent_ctx_ptr, first_ptr)           \
    do {                                                                \
        char first__;                                                   \
        asm volatile (                                                  \
            "lea 1f(%%rip), %%rax\n\t"                                  \
            "mov %%rax,   (%%rcx)\n\t"                                  \
            "mov %%rsp,  8(%%rcx)\n\t"                                  \
            "mov %%rbp, 16(%%rcx)\n\t"                                  \
            "mov %%rbx, 24(%%rcx)\n\t"                                  \
            "mov %%r12, 32(%%rcx)\n\t"                                  \
            "mov %%r13, 40(%%rcx)\n\t"                                  \
            "mov %%r14, 48(%%rcx)\n\t"                                  \
            "mov %%r15, 56(%%rcx)\n\t"                                  \
            "movb $1, %0\n\t"                                           \
            "jmp 2f\n\t"                                                \
            "1:\n\t"                                                    \
            "mov 16(%%rcx), %%rbp\n\t"                                  \
            "mov 24(%%rcx), %%rbx\n\t"                                  \
            "mov 32(%%rcx), %%r12\n\t"                                  \
            "mov 40(%%rcx), %%r13\n\t"                                  \
            "mov 48(%%rcx), %%r14\n\t"                                  \
            "mov 56(%%rcx), %%r15\n\t"                                  \
            "movb $0, %0\n\t"                                           \
            "2:\n\t"                                                    \
            : "=&g"(first__)                                            \
            : "c"(ctx_ptr)                                              \
            : MADI_CLOBBERS);                                           \
        *(first_ptr) = first__;                                         \
                                                                        \
        if (first__)                                                    \
            (ctx_ptr)->parent = (parent_ctx_ptr);                       \
    } while (false)
#endif

#define MADI_SAVE_CONTEXT_WITH_CALL__(parent_ctx_ptr, f, arg0, arg1)    \
    do {                                                                \
        asm volatile (                                                  \
            /* save red zone */                                         \
            "sub  $128, %%rsp\n\t"                                      \
            /* 16-byte sp alignment for SIMD registers */               \
            "mov  %%rsp, %%rax\n\t"                                     \
            "and  $0xFFFFFFFFFFFFFFF0, %%rsp\n\t"                       \
            "push %%rax\n\t"                                            \
            /* parent field of context */                               \
            "push %0\n\t"                                               \
            /* push callee-save registers */                            \
            "push %%r15\n\t"                                            \
            "push %%r14\n\t"                                            \
            "push %%r13\n\t"                                            \
            "push %%r12\n\t"                                            \
            "push %%rbx\n\t"                                            \
            "push %%rbp\n\t"                                            \
            /* sp */                                                    \
            "lea  -16(%%rsp), %%rax\n\t"                                \
            "push %%rax\n\t"                                            \
            /* ip */                                                    \
            "lea  1f(%%rip), %%rax\n\t"                                 \
            "push %%rax\n\t"                                            \
                                                                        \
            /* call function */                                         \
            "mov  %%rsp, %%rdi\n\t"                                     \
            "call *%1\n\t"                                              \
                                                                        \
            /* pop ip from stack */                                     \
            "add $8, %%rsp\n\t"                                         \
                                                                        \
            "1:\n\t" /* ip is popped with ret operation at resume */    \
                                                                        \
            /* pop sp */                                                \
            "add $8, %%rsp\n\t"                                         \
            /* pop callee-save registers */                             \
            "pop %%rbp\n\t"                                             \
            "pop %%rbx\n\t"                                             \
            "pop %%r12\n\t"                                             \
            "pop %%r13\n\t"                                             \
            "pop %%r14\n\t"                                             \
            "pop %%r15\n\t"                                             \
            /* parent field of context */                               \
            "add $8, %%rsp\n\t"                                         \
            /* revert sp alignmment */                                  \
            "pop %%rsp\n\t"                                             \
            /* restore red zone */                                      \
            "add $128, %%rsp\n\t"                                       \
            :                                                           \
            : "r"(parent_ctx_ptr), "r"(f), "S"(arg0), "d"(arg1)         \
            : "%rax", "%rdi",                                           \
              "%r8", "%r9", "%r10", "%r11",                             \
              "cc", "memory");                                          \
    } while (false)

    namespace {
        // FIXME: quick hack bacause direct use of
        // MADI_SAVE_CONTEXT_WITH_CALL__ occurs SEGV in optimization build.
        void MADI_SAVE_CONTEXT_WITH_CALL(context *ctx,
                                     void (*fp)(context *, void *, void *),
                                     void *arg0, void *arg1) MADI_NOINLINE MADI_UNUSED;

        void MADI_SAVE_CONTEXT_WITH_CALL(context *ctx,
                                         void (*fp)(context *, void *, void *),
                                         void *arg0, void *arg1)
        {
            MADI_SAVE_CONTEXT_WITH_CALL__(ctx, fp, arg0, arg1);
        }
    }

#define MADI_RESUME_CONTEXT(ctx) \
    do { \
        asm volatile ( \
            "mov %0, %%rsp\n\t" \
            "ret\n\t" \
            : \
            : "g"(ctx) \
            : MADI_CLOBBERS); \
    } while (false)

#define MADI_EXECUTE_ON_STACK(f, arg0, arg1, arg2, arg3, stack_ptr)     \
    do {                                                                \
        uint8_t *rsp__;                                                 \
        MADI_GET_CURRENT_SP(&rsp__);                                    \
                                                                        \
        uint8_t *stack__ = (uint8_t *)(stack_ptr);                      \
        uint8_t *top__ = rsp__ - 128;                                   \
        uint8_t *smaller_top__ = (top__ < stack__) ? top__ : stack__;   \
        asm volatile (                                                  \
            "mov %0, %%rsp\n\t"                                         \
            "call " MADI_FUNC(f) "\n\t"                                 \
            :                                                           \
            : "g"(smaller_top__),                                       \
              "D"(arg0), "S"(arg1), "d"(arg2), "c"(arg3)                \
            :);                                                         \
    } while (false)

#define MADI_CALL_ON_NEW_STACK(stack, stack_size,                       \
                               f, arg0, arg1, arg2, arg3)               \
    do {                                                                \
        /* calculate an initial stack pointer */                        \
        /* on the iso-address stack */                                  \
        uintptr_t stackintptr = (uintptr_t)(stack);                     \
        stackintptr = stackintptr + (stack_size);                       \
        stackintptr &= 0xFFFFFFFFFFFFFFF0;                              \
        uint8_t *stack_ptr = (uint8_t *)(stackintptr);                  \
                                                                        \
       asm volatile (                                                   \
            "mov %%rsp, %%rax\n\t"                                      \
            "mov %0, %%rsp\n\t"                                         \
            /* alignment for xmm register accesses */                   \
            "sub $0x8, %%rsp\n\t"                                       \
            "push %%rax\n\t"                                            \
            "call *%1\n\t"                                              \
            "pop %%rsp\n\t"                                             \
            :                                                           \
            : "g"(stack_ptr), "r"(f),                                   \
              "D"(arg0), "S"(arg1), "d"(arg2), "c"(arg3)                \
            : "%rax", "%r8", "%r9", "%r10", "%r11",                     \
              "cc", "memory");                                          \
    } while (false)

#endif

}

#endif
