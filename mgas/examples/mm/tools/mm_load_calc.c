
#define USE_MGAS 1
#include "../mm.h"

typedef struct {
    size_t flop;
    size_t n_leaves;
} result_t;

static result_t merge_result(result_t r0, result_t r1)
{
    result_t r = { r0.flop + r1.flop, r0.n_leaves + r1.n_leaves };
    return r;
}

static result_t mm_rec(const mm_t *mm, size_t key, size_t rest_depth,
                   size_t begin, size_t end)
{
    if (key < (begin >> rest_depth) || key > (end >> rest_depth)) {
        result_t r = { 0, 0 };
        return r;
    }

    ptr_t A = mm->A, B = mm->B, C = mm->C;
    size_t M = mm->M, N = mm->N, K = mm->K, ld = mm->ld;
    
    size_t elem_size = sizeof(elem_t);
    size_t stride = ld * elem_size;
    size_t countA[] = { M, K * elem_size };
    size_t countB[] = { K, N * elem_size };
    size_t countC[] = { M, N * elem_size };

    elem_t *a, *b, *c;
    mgas_handle_t h = MGAS_HANDLE_INIT;

    if (rest_depth == 0) {
        size_t flop = 2 * mm->M * mm->N * mm->K;

//        mm_print(mm);
//        printf("flop = %lf\n", (double)flop * 10e-6);

        result_t r = { flop, 1 };
        return r;
    } else {
        mm_t submm[2];
        divide_force(mm, submm);
        if (K == submm[0].K) {
            // divide with M or N
            result_t r0 = mm_rec(&submm[0], key * 2 + 0, rest_depth - 1,
                               begin, end);
            result_t r1 = mm_rec(&submm[1], key * 2 + 1, rest_depth - 1,
                               begin, end);
            return merge_result(r0, r1);
        } else {
            // divide with K
            result_t r0 = mm_rec(&submm[0], key, rest_depth, begin, end);
            result_t r1 = mm_rec(&submm[1], key, rest_depth, begin, end);
            return merge_result(r0, r1);
        }
    }
}

int main(int argc, char **argv)
{
    // parse arguments
    if (argc != 5) {
        fprintf(stderr, "Usage: %s N block_size leaf_size np\n", argv[0]);
        return 1;
    }
    size_t n = (size_t)atol(argv[1]);
    g_block_size = (size_t)atol(argv[2]);
    size_t leaf_size = (size_t)atol(argv[3]);
    size_t n_procs = (size_t)atol(argv[4]);
    
    // initialize
    mm_t mm = { 0, 0, 0, n, n, n, n, leaf_size };

    // divide computation into n_procs parts
    size_t max_depth = max_depth_of_C_tree(&mm, 0);
    size_t n_virtual_leaves = 0x1llu << max_depth;

    size_t me;
    for (me = 0; me < n_procs; me++) {
        size_t per_proc = n_virtual_leaves / n_procs;
        size_t mod = n_virtual_leaves % n_procs;
        size_t begin = (me * per_proc + (me < mod ? me : mod));
        size_t end = ((me + 1) * per_proc + (me + 1 < mod ? me + 1 : mod)) - 1;

        MM_DPUTS("begin = %zu, end = %zu", begin, end);

        result_t r = mm_rec(&mm, 0, max_depth, begin, end);

        printf("pid = %zu: %lf MFLOP, %zu leaves, depth = %zu\n",
               me, (double)r.flop * 1e-6, r.n_leaves, max_depth);
    }

    return 0;
}
