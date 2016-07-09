
#define USE_MGAS 1

extern "C" {
#include <mgas.h>
#include <myth.h>
}
#include "mgas_util.hpp"

#include <cassert>
#include <cmath>
#include <sys/time.h>

#include <iostream>
#include <utility>
#include <algorithm>

#define nullptr NULL // workaround

inline double get_dtime() {
    timeval tv;
    ::gettimeofday(&tv, nullptr);
    return tv.tv_sec + tv.tv_usec*1e-6;
}

int sequential_threshold = 1 << 3;
int localize_threshold = 1 << 3;

typedef int key_type;

inline bool is_power_of_2(std::size_t x) {
    while (x % 2 == 0) x /= 2;
    return x == 1;
}

inline void bitonic_compare_sequential(bool asc, key_type* array_left, key_type* array_right, std::size_t length) {
    for (std::size_t i = 0; i < length; i++) {
        key_type left = array_left[i], right = array_right[i];
        if (asc == (left > right)) {
            array_left[i] = right;
            array_right[i] = left;
        }
    }
}

struct compare_args {
    bool asc;
    mgas::array_range<key_type> left;
    mgas::array_range<key_type> right;
};

void* bitonic_compare(void* ptr) {
    compare_args a = *reinterpret_cast<compare_args*>(ptr);
    mgas::handle h;
    
    const std::size_t length = a.left.size();
    assert(length == a.right.size());
    
    if (length == 0) return nullptr;
    
    if (length <= localize_threshold) {
        a.left.localize(h, MGAS_REUSE); a.right.localize(h, MGAS_REUSE);
    }
    
    if (length <= sequential_threshold) {
        //assert(array_left != NULL && array_right != NULL);
        bitonic_compare_sequential(a.asc, a.left.local(), a.right.local(), length);
    } else {
        const std::size_t half = length / 2;
        
        compare_args a1 = { a.asc, a.left.slice(0, half)     , a.right.slice(0, half)      };
        compare_args a2 = { a.asc, a.left.slice(half, length), a.right.slice(half, length) };
        
        myth_thread_t th1, th2;
        
        th1 = myth_create(bitonic_compare, &a1);
        th2 = myth_create(bitonic_compare, &a2);
        myth_join(th1, nullptr);
        myth_join(th2, nullptr);
    }
    
    if (length <= localize_threshold) {
        a.left.commit(); a.right.commit();
    }
    
    return nullptr;
}

struct merge_args {
    bool asc;
    mgas::array_range<key_type> array;
};

void* bitonic_merge(void* ptr) {
    merge_args a = *reinterpret_cast<merge_args*>(ptr);
    mgas::handle h;
    
    std::size_t length = a.array.size();
    
    if (length <= 1) return nullptr;
    assert(is_power_of_2(length));
    const std::size_t half = length / 2;
    
    if (length <= localize_threshold) {
        a.array.localize(h, MGAS_REUSE);
    }
    
    {
        compare_args ac = { a.asc, a.array.slice(0, half), a.array.slice(half, length) };
        bitonic_compare(&ac);
    }
    
    merge_args a1 = { a.asc, a.array.slice(0, half) };
    merge_args a2 = { a.asc, a.array.slice(half, length) };
    
    if (length <= sequential_threshold) {
        bitonic_merge(&a1);
        bitonic_merge(&a2);
    } else {
        myth_thread_t th1, th2;
        th1 = myth_create(bitonic_merge, &a1);
        th2 = myth_create(bitonic_merge, &a2);
        myth_join(th1, nullptr);
        myth_join(th2, nullptr);
    }
    
    if (length <= localize_threshold) {
        a.array.commit();
    }
    
    return nullptr;
}

struct sort_args {
    bool asc;
    mgas::array_range<key_type> array;
};

void* bitonic_sort_recursive(void* ptr) {
    sort_args a = *reinterpret_cast<sort_args*>(ptr);
    mgas::handle h;
    
    std::size_t length = a.array.size();
    
    if (length <= 1) return nullptr;
    assert(is_power_of_2(length));
    const std::size_t half = length / 2;
    
    if (length <= localize_threshold) {
        a.array.localize(h, MGAS_REUSE);
    }
    
    if (length <= sequential_threshold) {
        if (a.asc)
            std::sort(a.array.local(), a.array.local() + a.array.size());
        else 
            std::sort(a.array.local(), a.array.local() + a.array.size(), std::greater<key_type>());
    } else {
        sort_args a1 = { true , a.array.slice(0, half) };
        sort_args a2 = { false, a.array.slice(half, length) };
        
        myth_thread_t th1, th2;
        th1 = myth_create(bitonic_sort_recursive, &a1);
        th2 = myth_create(bitonic_sort_recursive, &a2);
        myth_join(th1, nullptr);
        myth_join(th2, nullptr);
        
        merge_args am = { a.asc, a.array.slice(0, length) };
        bitonic_merge(&am);
    }
    
    if (length <= localize_threshold) {
        a.array.commit();
    }
    
    return nullptr;
}

/*inline void bitonic_sort_local(key_type* array, std::size_t length) {
    sort_args a = { true, array, length};
    bitonic_sort_helper(&a);
}*/
inline void bitonic_sort_gas(mgasptr_t array, std::size_t length) {
    sort_args a = { true, mgas::array_range<key_type>(array, length) };
    bitonic_sort_recursive(&a);
}

inline void bench(int length, int count, double& time_bitonic, double& time_sequential)
{
    key_type* array = new key_type[length];
    key_type* array_answer = new key_type[length];
    
    //std::cout << nworkers << std::endl;
    
    // Set the threshold.
    localize_threshold = length / 128;
    sequential_threshold = length / 256;
    
    size_t n_blocks[] = {4};
    size_t block_size[] = {length * sizeof(key_type) / n_blocks[0]};
    mgasptr_t ptr = mgas_all_dmalloc(length * sizeof(key_type), 1, block_size, n_blocks);
    
    mgas::remote_iterator<key_type> first(ptr), last = first + length;
    
    for (int i = 0; i < count; i++) {
        for (int pos = 0; pos < length; pos++)
            first[pos] = array_answer[pos] = std::rand();
        
        {
            const double start_time = get_dtime();
            bitonic_sort_gas(ptr, length);
            const double end_time = get_dtime();
            
            time_bitonic += end_time - start_time;
        }
        
        {
            const double start_time = get_dtime();
            std::sort(array_answer, array_answer + length);
            const double end_time = get_dtime();
            
            time_sequential += end_time - start_time;
        }
        
        bool is_ok = std::equal(first, last, array_answer);
        if (!is_ok)
            std::cerr << "ERROR" << std::endl;
    }
    
    delete[] array_answer;
    
    time_bitonic /= count;
    time_sequential /= count;
}

int main(int argc, char* argv[])
{
    myth_init();
    
    int n_workers = myth_get_num_workers();
    printf("n_workers = %d\n", n_workers);
    mgas_initialize_with_threads(&argc, &argv, n_workers);
    
    for (int i = 15; i <= 30; i++) {
        //int i = 24;
        const int n = 1 << i, count = 2;
        double time_bitonic, time_sequential;
        bench(n, count, time_bitonic, time_sequential);
        std::cout << n << "\t" << time_bitonic << "\t" << time_sequential << "\t" << count << std::endl;
    }
    
    mgas_finalize();
    myth_fini();
    
    return 0;
}

