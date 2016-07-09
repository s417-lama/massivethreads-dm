
#pragma once

#ifndef USE_MGAS
    #define USE_MGAS 1
#endif

#include <cassert>
#include <utility>

#include <mgas.h>

#include <iterator>

#define MGAS_NULL   (static_cast<mgasptr_t>(0))

namespace mgas {

class handle
{
public:
    handle() : impl_(MGAS_HANDLE_INIT) { }
    ~handle() { mgas_unlocalize(&impl_); }
    
    operator mgas_handle_t*() {
        return &impl_;
    }
    
private:
    handle(const handle&);
    handle& operator= (const handle&);
    
    mgas_handle_t impl_;
};

template <typename T>
class array_range
{
public:
    array_range() : remote_(MGAS_NULL), local_(NULL), size_(0) { }
    array_range(mgasptr_t remote, size_t size) : remote_(remote), local_(NULL), size_(size) { }
    array_range(T* local, size_t size) : remote_(MGAS_NULL), local_(local), size_(size) { }
    
    mgasptr_t remote() const { return remote_; }
    T* local() const { return local_; }
    size_t size() const { return size_; }
    
    array_range& localize(mgas_handle_t* handle, int/*mgas_flag_t*/ flag) {
        if (remote_ != MGAS_NULL) {
            if (!(local_ && flag == static_cast<int>(MGAS_RO))) {
                local_ = static_cast<T*>(mgas_localize(remote_, size_*sizeof(T), static_cast<mgas_flag_t>(flag), handle));
            }
        }
        return *this;
    }
    array_range& commit() {
        if (remote_ != MGAS_NULL) {
            mgas_commit(remote_, local_, sizeof(T)*size_);
        }
        return *this;
    }
    
    array_range slice(size_t start, size_t end) {
        assert(start < end && end <= size_);
        return array_range(remote_ != MGAS_NULL ? remote_ + sizeof(T)*start : MGAS_NULL,
                           local_ ? local_ + start : NULL,
                           end - start);
    }
    
private:
    array_range(mgasptr_t remote, T* local, size_t size) : remote_(remote), local_(local), size_(size) { }
    
    mgasptr_t remote_;
    T* local_;
    size_t size_;
};

template <typename T>
class remote_iterator : public std::iterator<std::input_iterator_tag, T>
{
    //static_assert(is_pod<T>::value);
    
    class dereference {
    public:
        explicit dereference(mgasptr_t ptr) : ptr_(ptr) { }
        
        operator T () const {
            T val;
            mgas_get(&val, ptr_, sizeof(T));
            return val;
        }
        const dereference& operator = (const T& val) const {
            // TODO: remove const_cast
            mgas_put(ptr_, const_cast<T*>(&val), sizeof(T));
            return *this;
        }
        
    private:
        mgasptr_t ptr_;
    };
    
public:
    remote_iterator() : ptr_(MGAS_NULL) { }
    explicit remote_iterator(mgasptr_t ptr) : ptr_(ptr) { }
    
    template <typename U>
    remote_iterator(remote_iterator<U> itr) : ptr_(itr.ptr_) {
        *this = itr;
    }
    
    mgasptr_t remote() const { return ptr_; }
    
    dereference operator* () const { return dereference(ptr_); }
    
    dereference operator[] (std::size_t index) const {
        return *(*this + index);
    }
    
    template <typename U>
    remote_iterator& operator = (const remote_iterator<U>& itr) {
        ptr_ = itr.ptr_;
        
        //static_assert(is_convertible<U*, T*>::value);
        T* check = static_cast<U*>(NULL);
        
        return *this;
    }
    
    remote_iterator& operator += (ptrdiff_t diff) {
        ptr_ += diff * sizeof(T);
        return *this;
    }
    
    bool operator == (const remote_iterator& itr) const {
        return ptr_ == itr.ptr_;
    }
    
private:
    mgasptr_t ptr_;
};

template <typename T>
inline remote_iterator<T> operator -= (remote_iterator<T>& ptr, ptrdiff_t diff) {
    return ptr += -diff;
}
template <typename T>
inline remote_iterator<T> operator + (const remote_iterator<T>& ptr, ptrdiff_t diff) {
    remote_iterator<T> result(ptr);
    return result += diff;
}
template <typename T>
inline remote_iterator<T> operator + (ptrdiff_t diff, const remote_iterator<T>& ptr) {
    remote_iterator<T> result(ptr);
    return result += diff;
}
template <typename T>
inline remote_iterator<T> operator - (const remote_iterator<T>& ptr, ptrdiff_t diff) {
    remote_iterator<T> result(ptr);
    return result -= diff;
}

template <typename T>
inline bool operator != (const remote_iterator<T>& a, const remote_iterator<T>& b) {
    return !(a == b);
}

template <typename T>
inline remote_iterator<T>& operator ++ (remote_iterator<T>& itr) {
    return itr += 1;
}
template <typename T>
inline remote_iterator<T>& operator -- (remote_iterator<T>& itr) {
    return itr -= 1;
}
template <typename T>
inline remote_iterator<T>& operator ++ (remote_iterator<T>& itr, int) {
    remote_iterator<T> result(itr);
    ++itr;
    return result;
}
template <typename T>
inline remote_iterator<T>& operator -- (remote_iterator<T>& itr, int) {
    remote_iterator<T> result(itr);
    --itr;
    return result;
}

/*

template <typename T>
class pointer
{
public:
    pointer() : local_(NULL), remote_(NULL) { }
    pointer(const pointer& p) : local_(p.local_), remote_(p.remote_) { } // default
    
    pointer(T* local) : local_(local), remote_(NULL) { }
    explicit pointer(mgasptr_t remote) : local_(NULL), remote_(remote) { }
    
    T* local() const { return local_; }
    mgasptr_t remote() const { return remote_; }
    
    pointer& localize(size_t size, mgas_handle_t& handle, mgas_flag_t flag = MGAS_REUSE) {
        local_ = static_cast<T*>(remote_, sizeof(T)*size, flag, &handle);
        return *this;
    }
    pointer& commit(size_t size, mgas_handle_t& handle, 
    
    template <typename U>
    pointer& operator = (const pointer<U>& p) {
        local_ = p.local_;
        remote_ = p.remote_;
        return *this;
    }
    
    pointer& operator += (ptrdiff_t diff) {
        local_ += diff;
        remote_ += diff * sizeof(T);
        return *this;
    }
    
private:
    T* local_;
    mgasptr_t remote_;
};

template <typename T>
inline pointer<T> operator -= (pointer<T>& ptr, ptrdiff_t diff) {
    return ptr += -diff;
}
template <typename T>
inline pointer<T> operator + (const pointer<T>& ptr, ptrdiff_t diff) {
    pointer<T> result(ptr);
    return result += diff;
}
template <typename T>
inline pointer<T> operator + (ptrdiff_t diff, const pointer<T>& ptr) {
    pointer<T> result(ptr);
    return result += diff;
}
template <typename T>
inline pointer<T> operator - (const pointer<T>& ptr, ptrdiff_t diff) {
    pointer<T> result(ptr);
    return result -= diff;
}

*/

}

