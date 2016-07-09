#include "taskq.h"
#include "debug.h"
#include "uth_comm.h"

using namespace madi;

void local_taskque::initialize(size_t capacity) {
    // touch OS pages
    deque_.resize(capacity);
    deque_.clear();
}

void local_taskque::finalize() {
    deque_.resize(0);
}

global_taskque::global_taskque() :
    top_(0), base_(0),
    n_entries_(0), entries_(NULL),
    lock_(0)
{
}

global_taskque::~global_taskque()
{
}

void global_taskque::initialize(uth_comm& c, taskq_entry *entries,
                                size_t n_entries)
{
    MADI_CHECK(n_entries <= INT_MAX);
    MADI_CHECK(entries != NULL);

    base_ = (int)n_entries / 2;
    top_ = base_;
    n_entries_ = (int)n_entries;
    entries_ = entries;

}

void global_taskque::finalize(uth_comm& c)
{
    top_ = 0;
    base_ = 0;
    n_entries_ = 0;
    entries_ = NULL;
}

