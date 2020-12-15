/**
 * @file   tm.c
 * @author Vincent Tournier
 *
 * @section LICENSE
 *
 * Hello
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
**/

// Requested features
#define _POSIX_C_SOURCE   200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

// External headers

// Internal headers
#include <tm.hpp>

// -------------------------------------------------------------------------- //

/** Define a proposition as likely true.
 * @param prop Proposition
**/
#undef likely
#ifdef __GNUC__
    #define likely(prop) \
        __builtin_expect((prop) ? 1 : 0, 1)
#else
    #define likely(prop) \
        (prop)
#endif

/** Define a proposition as likely false.
 * @param prop Proposition
**/
#undef unlikely
#ifdef __GNUC__
    #define unlikely(prop) \
        __builtin_expect((prop) ? 1 : 0, 0)
#else
    #define unlikely(prop) \
        (prop)
#endif

/** Define one or several attributes.
 * @param type... Attribute names
**/
#undef as
#ifdef __GNUC__
    #define as(type...) \
        __attribute__((type))
#else
    #define as(type...)
    #warning This compiler has no support for GCC attributes
#endif

// -------------------------------------------------------------------------- //
typedef struct Segment {
    size_t size;
    unsigned int start;

    Segment(size_t sz, unsigned int st) {
        start = st;
        size = sz;
    }
};

typedef struct Region {
    char* memory[2];
    bool* accessed;
    unsigned int* accessed_id;
    size_t alignment;
    vector<Segment*> segments;
    mutex vector_mutex;

    Region(size_t align) {
        memory[0] = new char[REGION_BYTE_SIZE];
        memory[1] = new char[REGION_BYTE_SIZE];
        accessed = new bool[REGION_BYTE_SIZE];
        accessed_id = new unsigned int[REGION_BYTE_SIZE];
        alignment = align;
        segments = vector<Segment*>(1, nullptr);
    }
};

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size as(unused), size_t align as(unused)) noexcept {
    Region* region = new Region(align);
    if(region == nullptr)
        return invalid_shared;

    region->segments[0] = new Segment(size, 0);

    if(region->segments[0] == nullptr)
        return invalid_shared;

    for(size_t i = 0 ; i < size ; i++) {
        region->memory[0][i] = 0;
        region->memory[1][i] = 0;
        region->accessed[i] = false;
        region->accessed_id[i] = 0;
    }

    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared as(unused)) noexcept {
    Region* region = (Region*) shared;
    for(Segment* segment : region->segments)
        delete segment;
    region->segments.clear();
    delete[] region->memory[0];
    delete[] region->memory[1];
    delete[] region->accessed;
    delete[] region->accessed_id;
    delete shared;
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared as(unused)) noexcept {
    // TODO: tm_start(shared_t)
    return NULL;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared as(unused)) noexcept {
    // TODO: tm_size(shared_t)
    return 0;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared as(unused)) noexcept {
    // TODO: tm_align(shared_t)
    return 0;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared as(unused), bool is_ro as(unused)) noexcept {
    // TODO: tm_begin(shared_t)
    return invalid_tx;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared as(unused), tx_t tx as(unused)) noexcept {
    // TODO: tm_end(shared_t, tx_t)
    return false;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared as(unused), tx_t tx as(unused), void const* source as(unused), size_t size as(unused), void* target as(unused)) noexcept {
    // TODO: tm_read(shared_t, tx_t, void const*, size_t, void*)
    return false;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared as(unused), tx_t tx as(unused), void const* source as(unused), size_t size as(unused), void* target as(unused)) noexcept {
    // TODO: tm_write(shared_t, tx_t, void const*, size_t, void*)
    return false;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
Alloc tm_alloc(shared_t shared, tx_t tx as(unused), size_t size, void** target) noexcept {
    Region* region = (Region*) shared;

    const lock_guard<mutex> lock(region->vector_mutex);

    unsigned int start_address = region->segments[0]->size;
    int seg_before = 1;
    bool found = false;
    for(size_t i = 1 ; i < region->segments.size() ; i++) {
        if(region->segments[i]->start - start_address >= size) {
            seg_before = i;
            found = true;
            break;
        }
        else {
            start_address = region->segments[i]->start + region->segments[i]->size;
        }
    }

    if(!found) {
        unsigned int last_elem = region->segments.size() - 1;
        start_address = region->segments[last_elem]->start + region->segments[last_elem]->size;
        if(start_address + size >= REGION_BYTE_SIZE)
            return Alloc::nomem;
    }

    auto iterator = region->segments.begin() + seg_before;
    region->segments.insert(iterator, new Segment(size, start_address));
    *target = &(region->segments[seg_before]->start);

    return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared as(unused), tx_t tx as(unused), void* target as(unused)) noexcept {
    // TODO: tm_free(shared_t, tx_t, void*)
    return false;
}
