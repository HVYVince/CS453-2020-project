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
struct Segment {
    size_t size;
    unsigned int start;
    atomic_bool deregister;

    Segment(size_t sz, unsigned int st) {
        start = st;
        size = sz;
        deregister.store(false);
    }
};

struct Region {
    char* memory[2];
    atomic_bool* accessed;
    atomic_uint* accessed_id;
    size_t alignment;
    vector<Segment*> segments;
    vector<Segment*> new_segments;
    mutex vector_mutex; //still needed ?
    mutex batcher_mutex;
    atomic_uint in_transaction;
    atomic_ulong waiting;
    condition_variable batcher;
    size_t initial_size;
    void* start_address = nullptr;

    Region(size_t align, size_t size) {
        memory[READ] = new char[REGION_BYTE_SIZE];
        memory[WRITE] = new char[REGION_BYTE_SIZE];
        accessed = new atomic_bool[REGION_BYTE_SIZE]; //UNNEEDED, just check accessed_id. Or transform for write access only
        accessed_id = new atomic_uint[REGION_BYTE_SIZE];
        alignment = align;
        segments = vector<Segment*>(1, nullptr);
        new_segments = vector<Segment*>();
        in_transaction.store(0);
        waiting.store(0);
        initial_size = size;
    }
};

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) noexcept {
    cout << "TM CREATE " << endl;
    Region* region = new Region(align, size);
    if(region == nullptr)
        return invalid_shared;

    region->segments[0] = new Segment(size, 0);
    region->start_address = &(region->segments[0]->start);

    if(region->segments[0] == nullptr)
        return invalid_shared;

    memset(&(region->memory[READ][0]), 0, size);
    memset(&(region->memory[WRITE][0]), 0, size);
    for(size_t i = 0 ; i < size ; i++) {
        region->accessed[i].store(false);
        region->accessed_id[i].store(0);
    }

    cout << "CREATION DONE" << endl;
    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) noexcept {
    cout << "TM DESTROY " << endl;
    Region* region = (Region*) shared;
    for(Segment* segment : region->segments)
        delete segment;
    region->segments.clear();
    region->new_segments.clear();
    delete[] region->memory[0];
    delete[] region->memory[1];
    delete[] region->accessed;
    delete[] region->accessed_id;
    delete region;
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) noexcept {
    // cout << "TM START " << endl;
    Region* region = (Region*) shared;
    return region->start_address;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) noexcept {
    cout << "TM SIZE " << endl;
    Region* region = (Region*) shared;
    return region->initial_size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) noexcept {
    cout << "TM ALIGN " << endl;
    Region* region = (Region*) shared;
    return region->alignment;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared , bool is_ro) noexcept {
    cout << "TM BEGIN " << endl;
    Region* region = (Region*) shared;
    tx_t id = ++(region->waiting);
    // cout << "ID UPDATED " << endl;
    if(is_ro)
        id += READ_ONLY_OFFSET;
    // cout << "LOCKING " << endl;
    unique_lock<mutex> lock(region->batcher_mutex);
    // cout << "LOCKED " << endl;
    if(region->in_transaction.load() != 0)
        region->batcher.wait(lock);
    // cout << "PASSED THROUGH " << endl;
    region->batcher_mutex.unlock();
    region->in_transaction++;

    // cout << id << endl;
    return id;
}

void end_transaction(Region* region) {
    cout << "ENDING TRANSACT " << endl;
    int left = --(region->in_transaction);
    if(left == 0) {
        for(size_t j = 0 ; j < region->segments.size() ; j++) {
            Segment* seg = region->segments[j];
            if(seg->deregister.load()) {
                region->segments.erase(region->segments.begin() + j);
                j--;
                continue;
            }
            for(size_t i = 0 ; i < seg->size ; i++) {
                int index = seg->start + i;
                if(region->accessed[index].load()) {
                    region->accessed_id[index].store(0);
                    region->memory[READ][index] = region->memory[WRITE][index];
                    region->accessed[index].store(false);
                }
            }
        }

        for(auto new_seg : region->new_segments) {
            for(size_t i = 1 ; i < region->segments.size() ; i++) {
                if(new_seg->start < region->segments[i]->start) {
                    auto iterator = region->segments.begin() + i;
                    region->segments.insert(iterator, new_seg);
                    break;
                }
            }
        }

        region->new_segments.clear();
        region->batcher.notify_all();
    }
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx as(unused)) noexcept {
    cout << "TM END " << endl;
    Region* region = (Region*) shared;
    end_transaction(region);
    return true;
}

bool check_segments(vector<Segment*> &segments, unsigned int source_address, size_t size) {
    for(auto seg : segments) { //concurrent accesses to new segments ?
        if(seg->start <= source_address && source_address < seg->start + seg->size)
            return source_address + size < seg->start + seg->size;
    }
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
bool tm_read(shared_t shared, tx_t tx, void const* source, size_t size, void* target ) noexcept {
    cout << "TM READ " << endl;
    Region* region = (Region*) shared;
    unsigned int source_address = *((unsigned int*) source);

    // if(!check_segments(region->segments, source_address, size) && !check_segments(region->new_segments, source_address, size)) {
    //     end_transaction(region); //SHOULD WE CHECK IN NEW SEGS ?
    //     return false;
    // }

    if(tx > READ_ONLY_OFFSET) {
        memcpy(target, (void*) &(region->memory[READ][source_address]), size);
        return true;
    }
    
    bool already_accessed = false;
    for(size_t i = source_address ; i < source_address + size ; i++) {
        already_accessed = already_accessed || region->accessed[i].load();
        if(already_accessed && region->accessed_id->load() != tx) {
            end_transaction(region);
            return false;
        }
    }

    if(already_accessed) {
        memcpy(target, (void*) &(region->memory[WRITE][source_address]), size);
        return true;
    }
    else {
        unsigned int no_tx = 0;
        for(size_t i = source_address ; i < source_address + size ; i++) {
            region->accessed[i].store(true);
            if(!region->accessed_id[i].compare_exchange_strong(no_tx, tx)) {
                if(region->accessed_id[i].load() != tx) {
                    end_transaction(region);
                    return false;
                }
                no_tx = 0;
            }
        }
        memcpy(target, (void*) &(region->memory[READ][source_address]), size);
        return true;
    }
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    cout << "TM WRITE " << endl;
    Region* region = (Region*) shared;
    unsigned int target_address = *((unsigned int*) target);

    // if(!check_segments(region->segments, target_address, size) && !check_segments(region->new_segments, target_address, size)) {
    //     end_transaction(region); // SHOULD WE CHECK IN NEW SEGS ?
    //     return false;
    // }

    for(size_t i = target_address ; i < target_address + size ; i++) { //SHOULD WE FUSE THIS WITH COMPARE AND SWAP LOOP ?
        unsigned int read_tx = region->accessed_id[i].load();
        if(read_tx != 0 && read_tx != tx) {
            end_transaction(region);
            return false;
        }
    }

    unsigned int no_tx = 0;
    for(size_t i = target_address ; i < target_address + size ; i++) { //SHOULD WE FUSE THIS WITH COMPARE AND SWAP LOOP ?
        if(region->accessed_id[i].compare_exchange_strong(no_tx, tx)) {
            if(region->accessed_id[i].load() != tx) {
                end_transaction(region);
                return false;
            }
            no_tx = 0;
        }
    }

    memcpy((void*) &(region->memory[READ][target_address]), source, size);
    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
Alloc tm_alloc(shared_t shared, tx_t tx as(unused), size_t size, void** target) noexcept {
    cout << "TM ALLOC " << endl;
    Region* region = (Region*) shared;

    // const lock_guard<mutex> lock(region->vector_mutex);

    unsigned int start_address = region->segments[0]->size;
    int seg_before = 1;
    bool found = false;
    for(size_t i = 1 ; i < region->segments.size() ; i++) {
        if(region->segments[i]->start - start_address >= size) {
            found = true;
            for(auto new_seg : region->new_segments) {
                if(new_seg->start == start_address)
                    found = false;
            }
            if(!found)
                continue;
            seg_before = i;
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

    region->new_segments.push_back(new Segment(size, start_address));
    *target = &(region->segments[seg_before]->start);

    for(size_t i = start_address ; i < start_address + size ; i++) {
        region->memory[READ][i] = 0;
        region->memory[WRITE][i] = 0;
        region->accessed[i].store(false);
        region->accessed_id[i].store(0);
    }

    return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx as(unused), void* target) noexcept {
    cout << "TM FREE " << endl;
    Region* region = (Region*) shared;

    for(size_t i = 0 ; i < region->segments.size() ; i++) {
        unsigned int target_address = *((unsigned int*) target);
        if(region->segments[i]->start == target_address) {
            if(i == 0)
                return false;
            region->segments[i]->deregister.store(true);
            return true;
        }
    }// Should also free in new segs ?

    return false;
}
