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
    void* start;
    atomic_bool deregister;
    atomic_bool* accessed;
    atomic_uint* accessed_id;

    Segment(size_t sz, void* st) {
        start = st;
        size = sz;
        deregister.store(false);
        accessed = new atomic_bool[sz];
        accessed_id = new atomic_uint[sz];
        for(size_t i = 0 ; i < sz ; i++) {
            accessed[i].store(false);
            accessed_id[i].store(0);
        }
    }
};

struct Region {
    void* memory[2];
    size_t alignment;
    size_t align_alloc;
    size_t delta_alloc;
    vector<Segment*> segments;
    vector<Segment*> new_segments;
    // mutex vector_mutex;
    mutex batcher_mutex;
    mutex commit_mutex;
    atomic_uint in_transaction;
    atomic_uint tx_generator;
    atomic_uint waiting;
    atomic_uint expected_out;
    condition_variable batcher;
    condition_variable commit_holder;
    // size_t initial_size;
    void* start_address;

    Region(size_t align) {
        // memory[READ] = new char[REGION_BYTE_SIZE];
        // memory[WRITE] = new char[REGION_BYTE_SIZE];
        alignment = align;
        segments = vector<Segment*>();//(1, nullptr);
        new_segments = vector<Segment*>();
        in_transaction.store(0);
        tx_generator.store(0);
        waiting.store(0);
        expected_out.store(1);
        // initial_size = size;
    }
};

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) noexcept {
    cout << "TM CREATE " << endl;
    Region* region = new Region(align);
    if(unlikely(!region))
        return invalid_shared;

    region->segments.push_back(new Segment(size, 0));
    size_t align_alloc = align < sizeof(void*) ? sizeof(void*) : align;

    if(unlikely(posix_memalign(&(region->memory[READ]), align_alloc, 2 * REGION_BYTE_SIZE) != 0)) {
        delete region;
        return invalid_shared;
    }
    // if(unlikely(posix_memalign(&(region->memory[WRITE]), align_alloc, REGION_BYTE_SIZE) != 0)) {
    //     delete region->memory[READ];
    //     delete region;
    //     return invalid_shared;
    // }

    region->segments[0]->start = region->memory[READ];
    region->start_address = region->segments[0]->start;
    region->align_alloc = align_alloc;

    if(unlikely(!region->segments[0]))
        return invalid_shared;

    memset(region->memory[READ], 0, size);
    memset(region->memory[WRITE], 0, size);

    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) noexcept {
    cout << "TM DESTROY " << endl;
    Region* region = (Region*) shared;
    for(Segment* segment : region->segments) {
        delete[] segment->accessed;
        delete[] segment->accessed_id;
        delete segment;
    }
    // region->segments.clear();
    // region->new_segments.clear();
    // free(region->memory);
    delete region;
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) noexcept {
    cout << "TM START " << endl;
    Region* region = (Region*) shared;
    cout << region->start_address << endl;
    return region->start_address;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) noexcept {
    cout << "TM SIZE " << endl;
    Region* region = (Region*) shared;
    return region->segments[0]->size;
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
    tx_t id = ++(region->tx_generator);
    // cout << "ID UPDATED " << endl;
    if(is_ro)
        id += READ_ONLY_OFFSET;
    // cout << "LOCKING " << endl;
    unique_lock<mutex> lock(region->batcher_mutex);
    region->waiting++;
    // cout << "LOCKED " << endl;
    if(region->in_transaction.load() != 0)
        region->batcher.wait(lock);
    // cout << "PASSED THROUGH " << endl;
    region->expected_out--;
    region->waiting--;
    region->in_transaction++;
    lock.unlock();

    cout << "IN TRANSACTION " << endl;
    cout << id << endl;
    return id;
}

void end_transaction(Region* region) {
    cout << "ENDING TRANSACT " << endl;
    // unique_lock<mutex> lock(region->batcher_mutex);  
    cout << "OUT " << region->expected_out.load() << endl;
    while(region->expected_out.load() != 0 && region->expected_out.load() < 100);  
    int left = --(region->in_transaction);
    // lock.unlock();
    cout << "LEFT " << left << endl;
    if(left == 0) {
        cout << "END OF BATCH " << endl;
        for(size_t j = 0 ; j < region->segments.size() ; j++) {
            Segment* seg = region->segments[j];

            if(seg->deregister.load()) {
                cout << "DEREGISTERING " << endl;
                delete[] seg->accessed;
                delete[] seg->accessed_id;
                delete seg;
                region->segments.erase(region->segments.begin() + j);
                j--;
                continue;
            }

            // cout << seg->size << endl;

            for(size_t i = 0 ; i < seg->size ; i++) {
                void* current = seg->start + i;
                // cout << current << endl;
                seg->accessed_id[i].store(0);
                seg->accessed[i].store(false);
                memcpy(current, current + WRITE_OFFSET, seg->size);
                // if(seg->accessed[i].load()) {
                //     // cout << current + WRITE_OFFSET << endl;
                //     seg->accessed[i].store(false);
                //     memcpy(current, current + WRITE_OFFSET, 1);
                // }
            }
        }

        // for(auto new_seg : region->new_segments) {
        //     for(size_t i = 1 ; i < region->segments.size() ; i++) {
        //         if(new_seg->start < region->segments[i]->start) {
        //             auto iterator = region->segments.begin() + i;
        //             region->segments.insert(iterator, new_seg);
        //             break;
        //         }
        //     }
        // }

        // region->new_segments.clear();
        region->expected_out.exchange(region->waiting);
        cout << "OUT BEFORE " << region->expected_out.load() << endl;
        region->waiting.store(0);
        region->commit_holder.notify_all();
        region->batcher.notify_all();
        return;
    }
    cout << "TRANSACT ENDED " << endl;
    unique_lock<mutex> output_lock(region->commit_mutex);
    region->commit_holder.wait(output_lock);
    output_lock.unlock();
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
    cout << "COMMIT COMMIT COMMIT COMMIT COMMIT COMMIT " << endl;
    return true;
}

Segment* check_segments(vector<Segment*> &segments, const void* source_address, size_t size) {
    // cout << segments.size() << endl;
    for(auto seg : segments) { //concurrent accesses to new segments ?
        void* end_address = seg->start + seg->size;
        // cout << source_address + size << endl;
        if(seg->start <= source_address && source_address < end_address) {
            if(source_address + size <= end_address)
                return seg;
            else
                return nullptr;
        }
    }
    cout << "SEG CHECK DEFAULT " << endl;
    return nullptr;
}
        // cout << seg->size << endl;
        // cout << source << endl;
        // cout << siz

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    cout << "TM READ " << endl;
    Region* region = (Region*) shared;
    // unsigned long source_address = *((unsigned long*) source);
    Segment* seg = check_segments(region->segments, source, size);
    if(seg == nullptr) {// && !check_segments(region->new_segments, source_address, size)) {
        cout << "SEG CHECK READ FAIL " << endl;
        end_transaction(region); //SHOULD WE CHECK IN NEW SEGS ?
        return false;
    }

    if(tx > READ_ONLY_OFFSET) {
        cout << "READ SUCCESSFUL READONLY " << endl;
        // cout << seg->start << endl;
        // cout << seg->size << endl;
        // cout << source << endl;
        // cout << size << endl;
        cout << source << endl;
        memcpy(target, source, size);
        return true;
    }

    unsigned long offset = (char*) source - (char*)seg->start;
    // cout << offset << endl;
    // cout << size << endl;
    unsigned int old_tx = 0;
    for(size_t i = offset ; i < offset + size ; i++) {
        if(seg->accessed[i].load()) {
            if(seg->accessed_id[i].load() != tx) {
                cout << "READ FAILED : WRITTEN BY OTHER " << endl;
                end_transaction(region);
                return false;
            }
            else {
                memcpy(target + (i - offset), source + WRITE_OFFSET + i, 1);
            }
        }
        old_tx = seg->accessed_id[i].exchange(tx);
        if(old_tx != tx && old_tx != 0) {
            cout << "READ FAILED : ACCESSED " << endl;
            end_transaction(region);
            return false;
        }
        memcpy(target + (i - offset), source + i, 1);
    }
    cout << "SUCCESS READ " << endl;
    return true;
    
    // bool already_accessed = false;
    // unsigned long offset = (char*)source - (char*)seg->start;
    // cout << offset << endl;
    // cout << size << endl;
    // for(size_t i = offset ; i < offset + size ; i++) {
    //     already_accessed = already_accessed || seg->accessed[i].load();
    //     if(already_accessed && seg->accessed_id[i].load() != tx) {
    //         cout << "READ FAIL : WRITTEN BY OTHER " << endl;
    //         end_transaction(region);
    //         return false;
    //     }
    // }

    // if(already_accessed) {
    //     memcpy(target, source + WRITE_OFFSET, size);
    //     cout << "READ SUCCESSFUL AUTOACCESS " << endl;
    //     return true;
    // }
    // else {
    //     unsigned int no_tx = 0;
    //     for(size_t i = offset ; i < offset + size ; i++) {
    //         // region->accessed[i].store(true);
    //         if(!seg->accessed_id[i].compare_exchange_strong(no_tx, tx)) {
    //             if(seg->accessed_id[i].load() != tx) {
    //                 cout << "READ FAIL SINCE " << endl;
    //                 end_transaction(region);
    //                 return false;
    //             }
    //             no_tx = 0;
    //         }
    //     }

    //     cout << "READ SUCCESSFUL CLASSIC " << endl;
    //     memcpy(target, source, size);
    //     return true;
    // }
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
    // cout << "SIZE " << size << endl;
    Region* region = (Region*) shared;
    // unsigned long target_address = *((unsigned long*) target);
    // cout << "TO ADDR " << target << endl;

    Segment* seg = check_segments(region->segments, target, size);
    if(seg == nullptr) {// && !check_segments(region->new_segments, target_address, size)) {
        cout << "FAILED WRITE SEG CHECK " << endl;
        end_transaction(region); // SHOULD WE CHECK IN NEW SEGS ?
        return false;
    }

    // for(size_t i = target_address ; i < target_address + size ; i++) { //SHOULD WE FUSE THIS WITH COMPARE AND SWAP LOOP ?
    //     unsigned int read_tx = region->accessed_id[i].load();
    //     // cout << read_tx << endl;
    //     // cout << tx << endl;
        
    //     if(read_tx != 0 && read_tx != tx) {
    //         cout << "FAILED WRITE ACCESSED REGION " << endl;
    //         end_transaction(region);
    //         return false;
    //     }
    // }


    unsigned long offset = (char*) target - (char*)seg->start;
    // cout << offset << endl;
    // cout << size << endl;
    unsigned int old_tx = 0;
    for(size_t i = offset ; i < offset + size ; i++) { //SHOULD WE FUSE THIS WITH COMPARE AND SWAP LOOP ?
        seg->accessed[i].store(true);
        old_tx = seg->accessed_id[i].exchange(tx);
        if(old_tx != 0 && old_tx != tx) {
            cout << old_tx << endl;
            cout << tx << endl;
            cout << "FAILED WRITE ACCESSED SINCE " << endl;
            end_transaction(region);
            return false;
            // cout << "WRITTEN BY ME, IS OK " << endl;
            // no_tx = 0;
        }
    }

    cout << "SUCCESS WRITE " << endl;
    cout << target + WRITE_OFFSET << endl;
    memcpy(target + WRITE_OFFSET, source, size);
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

    Segment* last_segment = region->segments.back();
    void* start_address = last_segment->start + last_segment->size;
    if(start_address >= region->start_address + WRITE_OFFSET)
        return Alloc::nomem;

    Segment* new_seg = new Segment(size, start_address);
    if(unlikely(new_seg == nullptr || new_seg->accessed == nullptr || new_seg->accessed_id == nullptr)) {
        return Alloc::abort;
    }
    region->segments.push_back(new_seg);
    memset(new_seg->start, 0, size);
    memset(new_seg->start + WRITE_OFFSET, 0, size);
    *target = start_address;

    // const lock_guard<mutex> lock(region->vector_mutex);

    // void* start_address = region->start_address + region->segments[0]->size;
    // int seg_before = 1;
    // bool found = false;
    // for(size_t i = 1 ; i < region->segments.size() ; i++) {




    //     if(region->segments[i]->start - start_address >= size) {
    //         found = true;
    //         for(auto new_seg : region->new_segments) {
    //             if(new_seg->start == start_address)
    //                 found = false;
    //         }
    //         if(!found)
    //             continue;
    //         seg_before = i;
    //         break;
    //     }
    //     else {
    //         start_address = region->segments[i]->start + region->segments[i]->size;
    //     }
    // }

    // if(!found) {
    //     unsigned long last_elem = region->segments.size() - 1;
    //     start_address = region->segments[last_elem]->start + region->segments[last_elem]->size;
    //     if(start_address + size >= REGION_BYTE_SIZE)
    //         return Alloc::nomem;
    // }

    // region->new_segments.push_back(new Segment(size, start_address));
    // *target = &(region->segments[seg_before]->start);

    // for(size_t i = start_address ; i < start_address + size ; i++) {
    //     region->memory[READ][i] = 0;
    //     region->memory[WRITE][i] = 0;
    //     region->accessed[i].store(false);
    //     region->accessed_id[i].store(0);
    // }

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

    for(Segment* seg : region->segments) {
        if(seg->start == target) {
            seg->deregister.store(true);
            return true;
        }
    }

    // for(size_t i = 0 ; i < region->segments.size() ; i++) {
    //     unsigned long target_address = *((unsigned long*) target);
    //     if(region->segments[i]->start == target_address) {
    //         if(i == 0)
    //             return false;
    //         region->segments[i]->deregister.store(true);
    //         return true;
    //     }
    // }// Should also free in new segs ?

    return false;
}
