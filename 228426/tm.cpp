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

struct Segment {
    Segment* prev; // Previous link in the chain
    Segment* next; // Next link in the chain
    size_t size_seg;
    void* memory_source;
    void* private_memory;
    bool deleted;

    Segment(void* source, size_t size, size_t align) {
        deleted = false;
        size_seg = size;
        memory_source = source;
        posix_memalign(&private_memory, align, size);
        memset(private_memory, 0, size);
    }
};

static void link_reset(Segment* link) {
    link->prev = link;
    link->next = link;
}

static void link_insert(Segment* link, Segment* base) {
    Segment* prev = base->prev;
    link->prev = prev;
    link->next = base;
    base->prev = link;
    prev->next = link;
}

static void link_remove(Segment* link) {
    Segment* prev = link->prev;
    Segment* next = link->next;
    prev->next = next;
    next->prev = prev;
}

struct Region {
    size_t init_size;
    size_t alignment;
    size_t align_alloc;
    size_t delta_alloc;
    atomic_uint segment_allocator;
    atomic_uint tx_generator;
    void* start_address;
    Segment* memory;
    Segment** transact_memories;
    void** reference_memory;
    mutex memory_lock;
    mutex deletion_lock;
    bool* readonly_transactions;

    vector<Segment*> *master_memory;

    Region(size_t size, size_t align) {
        init_size = size;
        alignment = align;
        segment_allocator.store(MAX_TRANSACTIONS + 1);
        tx_generator.store(0);
        master_memory = new vector<Segment*>();
    }
};

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) noexcept {
    Region* region = new Region(size, align);
    if(unlikely(!region)) {
        return invalid_shared;
    }

    region->align_alloc = align < sizeof(void*) ? sizeof(void*) : align;
    region->delta_alloc = (sizeof(Segment) + region->align_alloc - 1) / region->align_alloc * region->align_alloc;

    region->memory = new Segment(nullptr, size, region->align_alloc);
    region->master_memory->push_back(region->memory);
    region->memory->memory_source = region->memory->private_memory;
    region->start_address = region->memory->private_memory;    

    link_reset(region->memory);
    region->memory->size_seg = size;
    region->memory->memory_source = region->start_address;

    region->reference_memory = (void**)new char*[MAX_TRANSACTIONS];
    region->transact_memories = new Segment*[MAX_TRANSACTIONS];
    region->readonly_transactions = new bool[MAX_TRANSACTIONS];

    for(size_t i = 0 ; i < MAX_TRANSACTIONS ; i++) {
        region->transact_memories[i] = new Segment(region->start_address, size, region->align_alloc);
        link_reset(region->transact_memories[i]);
        region->readonly_transactions[i] = false;
        region->reference_memory[i] = nullptr;
    }

    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) noexcept {
    Region* region = (Region*) shared;
    Segment* init = region->transact_memories[0];
    Segment* iterator = init->next;
    delete region->master_memory;
    delete region;
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) noexcept {
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
    return region->init_size;
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

// void remove_segment(Region* region, int counter, Segment*)

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared , bool is_ro) noexcept {
    cout << "TM BEGIN " << endl;
    Region* region = (Region*) shared;
    unsigned int id = region->tx_generator++; //NO COMPARE AND SWAP

    tx_t tx = id % MAX_TRANSACTIONS;
    // if(id / MAX_TRANSACTIONS == 0)
    //     region->transact_memories[tx]->init(region->start_address, region->init_size);
    cout << tx << endl;
    Segment* memory = region->memory;
    // cout << region->transact_memories[tx]->deleted << endl;

    region->readonly_transactions[tx] = is_ro;
    region->reference_memory[tx] = memory;

    // cout << (void*)(region->transact_memories[tx]) + region->delta_alloc << endl;
    // cout << (void*)memory + region->delta_alloc << endl;
    // cout << region->init_size << endl;
    // cout << "COPY ??" << endl;//MAY NEED LOCKING WHEN CONCURRENT
    memcpy(region->transact_memories[tx]->private_memory, region->transact_memories[tx]->memory_source, region->init_size);
    // cout << "COPY. " << endl;
    Segment* head = memory;
    Segment* transact_head = region->transact_memories[tx];
    Segment* transact_iterator = transact_head->next;
    unsigned int counter = 0;
    // cout << "READY TO LOOP " << endl;

    while(memory != head) {
        // cout << "MEM LOOPING " << endl;
        Segment* memory_next = memory->next;
        Segment* transact_next = transact_iterator->next;
        Segment* seg = transact_iterator;
        seg->size_seg = memory->size_seg;
        seg->deleted = memory->deleted;
        seg->memory_source = memory->memory_source;
        memcpy(seg->private_memory, memory->private_memory, memory->size_seg);
        if(memory->deleted) { //PUT IN FUNCTION
            region->deletion_lock.lock();
            if(counter < region->master_memory->size()) {
                region->master_memory->at(counter) = region->master_memory->at(counter + 1);
                if(region->master_memory->at(counter == MAX_TRANSACTIONS)) {
                    for(int i = 0 ; i < MAX_TRANSACTIONS ; i++) {
                        Segment* remove = transact_iterator + (i - tx) * (region->delta_alloc + transact_iterator->size_seg);
                        link_remove(remove);
                    }
                    delete transact_iterator->memory_source;
                    delete (transact_iterator - tx * (region->delta_alloc + transact_iterator->size_seg));
                    region->master_memory->erase(region->master_memory->begin() + counter);
                    counter--;
                }
            }
            region->deletion_lock.unlock();
        }
        memory = memory_next;
        transact_iterator = transact_next;
        counter++;
    }
    // cout << "DONE " << endl;
    // cout << tx << endl;
    return tx;
}

// void end_transaction(Region* region) {

// }

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) noexcept {
    cout << "TM END " << endl;
    Region* region = (Region*) shared;
    Segment* end_memory = region->transact_memories[tx];
    if(region->readonly_transactions[tx]) {
        // cout << "READONLY COMMIT" << endl;
        return true;
    }
    // cout << "TEST OK " << endl;
    bool correct_allocator = region->segment_allocator.load() == tx;
    if(correct_allocator)
        region->segment_allocator.store(MAX_TRANSACTIONS + 1);
    else
        region->memory_lock.lock();

    // cout << "LOCKED " << endl;
    if(region->memory == region->reference_memory[tx]) {
        Segment* seg = region->transact_memories[tx];
        Segment* head = seg;
        do {
            // cout << "LOOPING" << endl;
            // cout << seg->memory_source << endl;
            memcpy(seg->memory_source, seg->private_memory, seg->size_seg);
            // cout << "COPIED" << endl;
            seg = seg->next;
        } while(seg != head);
        region->memory = region->transact_memories[tx];
        // cout << (void*)region->memory << endl;
        region->memory_lock.unlock();
        // cout << "COMMIT COMMIT" << endl;
        return true;
    }
    if(!correct_allocator)
        region->memory_lock.unlock();
    // cout << "FAILED " << endl;
    return false;
}

int check_segments(Segment** seg, const void* check_address) {
    // cout << "CHECK SEG " << endl;
    // cout << "LOOKING FOR " << check_address << endl;
    // Segment* current_seg = *seg;
    // Segment* head = ref;
    Segment* head = *seg;
    int offset = -1;
    do {
        // cout << "LOOPING head at " << (void*) head << " SEG AT " << (void*)*seg<< endl;
        void* start_address = head->memory_source;
        // cout << "GOT START " << (void*) start_address << endl;
        void* end_address = (char*) start_address + head->size_seg;
        // cout << "GOT END " << (void*) end_address << endl;
        if(!head->deleted && start_address <= check_address && check_address < end_address) {
            // cout << "FOUND " << endl;
            // cout << check_address << endl;
            // cout << start_address << endl;
            offset = (char*)check_address - (char*)start_address;
            *seg = head;
            return offset;
        }
        head = head->next;
        // current_seg = current_seg->next;
        // cout << "BOOLEAN : " <<( head != (*seg)) << endl;
    } while(head != *seg);
    // cout << "FAILED" << endl;
    return -1;
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
    // unsigned int offset = region->delta_alloc;
    Segment* seg = region->transact_memories[tx];
    int offset = check_segments(&seg, source);
    if(offset != -1) {
        // offset += in_seg_offset;
        // cout << offset << endl;
        // cout << seg->size_seg << endl;
        // cout << size << endl;
        memcpy(target, seg->private_memory + offset, size);
        return true;
    }
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
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    cout << "TM WRITE " << endl;
    Region* region = (Region*) shared;
    // unsigned int offset = region->delta_alloc;
    // cout << "tx " << tx << endl;
    Segment* seg = region->transact_memories[tx];
    // cout << "SEG " << (void*) seg << endl;
    // cout << "SEG " << (void*) seg->next << endl;
    int offset = check_segments(&seg, target);
    if(offset != -1) {
        // cout << "COPY" << endl;
        memcpy(seg->private_memory + offset, source, size);
        // cout << "COPIED" << endl;
        return true;
    }
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
    cout << "TM ALLOC " << endl;
    Region* region = (Region*) shared;
    if(region->segment_allocator.load() != tx)
        region->memory_lock.lock();
    if(region->memory != region->reference_memory[tx]) {
        region->memory_lock.unlock();
        return Alloc::abort;
    }
    region->segment_allocator.store(tx);

    // void* new_seg;
    // if(unlikely(posix_memalign(&new_seg, region->align_alloc, (region->delta_alloc + size) * MAX_TRANSACTIONS) != 0)) {
    //     region->memory_lock.unlock();
    //     return Alloc::nomem;
    // }
    // void* new_source;
    // if(unlikely(posix_memalign(&new_source, region->align_alloc, size) != 0)) {
    //     region->memory_lock.unlock();
    //     return Alloc::nomem;
    // }

    Segment* new_seg = new Segment(nullptr, size, region->align_alloc);
    new_seg->memory_source = new_seg->private_memory;

    for(size_t i = 0 ; i < MAX_TRANSACTIONS ; i++) {
        Segment* seg = new Segment(new_seg->private_memory, size, region->align_alloc);
        seg->memory_source = new_seg->private_memory;
        seg->deleted = false;
        seg->size_seg = size;
        link_insert(seg, region->transact_memories[i]);
    }
    region->deletion_lock.lock();
    region->master_memory->push_back(new_seg);
    region->deletion_lock.unlock();
    *target = new_seg->private_memory;
    return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void* target) noexcept {
    cout << "TM FREE " << endl;
    Region* region = (Region*) shared;
    if(region->segment_allocator.load() != tx)
        region->memory_lock.lock();
    if(region->memory != region->reference_memory[tx]) {
        region->memory_lock.unlock();
        return false;
    }
    region->segment_allocator.store(tx);
    int master_index = -1;
    for(size_t i = 0 ; i < region->master_memory->size() ; i++) {
        Segment* seg = region->master_memory->at(i);
        if(check_segments(&seg, target) != -1) {
            seg->deleted = true;
            master_index = i;
            break;
        }
    }
    if(master_index == -1) {
        region->memory_lock.unlock();
        return false;
    }

    for(size_t i = 0 ; i < MAX_TRANSACTIONS ; i++) {
        Segment* seg = region->transact_memories[i];
        int counter = master_index;
        while(counter > 0) {
            seg = seg->next;
            counter--;
        }
        seg->deleted = true;
    }
    region->memory_lock.unlock();
    return true;
}
