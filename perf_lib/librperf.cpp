#include "librperf.h"
#include <iostream>

#define MAX_QUEUE_SIZE 1000

extern "C" {
  void* create_atomic_queue() {
    return reinterpret_cast<void *> (new atomic_queue::AtomicQueue<uint32_t, MAX_QUEUE_SIZE, (uint32_t)-1, true, true, true, false>);
  }
  
  void* create_atomic_queue_64() {
    return reinterpret_cast<void *> (new atomic_queue::AtomicQueue<uint64_t, MAX_QUEUE_SIZE, (uint64_t)-1, true, true, true, false>);
  }

  int atomic_queue_is_empty(void *p) {
    atomic_queue::AtomicQueue<uint32_t, MAX_QUEUE_SIZE, (uint32_t)-1, true, true, true, false>* q = reinterpret_cast< atomic_queue::AtomicQueue<uint32_t, MAX_QUEUE_SIZE, (uint32_t)-1, true, true, true, false>* > (p);

    return q->was_empty();
  }
  
  int atomic_queue_is_empty_u64(void *p) {
    atomic_queue::AtomicQueue<uint64_t, MAX_QUEUE_SIZE, (uint64_t)-1, true, true, true, false>* q = reinterpret_cast< atomic_queue::AtomicQueue<uint64_t, MAX_QUEUE_SIZE, (uint64_t)-1, true, true, true, false>* > (p);

    return q->was_empty();
  }

  uint32_t atomic_queue_pop(void *p) {
    atomic_queue::AtomicQueue<uint32_t, MAX_QUEUE_SIZE, (uint32_t)-1, true, true, true, false>* q = reinterpret_cast< atomic_queue::AtomicQueue<uint32_t, MAX_QUEUE_SIZE, (uint32_t)-1, true, true, true, false>* > (p);

    return q->pop();
  }

  uint64_t atomic_queue_pop_u64(void *p) {
    atomic_queue::AtomicQueue<uint64_t, MAX_QUEUE_SIZE, (uint64_t)-1, true, true, true, false>* q = reinterpret_cast< atomic_queue::AtomicQueue<uint64_t, MAX_QUEUE_SIZE, (uint64_t)-1, true, true, true, false>* > (p);

    return q->pop();
  }
  
  uint32_t atomic_queue_try_pop(void *p, uint32_t *value) {
    atomic_queue::AtomicQueue<uint32_t, MAX_QUEUE_SIZE, (uint32_t)-1, true, true, true, false>* q = reinterpret_cast< atomic_queue::AtomicQueue<uint32_t, MAX_QUEUE_SIZE, (uint32_t)-1, true, true, true, false>* > (p);

    uint32_t val;
    uint32_t ret = q->try_pop(val);
    if(ret)
      *value = val;
    else
      *value = -1;

    return ret;
  }
  
  uint32_t atomic_queue_try_pop_u64(void *p, uint64_t *value) {
    atomic_queue::AtomicQueue<uint64_t, MAX_QUEUE_SIZE, (uint64_t)-1, true, true, true, false>* q = reinterpret_cast< atomic_queue::AtomicQueue<uint64_t, MAX_QUEUE_SIZE, (uint64_t)-1, true, true, true, false>* > (p);

    uint64_t val;
    uint32_t ret = q->try_pop(val);
    if(ret)
      *value = val;
    else
      *value = -1;

    return ret;
  }

  void atomic_queue_push(void *p, uint32_t* value) {
    atomic_queue::AtomicQueue<uint32_t, MAX_QUEUE_SIZE, (uint32_t)-1, true, true, true, false>* q = reinterpret_cast< atomic_queue::AtomicQueue<uint32_t, MAX_QUEUE_SIZE, (uint32_t)-1, true, true, true, false>* > (p);

    q->push(*value);
  }
  
  void atomic_queue_push_u64(void *p, uint64_t* value) {
    atomic_queue::AtomicQueue<uint64_t, MAX_QUEUE_SIZE, (uint64_t)-1, true, true, true, false>* q = reinterpret_cast< atomic_queue::AtomicQueue<uint64_t, MAX_QUEUE_SIZE, (uint64_t)-1, true, true, true, false>* > (p);

    q->push(*value);
  }
  
  uint32_t atomic_queue_try_push(void *p, uint32_t* value) {
    atomic_queue::AtomicQueue<uint32_t, MAX_QUEUE_SIZE, (uint32_t)-1, true, true, true, false>* q = reinterpret_cast< atomic_queue::AtomicQueue<uint32_t, MAX_QUEUE_SIZE, (uint32_t)-1, true, true, true, false>* > (p);

    return q->try_push(*value);
  }
  
  uint32_t atomic_queue_try_push_u64(void *p, uint64_t* value) {
    atomic_queue::AtomicQueue<uint64_t, MAX_QUEUE_SIZE, (uint64_t)-1, true, true, true, false>* q = reinterpret_cast< atomic_queue::AtomicQueue<uint64_t, MAX_QUEUE_SIZE, (uint64_t)-1, true, true, true, false>* > (p);

    return q->try_push(*value);
  }
  
  void* tb_create(uint64_t max_rate, uint64_t target_rate, uint64_t burstSize) {
    std::cout<<"TB Create: "<<max_rate<<" "<<target_rate<<" "<<burstSize<<std::endl;
    return reinterpret_cast<void *> (new PerfTB::PerfTokenBucket(max_rate, target_rate, burstSize));
  }

  bool tb_consume(void* p, uint64_t tokens) {
    PerfTB::PerfTokenBucket* tb = reinterpret_cast<PerfTB::PerfTokenBucket*> (p);
    return tb->consume(tokens);
  }

}

