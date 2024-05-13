#include "atomic_queue/include/atomic_queue/atomic_queue.h"
#include "token_bucket/tb.h"

#ifndef LIBRPERF_H
#define LIBRPERF_H

/* C wrapper compiler */

extern "C" {
  void* create_atomic_queue();
  void* create_atomic_queue_64();
  int atomic_queue_is_empty(void *p);
  int atomic_queue_is_empty_u64(void *p);
  uint32_t atomic_queue_pop(void *p);
  uint64_t atomic_queue_pop_u64(void *p);
  uint32_t atomic_queue_try_pop(void *p, uint32_t* value);
  uint32_t atomic_queue_try_pop_u64(void *p, uint64_t* value);
  void atomic_queue_push(void *p, uint32_t *value);
  void atomic_queue_push_u64(void *p, uint64_t *value);
  uint32_t atomic_queue_try_push(void *p, uint32_t *value);
  uint32_t atomic_queue_try_push_u64(void *p, uint64_t *value);

  void* tb_create(uint64_t max_rate, uint64_t target_rate, uint64_t burstSize);
  bool tb_consume(void* p, uint64_t tokens);
}

#endif
