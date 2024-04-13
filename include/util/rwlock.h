#ifndef __RWLOCK_H_
#define __RWLOCK_H_

#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>

#define cpu_relax() asm volatile("pause\n" : : : "memory")
#define mb() asm volatile ("" : : : "memory")

/**
 * Reader-Writer latch backed by pthread.h
 */
class Latch {
 public:
  Latch() { pthread_rwlock_init(&rwlock_, nullptr); };
  ~Latch() { pthread_rwlock_destroy(&rwlock_); }

  /**
   * Acquire a write latch.
   */
  int WLock() { return pthread_rwlock_wrlock(&rwlock_); }

  /**
   * Release a write latch.
   */
  int WUnlock() { return pthread_rwlock_unlock(&rwlock_); }

  /**
   * Acquire a read latch.
   */
  int RLock() { return pthread_rwlock_rdlock(&rwlock_); }

  /**
   * Try to acquire a read latch.
   */
  bool tryRLock() { return !pthread_rwlock_tryrdlock(&rwlock_); }

  /**
   * Release a read latch.
   */
  int RUnlock() { return pthread_rwlock_unlock(&rwlock_); }

 private:
  pthread_rwlock_t rwlock_;
};

static inline void AsmVolatilePause() {
#if defined(__i386__) || defined(__x86_64__)
  asm volatile("pause");
#elif defined(__aarch64__)
  asm volatile("isb");
#elif defined(__powerpc64__)
  asm volatile("or 27,27,27");
#endif
  // it's okay for other platforms to be no-ops
}

class SpinLatch {
 public:
  void WLock() {
    int8_t lock = 0;
    while (!_lock.compare_exchange_weak(lock, 1, std::memory_order_acquire)) {
      lock = 0;
    }
  }

  bool TryWLock() {
    int8_t lock = 0;
    return _lock.compare_exchange_weak(lock, 1, std::memory_order_acquire);
  }

  void WUnlock() { _lock.fetch_xor(1, std::memory_order_release); }

  void RLock() {
    int8_t lock = _lock.fetch_add(2, std::memory_order_acquire);
    while (lock & 1) {
      lock = _lock.load(std::memory_order_relaxed);
    }
  }

  bool TryRLock() {
    int8_t lock = _lock.fetch_add(2, std::memory_order_acquire);
    auto succ = !(_lock.load(std::memory_order_relaxed) & 1);
    if (!succ) {
      _lock.fetch_add(-2, std::memory_order_release);
    }
    return succ;
  }

  void RUnlock() { _lock.fetch_add(-2, std::memory_order_release); }

 private:
  std::atomic_int8_t _lock{0};
  char padding[63];
};

class SpinLock {
 public:
  void Lock() {
    while (_lock.test_and_set(std::memory_order_acquire))
      ;
  }

  void Unlock() { _lock.clear(std::memory_order_release); }

 private:
  std::atomic_flag _lock{0};
};

#endif