#ifndef PERF_TOKEN_BUCKET_H
#define PERF_TOKEN_BUCKET_H

#include <sys/shm.h>
#include <mutex>
#include <atomic>
#include <stdint.h>
#include <sys/time.h>

namespace PerfTB {

class PerfTokenBucket {
  public:
    PerfTokenBucket() : time_(0), timePerToken_(0), timePerBurst_(0) {}

    PerfTokenBucket(const uint64_t max_rate, const uint64_t rate, const uint64_t burstSize) {
      time_ = 0;
      maxRate = max_rate * 10;
      timePerToken_ = max_rate * 10 / rate;
      timePerBurst_ = burstSize * timePerToken_;
    }

    bool consume(const uint64_t tokens) {
      const uint64_t now =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
        .count() * (maxRate/1000000);
      const uint64_t timeNeeded =
        tokens * timePerToken_.load(std::memory_order_relaxed);
      const uint64_t minTime =
        now - timePerBurst_.load(std::memory_order_relaxed);
      uint64_t oldTime = time_.load(std::memory_order_relaxed);
      uint64_t newTime = oldTime;

      if (minTime > oldTime) {
        newTime = minTime;
      }

      for (;;) {
        newTime += timeNeeded;
        if (newTime > now) {
          return false;
        }
        if (time_.compare_exchange_weak(oldTime, newTime,
              std::memory_order_relaxed,
              std::memory_order_relaxed)) {
          return true;
        }
        newTime = oldTime;
      }

      return false;
    }
  private:
    std::atomic<uint64_t> time_;
    std::atomic<uint64_t> timePerToken_;
    std::atomic<uint64_t> timePerBurst_;
    uint64_t maxRate;
};

}
#endif
