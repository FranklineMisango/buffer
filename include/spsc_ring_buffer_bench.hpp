#ifndef DISRUPTOR_SPSC_RING_BUFFER_BENCH_HPP
#define DISRUPTOR_SPSC_RING_BUFFER_BENCH_HPP

#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace disruptor {

// ---------------------------------------------------------------------------
//  Simple high‑resolution timer (RAII style)
// ---------------------------------------------------------------------------
class Timer {
public:
  using Clock = std::chrono::high_resolution_clock;

  Timer()  { reset(); }
  void reset() { start_ = Clock::now(); }

  /// Elapsed time in nanoseconds
  std::uint64_t elapsed_ns() const {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               Clock::now() - start_)
        .count();
  }

  /// Elapsed time in seconds (as a double)
  double elapsed_s() const {
    return std::chrono::duration_cast<std::chrono::duration<double>>(
               Clock::now() - start_)
        .count();
  }

private:
  Clock::time_point start_;
};

// ---------------------------------------------------------------------------
//  Benchmark helpers
// ---------------------------------------------------------------------------

/// Run a single‑threaded baseline (no actual concurrency) to measure raw
/// throughput of push/pop without thread overhead.
template <typename Ring>
std::uint64_t baseline_ops_per_sec(Ring &ring, std::uint64_t ops) {
  using value_type = typename Ring::value_type;

  Timer t;
  for (std::uint64_t i = 0; i < ops; ++i) {
    while (!ring.try_push(value_type{})) { /* spin */ }
    value_type out;
    while (!ring.try_pop(out)) { /* spin */ }
  }
  auto ns = t.elapsed_ns();
  if (ns == 0) ns = 1;
  return (ops * 1000000000ULL) / ns;
}

/// Run a 1-producer 1-consumer benchmark.
template <typename Ring>
std::uint64_t spsc_ops_per_sec(Ring &ring, std::uint64_t ops) {
  using value_type = typename Ring::value_type;

  std::atomic<bool> start_flag{false};

  // Consumer thread
  std::atomic<std::uint64_t> consumed{0};
  std::thread consumer([&] {
    while (!start_flag.load(std::memory_order_acquire)) { /* spin */ }
    value_type out;
    while (consumed.load(std::memory_order_relaxed) < ops) {
      if (ring.try_pop(out)) {
        consumed.fetch_add(1, std::memory_order_relaxed);
      }
    }
  });

  // Producer thread
  std::thread producer([&] {
    while (!start_flag.load(std::memory_order_acquire)) { /* spin */ }
    for (std::uint64_t i = 0; i < ops; ++i) {
      while (!ring.try_push(value_type{})) { /* spin */ }
    }
  });

  Timer t;
  start_flag.store(true, std::memory_order_release);
  producer.join();
  consumer.join();
  auto ns = t.elapsed_ns();
  if (ns == 0) ns = 1;
  return (ops * 1000000000ULL) / ns;
}

/// Display a benchmark result
inline void print_result(const std::string &name,
                          std::uint64_t ops_per_sec,
                          std::uint64_t msg_count) {
  double us_per_msg = 1'000'000.0 / static_cast<double>(ops_per_sec);
  std::cout << "  " << name << ":\n"
            << "    Messages processed : " << msg_count << "\n"
            << "    Throughput         : " << ops_per_sec << " ops/sec\n"
            << "    Latency per msg    : " << us_per_msg << " µs\n";
}

}  // namespace disruptor

#endif  // DISRUPTOR_SPSC_RING_BUFFER_BENCH_HPP