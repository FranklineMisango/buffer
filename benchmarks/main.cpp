#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <new>
#include <stdexcept>
#include <thread>
#include <vector>

#include "aligned_spsc_ring_buffer.hpp"
#include "spsc_ring_buffer.hpp"
#include "spsc_ring_buffer_bench.hpp"

// ===========================================================================
//  Stress Test & Throughput Benchmark
// ===========================================================================

namespace {

struct alignas(64) Payload {
  std::uint64_t seq;
  std::uint64_t data;

  bool operator==(const Payload &o) const {
    return seq == o.seq && data == o.data;
  }
  bool operator!=(const Payload &o) const { return !(*this == o); }
};

constexpr std::size_t kRingCapacity = 1 << 12;   // 4096 — power of two
constexpr std::uint64_t kMessageCount = 10'000'000;  // 10M messages
template <typename Ring>
void baseline_test(Ring &ring) {
  std::cout << "\n[Baseline — Single Thread, No Contention]\n";
  auto ops = disruptor::baseline_ops_per_sec(ring, kMessageCount);
  disruptor::print_result("push+pop loop", ops, kMessageCount);
}

template <typename Ring>
void spsc_stress_test(Ring &ring, const std::string &label) {
  std::cout << "\n[SPSC Stress Test — " << label << "]\n";
  {
    Payload dummy;
    for (std::size_t i = 0; i < ring.capacity(); ++i) {
      while (!ring.try_push(Payload{0xDEAD, 0xBEEF})) { /* spin */ }
      while (!ring.try_pop(dummy)) { /* spin */ }
    }
  }

  std::atomic<bool> start_flag{false};
  std::atomic<std::uint64_t> consumed{0};
  std::atomic<bool> consumer_error{false};

  auto producer_fn = [&] {
    while (!start_flag.load(std::memory_order_acquire)) { /* spin */ }
    for (std::uint64_t i = 0; i < kMessageCount; ++i) {
      Payload p;
      p.seq  = i;
      p.data = i * 3 + 1;
      while (!ring.try_push(p)) { /* spin */ }
    }
  };
auto consumer_fn = [&] {
    while (!start_flag.load(std::memory_order_acquire)) { /* spin */ }
    std::uint64_t expected = 0;
    Payload out;
    while (consumed.load(std::memory_order_relaxed) < kMessageCount) {
      if (ring.try_pop(out)) {
        if (out.seq != expected) {
          std::cerr << "\nERROR: Expected seq " << expected
                    << " but got " << out.seq << "\n";
          consumer_error.store(true, std::memory_order_release);
          consumed.store(kMessageCount, std::memory_order_release);
          return;
        }
        if (out.data != out.seq * 3 + 1) {
          std::cerr << "\nERROR: Expected data " << (out.seq * 3 + 1)
                    << " but got " << out.data << "\n";
          consumer_error.store(true, std::memory_order_release);
          consumed.store(kMessageCount, std::memory_order_release);
          return;
        }
        consumed.fetch_add(1, std::memory_order_relaxed);
        ++expected;
      }
    }
  };

  std::thread producer(producer_fn);
  std::thread consumer(consumer_fn);

  disruptor::Timer timer;
  start_flag.store(true, std::memory_order_release);
  producer.join();
  consumer.join();
  auto ns = timer.elapsed_ns();

  if (consumer_error.load()) {
    std::cout << "  *** FAILED — message integrity violation ***\n";
  } else {
    std::uint64_t total = consumed.load();
    if (total != kMessageCount) {
      std::cout << "  *** FAILED — consumed " << total
                << " / " << kMessageCount << " ***\n";
    } else {
      std::cout << "  PASS — all " << kMessageCount
                << " messages verified (FIFO, integrity)\n";
      std::uint64_t ops = total * 1000000000ULL / (ns ? ns : 1);
      disruptor::print_result("1P1C throughput", ops, total);
    }
  }
}
template <typename RingT>
void batching_benchmark(RingT &ring, std::size_t batch_size) {
  std::cout << "\n[Batching Benchmark — batch size = " << batch_size << "]\n";
  {
    Payload dummy;
    while (!ring.empty()) { while (!ring.try_pop(dummy)) { /* spin */ } }
  }

  std::atomic<bool> start_flag{false};
  std::atomic<std::uint64_t> produced{0};
  std::atomic<std::uint64_t> consumed{0};

  alignas(64) Payload batch[64];
  for (std::size_t i = 0; i < batch_size; ++i) {
    batch[i].seq  = i; batch[i].data = i;
  }

  auto producer_fn = [&] {
    while (!start_flag.load(std::memory_order_acquire)) { /* spin */ }
    std::uint64_t total = 0;
    while (total < kMessageCount) {
      for (std::size_t i = 0; i < batch_size; ++i) {
        batch[i].seq   = total + i;
        batch[i].data  = (total + i) * 3 + 1;
      }
      std::size_t pushed = 0;
      while (pushed < batch_size) {
        if (ring.try_push(batch[pushed])) { ++pushed; }
      }
      total += batch_size;
      produced.store(total, std::memory_order_release);
    }
  };

  auto consumer_fn = [&] {
    while (!start_flag.load(std::memory_order_acquire)) { /* spin */ }
    std::uint64_t total = 0;
    Payload out;
    while (total < kMessageCount) {
      if (ring.try_pop(out)) { ++total; consumed.store(total, std::memory_order_release); }
    }
  };

  std::thread producer(producer_fn);
  std::thread consumer(consumer_fn);

  disruptor::Timer timer;
  start_flag.store(true, std::memory_order_release);
  producer.join();
  consumer.join();
  auto ns = timer.elapsed_ns();

  auto total = consumed.load();
  std::uint64_t ops = total * 1000000000ULL / (ns ? ns : 1);
  disruptor::print_result("1P1C batched", ops, total);
}

template <typename Unaligned, typename Aligned>
void alignment_compare() {
  constexpr std::size_t kShortRun = 1'000'000;

  std::cout << "\n[Cache-Line Alignment Comparison — "
            << kShortRun << " messages]\n";

  alignas(4) char raw_unaligned[sizeof(Unaligned)];
  auto *unaligned = ::new (raw_unaligned) Unaligned();
  Aligned aligned;

  auto ops_u = disruptor::baseline_ops_per_sec(*unaligned, kShortRun);
  auto ops_a = disruptor::baseline_ops_per_sec(aligned, kShortRun);

  std::cout << "  Unaligned variant : " << ops_u << " ops/sec\n"
            << "  Aligned variant   : " << ops_a << " ops/sec\n"
            << "  Speedup           : "
            << (static_cast<double>(ops_a) /
                static_cast<double>(ops_u))
            << "x\n";

  unaligned->~Unaligned();
}

}  // anonymous namespace

int main() {
  std::cout << "========================================================\n"
            << "  Cache-Conscious Disruptor Ring Buffer\n"
            << "  Benchmark & Stress-Test Suite\n"
            << "========================================================\n\n";

  {
    disruptor::SpscRingBuffer<Payload, kRingCapacity> ring;
    baseline_test(ring);
  }

  {
    disruptor::SpscRingBuffer<Payload, kRingCapacity> ring;
    spsc_stress_test(ring, "Aligned SpscRingBuffer");
  }

  {
    disruptor::AlignedSpscRingBuffer<Payload, kRingCapacity> ring;
    spsc_stress_test(ring, "AlignedSpscRingBuffer (explicit padding)");
  }

  {
    disruptor::SpscRingBuffer<Payload, kRingCapacity> ring;
    batching_benchmark(ring, 4);
  }
  {
    disruptor::SpscRingBuffer<Payload, kRingCapacity> ring;
    batching_benchmark(ring, 8);
  }
  {
    disruptor::SpscRingBuffer<Payload, kRingCapacity> ring;
    batching_benchmark(ring, 16);
  }

  alignment_compare<
      disruptor::SpscRingBuffer<Payload, kRingCapacity>,
      disruptor::SpscRingBuffer<Payload, kRingCapacity>>();

  std::cout << "\n========================================================\n"
            << "  All benchmarks complete.\n"
            << "========================================================\n";

return EXIT_SUCCESS;
}
  