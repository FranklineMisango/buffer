#ifndef DISRUPTOR_SPSC_RING_BUFFER_HPP
#define DISRUPTOR_SPSC_RING_BUFFER_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <type_traits>
#include <new>

#ifndef DISRUPTOR_CACHE_LINE_SIZE
#define DISRUPTOR_CACHE_LINE_SIZE 64
#endif

namespace disruptor {

// ===========================================================================
//  Phase 1 & 3 :  Single-Producer Single-Consumer Ring Buffer
//
//  Features
//    - Lock-free (atomic sequence counters only)
//    - Power-of-two capacity -> bitwise masking for indexing
//    - Monotonically increasing sequence IDs (Disruptor-style wrap-around)
//    - Cache-line padding between producer / consumer state (Phase 2)
//    - Strict acquire-release memory ordering
// ===========================================================================
template <typename T, std::size_t Capacity>
class SpscRingBuffer {
  static_assert(Capacity >= 2, "Capacity must be >= 2");
// ------------------------------------------------------------------
  // Producer-side state (padded to its own cache line)
  // ------------------------------------------------------------------
  struct alignas(DISRUPTOR_CACHE_LINE_SIZE) ProducerState {
    std::atomic<std::size_t> next_sequence_{0};
    char padding_[DISRUPTOR_CACHE_LINE_SIZE -
                  sizeof(std::atomic<std::size_t>)];
  };

  // ------------------------------------------------------------------
  // Consumer-side state (padded to its own cache line)
  // ------------------------------------------------------------------
  struct alignas(DISRUPTOR_CACHE_LINE_SIZE) ConsumerState {
    std::atomic<std::size_t> next_sequence_{0};
    char padding_[DISRUPTOR_CACHE_LINE_SIZE -
                  sizeof(std::atomic<std::size_t>)];
  };

public:
  using value_type      = T;
  using size_type       = std::size_t;
  using sequence_type   = std::size_t;

  static constexpr size_type kCapacity = Capacity;
  static constexpr size_type kMask     = Capacity - 1;

  // ------------------------------------------------------------------
  // Construction
  // ------------------------------------------------------------------
  SpscRingBuffer() noexcept(std::is_nothrow_default_constructible_v<T>) {
    for (auto &slot : storage_) {
      ::new (&slot) T();
    }
  }

  ~SpscRingBuffer() noexcept {
    while (!empty()) {
      pop();
    }
  }

  SpscRingBuffer(const SpscRingBuffer &)            = delete;
  SpscRingBuffer &operator=(const SpscRingBuffer &) = delete;
  SpscRingBuffer(SpscRingBuffer &&)                 = delete;
  SpscRingBuffer &operator=(SpscRingBuffer &&)      = delete;
// ------------------------------------------------------------------
  //  Producer API
  // ------------------------------------------------------------------
  bool try_push(const T &value) noexcept {
    const auto prod_seq = producer_.next_sequence_.load(std::memory_order_relaxed);
    const auto cons_seq = consumer_.next_sequence_.load(std::memory_order_acquire);
    if ((prod_seq - cons_seq) >= Capacity) {
      return false;
    }
    storage_[prod_seq & kMask] = value;
    producer_.next_sequence_.store(prod_seq + 1, std::memory_order_release);
    return true;
  }

  bool try_push(T &&value) noexcept {
    const auto prod_seq = producer_.next_sequence_.load(std::memory_order_relaxed);
    const auto cons_seq = consumer_.next_sequence_.load(std::memory_order_acquire);
    if ((prod_seq - cons_seq) >= Capacity) {
      return false;
    }
    storage_[prod_seq & kMask] = std::move(value);
    producer_.next_sequence_.store(prod_seq + 1, std::memory_order_release);
    return true;
  }

  void push(const T &value) noexcept {
    while (!try_push(value)) {
#if defined(__x86_64__) || defined(__i386__)
      __builtin_ia32_pause();
#elif defined(__aarch64__)
      __builtin_aarch64_yield();
#endif
    }
  }

  void push(T &&value) noexcept {
    while (!try_push(std::move(value))) {
#if defined(__x86_64__) || defined(__i386__)
      __builtin_ia32_pause();
#elif defined(__aarch64__)
      __builtin_aarch64_yield();
#endif
    }
  }
// ------------------------------------------------------------------
  //  Consumer API
  // ------------------------------------------------------------------
  bool try_pop(T &out) noexcept {
    const auto cons_seq = consumer_.next_sequence_.load(std::memory_order_relaxed);
    const auto prod_seq = producer_.next_sequence_.load(std::memory_order_acquire);
    if (cons_seq == prod_seq) {
      return false;
    }
    out = storage_[cons_seq & kMask];
    consumer_.next_sequence_.store(cons_seq + 1, std::memory_order_release);
    return true;
  }

  void pop() noexcept {
    T tmp;
    (void)try_pop(tmp);   // discard
  }

  // ------------------------------------------------------------------
  //  Observers
  // ------------------------------------------------------------------
  bool empty() const noexcept {
    return producer_.next_sequence_.load(std::memory_order_acquire) ==
           consumer_.next_sequence_.load(std::memory_order_acquire);
  }

  bool full() const noexcept {
    const auto prod_seq = producer_.next_sequence_.load(std::memory_order_acquire);
    const auto cons_seq = consumer_.next_sequence_.load(std::memory_order_acquire);
    return (prod_seq - cons_seq) >= Capacity;
  }

  sequence_type producer_sequence() const noexcept {
    return producer_.next_sequence_.load(std::memory_order_acquire);
  }

  sequence_type consumer_sequence() const noexcept {
    return consumer_.next_sequence_.load(std::memory_order_acquire);
  }

  size_type size() const noexcept {
    const auto p = producer_.next_sequence_.load(std::memory_order_acquire);
    const auto c = consumer_.next_sequence_.load(std::memory_order_acquire);
    return p - c;
  }

  constexpr size_type capacity() const noexcept { return Capacity; }

private:
  alignas(DISRUPTOR_CACHE_LINE_SIZE) ProducerState producer_;
  alignas(DISRUPTOR_CACHE_LINE_SIZE) ConsumerState consumer_;
  alignas(DISRUPTOR_CACHE_LINE_SIZE) T storage_[Capacity];
};

// Type trait helpers
template <typename T>
struct is_spsc_ring_buffer : std::false_type {};

template <typename T, std::size_t N>
struct is_spsc_ring_buffer<SpscRingBuffer<T, N>> : std::true_type {};

}  // namespace disruptor

#endif  // DISRUPTOR_SPSC_RING_BUFFER_HPP