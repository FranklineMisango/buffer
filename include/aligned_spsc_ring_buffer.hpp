#ifndef DISRUPTOR_ALIGNED_SPSC_RING_BUFFER_HPP
#define DISRUPTOR_ALIGNED_SPSC_RING_BUFFER_HPP

#include <atomic>
#include <cstddef>
#include <type_traits>

#ifndef DISRUPTOR_CACHE_LINE_SIZE
#define DISRUPTOR_CACHE_LINE_SIZE 64
#endif

namespace disruptor {

template <typename T, std::size_t Capacity>
class AlignedSpscRingBuffer {
  static_assert(Capacity >= 2, "Capacity must be >= 2");
  static_assert((Capacity & (Capacity - 1)) == 0,
                "Capacity must be a power of two");

  struct alignas(DISRUPTOR_CACHE_LINE_SIZE) ProducerState {
    std::atomic<std::size_t> next_sequence_{0};
    char pad0_[DISRUPTOR_CACHE_LINE_SIZE -
               sizeof(std::atomic<std::size_t>)];
  };

  struct alignas(DISRUPTOR_CACHE_LINE_SIZE) ConsumerState {
    std::atomic<std::size_t> next_sequence_{0};
    char pad0_[DISRUPTOR_CACHE_LINE_SIZE -
               sizeof(std::atomic<std::size_t>)];
  };

public:
  using value_type    = T;
  using size_type     = std::size_t;
  using sequence_type = std::size_t;

  static constexpr size_type kCapacity = Capacity;
  static constexpr size_type kMask     = Capacity - 1;

  AlignedSpscRingBuffer() noexcept(std::is_nothrow_default_constructible_v<T>) {
    for (auto &slot : storage_) {
      ::new (&slot) T();
    }
  }

  ~AlignedSpscRingBuffer() noexcept {
    while (!empty()) { pop(); }
  }

  AlignedSpscRingBuffer(const AlignedSpscRingBuffer &)            = delete;
  AlignedSpscRingBuffer &operator=(const AlignedSpscRingBuffer &) = delete;
  AlignedSpscRingBuffer(AlignedSpscRingBuffer &&)                 = delete;
  AlignedSpscRingBuffer &operator=(AlignedSpscRingBuffer &&)      = delete;
bool try_push(const T &value) noexcept {
    const auto prod_seq = producer_.next_sequence_.load(std::memory_order_relaxed);
    const auto cons_seq = consumer_.next_sequence_.load(std::memory_order_acquire);
    if ((prod_seq - cons_seq) >= Capacity) return false;
    storage_[prod_seq & kMask] = value;
    producer_.next_sequence_.store(prod_seq + 1, std::memory_order_release);
    return true;
  }

  bool try_push(T &&value) noexcept {
    const auto prod_seq = producer_.next_sequence_.load(std::memory_order_relaxed);
    const auto cons_seq = consumer_.next_sequence_.load(std::memory_order_acquire);
    if ((prod_seq - cons_seq) >= Capacity) return false;
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
bool try_pop(T &out) noexcept {
    const auto cons_seq = consumer_.next_sequence_.load(std::memory_order_relaxed);
    const auto prod_seq = producer_.next_sequence_.load(std::memory_order_acquire);
    if (cons_seq == prod_seq) return false;
    out = storage_[cons_seq & kMask];
    consumer_.next_sequence_.store(cons_seq + 1, std::memory_order_release);
    return true;
  }

  void pop() noexcept { T tmp; (void)try_pop(tmp); }

  bool empty() const noexcept {
    return producer_.next_sequence_.load(std::memory_order_acquire) ==
           consumer_.next_sequence_.load(std::memory_order_acquire);
  }

  bool full() const noexcept {
    const auto p = producer_.next_sequence_.load(std::memory_order_acquire);
    const auto c = consumer_.next_sequence_.load(std::memory_order_acquire);
    return (p - c) >= Capacity;
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

}  // namespace disruptor

#endif  // DISRUPTOR_ALIGNED_SPSC_RING_BUFFER_HPP