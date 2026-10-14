
# Buffer
## 1. Project Overview & Objectives

### **Core Concept**

Traditional inter-thread communication relies on lock-based queues (`std::mutex`, `std::condition_variable`), which incur heavy kernel context-switching overhead and cache invalidation storms. Inspired by the **LMAX Disruptor**, this project implements a high-performance, lock-free ring buffer utilizing atomic sequence numbers, circular buffer indexing, and strict cache-line alignment to achieve ultra-low latency message passing between threads.

### **Primary Objectives**

* **Zero Locks:** Eliminate OS-level mutexes, semaphores, and condition variables entirely in favor of atomic operations and memory fences.
* **Cache Line Consciousness:** Eliminate **false sharing** by aligning cursors and buffer boundaries to standard CPU cache line sizes (64 bytes).
* **Zero Allocation Runtime:** Pre-allocate all ring buffer storage at startup to eliminate dynamic memory allocation (`new`/`delete`) during critical-path message passing.

---

## 2. Architectural Specifications

### **A. Memory Layout & False Sharing Prevention**

CPUs fetch memory in 64-byte chunks (cache lines). If two threads write to separate variables residing on the *same* cache line, the CPU core caches constantly invalidate each other (cache thrashing), destroying performance.

* **Solution:** Use C++17 `alignas(64)` on producer/consumer sequence counters, head/tail pointers, and buffer metadata.
* **Padding:** Insert explicit padding arrays or utilize alignment specifiers to ensure independent thread variables sit strictly on separate cache lines.

### **B. Memory Ordering & Concurrency**

* Leverage explicit C++ atomic memory orders (`std::memory_order_acquire`, `std::memory_order_release`, `std::memory_order_relaxed`) rather than default sequential consistency (`std::memory_order_seq_cst`) to minimize unnecessary CPU pipeline stalls.

### **C. Ring Buffer Mathematics**

* Enforce a **power-of-two capacity** (e.g., 4096, 65536). This allows replacing expensive modulo arithmetic (`index % capacity`) with lightning-fast bitwise masking (`index & (capacity - 1)`).

---

## 3. Step-by-Step Implementation Roadmap

### **Phase 1: The Base SPSC (Single-Producer Single-Consumer) Ring Buffer**

* Implement a fixed-size template array.
* Create atomic `head` (producer index) and `tail` (consumer index) variables.
* Implement basic push and pop methods with full/empty checks using acquire-release semantics.

### **Phase 2: Cache Optimization & Alignment**

* Apply `alignas(64)` to the producer and consumer structures.
* Write a multi-threaded micro-benchmark to demonstrate throughput differences between unaligned and aligned variants.

### **Phase 3: Disruptor-Style Sequence Wrapping**

* Adopt the Disruptor model where sequence numbers monotonically increase rather than wrapping pointers directly.
* Implement gating sequences to prevent the producer from overwriting unconsumed slots (handling ring wrap-around safely without locks).

### **Phase 4: Comprehensive Test Suite**

* Build a stress test with 1 Producer thread and 1 Consumer thread pushing millions of messages.
* Verify message integrity (no drops, strict FIFO ordering) using sequence validation within the message payload.

---

## 4. Testing, Profiling, & Benchmarking Suite

Because this project focuses on hardware-level memory efficiency, standard unit tests are insufficient. You will validate your implementation using systems performance tools:

### **A. Measuring Cache Misses with Linux `perf**`

Run your benchmark binary through `perf` to inspect hardware counter metrics:

```bash
perf stat -e cache-misses,cache-references,L1-dcache-load-misses ./disruptor_benchmark

```

* *Success Metric:* Observe a dramatic reduction in `L1-dcache-load-misses` and `cache-misses` when comparing your `alignas(64)` version against an unaligned naive implementation.

### **B. Detecting False Sharing with Valgrind / Cachegrind**

Analyze cache behavior at the instruction and line level:

```bash
valgrind --tool=cachegrind ./disruptor_benchmark
cg_annotate cachegrind.out.<PID>

```

### **C. Throughput Profiling**

Measure total messages processed per second (Ops/sec) and average round-trip latency (nanoseconds per message) using `std::chrono::high_resolution_clock`.

---

## 5. Advanced Extensions (Optional Stretch Goals)

Once the core SPSC buffer is working and profiled, consider scaling up:

* **Multi-Producer Support (MPMC):** Implement atomic compare-and-swap (`std::atomic::compare_exchange_weak`) loops to allow multiple producer threads safely claiming slots on the ring.
* **Batching:** Allow producers to claim and push blocks of slots at once to maximize memory bus saturation.