// AetherLOB - lock-free single-producer / single-consumer ring buffer.
//
// Contract: exactly one thread calls try_push(), exactly one thread calls
// try_pop(). Under that contract both are wait-free.
//
// Memory ordering
//   The producer publishes a slot with a release store of the write index; the
//   consumer observes it with an acquire load, which makes the slot contents
//   visible. The read index is handed back the same way. No operation uses
//   memory_order_relaxed (or seq_cst): every cross-thread access is exactly
//   one release store or one acquire load.
//
//   Each side keeps a plain, thread-private copy of its own index and a cached
//   copy of the other side's index, so the common path performs a single
//   atomic operation (the publishing store) and never touches the other
//   thread's cache line. The opposite index is re-read only when the cached
//   value says the queue looks full (producer) or empty (consumer).
//
// Layout
//   Read-only fields, producer state and consumer state each sit on their own
//   64-byte line; slot storage is a separate pre-faulted mapping.
#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>

#include "arena.hpp"

namespace aether {

template <class T>
class SpscQueue {
    static_assert(std::is_trivially_copyable_v<T> && std::is_trivially_destructible_v<T>,
                  "SpscQueue slots are copied bytewise and never destroyed");
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free);

public:
    // Capacity is rounded up to a power of two (minimum 2).
    explicit SpscQueue(std::size_t min_capacity)
        : shared_{std::bit_ceil(min_capacity < 2 ? std::size_t{2} : min_capacity)},
          region_(shared_.capacity * sizeof(T)) {
        shared_.mask = shared_.capacity - 1;
        shared_.slots = static_cast<T*>(region_.data());
    }

    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;

    // Producer thread only. Returns false when the queue is full (backpressure);
    // the item is not enqueued and the queue is left untouched.
    [[nodiscard]] bool try_push(const T& item) noexcept {
        const std::uint64_t tail = producer_.tail_local;
        if (tail - producer_.head_cache == shared_.capacity) {
            producer_.head_cache = consumer_.head.load(std::memory_order_acquire);
            if (tail - producer_.head_cache == shared_.capacity) {
                return false;
            }
        }
        ::new (static_cast<void*>(shared_.slots + (tail & shared_.mask))) T(item);
        producer_.tail_local = tail + 1;
        producer_.tail.store(tail + 1, std::memory_order_release);  // publish
        return true;
    }

    // Consumer thread only. Returns false when the queue is empty.
    [[nodiscard]] bool try_pop(T& out) noexcept {
        const std::uint64_t head = consumer_.head_local;
        if (head == consumer_.tail_cache) {
            consumer_.tail_cache = producer_.tail.load(std::memory_order_acquire);
            if (head == consumer_.tail_cache) {
                return false;
            }
        }
        out = shared_.slots[head & shared_.mask];
        consumer_.head_local = head + 1;
        consumer_.head.store(head + 1, std::memory_order_release);  // free the slot
        return true;
    }

    [[nodiscard]] std::size_t capacity() const noexcept { return shared_.capacity; }

    // Any thread. A snapshot that may be stale by the time it returns.
    [[nodiscard]] std::size_t size_approx() const noexcept {
        const std::uint64_t head = consumer_.head.load(std::memory_order_acquire);
        const std::uint64_t tail = producer_.tail.load(std::memory_order_acquire);
        return static_cast<std::size_t>(tail - head);
    }

private:
    struct alignas(kCacheLine) Shared {  // written once in the constructor
        std::size_t capacity;
        std::size_t mask{0};
        T* slots{nullptr};
    };
    struct alignas(kCacheLine) Producer {
        std::atomic<std::uint64_t> tail{0};  // write index: next slot to fill
        std::uint64_t tail_local{0};         // producer's private copy of tail
        std::uint64_t head_cache{0};         // last read index the producer saw
    };
    struct alignas(kCacheLine) Consumer {
        std::atomic<std::uint64_t> head{0};  // read index: next slot to drain
        std::uint64_t head_local{0};         // consumer's private copy of head
        std::uint64_t tail_cache{0};         // last write index the consumer saw
    };
    static_assert(sizeof(Shared) == kCacheLine && sizeof(Producer) == kCacheLine &&
                  sizeof(Consumer) == kCacheLine);

    Shared shared_;
    Producer producer_;
    Consumer consumer_;
    Region region_;  // slot storage: its own pre-faulted, page-aligned mapping
};

}  // namespace aether
