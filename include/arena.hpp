// AetherLOB - pre-allocated memory arena.
//
// All memory is reserved, zeroed and pre-faulted once at construction (cold
// path, src/arena.cpp). After that, acquire()/release() are a handful of
// instructions each and never call new/malloc or enter the kernel.
#pragma once

#include <cstddef>
#include <cstdint>
#include <new>
#include <stdexcept>
#include <type_traits>

namespace aether {

inline constexpr std::size_t kCacheLine = 64;

// RAII owner of a page-aligned, zero-filled, pre-faulted anonymous mapping.
class Region {
public:
    explicit Region(std::size_t bytes);  // throws std::bad_alloc / std::invalid_argument
    ~Region();

    Region(const Region&) = delete;
    Region& operator=(const Region&) = delete;

    [[nodiscard]] void* data() const noexcept { return data_; }
    [[nodiscard]] std::size_t size() const noexcept { return bytes_; }

private:
    void* data_{nullptr};
    std::size_t bytes_{0};
};

// Fixed-capacity object pool. Slots are handed out from a LIFO free stack so
// the most recently released (cache-warm) node is reused first.
template <class T>
class Pool {
    static_assert(std::is_trivially_destructible_v<T>,
                  "Pool nodes are recycled without running destructors");
    static_assert(alignof(T) <= 4096, "Pool relies on page alignment of the backing region");

public:
    explicit Pool(std::uint32_t capacity)
        : slots_region_(checked(capacity) * sizeof(T)),
          free_region_(static_cast<std::size_t>(capacity) * sizeof(std::uint32_t)),
          slots_(static_cast<T*>(slots_region_.data())),
          free_(static_cast<std::uint32_t*>(free_region_.data())),
          capacity_(capacity),
          free_count_(capacity) {
        // Top of stack is slot 0, so a fresh pool hands out memory sequentially.
        for (std::uint32_t i = 0; i < capacity; ++i) {
            ::new (static_cast<void*>(free_ + i)) std::uint32_t(capacity - 1 - i);
        }
    }

    // Returns a value-initialised node, or nullptr when the pool is exhausted.
    [[nodiscard]] T* acquire() noexcept {
        if (free_count_ == 0) [[unlikely]] {
            return nullptr;
        }
        return ::new (static_cast<void*>(slots_ + free_[--free_count_])) T();
    }

    void release(T* node) noexcept {
        free_[free_count_++] = static_cast<std::uint32_t>(node - slots_);
    }

    [[nodiscard]] bool owns(const T* node) const noexcept {
        return node >= slots_ && node < slots_ + capacity_;
    }
    [[nodiscard]] bool full() const noexcept { return free_count_ == 0; }
    [[nodiscard]] std::uint32_t capacity() const noexcept { return capacity_; }
    [[nodiscard]] std::uint32_t in_use() const noexcept { return capacity_ - free_count_; }

private:
    static std::size_t checked(std::uint32_t capacity) {
        if (capacity == 0) {
            throw std::invalid_argument("aether::Pool capacity must be > 0");
        }
        return capacity;
    }

    Region slots_region_;
    Region free_region_;
    T* slots_;
    std::uint32_t* free_;
    std::uint32_t capacity_;
    std::uint32_t free_count_;
};

}  // namespace aether
