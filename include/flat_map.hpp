// AetherLOB - fixed-capacity open-addressing hash index (u64 key -> T*).
//
// Linear probing at <= 50% load with backward-shift deletion (no tombstones),
// backed by a pre-faulted Region. Never allocates or rehashes after
// construction, so lookup cost stays flat for the lifetime of the book.
#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <new>

#include "arena.hpp"

namespace aether {

template <class T>
class FlatMap {
public:
    explicit FlatMap(std::size_t max_entries)
        : capacity_(std::bit_ceil(max_entries < 8 ? std::size_t{16} : max_entries * 2)),
          region_(capacity_ * sizeof(Slot)),
          slots_(static_cast<Slot*>(region_.data())),
          mask_(capacity_ - 1),
          shift_(static_cast<unsigned>(64 - std::countr_zero(capacity_))) {
        for (std::size_t i = 0; i < capacity_; ++i) {
            ::new (static_cast<void*>(slots_ + i)) Slot{};
        }
    }

    [[nodiscard]] T* find(std::uint64_t key) const noexcept {
        for (std::size_t i = home(key);; i = (i + 1) & mask_) {
            const Slot& s = slots_[i];
            if (s.value == nullptr) {
                return nullptr;
            }
            if (s.key == key) {
                return s.value;
            }
        }
    }

    // Preconditions: key is absent, value != nullptr, size() < max_entries.
    void insert(std::uint64_t key, T* value) noexcept {
        std::size_t i = home(key);
        while (slots_[i].value != nullptr) {
            i = (i + 1) & mask_;
        }
        slots_[i] = Slot{key, value};
        ++size_;
    }

    bool erase(std::uint64_t key) noexcept {
        std::size_t i = home(key);
        for (;; i = (i + 1) & mask_) {
            if (slots_[i].value == nullptr) {
                return false;
            }
            if (slots_[i].key == key) {
                break;
            }
        }
        // Backward shift: pull later members of the probe chain into the hole
        // unless that would move them in front of their home slot.
        for (std::size_t j = (i + 1) & mask_; slots_[j].value != nullptr; j = (j + 1) & mask_) {
            const std::size_t dist_from_home = (j - home(slots_[j].key)) & mask_;
            if (dist_from_home >= ((j - i) & mask_)) {
                slots_[i] = slots_[j];
                i = j;
            }
        }
        slots_[i].value = nullptr;
        --size_;
        return true;
    }

    [[nodiscard]] std::size_t size() const noexcept { return size_; }

private:
    struct Slot {
        std::uint64_t key{0};
        T* value{nullptr};  // nullptr marks an empty slot
    };

    [[nodiscard]] std::size_t home(std::uint64_t key) const noexcept {
        return static_cast<std::size_t>((key * 0x9E3779B97F4A7C15ULL) >> shift_);
    }

    std::size_t capacity_;
    Region region_;
    Slot* slots_;
    std::size_t mask_;
    unsigned shift_;
    std::size_t size_{0};
};

}  // namespace aether
