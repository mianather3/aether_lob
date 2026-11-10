// AetherLOB - asynchronous L2 market snapshots and metric reports.
//
// The order book belongs to the matching thread and is never locked. An
// exporter thread that wants the current depth raises an atomic request; the
// matching thread notices it on an idle spin (or, under sustained load, at its
// next periodic telemetry flush, at most MatchingCounters::kPublishInterval
// commands later), copies the top N levels of each side into the spare half
// of a pre-allocated double buffer, and flips the "front" index. The exporter
// then reads the front half.
//
// The matching thread's whole cost is one atomic load per idle spin plus, per
// request served, a walk of at most 2 x kMaxSnapshotDepth price levels. It
// never waits for an exporter, takes no lock and allocates nothing.
//
// Exporters serialise among themselves (a mutex the matching thread never
// touches), so exactly one request is outstanding at a time. That is what
// makes the hand-off race-free: the matching thread writes the back buffer
// only while the exporter is waiting, and the exporter reads the front buffer
// only after the flip has been published.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <span>

#include "arena.hpp"
#include "order_book.hpp"
#include "telemetry.hpp"

namespace aether {

inline constexpr std::uint32_t kMaxSnapshotDepth = 32;

struct L2Level {
    Price price{0};
    Qty qty{0};             // total open quantity at this price
    Qty cumulative_qty{0};  // this level plus every better level on the same side
    std::uint32_t orders{0};
};

struct L2Snapshot {
    std::uint64_t sequence{0};        // commands the engine had applied when this was taken
    std::uint64_t resting_orders{0};  // whole book, not just the levels shown
    std::uint64_t level_count{0};
    std::uint32_t bid_levels{0};
    std::uint32_t ask_levels{0};
    L2Level bids[kMaxSnapshotDepth];  // best (highest) first
    L2Level asks[kMaxSnapshotDepth];  // best (lowest) first
};

// Copies the top `depth` levels of each side. The caller must own the book
// (be the matching thread, or hold a stopped engine).
void capture_l2(const OrderBook& book, std::uint32_t depth, std::uint64_t sequence, L2Snapshot& out) noexcept;

// The request flag and the double buffer.
class SnapshotChannel {
public:
    // ---- matching thread ----
    [[nodiscard]] bool requested() const noexcept {
        return requested_.load(std::memory_order_acquire) != served_local_;
    }
    // Captures into the back buffer, flips, and marks every request so far served.
    void serve(const OrderBook& book, std::uint32_t depth, std::uint64_t sequence) noexcept;

    // ---- exporter side (one request at a time; Engine::snapshot() enforces it) ----
    [[nodiscard]] std::uint64_t request() noexcept {
        const std::uint64_t ticket = requested_.load(std::memory_order_acquire) + 1;
        requested_.store(ticket, std::memory_order_release);
        return ticket;
    }
    [[nodiscard]] bool served(std::uint64_t ticket) const noexcept {
        return served_.load(std::memory_order_acquire) >= ticket;
    }
    [[nodiscard]] const L2Snapshot& front() const noexcept {
        return buffers_[front_.load(std::memory_order_acquire)];
    }
    [[nodiscard]] std::uint64_t served_count() const noexcept { return served_.load(std::memory_order_acquire); }

private:
    alignas(kCacheLine) std::atomic<std::uint64_t> requested_{0};  // written by exporters
    alignas(kCacheLine) std::atomic<std::uint64_t> served_{0};     // written by the matching thread
    std::atomic<std::uint32_t> front_{0};
    std::uint64_t served_local_{0};
    L2Snapshot buffers_[2];
};

// ---- Serialisation (cold path; caller-supplied buffers, no heap) ---------------------
// The text writers follow snprintf: they return the length the full output
// needs (excluding the terminator) and never write more than `capacity` bytes.

// {"sequence":..,"resting_orders":..,"levels":..,"bids":[{"price":..,"qty":..,"cum_qty":..,"orders":..},..],"asks":[..]}
std::size_t write_json(const L2Snapshot& snapshot, char* out, std::size_t capacity) noexcept;

// Compact little-endian record:
//   magic "AL2S"(4) version u16 reserved u16 sequence u64 resting_orders u64
//   level_count u64 bid_levels u32 ask_levels u32
//   then bid_levels + ask_levels x { price i64, qty u64, orders u32 }
// Returns the bytes written, or 0 if `out` is too small.
std::size_t write_binary(const L2Snapshot& snapshot, std::span<std::byte> out) noexcept;
[[nodiscard]] bool read_binary(std::span<const std::byte> in, L2Snapshot& out) noexcept;
[[nodiscard]] constexpr std::size_t binary_size(std::uint32_t bid_levels, std::uint32_t ask_levels) noexcept {
    return 40 + 20 * (static_cast<std::size_t>(bid_levels) + ask_levels);
}

// Counters plus p50/p90/p99/p99.9/p99.99/max for every latency histogram.
std::size_t write_metrics_json(const TelemetrySnapshot& metrics, char* out, std::size_t capacity) noexcept;
void print_metrics(std::FILE* out, const TelemetrySnapshot& metrics) noexcept;
void print_snapshot(std::FILE* out, const L2Snapshot& snapshot, std::uint32_t max_levels = 5) noexcept;

}  // namespace aether
