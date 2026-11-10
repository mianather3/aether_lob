// AetherLOB - in-memory telemetry: counters and latency histograms.
//
// Everything here is single-writer. Each counter block and each histogram is
// written by exactly one pipeline thread and may be read by any number of
// others at any time. A write is one plain load and one store of a value only
// the writer changes: wait-free, bounded, no lock prefix, no allocation.
// Blocks are cache-line aligned so the gateway's counters, the matching
// thread's counters and the histograms never share a line.
//
// Readers get each value atomically but not a cross-field snapshot: counters
// read while the pipeline is running can be a few commands apart.
#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>

#include "arena.hpp"

namespace aether {

// A value written by one thread and read by any.
class Counter {
public:
    void add(std::uint64_t n) noexcept {  // writer thread only
        value_.store(value_.load(std::memory_order_acquire) + n, std::memory_order_release);
    }
    void set(std::uint64_t v) noexcept { value_.store(v, std::memory_order_release); }
    [[nodiscard]] std::uint64_t read() const noexcept { return value_.load(std::memory_order_acquire); }

private:
    std::atomic<std::uint64_t> value_{0};
};
static_assert(sizeof(Counter) == 8);

// Written by the gateway (producer) thread.
struct alignas(kCacheLine) GatewayCounters {
    Counter received;            // messages decoded from the wire and offered to the engine queue
    Counter dropped_queue_full;  // messages NOT enqueued because the queue was full
    Counter invalid_bytes;       // wire bytes discarded: malformed, or a truncated trailing message
    Counter checksum_errors;     // reserved: the current wire format carries no checksum, so always 0
    Counter packets;             // ingest() calls: datagrams / stream reads
    Counter bytes;               // wire bytes decoded into messages
    Counter backpressure_waits;  // times the producer found the queue full and waited
};

// Written by the matching thread, batched: published when the queue runs
// empty and at least every kPublishInterval commands.
struct alignas(kCacheLine) MatchingCounters {
    static constexpr std::uint32_t kPublishInterval = 256;
    Counter accepted;        // commands the book applied (Status::Ok)
    Counter rejected;        // commands the book refused (bad qty, duplicate id, arena full, unknown id)
    Counter trades;          // fills
    Counter volume;          // total quantity matched
    Counter resting_orders;  // gauge: orders on the book
    Counter levels;          // gauge: live price levels, both sides
};
static_assert(sizeof(GatewayCounters) == kCacheLine && sizeof(MatchingCounters) == kCacheLine);

struct HistogramSummary {
    std::uint64_t count{0};
    std::uint64_t min{0};
    std::uint64_t max{0};
    double mean{0.0};
    std::uint64_t p50{0};
    std::uint64_t p90{0};
    std::uint64_t p99{0};
    std::uint64_t p999{0};
    std::uint64_t p9999{0};
};

// Log-linear (HDR-style) histogram of nanosecond latencies.
//
// Values below 64 ns get one bucket each. Above that, every power of two is
// split into 32 equal sub-buckets, so a reported percentile is never more
// than 1/32 (3.125%) above the true value at any magnitude. 1,024 buckets
// (8 KiB, fixed, inside the object) cover 0 ns to 2^36 ns (~68.7 s); larger
// values are clamped into the last bucket. Minimum, maximum and mean are
// tracked exactly.
class alignas(kCacheLine) LatencyHistogram {
public:
    static constexpr unsigned kSubBucketBits = 5;
    static constexpr std::size_t kBucketCount = 1024;
    static constexpr std::uint64_t kMaxTrackable = (1ULL << 36) - 1;

    [[nodiscard]] static constexpr std::size_t index_of(std::uint64_t value) noexcept {
        if (value > kMaxTrackable) {
            value = kMaxTrackable;
        }
        if (value < (2ULL << kSubBucketBits)) {
            return static_cast<std::size_t>(value);
        }
        const unsigned shift = static_cast<unsigned>(std::bit_width(value)) - 1 - kSubBucketBits;
        return (static_cast<std::size_t>(shift) << kSubBucketBits) + static_cast<std::size_t>(value >> shift);
    }
    // Smallest and largest value that land in bucket `index`.
    [[nodiscard]] static constexpr std::uint64_t lower_bound(std::size_t index) noexcept {
        if (index < (2U << kSubBucketBits)) {
            return index;
        }
        const unsigned shift = static_cast<unsigned>(index >> kSubBucketBits) - 1;
        return ((1ULL << kSubBucketBits) + (index & ((1U << kSubBucketBits) - 1))) << shift;
    }
    [[nodiscard]] static constexpr std::uint64_t upper_bound(std::size_t index) noexcept {
        if (index < (2U << kSubBucketBits)) {
            return index;
        }
        return lower_bound(index) + (1ULL << (static_cast<unsigned>(index >> kSubBucketBits) - 1)) - 1;
    }

    // Writer thread only. Three single-writer updates; no branches on the common path
    // beyond the min/max checks.
    void record(std::uint64_t ns) noexcept {
        std::atomic<std::uint64_t>& bucket = buckets_[index_of(ns)];
        bucket.store(bucket.load(std::memory_order_acquire) + 1, std::memory_order_release);
        sum_.store(sum_.load(std::memory_order_acquire) + ns, std::memory_order_release);
        if (ns > max_.load(std::memory_order_acquire)) {
            max_.store(ns, std::memory_order_release);
        }
        if (ns < min_.load(std::memory_order_acquire)) {
            min_.store(ns, std::memory_order_release);
        }
    }

    // Any thread. While the writer is active the result is a consistent-enough
    // view: every bucket is read atomically, the set of buckets is not.
    [[nodiscard]] HistogramSummary summarize() const noexcept {
        std::uint64_t counts[kBucketCount];
        std::uint64_t total = 0;
        for (std::size_t i = 0; i < kBucketCount; ++i) {
            counts[i] = buckets_[i].load(std::memory_order_acquire);
            total += counts[i];
        }
        HistogramSummary s;
        s.count = total;
        if (total == 0) {
            return s;
        }
        s.max = max_.load(std::memory_order_acquire);
        s.min = min_.load(std::memory_order_acquire);
        if (s.min > s.max) {
            s.min = s.max;  // a reader caught the very first record half-published
        }
        s.mean = static_cast<double>(sum_.load(std::memory_order_acquire)) / static_cast<double>(total);

        // A percentile is the upper edge of the bucket holding that rank,
        // capped at the exact maximum.
        const auto value_at = [&](double fraction) {
            std::uint64_t rank = static_cast<std::uint64_t>(fraction * static_cast<double>(total) + 0.999999);
            if (rank < 1) rank = 1;
            if (rank > total) rank = total;
            std::uint64_t seen = 0;
            for (std::size_t i = 0; i < kBucketCount; ++i) {
                seen += counts[i];
                if (seen >= rank) {
                    const std::uint64_t edge = upper_bound(i);
                    return edge < s.max ? edge : s.max;
                }
            }
            return s.max;
        };
        s.p50 = value_at(0.50);
        s.p90 = value_at(0.90);
        s.p99 = value_at(0.99);
        s.p999 = value_at(0.999);
        s.p9999 = value_at(0.9999);
        return s;
    }

    [[nodiscard]] std::uint64_t count() const noexcept {
        std::uint64_t total = 0;
        for (const auto& bucket : buckets_) {
            total += bucket.load(std::memory_order_acquire);
        }
        return total;
    }

private:
    std::atomic<std::uint64_t> sum_{0};
    std::atomic<std::uint64_t> max_{0};
    std::atomic<std::uint64_t> min_{UINT64_MAX};
    std::atomic<std::uint64_t> buckets_[kBucketCount]{};
};
static_assert(LatencyHistogram::index_of(LatencyHistogram::kMaxTrackable) == LatencyHistogram::kBucketCount - 1);

// Plain-value copy of everything, for reports.
struct TelemetrySnapshot {
    std::uint64_t received, accepted, rejected, dropped_queue_full, invalid_bytes, checksum_errors;
    std::uint64_t packets, bytes, backpressure_waits;
    std::uint64_t trades, volume, resting_orders, levels;
    HistogramSummary ingest_to_queue, queue_to_match, match_service, tick_to_trade;
};

struct Telemetry {
    GatewayCounters gateway;
    MatchingCounters matching;

    // All four are recorded by the matching thread when a stamped command completes.
    LatencyHistogram ingest_to_queue;  // packet received by the gateway -> command pushed to the queue
    LatencyHistogram queue_to_match;   // pushed to the queue -> match completed (queue wait + matching)
    LatencyHistogram match_service;    // popped by the matching thread -> match completed
    LatencyHistogram tick_to_trade;    // packet received -> match completed, commands that traded only

    [[nodiscard]] TelemetrySnapshot snapshot() const noexcept {
        return {gateway.received.read(),
                matching.accepted.read(),
                matching.rejected.read(),
                gateway.dropped_queue_full.read(),
                gateway.invalid_bytes.read(),
                gateway.checksum_errors.read(),
                gateway.packets.read(),
                gateway.bytes.read(),
                gateway.backpressure_waits.read(),
                matching.trades.read(),
                matching.volume.read(),
                matching.resting_orders.read(),
                matching.levels.read(),
                ingest_to_queue.summarize(),
                queue_to_match.summarize(),
                match_service.summarize(),
                tick_to_trade.summarize()};
    }
};

}  // namespace aether
