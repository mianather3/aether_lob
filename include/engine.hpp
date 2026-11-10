// AetherLOB - ingestion pipeline: gateway thread -> SPSC queue -> matching thread.
//
//   Gateway (producer)                       Matching engine (consumer)
//   wire bytes --decode--> Command --push--> [SpscQueue] --pop--> OrderBook
//
// The OrderBook is touched by the matching thread only, so it needs no
// synchronisation. The two threads share nothing but the queue and a handful
// of atomic flags/counters (release stores, acquire loads; no locks).
//
// Producer contract: at most ONE thread at a time may act as the gateway.
// That is either the thread spawned by start_gateway(), or a caller-owned
// thread using ingest()/submit()/try_submit() - never both concurrently.
#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <thread>

#include "affinity.hpp"
#include "order_book.hpp"
#include "protocol.hpp"
#include "spsc_queue.hpp"

namespace aether {

[[nodiscard]] inline std::uint64_t now_ns() noexcept {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          std::chrono::steady_clock::now().time_since_epoch())
                                          .count());
}

// Spin-wait hint: tells the core this is a busy-wait loop.
inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(__arm__)
    __asm__ __volatile__("yield");
#endif
}

// Callbacks run synchronously on the matching thread, inside the hot loop.
// They must be cheap, must not block, and must not call back into the Engine.
class EngineListener {
public:
    virtual ~EngineListener() = default;
    // One call per fill, in execution order. `taker` is the command being applied.
    virtual void on_trade(const Trade& /*trade*/, const Command& /*taker*/) noexcept {}
    // One call per command, after all of its fills.
    virtual void on_processed(const Command& /*command*/, const AddResult& /*result*/) noexcept {}
};

struct EngineConfig {
    BookConfig book{};
    std::size_t queue_capacity = 1U << 16;  // rounded up to a power of two
    // Consecutive empty polls the matching thread busy-spins before it starts
    // yielding the core. Raise it on a dedicated, isolated core.
    std::uint32_t idle_spins_before_yield = 1U << 14;
    bool stamp_ingress = true;  // gateway writes Command::ingress_ns (one clock read per message)
    // Thread placement (see affinity.hpp; -1 = unpinned). When both are set
    // they must name different physical cores. gateway_core applies to the
    // thread spawned by start_gateway() and is the default for NetworkGateway.
    int matching_core = -1;
    int gateway_core = -1;
    bool log_affinity = true;  // one stderr line per pinned thread at start-up
};

struct IngestResult {
    std::size_t consumed;  // bytes fully decoded and enqueued
    std::size_t messages;  // commands enqueued
    // Ok: buffer fully consumed. NeedMoreData: a partial message is left at
    // `consumed` (or the engine is not running). Anything else: protocol error.
    DecodeStatus status;
};

struct GatewayStats {
    std::uint64_t messages{0};
    std::uint64_t bytes{0};
    DecodeStatus status{DecodeStatus::Ok};  // Ok on clean EOF / stop
    bool io_error{false};
};

class Engine {
public:
    // Throws std::invalid_argument if matching_core and gateway_core are both
    // set but cannot give the two threads separate physical cores.
    explicit Engine(const EngineConfig& config = {}, EngineListener* listener = nullptr);
    ~Engine();  // stop()s if still running

    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    // Spawns the matching thread and returns once it has applied (and
    // verified) its core affinity. No-op if already running.
    void start();

    // Graceful shutdown: stops and joins the gateway thread (if any), then lets
    // the matching thread drain every command already queued before it exits.
    // Idempotent. Must not be called concurrently with a caller-owned producer.
    void stop() noexcept;

    [[nodiscard]] bool running() const noexcept { return running_.load(std::memory_order_acquire); }

    // ---- Gateway API (single producer thread) -------------------------------

    // Non-blocking. False means the queue is full: the command was NOT enqueued.
    [[nodiscard]] bool try_submit(const Command& command) noexcept;

    // Lossless: under backpressure, spins until the matching thread frees a
    // slot. Returns false only if the queue is full and the engine is not
    // running (nothing would ever drain it).
    bool submit(const Command& command) noexcept;

    // Decodes every complete wire message in `bytes` and submit()s it.
    IngestResult ingest(std::span<const std::byte> bytes) noexcept;

    // Spawns the gateway thread: reads wire bytes from a POSIX fd (stream
    // socket, pipe, ...) until EOF, stop(), an I/O error or a protocol error.
    // The fd stays owned by the caller. Returns false if a gateway thread
    // already exists.
    bool start_gateway(int fd);

    // Blocks until the gateway thread has exited (e.g. the peer closed the
    // socket) and returns its counters.
    GatewayStats join_gateway();

    // ---- Observation ---------------------------------------------------------

    [[nodiscard]] std::uint64_t submitted() const noexcept {
        return submitted_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::uint64_t processed() const noexcept {
        return processed_.load(std::memory_order_acquire);
    }
    // Times the producer found the queue full and had to wait.
    [[nodiscard]] std::uint64_t backpressure_events() const noexcept {
        return backpressure_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::size_t queue_capacity() const noexcept { return queue_.capacity(); }
    [[nodiscard]] std::size_t queue_depth() const noexcept { return queue_.size_approx(); }
    [[nodiscard]] const EngineConfig& config() const noexcept { return config_; }

    // Outcome of pinning the matching thread; meaningful once start() has returned.
    [[nodiscard]] PinResult matching_affinity() const noexcept {
        return matching_ready_.load(std::memory_order_acquire) ? matching_pin_ : PinResult{};
    }

    // The book belongs to the matching thread: only read it after stop().
    [[nodiscard]] const OrderBook& book() const noexcept { return book_; }

private:
    void matching_loop() noexcept;
    void gateway_loop(int fd) noexcept;
    void apply(const Command& command) noexcept;

    const EngineConfig config_;
    EngineListener* listener_;

    SpscQueue<Command> queue_;
    OrderBook book_;  // matching thread only

    // Producer-side state.
    alignas(kCacheLine) std::atomic<std::uint64_t> submitted_{0};
    std::atomic<std::uint64_t> backpressure_{0};
    std::uint64_t submitted_local_{0};
    std::uint64_t backpressure_local_{0};
    GatewayStats gateway_stats_{};

    // Consumer-side state.
    alignas(kCacheLine) std::atomic<std::uint64_t> processed_{0};
    std::uint64_t processed_local_{0};

    // Control flags (cold).
    alignas(kCacheLine) std::atomic<bool> running_{false};
    std::atomic<bool> stop_matching_{false};
    std::atomic<bool> stop_gateway_{false};
    std::atomic<bool> matching_ready_{false};
    PinResult matching_pin_{};  // written by the matching thread before matching_ready_

    std::thread matching_thread_;
    std::thread gateway_thread_;
};

}  // namespace aether
