#include "engine.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <stdexcept>

#include <poll.h>
#include <unistd.h>

namespace aether {

namespace {

EngineListener& null_listener() noexcept {
    static EngineListener listener;
    return listener;
}

}  // namespace

Engine::Engine(const EngineConfig& config, EngineListener* listener)
    : config_(config),
      listener_(listener != nullptr ? listener : &null_listener()),
      queue_(config.queue_capacity),
      book_(config.book) {
    if (config.matching_core >= 0 && config.gateway_core >= 0 &&
        same_physical_core(config.matching_core, config.gateway_core)) {
        throw std::invalid_argument(
            "aether::Engine: matching_core and gateway_core must be different physical cores");
    }
}

Engine::~Engine() { stop(); }

void Engine::start() {
    if (matching_thread_.joinable()) {
        return;
    }
    stop_matching_.store(false, std::memory_order_release);
    stop_gateway_.store(false, std::memory_order_release);
    matching_ready_.store(false, std::memory_order_release);
    running_.store(true, std::memory_order_release);
    matching_thread_ = std::thread([this] { matching_loop(); });
    while (!matching_ready_.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
}

void Engine::stop() noexcept {
    // Order matters: silence the producer first, so that everything it pushed
    // happens-before the stop flag the matching thread will observe.
    stop_gateway_.store(true, std::memory_order_release);
    if (gateway_thread_.joinable()) {
        gateway_thread_.join();
    }
    stop_matching_.store(true, std::memory_order_release);
    if (matching_thread_.joinable()) {
        matching_thread_.join();
    }
    running_.store(false, std::memory_order_release);
}

// ---- Matching thread ----------------------------------------------------------

void Engine::apply(const Command& command) noexcept {
    const auto on_trade = [this, &command](const Trade& trade) noexcept {
        listener_->on_trade(trade, command);
    };

    AddResult result{Status::Ok, 0, 0};
    switch (command.type) {
        case CommandType::Add:
            result = book_.add_limit(command.order_id, command.side, command.price, command.qty, on_trade);
            break;
        case CommandType::Cancel:
            result.status = book_.cancel(command.order_id);
            break;
        case CommandType::Market:
            result = book_.add_market(command.side, command.qty, on_trade);
            break;
    }
    listener_->on_processed(command, result);
    processed_.store(++processed_local_, std::memory_order_release);
}

void Engine::matching_loop() noexcept {
    // Placement happens here, once, before the first poll: nothing below this
    // block ever looks at affinity again.
    matching_pin_ = pin_current_thread(config_.matching_core);
    if (config_.log_affinity && config_.matching_core >= 0) {
        log_pin(stderr, "matching-engine thread", matching_pin_);
    }
    matching_ready_.store(true, std::memory_order_release);

    Command command;
    std::uint32_t idle = 0;
    for (;;) {
        if (queue_.try_pop(command)) {
            apply(command);
            idle = 0;
            continue;
        }
        if (stop_matching_.load(std::memory_order_acquire)) {
            // The producer was quiesced before the flag was set, so one more
            // pass is guaranteed to see everything that was ever enqueued.
            while (queue_.try_pop(command)) {
                apply(command);
            }
            return;
        }
        if (idle < config_.idle_spins_before_yield) {
            ++idle;
            cpu_relax();
        } else {
            std::this_thread::yield();
        }
    }
}

// ---- Gateway (producer) --------------------------------------------------------

bool Engine::try_submit(const Command& command) noexcept {
    if (!queue_.try_push(command)) {
        return false;
    }
    submitted_.store(++submitted_local_, std::memory_order_release);
    return true;
}

bool Engine::submit(const Command& command) noexcept {
    if (try_submit(command)) [[likely]] {
        return true;
    }
    backpressure_.store(++backpressure_local_, std::memory_order_release);
    std::uint32_t spins = 0;
    while (!try_submit(command)) {
        if (!running_.load(std::memory_order_acquire)) {
            return false;
        }
        if (++spins < 1024) {
            cpu_relax();
        } else {
            std::this_thread::yield();
        }
    }
    return true;
}

IngestResult Engine::ingest(std::span<const std::byte> bytes) noexcept {
    IngestResult result{0, 0, DecodeStatus::Ok};
    Command command;
    while (result.consumed < bytes.size()) {
        const DecodeResult decoded = decode(bytes.subspan(result.consumed), command);
        if (decoded.status != DecodeStatus::Ok) {
            result.status = decoded.status;
            return result;
        }
        command.ingress_ns = config_.stamp_ingress ? now_ns() : 0;
        if (!submit(command)) {
            result.status = DecodeStatus::NeedMoreData;  // not enqueued: caller keeps the bytes
            return result;
        }
        result.consumed += decoded.consumed;
        ++result.messages;
    }
    return result;
}

bool Engine::start_gateway(int fd) {
    if (gateway_thread_.joinable()) {
        return false;
    }
    gateway_stats_ = {};
    stop_gateway_.store(false, std::memory_order_release);
    gateway_thread_ = std::thread([this, fd] { gateway_loop(fd); });
    return true;
}

GatewayStats Engine::join_gateway() {
    if (gateway_thread_.joinable()) {
        gateway_thread_.join();
    }
    return gateway_stats_;  // safe: the join orders the gateway's writes before this read
}

void Engine::gateway_loop(int fd) noexcept {
    // Fixed receive buffer on the gateway thread's stack; a message split
    // across two reads is carried over at the front.
    std::byte buffer[1U << 16];
    std::size_t have = 0;

    const PinResult pin = pin_current_thread(config_.gateway_core);
    if (config_.log_affinity && config_.gateway_core >= 0) {
        log_pin(stderr, "gateway thread", pin);
    }

    while (!stop_gateway_.load(std::memory_order_acquire)) {
        pollfd pfd{fd, POLLIN, 0};
        const int ready = ::poll(&pfd, 1, 20 /* ms: bounds shutdown latency */);
        if (ready == 0 || (ready < 0 && errno == EINTR)) {
            continue;
        }
        if (ready < 0) {
            gateway_stats_.io_error = true;
            return;
        }

        const ssize_t got = ::read(fd, buffer + have, sizeof buffer - have);
        if (got < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            gateway_stats_.io_error = true;
            return;
        }
        if (got == 0) {  // EOF: a leftover partial message means a truncated stream
            gateway_stats_.status = have == 0 ? DecodeStatus::Ok : DecodeStatus::NeedMoreData;
            return;
        }
        have += static_cast<std::size_t>(got);
        gateway_stats_.bytes += static_cast<std::uint64_t>(got);

        const IngestResult result = ingest(std::span<const std::byte>(buffer, have));
        gateway_stats_.messages += result.messages;
        if (result.status != DecodeStatus::Ok && result.status != DecodeStatus::NeedMoreData) {
            gateway_stats_.status = result.status;  // protocol error: drop the session
            return;
        }
        have -= result.consumed;
        std::memmove(buffer, buffer + result.consumed, have);
        if (!running_.load(std::memory_order_acquire)) {
            return;  // matching thread is gone: nothing will drain the queue
        }
    }
}

}  // namespace aether
