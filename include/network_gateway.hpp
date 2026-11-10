// AetherLOB - non-blocking network ingestion gateway (UDP datagrams + TCP streams).
//
//   sockets --(epoll | kqueue | poll)--> recv/read into pre-allocated buffers
//           --decode--> Command --> Engine SPSC queue --> matching thread
//
// The gateway is the Engine's single producer. All sockets are O_NONBLOCK and
// every buffer is reserved (and pre-faulted) in the constructor: the receive
// path performs no allocation and builds no strings.
//
// Framing
//   UDP  A datagram is a self-contained frame holding zero or more whole
//        messages. Decoding stops at the first invalid or incomplete message;
//        the valid prefix is applied, the rest of that datagram is discarded
//        and counted (udp_malformed / udp_truncated). Messages never span
//        datagrams, so one bad packet cannot desynchronise the next.
//   TCP  A byte stream with no boundaries. Each connection owns a contiguous
//        scratch buffer; bytes are read straight into it behind any carried
//        partial message, complete messages are decoded in place, and the
//        (< 22 byte) remainder is moved to the front. A message split across
//        any number of reads is therefore reassembled exactly. An invalid
//        message cannot be skipped (lengths are implied by the type tag), so
//        the connection is closed; everything before it has been applied.
//
// Backpressure
//   When the engine queue is full the gateway waits for the matching thread
//   instead of dropping. While it waits it does not read, so TCP peers are
//   flow-controlled by the kernel and UDP overflow is dropped by the kernel at
//   the socket buffer (size it with rcvbuf_bytes).
//
// Shutdown order: stop() the gateway (or destroy it) before Engine::stop().
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <thread>
#include <vector>

#include "affinity.hpp"
#include "arena.hpp"
#include "engine.hpp"

namespace aether {

enum class PollBackend : std::uint8_t {
    Auto,  // epoll on Linux, kqueue on macOS/BSD, otherwise poll
    Poll,  // portable poll(2)
};

struct NetworkGatewayConfig {
    const char* bind_address = "127.0.0.1";  // IPv4 dotted quad
    bool enable_udp = true;
    std::uint16_t udp_port = 0;  // 0 = ephemeral; read it back with udp_port()
    bool enable_tcp = true;
    std::uint16_t tcp_port = 0;
    int rcvbuf_bytes = 4 << 20;  // SO_RCVBUF request; 0 keeps the OS default
    std::uint32_t max_connections = 16;         // simultaneous TCP streams
    std::uint32_t stream_buffer_bytes = 1U << 16;  // per-connection scratch buffer (>= 4096)
    int listen_backlog = 64;
    PollBackend backend = PollBackend::Auto;
    // Readiness wait used by the gateway thread. 0 = pure non-blocking busy
    // polling (lowest latency, one core at 100%); > 0 sleeps in the kernel.
    int wait_timeout_ms = 0;
    int core = -1;  // -1 = use EngineConfig::gateway_core
};

struct NetworkGatewayStats {
    std::uint64_t messages;             // commands enqueued to the engine
    std::uint64_t udp_datagrams;
    std::uint64_t udp_bytes;
    std::uint64_t udp_malformed;        // datagrams cut short by an invalid message
    std::uint64_t udp_truncated;        // datagrams ending inside a message
    std::uint64_t tcp_accepted;
    std::uint64_t tcp_rejected;         // refused: max_connections reached
    std::uint64_t tcp_closed;           // accepted connections that have ended
    std::uint64_t tcp_bytes;
    std::uint64_t tcp_protocol_errors;  // streams closed on an invalid message
    std::uint64_t tcp_truncated;        // streams that ended inside a message
    std::uint64_t dropped;              // frames discarded because the engine was not running
    std::uint64_t io_errors;
};

class NetworkGateway {
public:
    // Creates, tunes and binds the sockets. Throws std::system_error on any
    // socket failure and std::invalid_argument on a bad configuration.
    explicit NetworkGateway(Engine& engine, const NetworkGatewayConfig& config = {});
    ~NetworkGateway();  // stop()s and closes every socket

    NetworkGateway(const NetworkGateway&) = delete;
    NetworkGateway& operator=(const NetworkGateway&) = delete;

    // Spawns the gateway thread: pins it, then runs the readiness loop until stop().
    void start();
    void stop() noexcept;  // idempotent
    [[nodiscard]] bool threaded() const noexcept { return threaded_; }

    // ---- Caller-driven mode (only while no gateway thread is running) -------
    // The calling thread acts as the gateway / engine producer.

    // One readiness pass: waits up to timeout_ms (0 = return immediately),
    // then drains every ready socket. Returns the number of ready sockets.
    int poll_once(int timeout_ms = 0) noexcept;

    // Transport-independent entry points: exactly what the socket loop runs
    // after recv()/read(). They let frames from another source (kernel-bypass
    // NIC, shared memory, a replay file, a fuzzer) take the same code path.
    void on_datagram(std::span<const std::byte> datagram) noexcept;
    [[nodiscard]] int open_stream() noexcept;  // returns a stream id, or -1 if none is free
    // Appends bytes to the stream. Returns false once the stream has been
    // closed by a protocol error (the id is then free for reuse).
    bool feed_stream(int stream, std::span<const std::byte> bytes) noexcept;
    void close_stream(int stream) noexcept;  // end of stream (EOF)

    // ---- Observation (any thread) ---------------------------------------------
    [[nodiscard]] NetworkGatewayStats stats() const noexcept;
    [[nodiscard]] std::uint16_t udp_port() const noexcept { return udp_port_; }
    [[nodiscard]] std::uint16_t tcp_port() const noexcept { return tcp_port_; }
    [[nodiscard]] int udp_rcvbuf() const noexcept { return udp_rcvbuf_; }  // as granted by the OS
    [[nodiscard]] int tcp_rcvbuf() const noexcept { return tcp_rcvbuf_; }
    [[nodiscard]] const char* backend_name() const noexcept;
    // Outcome of pinning the gateway thread; meaningful once start() has returned.
    [[nodiscard]] PinResult affinity() const noexcept {
        return ready_.load(std::memory_order_acquire) ? pin_ : PinResult{};
    }

private:
    class Poller;
    enum Counter : std::size_t {
        kMessages, kUdpDatagrams, kUdpBytes, kUdpMalformed, kUdpTruncated, kTcpAccepted,
        kTcpRejected, kTcpClosed, kTcpBytes, kTcpProtocolErrors, kTcpTruncated, kDropped,
        kIoErrors, kCounterCount
    };
    struct Connection {
        std::byte* buffer{nullptr};
        std::uint32_t have{0};  // bytes of a partial message carried at the front
        int fd{-1};             // -1 for an in-process stream
        bool active{false};
    };

    void thread_main() noexcept;
    int run_once(int timeout_ms) noexcept;
    void drain_udp() noexcept;
    void accept_ready() noexcept;
    void drain_tcp(Connection& connection) noexcept;
    bool consume_stream(Connection& connection) noexcept;
    void end_stream(Connection& connection) noexcept;
    int allocate_connection(int fd) noexcept;
    void publish() noexcept;
    void close_sockets() noexcept;

    Engine& engine_;
    const NetworkGatewayConfig config_;

    Region buffers_;  // [datagram buffer][connection 0 scratch][connection 1 scratch]...
    std::byte* datagram_buffer_;
    std::vector<Connection> connections_;  // sized once in the constructor
    std::unique_ptr<Poller> poller_;

    int udp_fd_{-1};
    int listen_fd_{-1};
    std::uint16_t udp_port_{0};
    std::uint16_t tcp_port_{0};
    int udp_rcvbuf_{0};
    int tcp_rcvbuf_{0};

    // Counters: plain values owned by the gateway thread, mirrored into
    // atomics once per pass so readers never contend with the receive loop.
    std::uint64_t local_[kCounterCount]{};
    alignas(kCacheLine) std::atomic<std::uint64_t> shared_[kCounterCount]{};

    alignas(kCacheLine) std::atomic<bool> stop_{false};
    std::atomic<bool> ready_{false};
    PinResult pin_{};
    // Written by the controlling thread only: before the gateway thread is
    // created and after it is joined.
    bool threaded_{false};
    std::thread thread_;
};

}  // namespace aether
