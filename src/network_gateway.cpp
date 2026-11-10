#include "network_gateway.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <system_error>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

// AETHER_HAVE_EPOLL / AETHER_HAVE_KQUEUE come from CMake's header checks; fall
// back to the platform macros when the file is built some other way.
#if !defined(AETHER_HAVE_EPOLL) && !defined(AETHER_HAVE_KQUEUE)
#if defined(__linux__)
#define AETHER_HAVE_EPOLL 1
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
#define AETHER_HAVE_KQUEUE 1
#endif
#endif

#if defined(AETHER_HAVE_EPOLL)
#include <sys/epoll.h>
#elif defined(AETHER_HAVE_KQUEUE)
#include <sys/event.h>
#include <sys/time.h>
#endif

namespace aether {

namespace {

constexpr std::size_t kDatagramBufferBytes = 1U << 16;  // >= the largest UDP payload (65,507)
constexpr int kMaxEventsPerWait = 64;
constexpr int kMaxDatagramsPerWake = 64;  // fairness bounds: one busy socket
constexpr int kMaxReadsPerWake = 8;       // cannot starve the others
constexpr std::uint32_t kUdpToken = 0;
constexpr std::uint32_t kListenToken = 1;
constexpr std::uint32_t kFirstConnectionToken = 2;

[[noreturn]] void throw_errno(const char* what) {
    throw std::system_error(errno, std::generic_category(), what);
}

void set_nonblocking_cloexec(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        throw_errno("fcntl(O_NONBLOCK)");
    }
    const int fd_flags = ::fcntl(fd, F_GETFD, 0);
    if (fd_flags < 0 || ::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC) < 0) {
        throw_errno("fcntl(FD_CLOEXEC)");
    }
}

// Requests SO_RCVBUF and returns what the OS actually granted (it may clamp).
int tune_rcvbuf(int fd, int requested) noexcept {
    if (requested > 0) {
        ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &requested, sizeof requested);
    }
    int granted = 0;
    socklen_t len = sizeof granted;
    ::getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &granted, &len);
    return granted;
}

sockaddr_in make_address(const char* ip, std::uint16_t port) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    if (::inet_pton(AF_INET, ip, &address.sin_addr) != 1) {
        throw std::invalid_argument("aether::NetworkGateway: bind_address is not an IPv4 address");
    }
    return address;
}

std::uint16_t bound_port(int fd) {
    sockaddr_in address{};
    socklen_t len = sizeof address;
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &len) != 0) {
        throw_errno("getsockname");
    }
    return ntohs(address.sin_port);
}

bool would_block(int error) noexcept { return error == EAGAIN || error == EWOULDBLOCK; }

}  // namespace

// ---- Readiness multiplexer -------------------------------------------------------
// Level-triggered on every backend: a socket that still has data after a
// bounded drain is simply reported again by the next wait().
class NetworkGateway::Poller {
public:
    Poller(PollBackend requested, std::size_t max_fds) {
#if defined(AETHER_HAVE_EPOLL)
        if (requested == PollBackend::Auto) {
            kernel_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
            if (kernel_fd_ < 0) throw_errno("epoll_create1");
            name_ = "epoll";
            return;
        }
#elif defined(AETHER_HAVE_KQUEUE)
        if (requested == PollBackend::Auto) {
            kernel_fd_ = ::kqueue();
            if (kernel_fd_ < 0) throw_errno("kqueue");
            name_ = "kqueue";
            return;
        }
#endif
        (void)requested;
        name_ = "poll";
        pollfds_.reserve(max_fds);
        tokens_.reserve(max_fds);
    }

    ~Poller() {
        if (kernel_fd_ >= 0) ::close(kernel_fd_);
    }

    Poller(const Poller&) = delete;
    Poller& operator=(const Poller&) = delete;

    [[nodiscard]] const char* name() const noexcept { return name_; }

    bool add(int fd, std::uint32_t token) noexcept {
        if (kernel_fd_ >= 0) {
#if defined(AETHER_HAVE_EPOLL)
            epoll_event event{};
            event.events = EPOLLIN;
            event.data.u32 = token;
            return ::epoll_ctl(kernel_fd_, EPOLL_CTL_ADD, fd, &event) == 0;
#elif defined(AETHER_HAVE_KQUEUE)
            struct kevent change;
            EV_SET(&change, fd, EVFILT_READ, EV_ADD, 0, 0,
                   reinterpret_cast<void*>(static_cast<std::uintptr_t>(token)));
            return ::kevent(kernel_fd_, &change, 1, nullptr, 0, nullptr) == 0;
#endif
        }
        if (pollfds_.size() == pollfds_.capacity()) return false;  // never reallocate
        pollfds_.push_back(pollfd{fd, POLLIN, 0});
        tokens_.push_back(token);
        return true;
    }

    void remove(int fd) noexcept {
        if (kernel_fd_ >= 0) {
#if defined(AETHER_HAVE_EPOLL)
            ::epoll_ctl(kernel_fd_, EPOLL_CTL_DEL, fd, nullptr);
#elif defined(AETHER_HAVE_KQUEUE)
            struct kevent change;
            EV_SET(&change, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
            ::kevent(kernel_fd_, &change, 1, nullptr, 0, nullptr);
#endif
            return;
        }
        for (std::size_t i = 0; i < pollfds_.size(); ++i) {
            if (pollfds_[i].fd == fd) {
                pollfds_[i] = pollfds_.back();
                tokens_[i] = tokens_.back();
                pollfds_.pop_back();
                tokens_.pop_back();
                return;
            }
        }
    }

    // Writes up to `max` ready tokens; returns how many (0 on timeout or EINTR).
    int wait(std::uint32_t* ready, int max, int timeout_ms) noexcept {
        if (max > kMaxEventsPerWait) max = kMaxEventsPerWait;
        if (kernel_fd_ >= 0) {
#if defined(AETHER_HAVE_EPOLL)
            epoll_event events[kMaxEventsPerWait];
            const int n = ::epoll_wait(kernel_fd_, events, max, timeout_ms);
            for (int i = 0; i < n; ++i) ready[i] = events[i].data.u32;
            return n > 0 ? n : 0;
#elif defined(AETHER_HAVE_KQUEUE)
            struct kevent events[kMaxEventsPerWait];
            timespec timeout{timeout_ms / 1000, static_cast<long>(timeout_ms % 1000) * 1'000'000L};
            const int n = ::kevent(kernel_fd_, nullptr, 0, events, max, timeout_ms < 0 ? nullptr : &timeout);
            for (int i = 0; i < n; ++i) {
                ready[i] = static_cast<std::uint32_t>(reinterpret_cast<std::uintptr_t>(events[i].udata));
            }
            return n > 0 ? n : 0;
#endif
        }
        if (::poll(pollfds_.data(), static_cast<nfds_t>(pollfds_.size()), timeout_ms) <= 0) {
            return 0;
        }
        // Start the scan one past where the last one began, so that a full
        // batch never keeps favouring the lowest-numbered sockets.
        const std::size_t count = pollfds_.size();
        int n = 0;
        for (std::size_t k = 0; k < count && n < max; ++k) {
            const std::size_t i = (scan_start_ + k) % count;
            if (pollfds_[i].revents != 0) ready[n++] = tokens_[i];
        }
        scan_start_ = count != 0 ? (scan_start_ + 1) % count : 0;
        return n;
    }

private:
    int kernel_fd_{-1};
    const char* name_{"poll"};
    std::vector<pollfd> pollfds_;
    std::vector<std::uint32_t> tokens_;
    std::size_t scan_start_{0};
};

// ---- Construction / teardown -----------------------------------------------------

NetworkGateway::NetworkGateway(Engine& engine, const NetworkGatewayConfig& config)
    : engine_(engine),
      config_(config),
      buffers_(kDatagramBufferBytes +
               static_cast<std::size_t>(config.max_connections) * config.stream_buffer_bytes),
      datagram_buffer_(static_cast<std::byte*>(buffers_.data())),
      connections_(config.max_connections) {
    if (config.max_connections == 0 || config.stream_buffer_bytes < 4096) {
        throw std::invalid_argument(
            "aether::NetworkGateway: need max_connections >= 1 and stream_buffer_bytes >= 4096");
    }
    for (std::size_t i = 0; i < connections_.size(); ++i) {
        connections_[i].buffer = datagram_buffer_ + kDatagramBufferBytes + i * config.stream_buffer_bytes;
    }
    poller_ = std::make_unique<Poller>(config.backend, connections_.size() + kFirstConnectionToken);

    try {
        if (config.enable_udp) {
            udp_fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
            if (udp_fd_ < 0) throw_errno("socket(SOCK_DGRAM)");
            set_nonblocking_cloexec(udp_fd_);
            udp_rcvbuf_ = tune_rcvbuf(udp_fd_, config.rcvbuf_bytes);
            const sockaddr_in address = make_address(config.bind_address, config.udp_port);
            if (::bind(udp_fd_, reinterpret_cast<const sockaddr*>(&address), sizeof address) != 0) {
                throw_errno("bind(udp)");
            }
            udp_port_ = bound_port(udp_fd_);
            if (!poller_->add(udp_fd_, kUdpToken)) throw_errno("poller add(udp)");
        }
        if (config.enable_tcp) {
            listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
            if (listen_fd_ < 0) throw_errno("socket(SOCK_STREAM)");
            set_nonblocking_cloexec(listen_fd_);
            const int on = 1;
            ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof on);
            // Set before listen(): accepted sockets inherit the receive buffer.
            tcp_rcvbuf_ = tune_rcvbuf(listen_fd_, config.rcvbuf_bytes);
            const sockaddr_in address = make_address(config.bind_address, config.tcp_port);
            if (::bind(listen_fd_, reinterpret_cast<const sockaddr*>(&address), sizeof address) != 0) {
                throw_errno("bind(tcp)");
            }
            if (::listen(listen_fd_, config.listen_backlog) != 0) throw_errno("listen");
            tcp_port_ = bound_port(listen_fd_);
            if (!poller_->add(listen_fd_, kListenToken)) throw_errno("poller add(listener)");
        }
    } catch (...) {
        close_sockets();
        throw;
    }
}

NetworkGateway::~NetworkGateway() {
    stop();
    close_sockets();
}

void NetworkGateway::close_sockets() noexcept {
    for (Connection& connection : connections_) {
        if (connection.fd >= 0) ::close(connection.fd);
        connection.fd = -1;
        connection.active = false;
    }
    if (udp_fd_ >= 0) ::close(udp_fd_);
    if (listen_fd_ >= 0) ::close(listen_fd_);
    udp_fd_ = -1;
    listen_fd_ = -1;
}

const char* NetworkGateway::backend_name() const noexcept { return poller_->name(); }

NetworkGatewayStats NetworkGateway::stats() const noexcept {
    const auto get = [this](Counter c) { return shared_[c].load(std::memory_order_acquire); };
    return {get(kMessages),    get(kUdpDatagrams), get(kUdpBytes),          get(kUdpMalformed),
            get(kUdpTruncated), get(kTcpAccepted),  get(kTcpRejected),       get(kTcpClosed),
            get(kTcpBytes),     get(kTcpProtocolErrors), get(kTcpTruncated), get(kDropped),
            get(kIoErrors)};
}

void NetworkGateway::publish() noexcept {
    for (std::size_t i = 0; i < kCounterCount; ++i) {
        shared_[i].store(local_[i], std::memory_order_release);
    }
}

// ---- Threading ---------------------------------------------------------------------

void NetworkGateway::start() {
    if (threaded_) {
        return;
    }
    stop_.store(false, std::memory_order_release);
    ready_.store(false, std::memory_order_release);
    threaded_ = true;  // before the thread exists, so the thread itself may read it
    thread_ = std::thread([this] { thread_main(); });
    while (!ready_.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
}

void NetworkGateway::stop() noexcept {
    stop_.store(true, std::memory_order_release);
    if (thread_.joinable()) {
        thread_.join();
    }
    threaded_ = false;
}

void NetworkGateway::thread_main() noexcept {
    const int core = config_.core >= 0 ? config_.core : engine_.config().gateway_core;
    pin_ = pin_current_thread(core);
    if (engine_.config().log_affinity && core >= 0) {
        log_pin(stderr, "network-gateway thread", pin_);
    }
    ready_.store(true, std::memory_order_release);

    while (!stop_.load(std::memory_order_acquire)) {
        if (run_once(config_.wait_timeout_ms) == 0 && config_.wait_timeout_ms == 0) {
            cpu_relax();
        }
    }
}

int NetworkGateway::poll_once(int timeout_ms) noexcept {
    return threaded_ ? 0 : run_once(timeout_ms);
}

int NetworkGateway::run_once(int timeout_ms) noexcept {
    std::uint32_t ready[kMaxEventsPerWait];
    const int n = poller_->wait(ready, kMaxEventsPerWait, timeout_ms);
    for (int i = 0; i < n; ++i) {
        if (ready[i] == kUdpToken) {
            drain_udp();
        } else if (ready[i] == kListenToken) {
            accept_ready();
        } else {
            Connection& connection = connections_[ready[i] - kFirstConnectionToken];
            if (connection.active && connection.fd >= 0) {
                drain_tcp(connection);
            }
        }
    }
    if (n > 0) {
        publish();
    }
    return n;
}

// ---- UDP -----------------------------------------------------------------------------

void NetworkGateway::on_datagram(std::span<const std::byte> datagram) noexcept {
    ++local_[kUdpDatagrams];
    local_[kUdpBytes] += datagram.size();
    const IngestResult result = engine_.ingest(datagram);
    local_[kMessages] += result.messages;
    if (result.status != DecodeStatus::Ok) {
        if (result.status != DecodeStatus::NeedMoreData) {
            ++local_[kUdpMalformed];
        } else {
            // NeedMoreData means either the datagram ends mid-message or the
            // engine refused a valid one because it is not running.
            Command probe;
            const bool refused = decode(datagram.subspan(result.consumed), probe).status == DecodeStatus::Ok;
            ++local_[refused ? kDropped : kUdpTruncated];
        }
    }
    if (!threaded_) {
        publish();  // the threaded loop publishes once per pass instead
    }
}

void NetworkGateway::drain_udp() noexcept {
    for (int i = 0; i < kMaxDatagramsPerWake; ++i) {
        const ssize_t got = ::recv(udp_fd_, datagram_buffer_, kDatagramBufferBytes, 0);
        if (got < 0) {
            if (errno == EINTR) continue;
            if (!would_block(errno)) ++local_[kIoErrors];
            return;
        }
        on_datagram(std::span<const std::byte>(datagram_buffer_, static_cast<std::size_t>(got)));
    }
}

// ---- TCP -----------------------------------------------------------------------------

int NetworkGateway::allocate_connection(int fd) noexcept {
    for (std::size_t i = 0; i < connections_.size(); ++i) {
        if (!connections_[i].active) {
            connections_[i].active = true;
            connections_[i].fd = fd;
            connections_[i].have = 0;
            return static_cast<int>(i);
        }
    }
    return -1;
}

void NetworkGateway::accept_ready() noexcept {
    for (;;) {
        const int fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EINTR) continue;
            if (!would_block(errno) && errno != ECONNABORTED) ++local_[kIoErrors];
            return;
        }
        const int flags = ::fcntl(fd, F_GETFL, 0);
        const bool configured = flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0 &&
                                ::fcntl(fd, F_SETFD, FD_CLOEXEC) == 0;
        const int slot = configured ? allocate_connection(fd) : -1;
        if (slot < 0) {
            ::close(fd);
            ++local_[configured ? kTcpRejected : kIoErrors];
            continue;
        }
        if (!poller_->add(fd, kFirstConnectionToken + static_cast<std::uint32_t>(slot))) {
            connections_[static_cast<std::size_t>(slot)].active = false;
            connections_[static_cast<std::size_t>(slot)].fd = -1;
            ::close(fd);
            ++local_[kIoErrors];
            continue;
        }
        ++local_[kTcpAccepted];
    }
}

// Decodes every complete message at the front of the connection's buffer and
// moves the unfinished tail to the front. Returns false on a protocol error.
bool NetworkGateway::consume_stream(Connection& connection) noexcept {
    const std::span<const std::byte> pending(connection.buffer, connection.have);
    const IngestResult result = engine_.ingest(pending);
    local_[kMessages] += result.messages;
    if (result.status == DecodeStatus::UnknownType || result.status == DecodeStatus::BadSide) {
        ++local_[kTcpProtocolErrors];
        connection.have = 0;  // nothing after the bad message can be trusted
        return false;
    }
    std::uint32_t rest = connection.have - static_cast<std::uint32_t>(result.consumed);
    if (rest >= kMaxWireMsgSize) {
        // A whole message is still sitting there: the engine refused it
        // because it is not running. Discard rather than wedge the buffer.
        ++local_[kDropped];
        rest = 0;
    }
    std::memmove(connection.buffer, connection.buffer + result.consumed, rest);
    connection.have = rest;
    return true;
}

void NetworkGateway::end_stream(Connection& connection) noexcept {
    if (connection.have != 0) {
        ++local_[kTcpTruncated];
    }
    if (connection.fd >= 0) {
        poller_->remove(connection.fd);
        ::close(connection.fd);
    }
    connection.fd = -1;
    connection.have = 0;
    connection.active = false;
    ++local_[kTcpClosed];
}

void NetworkGateway::drain_tcp(Connection& connection) noexcept {
    for (int i = 0; i < kMaxReadsPerWake; ++i) {
        // Read directly behind the carried partial message: no intermediate copy.
        const std::size_t space = config_.stream_buffer_bytes - connection.have;
        const ssize_t got = ::read(connection.fd, connection.buffer + connection.have, space);
        if (got < 0) {
            if (errno == EINTR) continue;
            if (would_block(errno)) return;
            if (errno != ECONNRESET) ++local_[kIoErrors];
            end_stream(connection);
            return;
        }
        if (got == 0) {  // orderly EOF
            end_stream(connection);
            return;
        }
        local_[kTcpBytes] += static_cast<std::uint64_t>(got);
        connection.have += static_cast<std::uint32_t>(got);
        if (!consume_stream(connection)) {
            end_stream(connection);
            return;
        }
        if (static_cast<std::size_t>(got) < space) {
            return;  // short read: the socket is drained
        }
    }
}

int NetworkGateway::open_stream() noexcept {
    return threaded_ ? -1 : allocate_connection(-1);
}

bool NetworkGateway::feed_stream(int stream, std::span<const std::byte> bytes) noexcept {
    if (threaded_ || stream < 0 || static_cast<std::size_t>(stream) >= connections_.size()) {
        return false;
    }
    Connection& connection = connections_[static_cast<std::size_t>(stream)];
    if (!connection.active || connection.fd >= 0) {
        return false;
    }
    bool alive = true;
    while (alive && !bytes.empty()) {
        const std::size_t space = config_.stream_buffer_bytes - connection.have;
        const std::size_t chunk = bytes.size() < space ? bytes.size() : space;
        std::memcpy(connection.buffer + connection.have, bytes.data(), chunk);
        connection.have += static_cast<std::uint32_t>(chunk);
        local_[kTcpBytes] += chunk;
        bytes = bytes.subspan(chunk);
        alive = consume_stream(connection);
    }
    if (!alive) {
        end_stream(connection);
    }
    publish();
    return alive;
}

void NetworkGateway::close_stream(int stream) noexcept {
    if (threaded_ || stream < 0 || static_cast<std::size_t>(stream) >= connections_.size()) {
        return;
    }
    Connection& connection = connections_[static_cast<std::size_t>(stream)];
    if (connection.active && connection.fd < 0) {
        end_stream(connection);
        publish();
    }
}

}  // namespace aether
