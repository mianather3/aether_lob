// Network gateway tests over real loopback sockets: UDP datagram framing, TCP
// stream reassembly, error handling, connection limits, both readiness
// backends, and thread affinity reporting. Built to run under TSan/ASan.
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include "affinity.hpp"
#include "engine.hpp"
#include "network_gateway.hpp"
#include "protocol.hpp"
#include "test_common.hpp"

using namespace aether;

namespace {

constexpr BookConfig kBook{.max_orders = 1U << 16, .max_levels = 1U << 10};

template <class Predicate>
bool wait_until(Predicate&& done, int timeout_ms = 20'000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!done()) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    return true;
}

sockaddr_in loopback(std::uint16_t port) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    return address;
}

int udp_client(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    const sockaddr_in address = loopback(port);
    CHECK(fd >= 0 && ::connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof address) == 0);
    return fd;
}

int tcp_client(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    const sockaddr_in address = loopback(port);
    CHECK(fd >= 0 && ::connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof address) == 0);
    return fd;
}

bool write_all(int fd, const std::byte* data, std::size_t size) {
    while (size > 0) {
        const ssize_t n = ::write(fd, data, size);
        if (n <= 0) return false;
        data += n;
        size -= static_cast<std::size_t>(n);
    }
    return true;
}

struct TradeRecorder final : EngineListener {
    std::vector<Trade> trades;
    void on_trade(const Trade& trade, const Command&) noexcept override { trades.push_back(trade); }
};

bool same(const std::vector<Trade>& a, const std::vector<Trade>& b) {
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (a[i].maker_id != b[i].maker_id || a[i].taker_id != b[i].taker_id || a[i].price != b[i].price ||
            a[i].qty != b[i].qty || a[i].taker_side != b[i].taker_side) {
            return false;
        }
    }
    return true;
}

// Valid mixed flow, returned one message per element so tests can frame it freely.
std::vector<std::vector<std::byte>> make_messages(std::size_t count, std::uint64_t seed) {
    std::vector<std::vector<std::byte>> messages(count);
    std::uint64_t state = seed;
    auto rng = [&state]() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    };
    OrderId next_id = 1;
    for (auto& message : messages) {
        const std::uint64_t roll = rng() % 100;
        const Side side = (rng() & 1) != 0 ? Side::Buy : Side::Sell;
        if (roll < 30 && next_id > 1) {
            append(message, make_cancel(1 + rng() % (next_id - 1)));
        } else if (roll < 40) {
            append(message, make_market(side, static_cast<std::uint32_t>(1 + rng() % 80)));
        } else {
            const Price price = 1000 + static_cast<Price>(rng() % 40) - 20;
            append(message, make_add(next_id++, side, price, static_cast<std::uint32_t>(1 + rng() % 50)));
        }
    }
    return messages;
}

std::vector<std::byte> concatenate(const std::vector<std::vector<std::byte>>& messages) {
    std::vector<std::byte> bytes;
    for (const auto& message : messages) bytes.insert(bytes.end(), message.begin(), message.end());
    return bytes;
}

struct Expected {
    std::vector<Trade> trades;
    std::size_t orders{0};
    std::size_t levels{0};
};

Expected replay(std::span<const std::byte> bytes) {
    OrderBook book(kBook);
    Expected expected;
    const auto on_trade = [&](const Trade& t) { expected.trades.push_back(t); };
    Command cmd;
    while (!bytes.empty()) {
        const DecodeResult d = decode(bytes, cmd);
        if (d.status != DecodeStatus::Ok) break;
        bytes = bytes.subspan(d.consumed);
        switch (cmd.type) {
            case CommandType::Add: book.add_limit(cmd.order_id, cmd.side, cmd.price, cmd.qty, on_trade); break;
            case CommandType::Cancel: book.cancel(cmd.order_id); break;
            case CommandType::Market: book.add_market(cmd.side, cmd.qty, on_trade); break;
        }
    }
    expected.orders = book.order_count();
    expected.levels = book.level_count();
    return expected;
}

void check_book(const Engine& engine, const TradeRecorder& recorder, const Expected& expected) {
    CHECK(same(recorder.trades, expected.trades));
    CHECK(engine.book().order_count() == expected.orders);
    CHECK(engine.book().level_count() == expected.levels);
    CHECK(engine.book().check_invariants());
}

// ---- Socket configuration -----------------------------------------------------------

void test_sockets_are_bound_tuned_and_nonblocking() {
    Engine engine({.book = kBook});
    NetworkGateway gateway(engine, {.rcvbuf_bytes = 1 << 20});
    CHECK(gateway.udp_port() != 0 && gateway.tcp_port() != 0);
    // The OS reports what it actually granted, which is what the gateway exposes.
    CHECK(gateway.udp_rcvbuf() > 0 && gateway.tcp_rcvbuf() > 0);
#if defined(__APPLE__)
    // Darwin grants the request exactly (up to kern.ipc.maxsockbuf). Linux
    // doubles it but clamps to net.core.rmem_max, so no fixed bound holds there.
    CHECK(gateway.udp_rcvbuf() >= 1 << 20 && gateway.tcp_rcvbuf() >= 1 << 20);
#endif
    std::printf("    (backend=%s udp_port=%u tcp_port=%u SO_RCVBUF udp=%d tcp=%d)\n", gateway.backend_name(),
                gateway.udp_port(), gateway.tcp_port(), gateway.udp_rcvbuf(), gateway.tcp_rcvbuf());

    // Nothing is pending: a zero-timeout pass must return at once, not block.
    const auto before = std::chrono::steady_clock::now();
    for (int i = 0; i < 1000; ++i) CHECK(gateway.poll_once(0) == 0);
    CHECK(std::chrono::steady_clock::now() - before < std::chrono::seconds(2));

    NetworkGateway poll_gateway(engine, {.backend = PollBackend::Poll});
    CHECK(std::strcmp(poll_gateway.backend_name(), "poll") == 0);
    CHECK(std::strcmp(gateway.backend_name(), "poll") != 0);  // epoll or kqueue here

    bool threw = false;
    try {
        NetworkGateway bad(engine, {.bind_address = "not-an-ip"});
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    CHECK(threw);
}

// ---- UDP ------------------------------------------------------------------------------

void test_udp_datagram_framing(PollBackend backend) {
    const auto messages = make_messages(20'000, 0xABCDEF01ULL);
    const std::vector<std::byte> all = concatenate(messages);
    const Expected expected = replay(all);

    TradeRecorder recorder;
    Engine engine({.book = kBook, .queue_capacity = 256}, &recorder);
    engine.start();
    NetworkGateway gateway(engine, {.enable_tcp = false, .backend = backend});
    gateway.start();
    const int client = udp_client(gateway.udp_port());

    // 1..40 whole messages per datagram, acknowledged in small windows so the
    // loopback socket buffer can never overflow and drop one.
    std::uint64_t state = 7;
    std::uint64_t sent = 0;
    bool sent_ok = true;
    std::vector<std::byte> datagram;
    for (std::size_t i = 0; i < messages.size();) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        const std::size_t batch = 1 + static_cast<std::size_t>((state >> 33) % 40);
        datagram.clear();
        for (std::size_t k = 0; k < batch && i < messages.size(); ++k, ++i) {
            datagram.insert(datagram.end(), messages[i].begin(), messages[i].end());
        }
        sent_ok = sent_ok && ::send(client, datagram.data(), datagram.size(), 0) ==
                                 static_cast<ssize_t>(datagram.size());
        if (++sent % 16 == 0) {
            sent_ok = sent_ok && wait_until([&] { return gateway.stats().udp_datagrams == sent; });
        }
    }
    CHECK(sent_ok);
    CHECK(wait_until([&] { return gateway.stats().udp_datagrams == sent; }));

    gateway.stop();
    engine.stop();
    ::close(client);

    const NetworkGatewayStats stats = gateway.stats();
    CHECK(stats.messages == messages.size() && stats.udp_bytes == all.size());
    CHECK(stats.udp_malformed == 0 && stats.udp_truncated == 0 && stats.io_errors == 0);
    CHECK(engine.processed() == messages.size());
    check_book(engine, recorder, expected);
}

void test_udp_bad_datagrams_are_isolated() {
    Engine engine({.book = kBook});
    engine.start();
    NetworkGateway gateway(engine, {.enable_tcp = false});
    gateway.start();
    const int client = udp_client(gateway.udp_port());

    std::vector<std::byte> good;
    append(good, make_add(1, Side::Buy, 100, 5));

    std::vector<std::byte> truncated;  // one whole message, then half of another
    append(truncated, make_add(2, Side::Buy, 100, 5));
    append(truncated, make_add(3, Side::Buy, 100, 5));
    truncated.resize(truncated.size() - 10);

    std::vector<std::byte> malformed;  // one whole message, a bad type tag, then a valid message
    append(malformed, make_add(4, Side::Buy, 100, 5));
    malformed.push_back(std::byte{0x00});
    append(malformed, make_add(5, Side::Buy, 100, 5));

    std::vector<std::byte> after;  // the next datagram must decode cleanly again
    append(after, make_add(6, Side::Buy, 100, 5));

    CHECK(::send(client, good.data(), good.size(), 0) > 0);
    CHECK(::send(client, truncated.data(), truncated.size(), 0) > 0);
    CHECK(::send(client, malformed.data(), malformed.size(), 0) > 0);
    CHECK(::send(client, nullptr, 0, 0) == 0);  // empty datagram
    CHECK(::send(client, after.data(), after.size(), 0) > 0);
    CHECK(wait_until([&] { return gateway.stats().udp_datagrams == 5; }));

    gateway.stop();
    engine.stop();
    ::close(client);

    const NetworkGatewayStats stats = gateway.stats();
    CHECK(stats.messages == 4);  // orders 1, 2, 4 and 6
    CHECK(stats.udp_truncated == 1 && stats.udp_malformed == 1);
    CHECK(engine.book().find(1) != nullptr && engine.book().find(2) != nullptr);
    CHECK(engine.book().find(4) != nullptr && engine.book().find(6) != nullptr);
    CHECK(engine.book().find(3) == nullptr && engine.book().find(5) == nullptr);
}

// ---- TCP ------------------------------------------------------------------------------

void test_tcp_fragmented_stream_reassembly(PollBackend backend) {
    const auto messages = make_messages(60'000, 0x5151FACEULL);
    const std::vector<std::byte> all = concatenate(messages);
    const Expected expected = replay(all);

    TradeRecorder recorder;
    Engine engine({.book = kBook, .queue_capacity = 128}, &recorder);
    engine.start();
    // A small scratch buffer forces many wrap/compaction cycles.
    NetworkGateway gateway(engine, {.enable_udp = false, .stream_buffer_bytes = 4096, .backend = backend});
    gateway.start();
    const int client = tcp_client(gateway.tcp_port());
    const int on = 1;
    ::setsockopt(client, IPPROTO_TCP, TCP_NODELAY, &on, sizeof on);

    // Write sizes from 1 byte upward cut messages at every possible offset.
    std::uint64_t state = 3;
    bool wrote = true;
    for (std::size_t off = 0; off < all.size() && wrote;) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        const std::uint64_t pick = (state >> 33) % 100;
        std::size_t chunk = pick < 40 ? 1 + pick % 21 : 1 + (state >> 40) % 3000;
        if (chunk > all.size() - off) chunk = all.size() - off;
        wrote = write_all(client, all.data() + off, chunk);
        off += chunk;
    }
    CHECK(wrote);
    CHECK(wait_until([&] { return gateway.stats().tcp_bytes == all.size(); }));
    ::close(client);
    CHECK(wait_until([&] { return gateway.stats().tcp_closed == 1; }));

    gateway.stop();
    engine.stop();

    const NetworkGatewayStats stats = gateway.stats();
    CHECK(stats.tcp_accepted == 1 && stats.messages == messages.size());
    CHECK(stats.tcp_protocol_errors == 0 && stats.tcp_truncated == 0 && stats.io_errors == 0);
    CHECK(engine.backpressure_events() > 0);  // the 128-slot queue pushed back on the socket loop
    check_book(engine, recorder, expected);
}

void test_tcp_protocol_error_closes_only_that_connection() {
    Engine engine({.book = kBook});
    engine.start();
    NetworkGateway gateway(engine, {.enable_udp = false});
    gateway.start();

    const int bad = tcp_client(gateway.tcp_port());
    const int good = tcp_client(gateway.tcp_port());
    CHECK(wait_until([&] { return gateway.stats().tcp_accepted == 2; }));

    std::vector<std::byte> poisoned;
    append(poisoned, make_add(1, Side::Buy, 100, 5));
    AddOrderMsg bad_side = make_add(2, Side::Buy, 100, 5);
    bad_side.side = '?';
    append(poisoned, bad_side);
    append(poisoned, make_add(3, Side::Buy, 100, 5));  // after the error: must be ignored
    CHECK(write_all(bad, poisoned.data(), poisoned.size()));
    CHECK(wait_until([&] { return gateway.stats().tcp_closed == 1; }));

    // The peer sees the gateway hang up.
    char byte = 0;
    const ssize_t got = ::read(bad, &byte, 1);
    CHECK(got == 0 || (got < 0 && errno == ECONNRESET));

    std::vector<std::byte> fine;
    append(fine, make_add(4, Side::Sell, 200, 5));
    fine.push_back(std::byte{'X'});  // start of a cancel that never completes
    CHECK(write_all(good, fine.data(), fine.size()));
    CHECK(wait_until([&] { return gateway.stats().messages == 2; }));
    ::close(good);  // EOF inside a message
    CHECK(wait_until([&] { return gateway.stats().tcp_closed == 2; }));

    gateway.stop();
    engine.stop();
    ::close(bad);

    const NetworkGatewayStats stats = gateway.stats();
    CHECK(stats.tcp_protocol_errors == 1 && stats.tcp_truncated == 1);
    CHECK(stats.messages == 2);
    CHECK(engine.book().find(1) != nullptr && engine.book().find(4) != nullptr);
    CHECK(engine.book().find(2) == nullptr && engine.book().find(3) == nullptr);
}

void test_tcp_connection_limit_and_slot_reuse() {
    Engine engine({.book = kBook});
    engine.start();
    NetworkGateway gateway(engine, {.enable_udp = false, .max_connections = 2});
    gateway.start();

    const int a = tcp_client(gateway.tcp_port());
    const int b = tcp_client(gateway.tcp_port());
    CHECK(wait_until([&] { return gateway.stats().tcp_accepted == 2; }));
    const int c = tcp_client(gateway.tcp_port());  // over the limit: accepted by the kernel, then refused
    CHECK(wait_until([&] { return gateway.stats().tcp_rejected == 1; }));

    ::close(a);
    CHECK(wait_until([&] { return gateway.stats().tcp_closed == 1; }));
    const int d = tcp_client(gateway.tcp_port());  // reuses the freed slot
    CHECK(wait_until([&] { return gateway.stats().tcp_accepted == 3; }));

    std::vector<std::byte> bytes;
    append(bytes, make_add(9, Side::Buy, 100, 5));
    CHECK(write_all(d, bytes.data(), bytes.size()));
    CHECK(write_all(b, bytes.data(), 5));  // b and d interleave: per-connection reassembly
    CHECK(wait_until([&] { return gateway.stats().messages == 1; }));
    append(bytes, make_add(10, Side::Buy, 100, 5));
    CHECK(write_all(b, bytes.data() + 5, bytes.size() - 5));
    CHECK(wait_until([&] { return gateway.stats().messages == 3; }));

    gateway.stop();
    engine.stop();
    ::close(b);
    ::close(c);
    ::close(d);
    // Order 9 arrived twice (once per connection): the second is a duplicate id.
    CHECK(engine.processed() == 3 && engine.book().order_count() == 2);
}

// ---- Caller-driven mode -----------------------------------------------------------------

void test_caller_driven_polling_and_in_process_frames() {
    Engine engine({.book = kBook});
    engine.start();
    NetworkGateway gateway(engine, {.enable_tcp = false});  // no gateway thread: this thread polls
    const int client = udp_client(gateway.udp_port());

    std::vector<std::byte> bytes;
    append(bytes, make_add(1, Side::Buy, 100, 5));
    CHECK(::send(client, bytes.data(), bytes.size(), 0) > 0);
    CHECK(wait_until([&] { return gateway.poll_once(0) > 0; }));
    CHECK(gateway.stats().udp_datagrams == 1 && gateway.stats().messages == 1);

    // The same decode paths without a socket.
    bytes.clear();
    append(bytes, make_add(2, Side::Buy, 100, 5));
    append(bytes, make_add(3, Side::Buy, 100, 5));
    gateway.on_datagram(bytes);
    const int stream = gateway.open_stream();
    CHECK(stream >= 0);
    CHECK(gateway.feed_stream(stream, std::span<const std::byte>(bytes).first(30)));
    CHECK(gateway.stats().messages == 4);  // 1 + 2 + the first whole message of the stream
    CHECK(gateway.feed_stream(stream, std::span<const std::byte>(bytes).subspan(30)));
    CHECK(gateway.stats().messages == 5);
    const std::byte junk[1] = {std::byte{0xFF}};
    CHECK(!gateway.feed_stream(stream, junk));  // protocol error closes the stream
    CHECK(!gateway.feed_stream(stream, bytes));  // and it stays closed
    CHECK(gateway.stats().tcp_protocol_errors == 1 && gateway.stats().tcp_closed == 1);

    engine.stop();
    ::close(client);
    CHECK(engine.processed() == 5 && engine.book().order_count() == 3);
}

void test_frames_dropped_when_engine_is_stopped() {
    // The engine never starts, so its 2-slot queue fills and then refuses.
    Engine engine({.book = kBook, .queue_capacity = 2});
    NetworkGateway gateway(engine, {.enable_udp = false, .enable_tcp = false});
    std::vector<std::byte> bytes;
    for (OrderId id = 1; id <= 5; ++id) append(bytes, make_add(id, Side::Buy, 100, 1));
    gateway.on_datagram(bytes);
    CHECK(gateway.stats().messages == 2 && gateway.stats().dropped == 1);
    CHECK(gateway.stats().udp_truncated == 0 && gateway.stats().udp_malformed == 0);
}

// ---- Affinity -------------------------------------------------------------------------------

void test_affinity_reporting() {
    CHECK(pin_current_thread(-1).status == PinStatus::NotRequested);
    CHECK(same_physical_core(3, 3));

    bool threw = false;
    try {
        Engine clash({.book = kBook, .matching_core = 1, .gateway_core = 1});
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    CHECK(threw);  // both threads on one core is a configuration error

    const CorePlan plan = default_core_plan();
    std::printf("    (default plan: gateway core %d, matching core %d)\n", plan.gateway_core, plan.matching_core);
    if (plan.gateway_core < 0) {
        return;  // single-core machine: nothing to place
    }
    CHECK(!same_physical_core(plan.gateway_core, plan.matching_core));

    Engine engine({.book = kBook, .matching_core = plan.matching_core, .gateway_core = plan.gateway_core});
    CHECK(engine.matching_affinity().status == PinStatus::NotRequested);  // not started yet
    engine.start();
    NetworkGateway gateway(engine);
    gateway.start();

    const PinResult matching = engine.matching_affinity();
    const PinResult ingest = gateway.affinity();
    CHECK(matching.requested == plan.matching_core && ingest.requested == plan.gateway_core);
    CHECK(matching.status != PinStatus::NotRequested && ingest.status != PinStatus::NotRequested);
#if defined(__linux__)
    // A hard pin, verified by the kernel's own view of where each thread runs.
    CHECK(matching.status == PinStatus::Pinned && matching.observed_cpu == plan.matching_core);
    CHECK(ingest.status == PinStatus::Pinned && ingest.observed_cpu == plan.gateway_core);
#else
    // Darwin: an accepted hint (Intel) or an explicit "not supported" (Apple Silicon); never silent.
    CHECK(matching.status == PinStatus::Advisory || matching.status == PinStatus::Unsupported);
    CHECK(ingest.status == matching.status);
#endif

    // The pipeline works identically whatever the placement outcome.
    const int client = udp_client(gateway.udp_port());
    std::vector<std::byte> bytes;
    append(bytes, make_add(1, Side::Buy, 100, 5));
    CHECK(::send(client, bytes.data(), bytes.size(), 0) > 0);
    CHECK(wait_until([&] { return engine.processed() == 1; }));
    gateway.stop();
    engine.stop();
    ::close(client);
}

}  // namespace

int main() {
    std::signal(SIGPIPE, SIG_IGN);  // writing to a connection the gateway closed must not kill the test
    using aether_test::run;
    run("sockets_are_bound_tuned_and_nonblocking", test_sockets_are_bound_tuned_and_nonblocking);
    run("udp_datagram_framing [epoll/kqueue]", [] { test_udp_datagram_framing(PollBackend::Auto); });
    run("udp_datagram_framing [poll]", [] { test_udp_datagram_framing(PollBackend::Poll); });
    run("udp_bad_datagrams_are_isolated", test_udp_bad_datagrams_are_isolated);
    run("tcp_fragmented_stream_reassembly [epoll/kqueue]",
        [] { test_tcp_fragmented_stream_reassembly(PollBackend::Auto); });
    run("tcp_fragmented_stream_reassembly [poll]", [] { test_tcp_fragmented_stream_reassembly(PollBackend::Poll); });
    run("tcp_protocol_error_closes_only_that_connection", test_tcp_protocol_error_closes_only_that_connection);
    run("tcp_connection_limit_and_slot_reuse", test_tcp_connection_limit_and_slot_reuse);
    run("caller_driven_polling_and_in_process_frames", test_caller_driven_polling_and_in_process_frames);
    run("frames_dropped_when_engine_is_stopped", test_frames_dropped_when_engine_is_stopped);
    run("affinity_reporting", test_affinity_reporting);
    return aether_test::summary();
}
