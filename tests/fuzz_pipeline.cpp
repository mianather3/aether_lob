// AetherLOB adversarial differential fuzzer.
//
// Usage: aether_fuzz_pipeline [payloads=1000000] [seed=0xAE7E4B0B]
//
// A "payload" is one UDP datagram or one TCP stream chunk handed to the
// NetworkGateway. Payloads start as valid order flow and are then corrupted:
// truncated, bit-flipped, scrambled, overwritten, shifted by inserted/deleted
// bytes, or replaced by pure noise. Every payload is fed to
//
//   (a) the real pipeline: NetworkGateway -> SPSC queue -> matching thread, and
//   (b) an independent reference model written here from the protocol spec:
//       its own byte parser, its own framing rules and a naive std::map book.
//       It shares no code with the engine.
//
// At the end of each scenario the two must agree exactly on: every command's
// outcome, every trade, the complete L3 book (each resting order, in priority
// order) and the gateway's error counters. Any divergence, sanitizer report,
// exception or stall fails the run.
//
// Scenario kinds
//   normal   in-process frames (the gateway's transport-independent entry
//            points), datagrams and up to three interleaved streams
//   burst    huge frames into a 2..64 slot queue while the matching thread is
//            periodically stalled: the queue must hit 100% full (asserted) and
//            the gateway must apply backpressure without losing a command.
//            Half of these are one-sided "cascade" sell-offs.
//   sockets  the same flow over real loopback UDP + TCP sockets with the
//            gateway thread running (kqueue/epoll and poll backends alternate)
//
// Inputs are a pure function of the seed, so a failure is reproducible.
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iterator>
#include <list>
#include <map>
#include <span>
#include <thread>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "engine.hpp"
#include "network_gateway.hpp"

namespace {

using Bytes = std::vector<std::uint8_t>;

struct Rng {
    std::uint64_t state;
    std::uint64_t next() noexcept {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    }
    std::uint64_t below(std::uint64_t n) noexcept { return next() % n; }  // n > 0
    bool chance(unsigned percent) noexcept { return below(100) < percent; }
};

// =====================================================================================
// Reference model: deliberately simple, deliberately independent of include/.
// =====================================================================================
namespace ref {

enum class Kind { Add, Cancel, Market };
struct Cmd {
    Kind kind;
    std::uint64_t id;
    bool buy;
    std::int64_t price;
    std::uint64_t qty;
};

enum class Parse { Ok, Need, Bad };

std::uint64_t le(const std::uint8_t* p, int bytes) {
    std::uint64_t v = 0;
    for (int i = bytes - 1; i >= 0; --i) v = (v << 8) | p[i];
    return v;
}

// Wire spec: 'A' id:8 side:1 price:8 qty:4 | 'X' id:8 | 'M' side:1 qty:4, little-endian.
// A message that is not fully present is "Need" (checked before its contents).
Parse parse(const std::uint8_t* p, std::size_t n, Cmd& out, std::size_t& used) {
    if (n == 0) return Parse::Need;
    if (p[0] == 'A') {
        if (n < 22) return Parse::Need;
        if (p[9] != 'B' && p[9] != 'S') return Parse::Bad;
        out = {Kind::Add, le(p + 1, 8), p[9] == 'B', static_cast<std::int64_t>(le(p + 10, 8)), le(p + 18, 4)};
        used = 22;
        return Parse::Ok;
    }
    if (p[0] == 'X') {
        if (n < 9) return Parse::Need;
        out = {Kind::Cancel, le(p + 1, 8), false, 0, 0};
        used = 9;
        return Parse::Ok;
    }
    if (p[0] == 'M') {
        if (n < 6) return Parse::Need;
        if (p[1] != 'B' && p[1] != 'S') return Parse::Bad;
        out = {Kind::Market, 0, p[1] == 'B', 0, le(p + 2, 4)};
        used = 6;
        return Parse::Ok;
    }
    return Parse::Bad;
}

enum class Result { Ok, InvalidQty, DuplicateId, Exhausted, NotFound };
struct Outcome {
    Result result;
    std::uint64_t filled;
    std::uint64_t resting;
};
struct Fill {
    std::uint64_t maker;
    std::uint64_t taker;
    std::int64_t price;
    std::uint64_t qty;
    bool taker_buy;
};
struct RestingOrder {
    bool buy;
    std::int64_t price;
    std::uint64_t id;
    std::uint64_t qty;
};

class Book {
public:
    Book(std::size_t max_orders, std::size_t max_levels) : max_orders_(max_orders), max_levels_(max_levels) {}

    void apply(const Cmd& c) {
        switch (c.kind) {
            case Kind::Add: outcomes.push_back(add(c)); break;
            case Kind::Cancel: outcomes.push_back(cancel(c.id)); break;
            case Kind::Market:
                if (c.qty == 0) {
                    outcomes.push_back({Result::InvalidQty, 0, 0});
                } else {
                    const std::uint64_t left = sweep(0, c.buy, c.qty, false, 0);
                    outcomes.push_back({Result::Ok, c.qty - left, 0});
                }
                break;
        }
    }

    // Every resting order: bids best-first, then asks best-first, FIFO within a price.
    std::vector<RestingOrder> snapshot() const {
        std::vector<RestingOrder> all;
        for (auto level = bids_.rbegin(); level != bids_.rend(); ++level) {
            for (const auto& [id, qty] : level->second) all.push_back({true, level->first, id, qty});
        }
        for (const auto& [price, queue] : asks_) {
            for (const auto& [id, qty] : queue) all.push_back({false, price, id, qty});
        }
        return all;
    }

    std::vector<Outcome> outcomes;
    std::vector<Fill> fills;

private:
    using Queue = std::list<std::pair<std::uint64_t, std::uint64_t>>;  // (id, open qty), oldest first
    using Levels = std::map<std::int64_t, Queue>;

    Outcome add(const Cmd& c) {
        if (c.qty == 0) return {Result::InvalidQty, 0, 0};
        // Capacity is reserved before matching: a full order or level arena rejects the order outright.
        if (live_.size() >= max_orders_ || bids_.size() + asks_.size() >= max_levels_) {
            return {Result::Exhausted, 0, 0};
        }
        if (live_.count(c.id) != 0) return {Result::DuplicateId, 0, 0};
        const std::uint64_t left = sweep(c.id, c.buy, c.qty, true, c.price);
        if (left != 0) {
            (c.buy ? bids_ : asks_)[c.price].emplace_back(c.id, left);
            live_[c.id] = {c.buy, c.price};
        }
        return {Result::Ok, c.qty - left, left};
    }

    Outcome cancel(std::uint64_t id) {
        const auto found = live_.find(id);
        if (found == live_.end()) return {Result::NotFound, 0, 0};
        Levels& side = found->second.first ? bids_ : asks_;
        const auto level = side.find(found->second.second);
        for (auto order = level->second.begin(); order != level->second.end(); ++order) {
            if (order->first == id) {
                level->second.erase(order);
                break;
            }
        }
        if (level->second.empty()) side.erase(level);
        live_.erase(found);
        return {Result::Ok, 0, 0};
    }

    std::uint64_t sweep(std::uint64_t taker, bool buy, std::uint64_t qty, bool limited, std::int64_t limit) {
        Levels& opposite = buy ? asks_ : bids_;
        while (qty != 0 && !opposite.empty()) {
            const auto best = buy ? opposite.begin() : std::prev(opposite.end());
            if (limited && (buy ? best->first > limit : best->first < limit)) break;
            auto& [maker_id, maker_qty] = best->second.front();
            const std::uint64_t fill = qty < maker_qty ? qty : maker_qty;
            fills.push_back({maker_id, taker, best->first, fill, buy});
            qty -= fill;
            maker_qty -= fill;
            if (maker_qty == 0) {
                live_.erase(maker_id);
                best->second.pop_front();
                if (best->second.empty()) opposite.erase(best);
            }
        }
        return qty;
    }

    std::size_t max_orders_;
    std::size_t max_levels_;
    Levels bids_;
    Levels asks_;
    std::map<std::uint64_t, std::pair<bool, std::int64_t>> live_;  // id -> (buy, price)
};

// The gateway's framing rules, restated.
struct Gateway {
    explicit Gateway(Book& b) : book(b) {}

    // A datagram is applied up to its first incomplete or invalid message.
    void datagram(const Bytes& d) {
        ++datagrams;
        datagram_bytes += d.size();
        std::size_t at = 0;
        while (at < d.size()) {
            Cmd cmd{};
            std::size_t used = 0;
            const Parse p = parse(d.data() + at, d.size() - at, cmd, used);
            if (p == Parse::Need) { ++truncated_datagrams; return; }
            if (p == Parse::Bad) { ++malformed_datagrams; return; }
            book.apply(cmd);
            ++messages;
            at += used;
        }
    }

    // A stream carries partial messages over; an invalid message kills it.
    struct Stream {
        Bytes pending;
    };
    bool feed(Stream& s, const std::uint8_t* data, std::size_t size) {
        s.pending.insert(s.pending.end(), data, data + size);
        std::size_t at = 0;
        bool alive = true;
        while (at < s.pending.size()) {
            Cmd cmd{};
            std::size_t used = 0;
            const Parse p = parse(s.pending.data() + at, s.pending.size() - at, cmd, used);
            if (p == Parse::Need) break;
            if (p == Parse::Bad) { alive = false; break; }
            book.apply(cmd);
            ++messages;
            at += used;
        }
        if (!alive) {
            ++stream_errors;
            ++streams_closed;
            s.pending.clear();
            return false;
        }
        s.pending.erase(s.pending.begin(), s.pending.begin() + static_cast<std::ptrdiff_t>(at));
        return true;
    }
    void close(Stream& s) {
        if (!s.pending.empty()) ++truncated_streams;
        ++streams_closed;
        s.pending.clear();
    }

    Book& book;
    std::uint64_t messages{0}, datagrams{0}, datagram_bytes{0}, malformed_datagrams{0};
    std::uint64_t truncated_datagrams{0}, stream_errors{0}, truncated_streams{0}, streams_closed{0};
};

}  // namespace ref

// =====================================================================================
// Payload generation and corruption
// =====================================================================================

void put_le(Bytes& out, std::uint64_t value, int bytes) {
    for (int i = 0; i < bytes; ++i) out.push_back(static_cast<std::uint8_t>(value >> (8 * i)));
}

struct FlowProfile {
    std::uint64_t id_space;
    bool cascade;  // one-sided sell-off: deep marketable sells and market sells
};

void gen_messages(Rng& rng, Bytes& out, std::size_t count, const FlowProfile& profile) {
    for (std::size_t i = 0; i < count; ++i) {
        const std::uint64_t roll = rng.below(100);
        const bool buy = profile.cascade ? rng.chance(12) : rng.chance(50);
        const std::uint8_t side = buy ? 'B' : 'S';
        if (roll < 55) {
            std::int64_t price = 1000 + static_cast<std::int64_t>(rng.below(31)) - 15;
            if (profile.cascade && !buy) price -= static_cast<std::int64_t>(rng.below(40));
            const std::uint64_t qty = rng.chance(3) ? 0 : 1 + rng.below(60);
            out.push_back('A');
            put_le(out, 1 + rng.below(profile.id_space), 8);
            out.push_back(side);
            put_le(out, static_cast<std::uint64_t>(price), 8);
            put_le(out, qty, 4);
        } else if (roll < (profile.cascade ? 70U : 85U)) {
            out.push_back('X');
            put_le(out, 1 + rng.below(profile.id_space), 8);
        } else {
            out.push_back('M');
            out.push_back(side);
            put_le(out, rng.chance(2) ? 0 : 1 + rng.below(120), 4);
        }
    }
}

enum Mutation : std::size_t {
    kNone, kTruncate, kBitFlip, kScramble, kOverwrite, kInsert, kDelete, kNoise, kEmpty, kMutationCount
};
const char* const kMutationNames[kMutationCount] = {"valid",     "truncated", "bit-flipped", "scrambled", "overwritten",
                                                    "byte-inserted", "byte-deleted", "pure-noise", "empty"};

// Corrupts bytes[from, end) in place and reports what it did.
Mutation mutate(Rng& rng, Bytes& bytes, std::size_t from) {
    const std::size_t len = bytes.size() - from;
    const Mutation kind = static_cast<Mutation>(1 + rng.below(kMutationCount - 1));
    if (len == 0 && kind != kNoise) return kNone;
    std::uint8_t* data = bytes.data() + from;
    switch (kind) {
        case kTruncate:
            bytes.resize(from + rng.below(len));
            break;
        case kBitFlip:
            for (std::uint64_t flips = 1 + rng.below(8); flips > 0; --flips) {
                data[rng.below(len)] ^= static_cast<std::uint8_t>(1U << rng.below(8));
            }
            break;
        case kScramble: {  // shuffle a window of bytes
            const std::size_t start = rng.below(len);
            const std::size_t span = 1 + rng.below(len - start < 64 ? len - start : 64);
            for (std::size_t i = span - 1; i > 0; --i) std::swap(data[start + i], data[start + rng.below(i + 1)]);
            break;
        }
        case kOverwrite: {  // stomp a window with random bytes
            const std::size_t start = rng.below(len);
            const std::size_t span = 1 + rng.below(len - start < 32 ? len - start : 32);
            for (std::size_t i = 0; i < span; ++i) data[start + i] = static_cast<std::uint8_t>(rng.next());
            break;
        }
        case kInsert:  // shifts every later message off its framing boundary
            bytes.insert(bytes.begin() + static_cast<std::ptrdiff_t>(from + rng.below(len + 1)),
                         static_cast<std::uint8_t>(rng.next()));
            break;
        case kDelete:
            bytes.erase(bytes.begin() + static_cast<std::ptrdiff_t>(from + rng.below(len)));
            break;
        case kNoise:
            bytes.resize(from);
            for (std::uint64_t n = 1 + rng.below(96); n > 0; --n) bytes.push_back(static_cast<std::uint8_t>(rng.next()));
            break;
        case kEmpty:
            bytes.resize(from);
            break;
        default:
            break;
    }
    return kind;
}

std::span<const std::byte> as_span(const std::uint8_t* data, std::size_t size) {
    return {reinterpret_cast<const std::byte*>(data), size};
}

// =====================================================================================
// Pipeline observation
// =====================================================================================

struct Recorder final : aether::EngineListener {
    struct Outcome {
        aether::Status status;
        std::uint64_t filled;
        std::uint64_t resting;
    };
    std::vector<aether::Trade> trades;
    std::vector<Outcome> outcomes;
    std::uint32_t stall_every{0};  // burst mode: periodically freeze the matching thread

    void on_trade(const aether::Trade& trade, const aether::Command&) noexcept override { trades.push_back(trade); }
    void on_processed(const aether::Command&, const aether::AddResult& r) noexcept override {
        outcomes.push_back({r.status, r.filled, r.resting});
        if (stall_every != 0 && outcomes.size() % stall_every == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(40));
        }
    }
};

ref::Result to_ref(aether::Status status) {
    switch (status) {
        case aether::Status::Ok: return ref::Result::Ok;
        case aether::Status::InvalidQty: return ref::Result::InvalidQty;
        case aether::Status::DuplicateId: return ref::Result::DuplicateId;
        case aether::Status::PoolExhausted: return ref::Result::Exhausted;
        case aether::Status::NotFound: return ref::Result::NotFound;
    }
    return ref::Result::Ok;
}

std::vector<ref::RestingOrder> snapshot(const aether::OrderBook& book) {
    std::vector<ref::RestingOrder> all;
    for (const aether::Side side : {aether::Side::Buy, aether::Side::Sell}) {
        for (const aether::PriceLevel* level = book.best(side); level != nullptr; level = level->next) {
            for (const aether::Order* order = level->orders.front(); order != nullptr; order = order->next) {
                all.push_back({side == aether::Side::Buy, order->price, order->id, order->qty});
            }
        }
    }
    return all;
}

// =====================================================================================
// Scenario driver
// =====================================================================================

struct Totals {
    std::uint64_t payloads{0}, scenarios{0}, burst_scenarios{0}, socket_scenarios{0};
    std::uint64_t saturated_scenarios{0}, backpressure_events{0};
    std::uint64_t messages{0}, trades{0}, rejected_commands{0}, resting_checked{0};
    std::uint64_t malformed_datagrams{0}, truncated_datagrams{0}, stream_errors{0}, truncated_streams{0};
    std::uint64_t by_mutation[kMutationCount]{};
};

std::atomic<std::uint64_t> g_progress{0};  // watchdog heartbeat

#define EXPECT(cond)                                                              \
    do {                                                                          \
        if (!(cond)) {                                                            \
            std::printf("  MISMATCH %s:%d  %s\n", __FILE__, __LINE__, #cond);     \
            return false;                                                         \
        }                                                                         \
    } while (false)

template <class Predicate>
bool spin_until(Predicate&& done) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    for (std::uint32_t spins = 0; !done(); ++spins) {
        if ((spins & 0xFFF) == 0xFFF && std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::yield();
    }
    return true;
}

bool write_all(int fd, const std::uint8_t* data, std::size_t size) {
    while (size > 0) {
        const ssize_t n = ::write(fd, data, size);
        if (n <= 0) return false;
        data += n;
        size -= static_cast<std::size_t>(n);
    }
    return true;
}

int connect_loopback(int type, std::uint16_t port) {
    const int fd = ::socket(AF_INET, type, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (fd < 0 || ::connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof address) != 0) {
        if (fd >= 0) ::close(fd);
        return -1;
    }
    return fd;
}

bool verify(const aether::Engine& engine, const Recorder& recorder, const aether::NetworkGatewayStats& stats,
            const ref::Gateway& model, Totals& totals) {
    const ref::Book& expected = model.book;

    // Gateway framing decisions.
    EXPECT(stats.messages == model.messages);
    EXPECT(stats.udp_datagrams == model.datagrams);
    EXPECT(stats.udp_bytes == model.datagram_bytes);
    EXPECT(stats.udp_malformed == model.malformed_datagrams);
    EXPECT(stats.udp_truncated == model.truncated_datagrams);
    EXPECT(stats.tcp_protocol_errors == model.stream_errors);
    EXPECT(stats.tcp_truncated == model.truncated_streams);
    EXPECT(stats.tcp_closed == model.streams_closed);
    EXPECT(stats.dropped == 0 && stats.io_errors == 0 && stats.tcp_rejected == 0);

    // Nothing lost or duplicated between the threads.
    EXPECT(engine.submitted() == model.messages);
    EXPECT(engine.processed() == model.messages);

    // Per-command outcomes, in order.
    EXPECT(recorder.outcomes.size() == expected.outcomes.size());
    for (std::size_t i = 0; i < expected.outcomes.size(); ++i) {
        const Recorder::Outcome& got = recorder.outcomes[i];
        const ref::Outcome& want = expected.outcomes[i];
        EXPECT(to_ref(got.status) == want.result && got.filled == want.filled && got.resting == want.resting);
        totals.rejected_commands += want.result != ref::Result::Ok;
    }

    // Every trade, in order.
    EXPECT(recorder.trades.size() == expected.fills.size());
    for (std::size_t i = 0; i < expected.fills.size(); ++i) {
        const aether::Trade& got = recorder.trades[i];
        const ref::Fill& want = expected.fills[i];
        EXPECT(got.maker_id == want.maker && got.taker_id == want.taker && got.price == want.price &&
               got.qty == want.qty && (got.taker_side == aether::Side::Buy) == want.taker_buy);
    }

    // The complete book, order by order, plus the engine's own structural audit.
    const std::vector<ref::RestingOrder> got_book = snapshot(engine.book());
    const std::vector<ref::RestingOrder> want_book = expected.snapshot();
    EXPECT(got_book.size() == want_book.size());
    for (std::size_t i = 0; i < want_book.size(); ++i) {
        EXPECT(got_book[i].buy == want_book[i].buy && got_book[i].price == want_book[i].price &&
               got_book[i].id == want_book[i].id && got_book[i].qty == want_book[i].qty);
    }
    EXPECT(engine.book().order_count() == want_book.size());
    EXPECT(engine.book().check_invariants());

    totals.messages += model.messages;
    totals.trades += expected.fills.size();
    totals.resting_checked += want_book.size();
    totals.malformed_datagrams += model.malformed_datagrams;
    totals.truncated_datagrams += model.truncated_datagrams;
    totals.stream_errors += model.stream_errors;
    totals.truncated_streams += model.truncated_streams;
    return true;
}

enum class ScenarioKind { Normal, Burst, Sockets };

bool run_scenario(std::uint64_t seed, std::uint64_t index, Totals& totals) {
    Rng rng{seed * 0x9E3779B97F4A7C15ULL + index * 0xD1B54A32D192ED03ULL + 1};
    for (int i = 0; i < 8; ++i) rng.next();

    const std::uint64_t kind_roll = rng.below(100);
    const ScenarioKind kind = kind_roll < 70 ? ScenarioKind::Normal
                              : kind_roll < 90 ? ScenarioKind::Burst : ScenarioKind::Sockets;
    const bool burst = kind == ScenarioKind::Burst;
    const bool sockets = kind == ScenarioKind::Sockets;

    static constexpr std::uint32_t kOrderCaps[] = {24, 256, 4096};
    static constexpr std::uint32_t kLevelCaps[] = {6, 48, 512};
    static constexpr std::size_t kBurstQueues[] = {2, 4, 16, 64};
    static constexpr std::size_t kQueues[] = {2, 16, 256, 4096};
    const aether::BookConfig book_config{kOrderCaps[rng.below(3)], kLevelCaps[rng.below(3)]};
    const std::size_t queue_capacity = burst ? kBurstQueues[rng.below(4)] : kQueues[rng.below(4)];
    const FlowProfile profile{burst ? 4000U : (rng.chance(50) ? 300U : 5000U), burst && rng.chance(50)};
    const std::size_t payload_count = burst ? 60 + rng.below(240) : sockets ? 150 + rng.below(250) : 1000 + rng.below(3000);
    const std::size_t max_messages = burst ? 2500 : 40;

    ref::Book ref_book(book_config.max_orders, book_config.max_levels);
    ref::Gateway model(ref_book);

    Recorder recorder;
    recorder.stall_every = burst ? 256 : 0;
    aether::Engine engine({.book = book_config, .queue_capacity = queue_capacity}, &recorder);
    engine.start();
    aether::NetworkGateway gateway(
        engine, {.enable_udp = sockets,
                 .enable_tcp = sockets,
                 .max_connections = 4,
                 .stream_buffer_bytes = 4096,
                 .backend = (index & 1) != 0 ? aether::PollBackend::Poll : aether::PollBackend::Auto});

    constexpr int kStreams = 3;
    struct StreamState {
        int id{-1};          // gateway stream id (in-process) or client socket fd (sockets)
        Bytes outbox;        // bytes generated but not yet sent: lets chunks end mid-message
        ref::Gateway::Stream model;
    };
    StreamState streams[kStreams];
    const int stream_count = sockets ? 1 : kStreams;  // one socket stream keeps arrival order deterministic
    int udp_fd = -1;
    std::uint64_t accepted = 0;
    bool ok = true;

    const auto open_stream = [&](StreamState& s) {
        if (!sockets) {
            s.id = gateway.open_stream();
            return s.id >= 0;
        }
        s.id = connect_loopback(SOCK_STREAM, gateway.tcp_port());
        ++accepted;
        return s.id >= 0 && spin_until([&] { return gateway.stats().tcp_accepted == accepted; });
    };
    // Everything sent so far has been decoded: required before changing channel,
    // otherwise two sockets could be drained in either order.
    const auto synced = [&] {
        const bool done = spin_until([&] {
            const aether::NetworkGatewayStats s = gateway.stats();
            return s.messages == model.messages && s.udp_datagrams == model.datagrams &&
                   s.tcp_closed == model.streams_closed;
        });
        if (!done) {
            const aether::NetworkGatewayStats s = gateway.stats();
            std::printf("  acknowledgement timeout: messages %llu/%llu datagrams %llu/%llu closed %llu/%llu\n",
                        static_cast<unsigned long long>(s.messages), static_cast<unsigned long long>(model.messages),
                        static_cast<unsigned long long>(s.udp_datagrams), static_cast<unsigned long long>(model.datagrams),
                        static_cast<unsigned long long>(s.tcp_closed), static_cast<unsigned long long>(model.streams_closed));
        }
        return done;
    };

    if (sockets) {
        gateway.start();
        udp_fd = connect_loopback(SOCK_DGRAM, gateway.udp_port());
        ok = udp_fd >= 0;
    }
    for (int i = 0; ok && i < stream_count; ++i) ok = open_stream(streams[i]);
    if (!ok) std::printf("  scenario setup failed (sockets=%d)\n", sockets ? 1 : 0);

    Bytes payload;
    std::uint64_t unsynced = 0;
    for (std::size_t p = 0; ok && p < payload_count; ++p) {
        const std::size_t messages = 1 + rng.below(burst && rng.chance(70) ? max_messages : 40);
        if (rng.chance(50)) {
            // ---- datagram ----
            payload.clear();
            gen_messages(rng, payload, sockets && messages > 50 ? 50 : messages, profile);
            const Mutation mutation = rng.chance(50) ? mutate(rng, payload, 0) : kNone;
            ++totals.by_mutation[mutation];
            model.datagram(payload);
            if (sockets) {
                ok = ::send(udp_fd, payload.data(), payload.size(), 0) == static_cast<ssize_t>(payload.size());
                if (!ok) std::printf("  udp send of %zu bytes failed: errno %d\n", payload.size(), errno);
                if (ok && ++unsynced >= 8) {  // small windows: the socket buffer can never overflow
                    ok = synced();
                    unsynced = 0;
                }
            } else {
                gateway.on_datagram(as_span(payload.data(), payload.size()));
            }
        } else {
            // ---- stream chunk ----
            StreamState& s = streams[rng.below(static_cast<std::uint64_t>(stream_count))];
            const std::size_t fresh_from = s.outbox.size();
            gen_messages(rng, s.outbox, messages, profile);
            const Mutation mutation = rng.chance(10) ? mutate(rng, s.outbox, fresh_from) : kNone;
            ++totals.by_mutation[mutation];
            // Send an arbitrary prefix: usually cuts a message in two.
            const std::size_t send = s.outbox.empty() ? 0 : (rng.chance(25) ? s.outbox.size() : rng.below(s.outbox.size() + 1));
            // Sockets: flush the datagram channel first, so this chunk cannot overtake it.
            if (sockets) ok = synced();
            const bool model_alive = model.feed(s.model, s.outbox.data(), send);
            bool alive = model_alive;
            if (sockets) {
                ok = ok && write_all(s.id, s.outbox.data(), send) && synced();
                unsynced = 0;
            } else {
                alive = gateway.feed_stream(s.id, as_span(s.outbox.data(), send));
                if (alive != model_alive) {
                    std::printf("  MISMATCH stream liveness: gateway=%d model=%d\n", alive ? 1 : 0, model_alive ? 1 : 0);
                    ok = false;
                }
            }
            s.outbox.erase(s.outbox.begin(), s.outbox.begin() + static_cast<std::ptrdiff_t>(send));
            // A killed stream is replaced; a live one is occasionally closed mid-flight (EOF).
            const bool eof = alive && rng.chance(2);
            if (ok && (!alive || eof)) {
                if (eof) model.close(s.model);
                if (sockets) {
                    ::close(s.id);
                    ok = synced();
                } else if (eof) {
                    gateway.close_stream(s.id);
                }
                s.outbox.clear();
                ok = ok && open_stream(s);
            }
        }
        ++totals.payloads;
        g_progress.fetch_add(1, std::memory_order_release);
    }

    // Orderly end: EOF every stream, quiesce the producer, then drain the engine.
    for (int i = 0; ok && i < stream_count; ++i) {
        model.close(streams[i].model);
        if (sockets) {
            ::close(streams[i].id);
        } else {
            gateway.close_stream(streams[i].id);
        }
    }
    if (sockets) {
        ok = ok && synced();
        if (udp_fd >= 0) ::close(udp_fd);
        gateway.stop();
    }
    const std::uint64_t backpressure = engine.backpressure_events();
    engine.stop();
    if (!ok) {
        std::printf("  scenario aborted before verification (I/O failure or acknowledgement timeout)\n");
        return false;
    }

    if (!verify(engine, recorder, gateway.stats(), model, totals)) return false;

    ++totals.scenarios;
    totals.burst_scenarios += burst;
    totals.socket_scenarios += sockets;
    totals.backpressure_events += backpressure;
    if (burst) {
        totals.saturated_scenarios += backpressure != 0;
        // With the matching thread stalling every 256 commands, any burst this
        // long must have found the queue completely full at least once.
        if (model.messages >= 4 * (queue_capacity + recorder.stall_every) && backpressure == 0) {
            std::printf("  burst scenario never saturated its %zu-slot queue (%llu messages)\n", queue_capacity,
                        static_cast<unsigned long long>(model.messages));
            return false;
        }
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    std::signal(SIGPIPE, SIG_IGN);
    const std::uint64_t target = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 1'000'000;
    const std::uint64_t seed = argc > 2 ? std::strtoull(argv[2], nullptr, 0) : 0xAE7E4B0BULL;

    // Deadlock detector: no payload completing for 120 s means something is wedged.
    std::atomic<bool> finished{false};
    std::thread watchdog([&] {
        std::uint64_t last = 0;
        int stalled_ticks = 0;
        while (!finished.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            const std::uint64_t now = g_progress.load(std::memory_order_acquire);
            stalled_ticks = now == last ? stalled_ticks + 1 : 0;
            last = now;
            if (stalled_ticks >= 1200) {
                std::fprintf(stderr, "FUZZ FAILED: no progress for 120 s (deadlock) after %llu payloads\n",
                             static_cast<unsigned long long>(now));
                std::_Exit(3);
            }
        }
    });

    Totals totals;
    bool ok = true;
    std::uint64_t index = 0;
    const auto started = std::chrono::steady_clock::now();
    try {
        for (; ok && totals.payloads < target; ++index) {
            ok = run_scenario(seed, index, totals);
            g_progress.fetch_add(1, std::memory_order_release);
        }
    } catch (const std::exception& e) {
        std::printf("  UNHANDLED EXCEPTION: %s\n", e.what());
        ok = false;
    } catch (...) {
        std::printf("  UNHANDLED EXCEPTION (unknown type)\n");
        ok = false;
    }
    finished.store(true, std::memory_order_release);
    watchdog.join();
    const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();

    if (!ok) {
        std::printf("FUZZ FAILED at scenario %llu (reproduce: %s %llu 0x%llx)\n",
                    static_cast<unsigned long long>(index - 1), argv[0], static_cast<unsigned long long>(target),
                    static_cast<unsigned long long>(seed));
        return 1;
    }

    const auto u = [](std::uint64_t v) { return static_cast<unsigned long long>(v); };
    std::printf("AetherLOB differential fuzz: seed 0x%llx, %.1f s\n", u(seed), seconds);
    std::printf("  payloads fed            %llu in %llu scenarios (%llu burst, %llu over loopback sockets)\n",
                u(totals.payloads), u(totals.scenarios), u(totals.burst_scenarios), u(totals.socket_scenarios));
    std::uint64_t corrupted = 0;
    for (std::size_t m = 1; m < kMutationCount; ++m) corrupted += totals.by_mutation[m];
    std::printf("  corrupted payloads      %llu (the rest are valid flow, including every burst)\n", u(corrupted));
    std::printf("  payload kinds          ");
    for (std::size_t m = 0; m < kMutationCount; ++m) std::printf(" %s=%llu", kMutationNames[m], u(totals.by_mutation[m]));
    std::printf("\n");
    std::printf("  commands verified       %llu (%llu rejected by the book), %llu trades, %llu resting orders compared\n",
                u(totals.messages), u(totals.rejected_commands), u(totals.trades), u(totals.resting_checked));
    std::printf("  gateway rejections      %llu malformed + %llu truncated datagrams, %llu streams killed, %llu streams cut mid-message\n",
                u(totals.malformed_datagrams), u(totals.truncated_datagrams), u(totals.stream_errors),
                u(totals.truncated_streams));
    std::printf("  queue saturation        %llu/%llu burst scenarios hit a 100%% full queue, %llu backpressure events in total\n",
                u(totals.saturated_scenarios), u(totals.burst_scenarios), u(totals.backpressure_events));
    std::printf("FUZZ PASSED: pipeline and reference model agree on every outcome, trade and book state\n");
    return 0;
}
