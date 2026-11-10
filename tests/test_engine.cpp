// Wire protocol and threaded ingestion pipeline tests. The pipeline tests
// assert determinism: the gateway -> SPSC queue -> matching thread path must
// produce exactly the trades a single-threaded book produces for the same
// byte stream. Built to run under TSan/ASan.
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <thread>
#include <vector>

#include <sys/socket.h>
#include <unistd.h>

#include "engine.hpp"
#include "protocol.hpp"
#include "test_common.hpp"

using namespace aether;

namespace {

constexpr BookConfig kBook{.max_orders = 1U << 18, .max_levels = 1U << 10};

bool same(const Trade& a, const Trade& b) {
    return a.maker_id == b.maker_id && a.taker_id == b.taker_id && a.price == b.price &&
           a.qty == b.qty && a.taker_side == b.taker_side;
}

bool same(const std::vector<Trade>& a, const std::vector<Trade>& b) {
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (!same(a[i], b[i])) return false;
    }
    return true;
}

struct TradeRecorder final : EngineListener {
    std::vector<Trade> trades;
    std::uint64_t processed{0};
    std::uint64_t rejected{0};
    void on_trade(const Trade& trade, const Command&) noexcept override { trades.push_back(trade); }
    void on_processed(const Command&, const AddResult& result) noexcept override {
        ++processed;
        rejected += result.status != Status::Ok;
    }
};

struct Stream {
    std::vector<std::byte> bytes;
    std::size_t messages{0};
};

// Mixed add / cancel / market flow around a fixed mid price.
Stream make_stream(std::size_t count, std::uint64_t seed) {
    Stream stream;
    std::uint64_t state = seed;
    auto rng = [&state]() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    };
    OrderId next_id = 1;
    for (std::size_t i = 0; i < count; ++i) {
        const std::uint64_t roll = rng() % 100;
        const Side side = (rng() & 1) != 0 ? Side::Buy : Side::Sell;
        if (roll < 35 && next_id > 1) {
            append(stream.bytes, make_cancel(1 + rng() % (next_id - 1)));
        } else if (roll < 45) {
            append(stream.bytes, make_market(side, static_cast<std::uint32_t>(1 + rng() % 80)));
        } else {
            const Price price = 1000 + static_cast<Price>(rng() % 40) - 20;
            append(stream.bytes, make_add(next_id++, side, price, static_cast<std::uint32_t>(1 + rng() % 50)));
        }
    }
    stream.messages = count;
    return stream;
}

struct Expected {
    std::vector<Trade> trades;
    std::size_t orders{0};
    std::size_t levels{0};
    Price best_bid{0};
    Price best_ask{0};
};

// Ground truth: decode and apply the stream on one thread, no queue involved.
Expected replay_single_threaded(const Stream& stream) {
    OrderBook book(kBook);
    Expected expected;
    const auto on_trade = [&](const Trade& t) { expected.trades.push_back(t); };
    std::span<const std::byte> rest(stream.bytes);
    Command cmd;
    while (!rest.empty()) {
        const DecodeResult d = decode(rest, cmd);
        if (d.status != DecodeStatus::Ok) break;
        rest = rest.subspan(d.consumed);
        switch (cmd.type) {
            case CommandType::Add: book.add_limit(cmd.order_id, cmd.side, cmd.price, cmd.qty, on_trade); break;
            case CommandType::Cancel: book.cancel(cmd.order_id); break;
            case CommandType::Market: book.add_market(cmd.side, cmd.qty, on_trade); break;
        }
    }
    CHECK(rest.empty());
    expected.orders = book.order_count();
    expected.levels = book.level_count();
    expected.best_bid = book.best_bid() != nullptr ? book.best_bid()->price : 0;
    expected.best_ask = book.best_ask() != nullptr ? book.best_ask()->price : 0;
    return expected;
}

void check_matches(const Engine& engine, const TradeRecorder& recorder, const Expected& expected,
                   std::size_t messages) {
    const OrderBook& book = engine.book();
    CHECK(engine.submitted() == messages);
    CHECK(engine.processed() == messages);
    CHECK(recorder.processed == messages);
    CHECK(same(recorder.trades, expected.trades));
    CHECK(book.order_count() == expected.orders);
    CHECK(book.level_count() == expected.levels);
    CHECK((book.best_bid() != nullptr ? book.best_bid()->price : 0) == expected.best_bid);
    CHECK((book.best_ask() != nullptr ? book.best_ask()->price : 0) == expected.best_ask);
    CHECK(book.check_invariants());
}

// ---- Wire protocol ---------------------------------------------------------------

void test_wire_layout_is_little_endian_and_packed() {
    const AddOrderMsg add = make_add(0x0102030405060708ULL, Side::Sell, -2, 0xA1B2C3D4U);
    const unsigned char expected[22] = {
        'A',                                             // type
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,  // order_id, LSB first
        'S',                                             // side
        0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,  // price = -2 (two's complement)
        0xD4, 0xC3, 0xB2, 0xA1,                          // qty
    };
    CHECK(sizeof add == sizeof expected);
    CHECK(std::memcmp(&add, expected, sizeof expected) == 0);

    const CancelOrderMsg cancel = make_cancel(0x1122334455667788ULL);
    const unsigned char expected_cancel[9] = {'X', 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11};
    CHECK(std::memcmp(&cancel, expected_cancel, sizeof expected_cancel) == 0);

    const MarketOrderMsg market = make_market(Side::Buy, 258);
    const unsigned char expected_market[6] = {'M', 'B', 0x02, 0x01, 0x00, 0x00};
    CHECK(std::memcmp(&market, expected_market, sizeof expected_market) == 0);
}

void test_decode_round_trip_and_errors() {
    std::vector<std::byte> bytes;
    append(bytes, make_add(77, Side::Buy, -123456789012345LL, 4'000'000'000U));
    append(bytes, make_cancel(UINT64_MAX));
    append(bytes, make_market(Side::Sell, 9));

    std::span<const std::byte> rest(bytes);
    Command cmd;
    DecodeResult d = decode(rest, cmd);
    CHECK(d.status == DecodeStatus::Ok && d.consumed == 22);
    CHECK(cmd.type == CommandType::Add && cmd.order_id == 77 && cmd.side == Side::Buy);
    CHECK(cmd.price == -123456789012345LL && cmd.qty == 4'000'000'000ULL);
    rest = rest.subspan(d.consumed);

    d = decode(rest, cmd);
    CHECK(d.status == DecodeStatus::Ok && d.consumed == 9);
    CHECK(cmd.type == CommandType::Cancel && cmd.order_id == UINT64_MAX);
    rest = rest.subspan(d.consumed);

    d = decode(rest, cmd);
    CHECK(d.status == DecodeStatus::Ok && d.consumed == 6);
    CHECK(cmd.type == CommandType::Market && cmd.side == Side::Sell && cmd.qty == 9);
    CHECK(rest.subspan(d.consumed).empty());

    // Every strict prefix of a message is "need more data", never a misparse.
    // Each prefix is copied to an exactly-sized heap block so ASan would flag
    // any read past the end.
    bool prefixes_ok = true;
    for (std::size_t len = 0; len < sizeof(AddOrderMsg); ++len) {
        const std::vector<std::byte> prefix(bytes.begin(), bytes.begin() + static_cast<std::ptrdiff_t>(len));
        const DecodeResult p = decode(prefix, cmd);
        prefixes_ok = prefixes_ok && p.status == DecodeStatus::NeedMoreData && p.consumed == 0;
    }
    CHECK(prefixes_ok);

    std::vector<std::byte> bad_type(32, std::byte{'Z'});
    CHECK(decode(bad_type, cmd).status == DecodeStatus::UnknownType);

    AddOrderMsg bad_side = make_add(1, Side::Buy, 1, 1);
    bad_side.side = 'Q';
    std::vector<std::byte> bad_side_bytes;
    append(bad_side_bytes, bad_side);
    CHECK(decode(bad_side_bytes, cmd).status == DecodeStatus::BadSide);
}

// ---- Threaded pipeline -----------------------------------------------------------

void test_pipeline_matches_single_threaded_replay() {
    const Stream stream = make_stream(300'000, 0xC0FFEE1234ULL);
    const Expected expected = replay_single_threaded(stream);
    CHECK(expected.trades.size() > 10'000);

    TradeRecorder recorder;
    // A deliberately tiny queue keeps the producer in backpressure.
    Engine engine({.book = kBook, .queue_capacity = 64}, &recorder);
    engine.start();

    IngestResult result{};
    std::thread gateway([&] { result = engine.ingest(stream.bytes); });
    gateway.join();
    engine.stop();

    CHECK(result.status == DecodeStatus::Ok);
    CHECK(result.consumed == stream.bytes.size() && result.messages == stream.messages);
    CHECK(engine.backpressure_events() > 0);
    check_matches(engine, recorder, expected, stream.messages);
    std::printf("    (%zu messages, %zu trades, %llu backpressure events on a %zu-slot queue)\n",
                stream.messages, recorder.trades.size(),
                static_cast<unsigned long long>(engine.backpressure_events()), engine.queue_capacity());
}

void write_all(int fd, const std::byte* data, std::size_t size) {
    while (size > 0) {
        const ssize_t n = ::write(fd, data, size);
        if (n <= 0) {
            CHECK(n > 0);
            return;
        }
        data += n;
        size -= static_cast<std::size_t>(n);
    }
}

void test_socket_gateway_reassembles_fragmented_stream() {
    const Stream stream = make_stream(100'000, 0xFEEDFACEULL);
    const Expected expected = replay_single_threaded(stream);

    int fds[2];
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    TradeRecorder recorder;
    Engine engine({.book = kBook, .queue_capacity = 1024}, &recorder);
    engine.start();
    CHECK(engine.start_gateway(fds[0]));
    CHECK(!engine.start_gateway(fds[0]));  // only one gateway thread at a time

    // Irregular write sizes split messages at arbitrary byte boundaries.
    std::uint64_t state = 99;
    for (std::size_t off = 0; off < stream.bytes.size();) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        std::size_t chunk = 1 + static_cast<std::size_t>((state >> 33) % 997);
        if (chunk > stream.bytes.size() - off) chunk = stream.bytes.size() - off;
        write_all(fds[1], stream.bytes.data() + off, chunk);
        off += chunk;
    }
    ::close(fds[1]);  // EOF ends the gateway session

    const GatewayStats stats = engine.join_gateway();
    engine.stop();
    ::close(fds[0]);

    CHECK(stats.status == DecodeStatus::Ok && !stats.io_error);
    CHECK(stats.messages == stream.messages && stats.bytes == stream.bytes.size());
    check_matches(engine, recorder, expected, stream.messages);
}

void test_socket_gateway_reports_bad_streams() {
    for (const bool truncated : {true, false}) {
        std::vector<std::byte> bytes;
        append(bytes, make_add(1, Side::Buy, 100, 5));
        append(bytes, make_add(2, Side::Sell, 101, 5));
        if (truncated) {
            bytes.resize(bytes.size() - 3);  // peer hangs up mid-message
        } else {
            bytes.push_back(std::byte{0x7F});  // unknown type tag
            append(bytes, make_add(3, Side::Buy, 100, 5));
        }

        int fds[2];
        CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
        Engine engine({.book = {.max_orders = 64, .max_levels = 16}, .queue_capacity = 16});
        engine.start();
        CHECK(engine.start_gateway(fds[0]));
        write_all(fds[1], bytes.data(), bytes.size());
        ::close(fds[1]);
        const GatewayStats stats = engine.join_gateway();
        engine.stop();
        ::close(fds[0]);

        CHECK(stats.status == (truncated ? DecodeStatus::NeedMoreData : DecodeStatus::UnknownType));
        CHECK(stats.messages == (truncated ? 1U : 2U));  // only whole, valid messages get through
        CHECK(engine.processed() == stats.messages);
        CHECK(engine.book().check_invariants());
    }
}

void test_stop_drains_queue() {
    // stop() immediately after a burst must still apply every queued command.
    constexpr std::uint64_t kBurst = 20'000;
    for (int round = 0; round < 25; ++round) {
        Engine engine({.book = kBook, .queue_capacity = 1U << 15});
        engine.start();
        bool all_submitted = true;
        for (std::uint64_t i = 0; i < kBurst; ++i) {
            Command cmd;
            cmd.type = CommandType::Add;
            cmd.order_id = i + 1;
            cmd.side = Side::Buy;
            cmd.price = 100 + static_cast<Price>(i % 50);
            cmd.qty = 1;
            all_submitted = engine.submit(cmd) && all_submitted;
        }
        engine.stop();
        engine.stop();  // idempotent
        CHECK(all_submitted);
        CHECK(!engine.running());
        CHECK(engine.processed() == kBurst);
        CHECK(engine.book().order_count() == kBurst);
    }
}

void test_backpressure_without_consumer_and_restart() {
    Engine engine({.book = {.max_orders = 64, .max_levels = 16}, .queue_capacity = 8});
    Command cmd;
    cmd.type = CommandType::Add;
    cmd.side = Side::Sell;
    cmd.price = 500;
    cmd.qty = 1;

    // Not started: the queue fills, then both submit flavours refuse (no hang, no drop-on-floor).
    for (OrderId id = 1; id <= 8; ++id) {
        cmd.order_id = id;
        CHECK(engine.try_submit(cmd));
    }
    cmd.order_id = 9;
    CHECK(!engine.try_submit(cmd));
    CHECK(!engine.submit(cmd));
    CHECK(engine.submitted() == 8 && engine.processed() == 0);

    engine.start();
    CHECK(engine.submit(cmd));  // now drains, so the 9th gets through
    engine.stop();
    CHECK(engine.processed() == 9 && engine.book().order_count() == 9);

    // The engine can be restarted and keeps its book.
    engine.start();
    cmd.order_id = 10;
    CHECK(engine.submit(cmd));
    engine.stop();
    CHECK(engine.processed() == 10 && engine.book().order_count() == 10);
    CHECK(engine.book().check_invariants());
}

void test_destructor_stops_running_engine() {
    TradeRecorder recorder;
    {
        Engine engine({.book = {.max_orders = 64, .max_levels = 16}, .queue_capacity = 16}, &recorder);
        engine.start();
        Command cmd;
        cmd.order_id = 1;
        cmd.price = 10;
        cmd.qty = 1;
        CHECK(engine.submit(cmd));
    }  // ~Engine joins both threads after draining
    CHECK(recorder.processed == 1);
}

}  // namespace

int main() {
    using aether_test::run;
    run("wire_layout_is_little_endian_and_packed", test_wire_layout_is_little_endian_and_packed);
    run("decode_round_trip_and_errors", test_decode_round_trip_and_errors);
    run("pipeline_matches_single_threaded_replay", test_pipeline_matches_single_threaded_replay);
    run("socket_gateway_reassembles_fragmented_stream", test_socket_gateway_reassembles_fragmented_stream);
    run("socket_gateway_reports_bad_streams", test_socket_gateway_reports_bad_streams);
    run("stop_drains_queue", test_stop_drains_queue);
    run("backpressure_without_consumer_and_restart", test_backpressure_without_consumer_and_restart);
    run("destructor_stops_running_engine", test_destructor_stops_running_engine);
    return aether_test::summary();
}
