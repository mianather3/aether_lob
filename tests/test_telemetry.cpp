// Telemetry tests: hardware timer calibration, histogram bucket maths and
// percentiles, engine counters and latency recording, live L2 snapshots taken
// while the pipeline is running, and the JSON / binary exporters.
// Built to run under TSan/ASan.
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "engine.hpp"
#include "market_snapshot.hpp"
#include "protocol.hpp"
#include "telemetry.hpp"
#include "test_common.hpp"
#include "tsc_timer.hpp"

using namespace aether;

namespace {

constexpr BookConfig kBook{.max_orders = 1U << 16, .max_levels = 1U << 10};

// ---- Hardware timer --------------------------------------------------------------------

void test_timer_is_monotonic_and_calibrated() {
    const tsc::Calibration& cal = tsc::calibrate();
    CHECK(&cal == &tsc::calibrate());  // measured once, then cached
    CHECK(cal.ticks_per_second > 1'000'000 && cal.ns_per_tick > 0.0 && cal.ns_per_tick < 256.0);
    CHECK(tsc::to_ns(0) == 0);

    bool monotonic = true;
    std::uint64_t previous = tsc::now();
    for (int i = 0; i < 1'000'000; ++i) {
        const std::uint64_t t = (i & 1) != 0 ? tsc::now() : tsc::now_end();
        monotonic = monotonic && t >= previous;
        previous = t;
    }
    CHECK(monotonic);

    // The cached conversion must agree with the OS clock over a real interval.
    const auto wall_start = std::chrono::steady_clock::now();
    const std::uint64_t tick_start = tsc::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    const std::uint64_t tick_end = tsc::now_end();
    const auto wall_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::steady_clock::now() - wall_start).count();
    const double ratio = static_cast<double>(tsc::elapsed_ns(tick_start, tick_end)) / static_cast<double>(wall_ns);
    CHECK(ratio > 0.98 && ratio < 1.02);

    // Fast fixed-point path and slow exact path agree where they meet.
    const std::uint64_t edge = 1ULL << 32;
    const double fast = static_cast<double>(tsc::to_ns(edge - 1));
    const double slow = static_cast<double>(tsc::to_ns(edge));
    CHECK(slow >= fast && (slow - fast) / slow < 1e-6);
    CHECK(tsc::elapsed_ns(100, 50) == 0);  // never negative

    // Cost of one timestamp, for the record.
    constexpr int kReads = 2'000'000;
    std::uint64_t sink = 0;
    const std::uint64_t t0 = tsc::now();
    for (int i = 0; i < kReads; ++i) sink += tsc::now();
    const std::uint64_t t1 = tsc::now_end();
    std::printf("    (counter=%s, %.2f ns/tick = %llu Hz via %s, invariant=%d, ~%.1f ns per read, ratio to OS clock %.4f)\n",
                cal.counter, cal.ns_per_tick, static_cast<unsigned long long>(cal.ticks_per_second), cal.method,
                cal.invariant ? 1 : 0, static_cast<double>(tsc::elapsed_ns(t0, t1)) / kReads, ratio);
    CHECK(sink != 0);
}

// ---- Histogram ----------------------------------------------------------------------------

void test_histogram_bucket_maths() {
    using H = LatencyHistogram;
    CHECK(alignof(H) == 64 && sizeof(H) % 64 == 0);
    CHECK(alignof(GatewayCounters) == 64 && alignof(MatchingCounters) == 64);

    // Exact below 64 ns.
    bool exact = true;
    for (std::uint64_t v = 0; v < 64; ++v) {
        exact = exact && H::index_of(v) == v && H::lower_bound(v) == v && H::upper_bound(v) == v;
    }
    CHECK(exact);

    // Buckets tile the range with no gaps or overlaps...
    bool tiled = true;
    for (std::size_t i = 1; i < H::kBucketCount; ++i) {
        tiled = tiled && H::lower_bound(i) == H::upper_bound(i - 1) + 1;
        tiled = tiled && H::index_of(H::lower_bound(i)) == i && H::index_of(H::upper_bound(i)) == i;
    }
    CHECK(tiled);
    CHECK(H::upper_bound(H::kBucketCount - 1) == H::kMaxTrackable);

    // ...every value lands in the bucket that contains it, and no bucket is
    // wider than 1/32 of its lower edge (the 3.125% error bound).
    bool contained = true;
    bool precise = true;
    std::uint64_t state = 0x1234567ULL;
    for (int i = 0; i < 2'000'000; ++i) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        const std::uint64_t v = state >> (28 + state % 36);  // spread across every magnitude
        const std::size_t idx = H::index_of(v);
        if (v > H::kMaxTrackable) {
            contained = contained && idx == H::kBucketCount - 1;  // clamped, not out of bounds
            continue;
        }
        contained = contained && H::lower_bound(idx) <= v && v <= H::upper_bound(idx);
        const std::uint64_t width = H::upper_bound(idx) - H::lower_bound(idx) + 1;
        precise = precise && (width == 1 || width * 32 <= H::lower_bound(idx));
    }
    CHECK(contained);
    CHECK(precise);
    CHECK(H::index_of(UINT64_MAX) == H::kBucketCount - 1);
}

void test_histogram_percentiles() {
    LatencyHistogram h;
    HistogramSummary s = h.summarize();
    CHECK(s.count == 0 && s.max == 0 && s.min == 0 && s.p99 == 0);

    for (std::uint64_t v = 1; v <= 100'000; ++v) h.record(v);  // uniform 1..100000 ns
    s = h.summarize();
    CHECK(s.count == 100'000 && h.count() == 100'000);
    CHECK(s.min == 1 && s.max == 100'000);  // exact
    CHECK(s.mean > 50'000.4 && s.mean < 50'000.6);  // exact: 50000.5

    // A reported percentile is the true one or up to 3.125% above it, never below.
    const auto within = [](std::uint64_t got, std::uint64_t truth) {
        return got >= truth && static_cast<double>(got) <= static_cast<double>(truth) * 1.03125;
    };
    CHECK(within(s.p50, 50'000));
    CHECK(within(s.p90, 90'000));
    CHECK(within(s.p99, 99'000));
    CHECK(within(s.p999, 99'900));
    CHECK(within(s.p9999, 99'990));
    CHECK(s.p50 <= s.p90 && s.p90 <= s.p99 && s.p99 <= s.p999 && s.p999 <= s.p9999 && s.p9999 <= s.max);

    // A single outlier owns the tail without disturbing the body.
    LatencyHistogram spike;
    for (int i = 0; i < 9'999; ++i) spike.record(100);
    spike.record(5'000'000);
    s = spike.summarize();
    CHECK(within(s.p50, 100) && within(s.p99, 100) && within(s.p999, 100));
    CHECK(s.max == 5'000'000 && within(s.p9999, 100));  // rank 9999 of 10000 is still in the body

    LatencyHistogram huge;  // beyond the trackable range: clamped, max still exact
    huge.record(UINT64_MAX / 4);
    CHECK(huge.summarize().count == 1 && huge.summarize().max == UINT64_MAX / 4);
}

void test_histogram_single_writer_many_readers() {
    constexpr std::uint64_t kRecords = 3'000'000;
    LatencyHistogram h;
    std::atomic<bool> done{false};
    std::atomic<bool> went_backwards{false};

    std::vector<std::thread> readers;
    for (int r = 0; r < 2; ++r) {
        readers.emplace_back([&] {
            std::uint64_t last = 0;
            while (!done.load(std::memory_order_acquire)) {
                const HistogramSummary s = h.summarize();
                if (s.count < last || s.p50 > s.max) went_backwards.store(true, std::memory_order_release);
                last = s.count;
            }
        });
    }
    for (std::uint64_t i = 0; i < kRecords; ++i) h.record(40 + (i * 2654435761ULL) % 5000);
    done.store(true, std::memory_order_release);
    for (std::thread& reader : readers) reader.join();

    CHECK(!went_backwards.load(std::memory_order_acquire));
    CHECK(h.count() == kRecords);  // nothing lost to concurrent reads
}

// ---- Engine counters and latency ---------------------------------------------------------

struct TradeTotals final : EngineListener {
    std::uint64_t trades{0};
    std::uint64_t volume{0};
    std::uint64_t commands_with_fills{0};
    std::uint64_t ok{0};
    bool traded{false};
    std::uint32_t stall_first_ms{0};
    void on_trade(const Trade& trade, const Command&) noexcept override {
        ++trades;
        volume += trade.qty;
        traded = true;
    }
    void on_processed(const Command&, const AddResult& result) noexcept override {
        ok += result.status == Status::Ok;
        commands_with_fills += traded;
        traded = false;
        if (stall_first_ms != 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(stall_first_ms));
            stall_first_ms = 0;
        }
    }
};

std::vector<std::byte> make_stream(std::size_t count, std::uint64_t seed) {
    std::vector<std::byte> bytes;
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
        if (roll < 30 && next_id > 1) {
            append(bytes, make_cancel(1 + rng() % (next_id + 20)));  // some ids never existed: rejections
        } else if (roll < 40) {
            append(bytes, make_market(side, static_cast<std::uint32_t>(rng() % 80)));  // qty 0 now and then
        } else {
            const Price price = 1000 + static_cast<Price>(rng() % 40) - 20;
            append(bytes, make_add(next_id++, side, price, static_cast<std::uint32_t>(1 + rng() % 50)));
        }
    }
    return bytes;
}

void test_engine_counters_match_ground_truth() {
    constexpr std::size_t kMessages = 100'000;
    const std::vector<std::byte> stream = make_stream(kMessages, 0x7E1E);

    TradeTotals truth;
    Engine engine({.book = kBook, .queue_capacity = 256}, &truth);
    engine.start();
    // Feed in 1 KiB "packets"; a carried partial message makes sizes irregular.
    std::size_t packets = 0;
    for (std::size_t at = 0; at < stream.size();) {
        const std::size_t len = stream.size() - at < 1024 ? stream.size() - at : 1024;
        const IngestResult r = engine.ingest(std::span<const std::byte>(stream).subspan(at, len));
        at += r.consumed;
        ++packets;
        CHECK(r.status == DecodeStatus::Ok || r.status == DecodeStatus::NeedMoreData);
    }
    engine.stop();

    const TelemetrySnapshot m = engine.metrics();
    CHECK(m.received == kMessages && m.dropped_queue_full == 0);
    CHECK(m.accepted + m.rejected == kMessages);
    CHECK(m.accepted == truth.ok && m.rejected > 0);
    CHECK(m.trades == truth.trades && m.volume == truth.volume && m.trades > 1000);
    CHECK(m.resting_orders == engine.book().order_count() && m.levels == engine.book().level_count());
    CHECK(m.packets == packets && m.bytes == stream.size());
    CHECK(m.invalid_bytes == 0 && m.checksum_errors == 0);
    CHECK(m.backpressure_waits == engine.backpressure_events());

    // One latency sample per command; order-processing latency only for commands that traded.
    CHECK(m.ingest_to_queue.count == kMessages && m.queue_to_match.count == kMessages);
    CHECK(m.match_service.count == kMessages);
    CHECK(m.tick_to_trade.count == truth.commands_with_fills && m.tick_to_trade.count > 0);
    // Intervals nest: the matching step is inside queue->match, which is inside order-processing latency.
    CHECK(m.match_service.max <= m.queue_to_match.max);
    CHECK(m.tick_to_trade.max <= m.queue_to_match.max + m.ingest_to_queue.max);
    CHECK(m.queue_to_match.p50 <= m.queue_to_match.p99 && m.queue_to_match.p99 <= m.queue_to_match.max);
}

void test_invalid_and_dropped_accounting() {
    {
        Engine engine({.book = kBook});
        engine.start();
        std::vector<std::byte> bytes;
        append(bytes, make_add(1, Side::Buy, 100, 5));
        const std::size_t good = bytes.size();
        bytes.insert(bytes.end(), 37, std::byte{0xEE});  // garbage: unusable from the first bad byte on
        CHECK(engine.ingest(bytes).status == DecodeStatus::UnknownType);
        engine.note_invalid_bytes(5);  // what a framing layer reports for a cut-off tail
        engine.stop();
        const TelemetrySnapshot m = engine.metrics();
        CHECK(m.received == 1 && m.accepted == 1 && m.bytes == good);
        CHECK(m.invalid_bytes == 37 + 5);
    }
    {
        // Never started: a 4-slot queue fills, then every further message is a counted drop.
        Engine engine({.book = kBook, .queue_capacity = 4});
        Command cmd;
        cmd.qty = 1;
        int pushed = 0;
        for (OrderId id = 1; id <= 10; ++id) {
            cmd.order_id = id;
            pushed += engine.try_submit(cmd) ? 1 : 0;
        }
        CHECK(pushed == 4);
        CHECK(!engine.submit(cmd));  // blocking flavour gives up too: nothing can drain the queue
        const TelemetrySnapshot m = engine.metrics();
        CHECK(m.received == 11 && m.dropped_queue_full == 7 && m.backpressure_waits == 1);
        CHECK(m.accepted == 0 && m.rejected == 0);
        CHECK(m.received == engine.submitted() + m.dropped_queue_full);
    }
}

void test_latency_reflects_real_delays() {
    // The matching thread stalls 25 ms on the first command while a second one
    // is already queued: that wait must show up in queue->match, not in the
    // matching step itself.
    TradeTotals listener;
    listener.stall_first_ms = 25;
    Engine engine({.book = kBook}, &listener);
    engine.start();
    std::vector<std::byte> bytes;
    append(bytes, make_add(1, Side::Buy, 100, 5));
    append(bytes, make_add(2, Side::Buy, 100, 5));
    CHECK(engine.ingest(bytes).messages == 2);
    engine.stop();

    const TelemetrySnapshot m = engine.metrics();
    CHECK(m.queue_to_match.count == 2);
    CHECK(m.queue_to_match.max >= 20'000'000 && m.queue_to_match.max < 2'000'000'000);
    CHECK(m.match_service.max < 5'000'000);
    CHECK(m.ingest_to_queue.max < 5'000'000);
}

void test_timestamps_can_be_switched_off() {
    Engine engine({.book = kBook, .stamp_ingress = false});
    engine.start();
    std::vector<std::byte> bytes;
    append(bytes, make_add(1, Side::Sell, 100, 5));
    append(bytes, make_add(2, Side::Buy, 100, 3));
    CHECK(engine.ingest(bytes).messages == 2);
    engine.stop();
    const TelemetrySnapshot m = engine.metrics();
    CHECK(m.queue_to_match.count == 0 && m.tick_to_trade.count == 0 && m.ingest_to_queue.count == 0);
    CHECK(m.received == 2 && m.accepted == 2 && m.trades == 1 && m.volume == 3);  // counters still kept
}

// ---- L2 snapshots ---------------------------------------------------------------------------

bool well_formed(const L2Snapshot& s) {
    bool ok = s.bid_levels <= kMaxSnapshotDepth && s.ask_levels <= kMaxSnapshotDepth;
    Qty cumulative = 0;
    for (std::uint32_t i = 0; ok && i < s.bid_levels; ++i) {
        cumulative += s.bids[i].qty;
        ok = s.bids[i].qty > 0 && s.bids[i].orders > 0 && s.bids[i].cumulative_qty == cumulative &&
             (i == 0 || s.bids[i].price < s.bids[i - 1].price);
    }
    cumulative = 0;
    for (std::uint32_t i = 0; ok && i < s.ask_levels; ++i) {
        cumulative += s.asks[i].qty;
        ok = s.asks[i].qty > 0 && s.asks[i].orders > 0 && s.asks[i].cumulative_qty == cumulative &&
             (i == 0 || s.asks[i].price > s.asks[i - 1].price);
    }
    if (ok && s.bid_levels != 0 && s.ask_levels != 0) ok = s.bids[0].price < s.asks[0].price;  // never crossed
    return ok;
}

void test_snapshot_of_running_engine() {
    Engine engine({.book = kBook, .snapshot_depth = 3});
    L2Snapshot snap;
    CHECK(engine.snapshot(snap) && snap.bid_levels == 0 && snap.ask_levels == 0);  // before start: read directly

    engine.start();
    std::vector<std::byte> bytes;
    append(bytes, make_add(1, Side::Buy, 100, 10));
    append(bytes, make_add(2, Side::Buy, 100, 5));
    append(bytes, make_add(3, Side::Buy, 99, 7));
    append(bytes, make_add(4, Side::Buy, 98, 1));
    append(bytes, make_add(5, Side::Buy, 97, 1));   // 4th bid level: beyond depth 3
    append(bytes, make_add(6, Side::Sell, 101, 4));
    append(bytes, make_add(7, Side::Sell, 103, 6));
    CHECK(engine.ingest(bytes).messages == 7);
    while (engine.processed() < 7) std::this_thread::yield();

    CHECK(engine.snapshot(snap));  // served by the live matching thread
    CHECK(engine.snapshots_served() == 1);
    CHECK(snap.sequence == 7 && snap.resting_orders == 7 && snap.level_count == 6);
    CHECK(snap.bid_levels == 3 && snap.ask_levels == 2);
    CHECK(snap.bids[0].price == 100 && snap.bids[0].qty == 15 && snap.bids[0].orders == 2);
    CHECK(snap.bids[1].price == 99 && snap.bids[1].cumulative_qty == 22);
    CHECK(snap.bids[2].price == 98 && snap.bids[2].cumulative_qty == 23);
    CHECK(snap.asks[0].price == 101 && snap.asks[0].qty == 4);
    CHECK(snap.asks[1].price == 103 && snap.asks[1].cumulative_qty == 10);
    CHECK(well_formed(snap));

    engine.stop();
    L2Snapshot after;
    CHECK(engine.snapshot(after) && after.sequence == 7 && after.bids[0].qty == 15);  // stopped: read directly
}

void test_snapshots_under_load_from_several_exporters() {
    // A producer floods the engine while three exporter threads keep asking
    // for depth. Every snapshot must be internally consistent, sequences must
    // never go backwards, and the flow must be completely unaffected.
    constexpr std::size_t kMessages = 400'000;
    const std::vector<std::byte> stream = make_stream(kMessages, 0x5A47);

    OrderBook reference(kBook);
    {
        std::span<const std::byte> rest(stream);
        Command cmd;
        while (!rest.empty()) {
            const DecodeResult d = decode(rest, cmd);
            rest = rest.subspan(d.consumed);
            switch (cmd.type) {
                case CommandType::Add: reference.add_limit(cmd.order_id, cmd.side, cmd.price, cmd.qty); break;
                case CommandType::Cancel: reference.cancel(cmd.order_id); break;
                case CommandType::Market: reference.add_market(cmd.side, cmd.qty); break;
            }
        }
    }

    Engine engine({.book = kBook, .queue_capacity = 1024, .snapshot_depth = 8});
    engine.start();
    std::atomic<bool> sending{true};
    std::atomic<bool> bad{false};

    std::vector<std::thread> exporters;
    for (int e = 0; e < 3; ++e) {
        exporters.emplace_back([&] {
            L2Snapshot snap;
            std::uint64_t last_sequence = 0;
            while (sending.load(std::memory_order_acquire)) {
                if (!engine.snapshot(snap) || !well_formed(snap) || snap.sequence < last_sequence) {
                    bad.store(true, std::memory_order_release);
                }
                last_sequence = snap.sequence;
            }
        });
    }
    // Do not stop until the exporters have demonstrably worked against live flow.
    std::size_t at = 0;
    while (at < stream.size()) {
        const std::size_t len = stream.size() - at < 512 ? stream.size() - at : 512;
        at += engine.ingest(std::span<const std::byte>(stream).subspan(at, len)).consumed;
    }
    while (engine.snapshots_served() < 50) std::this_thread::yield();
    sending.store(false, std::memory_order_release);
    for (std::thread& exporter : exporters) exporter.join();
    engine.stop();

    CHECK(!bad.load(std::memory_order_acquire));
    CHECK(engine.snapshots_served() >= 50);
    CHECK(engine.processed() == kMessages);
    CHECK(engine.book().order_count() == reference.order_count());
    CHECK(engine.book().level_count() == reference.level_count());
    CHECK(engine.book().check_invariants());

    L2Snapshot final_snapshot;
    L2Snapshot expected;
    CHECK(engine.snapshot(final_snapshot));
    capture_l2(reference, 8, kMessages, expected);
    CHECK(final_snapshot.bid_levels == expected.bid_levels && final_snapshot.ask_levels == expected.ask_levels);
    CHECK(final_snapshot.bid_levels == 0 || (final_snapshot.bids[0].price == expected.bids[0].price &&
                                             final_snapshot.bids[0].qty == expected.bids[0].qty));
    std::printf("    (%llu live snapshots served during %zu messages)\n",
                static_cast<unsigned long long>(engine.snapshots_served()), kMessages);
}

// ---- Exporters -----------------------------------------------------------------------------

// Minimal structural JSON check: balanced braces/brackets outside strings, no trailing commas.
bool json_shaped(const std::string& text) {
    int depth = 0;
    bool in_string = false;
    char previous = 0;
    for (const char c : text) {
        if (in_string) {
            if (c == '"') in_string = false;
            continue;
        }
        if (c == '"') in_string = true;
        if (c == '{' || c == '[') ++depth;
        if (c == '}' || c == ']') {
            if (previous == ',' || --depth < 0) return false;
        }
        previous = c;
    }
    return depth == 0 && !in_string && !text.empty() && text.front() == '{' && text.back() == '}';
}

void test_snapshot_and_metrics_exporters() {
    OrderBook book({.max_orders = 64, .max_levels = 16});
    book.add_limit(1, Side::Buy, 100, 10);
    book.add_limit(2, Side::Buy, -5, 3);  // negative prices are legal ticks
    book.add_limit(3, Side::Sell, 101, 4);
    L2Snapshot snap;
    capture_l2(book, 10, 42, snap);

    char json[4096];
    const std::size_t json_len = write_json(snap, json, sizeof json);
    CHECK(json_len < sizeof json && json_len == std::strlen(json));
    CHECK(std::string(json) ==
          "{\"sequence\":42,\"resting_orders\":3,\"levels\":3,"
          "\"bids\":[{\"price\":100,\"qty\":10,\"cum_qty\":10,\"orders\":1},"
          "{\"price\":-5,\"qty\":3,\"cum_qty\":13,\"orders\":1}],"
          "\"asks\":[{\"price\":101,\"qty\":4,\"cum_qty\":4,\"orders\":1}]}");
    // snprintf contract: a too-small buffer is never overrun and the needed size is still reported.
    char tiny[16];
    CHECK(write_json(snap, tiny, sizeof tiny) == json_len && std::strlen(tiny) < sizeof tiny);
    CHECK(write_json(snap, nullptr, 0) == json_len);

    // Binary round trip, exact size, little-endian header, and rejection of bad input.
    std::byte wire[2048];
    const std::size_t wire_len = write_binary(snap, wire);
    CHECK(wire_len == binary_size(2, 1) && wire_len == 40 + 3 * 20);
    CHECK(std::memcmp(wire, "AL2S\x01\x00\x00\x00\x2A\x00\x00\x00\x00\x00\x00\x00", 16) == 0);
    L2Snapshot decoded;
    CHECK(read_binary(std::span<const std::byte>(wire, wire_len), decoded));
    CHECK(decoded.sequence == 42 && decoded.resting_orders == 3 && decoded.level_count == 3);
    CHECK(decoded.bid_levels == 2 && decoded.ask_levels == 1);
    CHECK(decoded.bids[1].price == -5 && decoded.bids[1].qty == 3 && decoded.bids[1].cumulative_qty == 13);
    CHECK(decoded.asks[0].price == 101 && decoded.asks[0].orders == 1);
    CHECK(write_binary(snap, std::span<std::byte>(wire, wire_len - 1)) == 0);  // too small: nothing written
    CHECK(!read_binary(std::span<const std::byte>(wire, wire_len - 1), decoded));  // truncated
    wire[0] = std::byte{'X'};
    CHECK(!read_binary(std::span<const std::byte>(wire, wire_len), decoded));  // bad magic

    // Metrics report from a real run.
    Engine engine({.book = kBook});
    engine.start();
    const std::vector<std::byte> stream = make_stream(20'000, 99);
    CHECK(engine.ingest(stream).messages == 20'000);
    engine.stop();
    static char report[8192];
    const std::size_t report_len = write_metrics_json(engine.metrics(), report, sizeof report);
    const std::string text(report);
    CHECK(report_len == text.size() && json_shaped(text));
    for (const char* key : {"\"received\":20000", "\"accepted\":", "\"rejected\":", "\"dropped_queue_full\":0",
                            "\"invalid_bytes\":0", "\"checksum_errors\":0", "\"trades\":", "\"volume\":",
                            "\"resting_orders\":", "\"levels\":", "\"ingest_to_queue\":", "\"queue_to_match\":",
                            "\"match_service\":", "\"tick_to_trade\":", "\"p50\":", "\"p90\":", "\"p99\":",
                            "\"p99.9\":", "\"p99.99\":", "\"max\":"}) {
        if (text.find(key) == std::string::npos) {
            std::printf("  missing key %s\n", key);
            CHECK(false);
        }
    }
    CHECK(json_shaped(std::string(json)));
}

}  // namespace

int main() {
    using aether_test::run;
    run("timer_is_monotonic_and_calibrated", test_timer_is_monotonic_and_calibrated);
    run("histogram_bucket_maths", test_histogram_bucket_maths);
    run("histogram_percentiles", test_histogram_percentiles);
    run("histogram_single_writer_many_readers", test_histogram_single_writer_many_readers);
    run("engine_counters_match_ground_truth", test_engine_counters_match_ground_truth);
    run("invalid_and_dropped_accounting", test_invalid_and_dropped_accounting);
    run("latency_reflects_real_delays", test_latency_reflects_real_delays);
    run("timestamps_can_be_switched_off", test_timestamps_can_be_switched_off);
    run("snapshot_of_running_engine", test_snapshot_of_running_engine);
    run("snapshots_under_load_from_several_exporters", test_snapshots_under_load_from_several_exporters);
    run("snapshot_and_metrics_exporters", test_snapshot_and_metrics_exporters);
    return aether_test::summary();
}
