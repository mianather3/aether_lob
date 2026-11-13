// AetherLOB latency profiler.
//
// Usage: aether_bench_latency_profile [--json FILE] [--seconds S] [--max-messages N] [rate ...]
//        default rates: 100000 500000 1000000 5000000 (messages per second)
//
// Replays a stressful synthetic market against the full pipeline (wire bytes ->
// gateway decode -> SPSC queue -> matching thread) at each arrival rate, with
// open-loop Poisson arrivals, and reports the engine's own telemetry:
//
//   ingest->queue   packet received by the gateway -> command pushed to the queue
//   queue->match    pushed to the queue -> match completed (queue wait + matching)
//   match service   popped by the matching thread -> match completed
//   order-processing latency   packet received -> match completed, for commands that traded
//   schedule lag    how late the producer sent each message versus its Poisson
//                   due time (measured here, not by the engine). Non-zero lag
//                   means the *producer* could not keep up, and the engine
//                   latencies above then understate what a client would see.
//
// The replay: a deep seeded book (40,000 resting orders over 200 price levels
// a side), then a stationary flow of
//   55%  resting limit adds   (70% within 10 ticks of the touch, 30% up to 200 ticks deep)
//   25%  cancels              (of orders that are really resting, plus the odd stale id:
//                              a cancel that loses the race with a fill, i.e. a reject)
//   20%  aggressive orders    (15% marketable limits crossing 0-2 ticks through the touch,
//                              5% multi-level market sweeps)
// The generator runs its own OrderBook alongside so that it prices orders off
// the real touch and cancels orders that exist, and it scales aggressive size
// by (resting orders / 40,000): taker flow thins out as the book thins and
// grows as it fills, which is what keeps the depth stationary over millions of
// messages. The depth it leaves behind is sampled throughout and printed, and
// the run aborts if the book ever gets shallower than kMinRestingOrders or
// kMinLevelsPerSide. While the flow runs, a separate exporter thread pulls
// live L2 snapshots.
//
// A final single-threaded pass measures the matching core alone: time per
// command and per matched fill, plus CPU cycles and instructions where the OS
// exposes them to an unprivileged process.
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <vector>

#if defined(__APPLE__)
#include <libproc.h>
#include <unistd.h>
#endif

#include "engine.hpp"
#include "market_snapshot.hpp"
#include "protocol.hpp"
#include "telemetry.hpp"
#include "tsc_timer.hpp"

using namespace aether;

namespace {

struct XorShift64 {
    std::uint64_t state;
    std::uint64_t operator()() noexcept {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    }
    std::uint64_t below(std::uint64_t n) noexcept { return (*this)() % n; }
    double uniform() noexcept {  // (0, 1]
        return (static_cast<double>((*this)() >> 11) + 1.0) * (1.0 / 9007199254740992.0);
    }
};

const BookConfig kBook{.max_orders = 1U << 22, .max_levels = 1U << 15};
constexpr Price kStartMid = 1'000'000;

// The depth the replay must sustain from the first message to the last.
constexpr std::size_t kTargetRestingOrders = 40'000;
constexpr std::size_t kMinRestingOrders = 10'000;
constexpr std::size_t kMinLevelsPerSide = 50;
constexpr Price kNearTouchTicks = 50;
constexpr std::size_t kDepthSampleEvery = 1024;  // messages between depth samples

struct DepthTrace {
    std::size_t samples{0};
    std::size_t min_orders{SIZE_MAX}, max_orders{0};
    std::size_t min_levels_per_side{SIZE_MAX};  // the thinner side, at its thinnest
    std::size_t min_orders_near_touch{SIZE_MAX};  // resting within kNearTouchTicks of the touch, both sides
    Price min_mid{kStartMid}, max_mid{kStartMid};

    void sample(const OrderBook& book) noexcept {
        std::size_t near_touch = 0;
        for (const Side side : {Side::Buy, Side::Sell}) {
            const PriceLevel* const best = book.best(side);
            std::size_t levels = 0;
            for (const PriceLevel* level = best; level != nullptr; level = level->next) {
                ++levels;
                const Price away = side == Side::Buy ? best->price - level->price : level->price - best->price;
                if (away < kNearTouchTicks) near_touch += level->order_count;
            }
            if (levels < min_levels_per_side) min_levels_per_side = levels;
        }
        const std::size_t orders = book.order_count();
        if (orders < min_orders) min_orders = orders;
        if (orders > max_orders) max_orders = orders;
        if (near_touch < min_orders_near_touch) min_orders_near_touch = near_touch;
        if (book.best_bid() != nullptr && book.best_ask() != nullptr) {
            const Price mid = (book.best_bid()->price + book.best_ask()->price) / 2;
            if (mid < min_mid) min_mid = mid;
            if (mid > max_mid) max_mid = mid;
        }
        ++samples;
    }
};

struct Replay {
    std::vector<Command> seed;           // resting book built before measurement starts
    std::vector<std::byte> bytes;        // the measured flow, wire-encoded
    std::vector<std::uint32_t> offsets;  // message i is bytes[offsets[i], offsets[i + 1])
    std::uint64_t resting_adds{0}, cancels{0}, stale_cancels{0}, marketable{0}, sweeps{0};
    DepthTrace depth;
};

Replay generate_replay(std::size_t messages) {
    Replay replay;
    replay.bytes.reserve(messages * kMaxWireMsgSize);
    replay.offsets.reserve(messages + 1);
    XorShift64 rng{0xA57E12B00C0FFEEULL};

    // The generator's own copy of the book: every message is applied to it as
    // it is emitted, so it always knows the touch and which orders still rest.
    const auto shadow = std::make_unique<OrderBook>(kBook);
    std::vector<OrderId> candidates;  // ids that were resting when added; may have filled since
    candidates.reserve(messages);
    OrderId next_id = 1;

    // Seed: 200 levels per side, 100 orders per level.
    for (Price depth = 1; depth <= 200; ++depth) {
        for (int k = 0; k < 100; ++k) {
            for (const Side side : {Side::Buy, Side::Sell}) {
                Command cmd;
                cmd.type = CommandType::Add;
                cmd.order_id = next_id++;
                cmd.side = side;
                cmd.price = side == Side::Buy ? kStartMid - depth : kStartMid + depth;
                cmd.qty = 50 + rng.below(400);
                replay.seed.push_back(cmd);
                shadow->add_limit(cmd.order_id, cmd.side, cmd.price, cmd.qty);
                candidates.push_back(cmd.order_id);
            }
        }
    }

    const auto emit = [&replay](const auto& message) {
        replay.offsets.push_back(static_cast<std::uint32_t>(replay.bytes.size()));
        append(replay.bytes, message);
    };
    const auto add = [&](Side side, Price price, std::uint32_t qty) {
        const OrderId id = next_id++;
        emit(make_add(id, side, price, qty));
        if (shadow->add_limit(id, side, price, qty).resting != 0) candidates.push_back(id);
    };
    // The far touch as `side` sees it: the price a passive order must stay
    // short of and an aggressive one must reach.
    const auto far_touch = [&](Side side) {
        const PriceLevel* const far = shadow->best(side == Side::Buy ? Side::Sell : Side::Buy);
        if (far != nullptr) return far->price;
        const PriceLevel* const own = shadow->best(side);  // one side empty: quote around the other
        if (own != nullptr) return side == Side::Buy ? own->price + 1 : own->price - 1;
        return kStartMid;
    };

    int drift = 0;  // -1, 0, +1: which way the taker flow leans in this regime
    while (replay.offsets.size() < messages) {
        const std::size_t sent = replay.offsets.size();
        if (sent % kDepthSampleEvery == 0) replay.depth.sample(*shadow);
        if (sent % 65536 == 0) drift = static_cast<int>(rng.below(3)) - 1;

        const std::uint64_t roll = rng.below(100);
        if (roll < 55) {  // resting limit add: never crosses
            const Side side = (rng() & 1) != 0 ? Side::Buy : Side::Sell;
            const bool deep = rng.below(10) < 3;
            const Price ticks = deep ? 11 + static_cast<Price>(rng.below(190)) : 1 + static_cast<Price>(rng.below(10));
            const auto qty = static_cast<std::uint32_t>(deep ? 100 + rng.below(900) : 1 + rng.below(200));
            const Price touch = far_touch(side);
            add(side, side == Side::Buy ? touch - ticks : touch + ticks, qty);
            ++replay.resting_adds;
        } else if (roll < 80) {  // cancel
            OrderId target = 0;
            bool stale = false;
            while (!candidates.empty()) {
                const std::size_t pick = rng.below(candidates.size());
                target = candidates[pick];
                candidates[pick] = candidates.back();
                candidates.pop_back();
                if (shadow->find(target) != nullptr) break;
                // Already filled. Mostly skip it; now and then send the cancel
                // anyway, as a client that has not seen the fill yet would.
                if (rng.below(16) == 0) {
                    stale = true;
                    break;
                }
            }
            emit(make_cancel(target));  // id 0 never rests: a reject, only if nothing is left to cancel
            shadow->cancel(target);
            ++replay.cancels;
            if (stale) ++replay.stale_cancels;
        } else {  // aggressive: sized in proportion to the depth that is there to take
            const Side side = rng.below(100) < static_cast<std::uint64_t>(50 + 8 * drift) ? Side::Buy : Side::Sell;
            double scale = static_cast<double>(shadow->order_count()) / static_cast<double>(kTargetRestingOrders);
            scale = scale < 0.25 ? 0.25 : (scale > 4.0 ? 4.0 : scale);
            if (roll < 95) {  // marketable limit: crosses 0-2 ticks through the touch, remainder rests
                const Price through = static_cast<Price>(rng.below(3));
                const Price touch = far_touch(side);
                const auto qty = static_cast<std::uint32_t>(1.0 + scale * static_cast<double>(10 + rng.below(140)));
                add(side, side == Side::Buy ? touch + through : touch - through, qty);
                ++replay.marketable;
            } else {  // market sweep through several levels
                const auto qty = static_cast<std::uint32_t>(1.0 + scale * static_cast<double>(200 + rng.below(600)));
                emit(make_market(side, qty));
                shadow->add_market(side, qty);
                ++replay.sweeps;
            }
        }
    }
    replay.depth.sample(*shadow);
    replay.offsets.push_back(static_cast<std::uint32_t>(replay.bytes.size()));  // end sentinel
    return replay;
}

struct RunResult {
    double target_rate{0};
    double achieved_rate{0};
    std::size_t messages{0};
    TelemetrySnapshot metrics{};
    HistogramSummary schedule_lag{};
    std::uint64_t snapshots{0};
    std::uint64_t snapshot_wait_max_ns{0};
    L2Snapshot last_snapshot{};
};

CorePlan g_cores;

RunResult run_rate(const Replay& replay, double rate, std::size_t messages) {
    RunResult result;
    result.target_rate = rate;
    result.messages = messages;

    // Poisson process: exponential gaps, as cumulative counter ticks.
    std::vector<std::uint64_t> due(messages);
    {
        XorShift64 rng{0x5EEDF00DCAFEULL};
        const double ticks_per_gap = static_cast<double>(tsc::calibrate().ticks_per_second) / rate;
        double t = 0.0;
        for (std::size_t i = 0; i < messages; ++i) {
            t += -std::log(rng.uniform()) * ticks_per_gap;
            due[i] = static_cast<std::uint64_t>(t);
        }
    }

    Engine engine({.book = kBook,
                   .queue_capacity = 1U << 16,
                   .idle_spins_before_yield = UINT32_MAX,
                   .matching_core = g_cores.matching_core,
                   .log_affinity = false,
                   .snapshot_depth = 10});
    engine.start();
    for (const Command& cmd : replay.seed) engine.submit(cmd);  // unstamped: not part of any histogram
    while (engine.processed() < replay.seed.size()) cpu_relax();

    // Exporter: a third thread pulling depth every 10 ms while the flow runs.
    std::atomic<bool> exporting{true};
    std::thread exporter([&] {
        L2Snapshot snap;
        while (exporting.load(std::memory_order_acquire)) {
            const std::uint64_t asked = tsc::now();
            if (engine.snapshot(snap, 500)) {
                const std::uint64_t waited = tsc::elapsed_ns(asked, tsc::now_end());
                if (waited > result.snapshot_wait_max_ns) result.snapshot_wait_max_ns = waited;
                ++result.snapshots;
                result.last_snapshot = snap;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    const auto schedule_lag = std::make_unique<LatencyHistogram>();  // producer-side, written by this thread
    const std::span<const std::byte> bytes(replay.bytes);
    const std::uint64_t start = tsc::now() + tsc::calibrate().ticks_per_second / 1000;  // 1 ms settle
    std::uint64_t now = 0;
    for (std::size_t i = 0; i < messages; ++i) {
        const std::uint64_t at = start + due[i];
        while ((now = tsc::now()) < at) {
            cpu_relax();
        }
        schedule_lag->record(tsc::to_ns(now - at));
        engine.ingest(bytes.subspan(replay.offsets[i], replay.offsets[i + 1] - replay.offsets[i]));
    }
    const std::uint64_t sent = tsc::now();
    while (engine.processed() < replay.seed.size() + messages) cpu_relax();
    exporting.store(false, std::memory_order_release);
    exporter.join();
    engine.stop();

    result.achieved_rate = static_cast<double>(messages) / (static_cast<double>(tsc::to_ns(sent - start)) / 1e9);
    result.metrics = engine.metrics();
    result.schedule_lag = schedule_lag->summarize();
    if (!engine.book().check_invariants()) {
        std::fprintf(stderr, "book invariant check FAILED\n");
        std::exit(1);
    }
    return result;
}

unsigned long long ull(std::uint64_t v) { return static_cast<unsigned long long>(v); }

void print_row(const char* name, const HistogramSummary& h) {
    std::printf("  | %-13s | %9llu | %7llu | %7llu | %7llu | %8llu | %8llu | %10llu |\n", name, ull(h.count), ull(h.p50),
                ull(h.p90), ull(h.p99), ull(h.p999), ull(h.p9999), ull(h.max));
}

void print_run(const RunResult& r) {
    const TelemetrySnapshot& m = r.metrics;
    std::printf("\n=== %.0f msg/s target, %.0f msg/s achieved, %zu messages ===\n", r.target_rate, r.achieved_rate,
                r.messages);
    std::printf("  received=%llu accepted=%llu rejected=%llu dropped=%llu invalid_bytes=%llu backpressure_waits=%llu\n",
                ull(m.received), ull(m.accepted), ull(m.rejected), ull(m.dropped_queue_full), ull(m.invalid_bytes),
                ull(m.backpressure_waits));
    std::printf("  trades=%llu volume=%llu resting_orders=%llu levels=%llu | %llu live L2 snapshots, slowest took %llu ns\n",
                ull(m.trades), ull(m.volume), ull(m.resting_orders), ull(m.levels), ull(r.snapshots),
                ull(r.snapshot_wait_max_ns));
    std::printf("  | %-13s | %9s | %7s | %7s | %7s | %8s | %8s | %10s |\n", "latency (ns)", "samples", "p50", "p90",
                "p99", "p99.9", "p99.99", "max");
    std::printf("  |---------------|-----------|---------|---------|---------|----------|----------|------------|\n");
    print_row("ingest->queue", m.ingest_to_queue);
    print_row("queue->match", m.queue_to_match);
    print_row("match service", m.match_service);
    print_row("order-processing latency", m.tick_to_trade);
    print_row("schedule lag", r.schedule_lag);
}

void print_matrix(const char* title, const std::vector<RunResult>& runs, HistogramSummary TelemetrySnapshot::* which) {
    std::printf("\n  %s (ns)\n", title);
    std::printf("  | %-11s | %7s | %7s | %7s | %8s | %8s | %10s |\n", "rate (msg/s)", "p50", "p90", "p99", "p99.9",
                "p99.99", "max");
    std::printf("  |--------------|---------|---------|---------|----------|----------|------------|\n");
    for (const RunResult& r : runs) {
        const HistogramSummary& h = r.metrics.*which;
        std::printf("  | %12.0f | %7llu | %7llu | %7llu | %8llu | %8llu | %10llu |\n", r.target_rate, ull(h.p50),
                    ull(h.p90), ull(h.p99), ull(h.p999), ull(h.p9999), ull(h.max));
    }
}

// ---- Matching core in isolation ------------------------------------------------------

struct CpuUsage {
    bool available{false};
    std::uint64_t cycles{0};
    std::uint64_t instructions{0};
};

// Whole-process CPU cycle and instruction counts, where an unprivileged
// process can read them (Darwin's rusage). Elsewhere: not available.
CpuUsage cpu_usage() {
    CpuUsage usage;
#if defined(__APPLE__)
    rusage_info_v4 info{};
    if (::proc_pid_rusage(::getpid(), RUSAGE_INFO_V4, reinterpret_cast<rusage_info_t*>(&info)) == 0 &&
        info.ri_cycles != 0) {
        usage.available = true;
        usage.cycles = info.ri_cycles;
        usage.instructions = info.ri_instructions;
    }
#endif
    return usage;
}

void profile_matching_core(const Replay& replay, std::size_t messages) {
    // Decode once, up front, so only book operations are measured.
    std::vector<Command> commands;
    commands.reserve(messages);
    {
        std::span<const std::byte> rest(replay.bytes);
        Command cmd;
        while (!rest.empty() && commands.size() < messages) {
            const DecodeResult d = decode(rest, cmd);
            if (d.status != DecodeStatus::Ok) break;
            rest = rest.subspan(d.consumed);
            commands.push_back(cmd);
        }
    }
    const auto seed_book = [&replay](OrderBook& book) {
        for (const Command& cmd : replay.seed) book.add_limit(cmd.order_id, cmd.side, cmd.price, cmd.qty);
    };
    std::uint64_t fills = 0;
    const auto count_fill = [&fills](const Trade&) noexcept { ++fills; };
    const auto apply = [&](OrderBook& book, const Command& cmd) {
        switch (cmd.type) {
            case CommandType::Add: book.add_limit(cmd.order_id, cmd.side, cmd.price, cmd.qty, count_fill); break;
            case CommandType::Cancel: book.cancel(cmd.order_id); break;
            case CommandType::Market: book.add_market(cmd.side, cmd.qty, count_fill); break;
        }
    };

    // Pass A: uninstrumented loop, bracketed by the OS's cycle/instruction counters.
    double ns_per_command = 0.0;
    double cycles_per_ns = 0.0;
    CpuUsage delta;
    {
        OrderBook book(kBook);
        seed_book(book);
        fills = 0;
        const CpuUsage before = cpu_usage();
        const std::uint64_t t0 = tsc::now();
        for (const Command& cmd : commands) apply(book, cmd);
        const std::uint64_t t1 = tsc::now_end();
        const CpuUsage after = cpu_usage();
        const double total_ns = static_cast<double>(tsc::to_ns(t1 - t0));
        ns_per_command = total_ns / static_cast<double>(commands.size());
        delta.available = before.available && after.available;
        delta.cycles = after.cycles - before.cycles;
        delta.instructions = after.instructions - before.instructions;
        if (delta.available) cycles_per_ns = static_cast<double>(delta.cycles) / total_ns;
    }
    const std::uint64_t total_fills = fills;

    // Pass B: time each command, split by whether it matched. A single reading
    // is quantised to the counter tick, but the quantisation error is unbiased,
    // so the sums over hundreds of thousands of commands are accurate.
    std::uint64_t matched_ticks = 0, matched_commands = 0, matched_fills = 0;
    std::uint64_t passive_ticks = 0, passive_commands = 0;
    {
        OrderBook book(kBook);
        seed_book(book);
        for (const Command& cmd : commands) {
            fills = 0;
            const std::uint64_t t0 = tsc::now();
            apply(book, cmd);
            const std::uint64_t t1 = tsc::now_end();
            if (fills != 0) {
                matched_ticks += t1 - t0;
                matched_fills += fills;
                ++matched_commands;
            } else {
                passive_ticks += t1 - t0;
                ++passive_commands;
            }
        }
    }
    const double ticks_to_ns = tsc::calibrate().ns_per_tick;
    const double matched_ns = static_cast<double>(matched_ticks) * ticks_to_ns;
    const double passive_ns = static_cast<double>(passive_ticks) * ticks_to_ns;
    const double ns_per_matched_command = matched_commands != 0 ? matched_ns / static_cast<double>(matched_commands) : 0;
    const double ns_per_passive_command = passive_commands != 0 ? passive_ns / static_cast<double>(passive_commands) : 0;
    const double ns_per_fill = matched_fills != 0 ? matched_ns / static_cast<double>(matched_fills) : 0;

    std::printf("\n=== Matching core alone (single thread, no queue, %zu commands, %llu fills) ===\n", commands.size(),
                ull(total_fills));
    std::printf("  whole replay            %.1f ns per command\n", ns_per_command);
    if (delta.available) {
        std::printf("  CPU counters (process)  %.1f cycles and %.1f instructions per command, IPC %.2f, %.2f cycles/ns\n",
                    static_cast<double>(delta.cycles) / static_cast<double>(commands.size()),
                    static_cast<double>(delta.instructions) / static_cast<double>(commands.size()),
                    static_cast<double>(delta.instructions) / static_cast<double>(delta.cycles), cycles_per_ns);
    } else {
        std::printf("  CPU counters            not readable by an unprivileged process on this OS\n");
    }
    std::printf("  commands that matched   %llu, %.2f fills each, %.1f ns per command\n", ull(matched_commands),
                matched_commands != 0 ? static_cast<double>(matched_fills) / static_cast<double>(matched_commands) : 0.0,
                ns_per_matched_command);
    std::printf("  commands with no fill   %llu, %.1f ns per command (includes ~1 timer read of overhead, as above)\n",
                ull(passive_commands), ns_per_passive_command);
    std::printf("  per matched fill        %.1f ns", ns_per_fill);
    if (delta.available) {
        std::printf("  ~= %.0f cycles (estimate: ns x the %.2f cycles/ns measured above)", ns_per_fill * cycles_per_ns,
                    cycles_per_ns);
    }
    std::printf("\n");
    std::printf("  cache misses / stalls   not measured: this OS exposes no cache-miss or stall counters to an\n"
                "                          unprivileged process. On Linux run this binary under\n"
                "                          `perf stat -e cycles,instructions,cache-misses,branch-misses`.\n");
}

}  // namespace

int main(int argc, char** argv) {
    const char* json_path = nullptr;
    double seconds = 2.0;
    std::size_t max_messages = 4'000'000;
    std::vector<double> rates;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--json") == 0 && i + 1 < argc) {
            json_path = argv[++i];
        } else if (std::strcmp(argv[i], "--seconds") == 0 && i + 1 < argc) {
            seconds = std::strtod(argv[++i], nullptr);
        } else if (std::strcmp(argv[i], "--max-messages") == 0 && i + 1 < argc) {
            max_messages = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strtod(argv[i], nullptr) > 0.0) {
            rates.push_back(std::strtod(argv[i], nullptr));
        } else {
            std::fprintf(stderr, "usage: %s [--json FILE] [--seconds S] [--max-messages N] [rate_per_sec ...]\n", argv[0]);
            return 2;
        }
    }
    if (rates.empty()) rates = {100'000, 500'000, 1'000'000, 5'000'000};
    if (!(seconds > 0.0) || max_messages == 0) return 2;

    const tsc::Calibration& timer = tsc::calibrate();
    std::printf("AetherLOB latency profile\n");
    std::printf("  timer: %s, %.3f ns per tick (%llu Hz, %s), invariant=%s\n", timer.counter, timer.ns_per_tick,
                ull(timer.ticks_per_second), timer.method, timer.invariant ? "yes" : "no");
    std::printf("  histogram: %zu log-linear buckets, <= 3.125%% relative error; every figure is also quantised to the timer tick\n",
                LatencyHistogram::kBucketCount);

    g_cores = default_core_plan();
    log_pin(stdout, "gateway thread (main)", pin_current_thread(g_cores.gateway_core));

    std::size_t largest = 0;
    std::vector<std::size_t> counts;
    for (const double rate : rates) {
        std::size_t n = static_cast<std::size_t>(rate * seconds);
        if (n > max_messages) n = max_messages;
        if (n < 1000) n = 1000;
        counts.push_back(n);
        if (n > largest) largest = n;
    }
    const Replay replay = generate_replay(largest);
    const DepthTrace& depth = replay.depth;
    const double pct = 100.0 / static_cast<double>(largest);
    std::printf("  replay: %zu seeded resting orders, then %zu messages = %llu resting adds (%.1f%%), %llu cancels "
                "(%.1f%%, %llu of them stale), %llu marketable limits + %llu market sweeps (%.1f%%)\n",
                replay.seed.size(), largest, ull(replay.resting_adds), static_cast<double>(replay.resting_adds) * pct,
                ull(replay.cancels), static_cast<double>(replay.cancels) * pct, ull(replay.stale_cancels),
                ull(replay.marketable), ull(replay.sweeps),
                static_cast<double>(replay.marketable + replay.sweeps) * pct);
    std::printf("  book depth over the replay (%zu samples): resting orders %zu..%zu, thinner side never below %zu "
                "price levels,\n"
                "    never fewer than %zu orders within %lld ticks of the touch, mid moved %lld..%lld ticks from its start\n",
                depth.samples, depth.min_orders, depth.max_orders, depth.min_levels_per_side,
                depth.min_orders_near_touch, static_cast<long long>(kNearTouchTicks),
                static_cast<long long>(depth.min_mid - kStartMid), static_cast<long long>(depth.max_mid - kStartMid));
    if (depth.min_orders < kMinRestingOrders || depth.min_levels_per_side < kMinLevelsPerSide) {
        std::fprintf(stderr, "replay FAILED to sustain liquidity: need >= %zu resting orders and >= %zu levels a side\n",
                     kMinRestingOrders, kMinLevelsPerSide);
        return 1;
    }

    std::vector<RunResult> runs;
    for (std::size_t i = 0; i < rates.size(); ++i) {
        runs.push_back(run_rate(replay, rates[i], counts[i]));
        print_run(runs.back());
    }

    std::printf("\n=== Latency percentile matrix ===");
    print_matrix("order-processing latency: packet received -> match completed, commands that traded", runs,
                 &TelemetrySnapshot::tick_to_trade);
    print_matrix("ingestion-to-queue: packet received -> pushed to the SPSC queue", runs,
                 &TelemetrySnapshot::ingest_to_queue);
    print_matrix("queue-to-match: pushed to the queue -> match completed", runs, &TelemetrySnapshot::queue_to_match);
    print_matrix("match service: dequeued -> match completed", runs, &TelemetrySnapshot::match_service);

    if (!runs.empty()) {
        std::printf("\n=== Last live L2 snapshot of the final run ===\n");
        print_snapshot(stdout, runs.back().last_snapshot, 5);
    }

    profile_matching_core(replay, largest < 2'000'000 ? largest : 2'000'000);

    if (json_path != nullptr) {
        std::FILE* file = std::fopen(json_path, "w");
        if (file == nullptr) {
            std::perror(json_path);
            return 1;
        }
        static char buffer[8192];
        std::fprintf(file, "{\"timer\":{\"counter\":\"%s\",\"ns_per_tick\":%.6f,\"ticks_per_second\":%llu},\"runs\":[",
                     timer.counter, timer.ns_per_tick, ull(timer.ticks_per_second));
        for (std::size_t i = 0; i < runs.size(); ++i) {
            write_metrics_json(runs[i].metrics, buffer, sizeof buffer);
            std::fprintf(file, "%s{\"target_rate\":%.0f,\"achieved_rate\":%.0f,\"messages\":%zu,\"metrics\":%s}",
                         i == 0 ? "" : ",", runs[i].target_rate, runs[i].achieved_rate, runs[i].messages, buffer);
        }
        std::fprintf(file, "]}\n");
        std::fclose(file);
        std::printf("\nJSON report written to %s\n", json_path);
    }
    return 0;
}
