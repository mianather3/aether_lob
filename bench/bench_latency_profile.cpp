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
//   tick-to-trade   packet received -> match completed, for commands that traded
//   schedule lag    how late the producer sent each message versus its Poisson
//                   due time (measured here, not by the engine). Non-zero lag
//                   means the *producer* could not keep up, and the engine
//                   latencies above then understate what a client would see.
//
// The replay: a deep seeded book (40,000 resting orders over 800 price
// levels), then a drifting mid with passive adds, deep resting adds, cancels,
// modifications (cancel + re-add, which is what a price change is: the order
// loses its queue position), marketable limit orders and multi-level market
// sweeps. While it runs, a separate exporter thread pulls live L2 snapshots.
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

struct Replay {
    std::vector<Command> seed;           // resting book built before measurement starts
    std::vector<std::byte> bytes;        // the measured flow, wire-encoded
    std::vector<std::uint32_t> offsets;  // message i is bytes[offsets[i], offsets[i + 1])
    std::uint64_t adds{0}, cancels{0}, modifies{0}, aggressive{0}, sweeps{0};
};

Replay generate_replay(std::size_t messages) {
    Replay replay;
    replay.bytes.reserve(messages * kMaxWireMsgSize);
    replay.offsets.reserve(messages + 2);
    XorShift64 rng{0xA57E12B00C0FFEEULL};

    struct Live {
        OrderId id;
        Side side;
    };
    std::vector<Live> live;
    live.reserve(messages);
    OrderId next_id = 1;
    Price mid = kStartMid;

    // Deep book: 400 levels per side, 50 orders per level.
    for (Price depth = 1; depth <= 400; ++depth) {
        for (int k = 0; k < 50; ++k) {
            for (const Side side : {Side::Buy, Side::Sell}) {
                Command cmd;
                cmd.type = CommandType::Add;
                cmd.order_id = next_id++;
                cmd.side = side;
                cmd.price = side == Side::Buy ? mid - depth : mid + depth;
                cmd.qty = 50 + rng.below(400);
                replay.seed.push_back(cmd);
                live.push_back({cmd.order_id, side});
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
        live.push_back({id, side});
    };
    const auto take_live = [&]() {
        const std::size_t pick = rng.below(live.size());
        const Live order = live[pick];
        live[pick] = live.back();
        live.pop_back();
        return order;
    };

    while (replay.offsets.size() < messages) {
        if (replay.offsets.size() % 64 == 0) mid += static_cast<Price>(rng.below(3)) - 1;  // drifting market
        const std::uint64_t roll = rng.below(100);
        const Side side = (rng() & 1) != 0 ? Side::Buy : Side::Sell;
        const auto away = [&](Price ticks) { return side == Side::Buy ? mid - ticks : mid + ticks; };

        if (roll < 34 || live.empty()) {  // passive order near the touch
            add(side, away(1 + static_cast<Price>(rng.below(10))), static_cast<std::uint32_t>(1 + rng.below(200)));
            ++replay.adds;
        } else if (roll < 42) {  // deep resting liquidity
            add(side, away(20 + static_cast<Price>(rng.below(380))), static_cast<std::uint32_t>(100 + rng.below(1900)));
            ++replay.adds;
        } else if (roll < 70) {  // cancellation (some targets have already traded: a real-world reject)
            emit(make_cancel(take_live().id));
            ++replay.cancels;
        } else if (roll < 82) {  // modification: cancel, then re-enter at a new price and size
            const Live old = take_live();
            emit(make_cancel(old.id));
            const Price ticks = 1 + static_cast<Price>(rng.below(12));
            add(old.side, old.side == Side::Buy ? mid - ticks : mid + ticks, static_cast<std::uint32_t>(1 + rng.below(250)));
            ++replay.modifies;
        } else if (roll < 94) {  // marketable limit: crosses 1-3 ticks through the touch
            add(side, away(-(1 + static_cast<Price>(rng.below(3)))), static_cast<std::uint32_t>(50 + rng.below(550)));
            ++replay.aggressive;
        } else {  // market sweep through several levels
            emit(make_market(side, static_cast<std::uint32_t>(2000 + rng.below(18000))));
            ++replay.sweeps;
        }
    }
    // End sentinel. A modification emits two messages, so there may be one
    // more than asked for; either way offsets[i + 1] exists for every i < messages.
    replay.offsets.push_back(static_cast<std::uint32_t>(replay.bytes.size()));
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
    print_row("tick-to-trade", m.tick_to_trade);
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
    std::printf("  replay: %zu seeded resting orders, then %zu messages = %llu adds, %llu cancels, %llu modifies "
                "(cancel+add), %llu marketable limits, %llu market sweeps\n",
                replay.seed.size(), largest, ull(replay.adds), ull(replay.cancels), ull(replay.modifies),
                ull(replay.aggressive), ull(replay.sweeps));

    std::vector<RunResult> runs;
    for (std::size_t i = 0; i < rates.size(); ++i) {
        runs.push_back(run_rate(replay, rates[i], counts[i]));
        print_run(runs.back());
    }

    std::printf("\n=== Latency percentile matrix ===");
    print_matrix("tick-to-trade: packet received -> match completed, commands that traded", runs,
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
