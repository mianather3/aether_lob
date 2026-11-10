// AetherLOB pipeline benchmark: wire bytes -> gateway decode -> SPSC queue ->
// matching thread -> OrderBook, with open-loop Poisson arrivals.
//
// Usage: aether_bench_pipeline [messages=2000000] [rate_per_sec=1000000]
//
// Three latency distributions are reported, all ending when the matching
// thread has finished applying the command (all of its fills done):
//
//   queue+match    from gateway queue insertion (Command::ingress_ns)
//   tick-to-trade  the same measure, restricted to commands that traded
//   vs schedule    from the message's *scheduled* Poisson arrival time. This
//                  one also charges any time the producer ran late, so it is
//                  free of coordinated omission and is the number to trust
//                  when the system is close to saturation.
//
// Figures include two clock reads per message and are quantised to the
// platform timer tick (~41 ns on Apple Silicon). Threads are not pinned.
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <span>
#include <vector>

#include "engine.hpp"
#include "protocol.hpp"

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
    double uniform() noexcept {  // (0, 1]
        return (static_cast<double>((*this)() >> 11) + 1.0) * (1.0 / 9007199254740992.0);
    }
};

struct Flow {
    std::vector<std::byte> bytes;
    std::vector<std::uint32_t> offsets;  // message i occupies [offsets[i], offsets[i + 1])
};

Flow generate_flow(std::size_t count) {
    Flow flow;
    flow.bytes.reserve(count * kMaxWireMsgSize);
    flow.offsets.reserve(count + 1);
    std::vector<OrderId> maybe_live;
    maybe_live.reserve(count);

    XorShift64 rng{0xA57E12B00C0FFEEULL};
    const Price mid = 100'000;
    OrderId next_id = 1;

    for (std::size_t i = 0; i < count; ++i) {
        flow.offsets.push_back(static_cast<std::uint32_t>(flow.bytes.size()));
        const std::uint64_t roll = rng() % 100;
        const Side side = (rng() & 1) != 0 ? Side::Buy : Side::Sell;
        if (roll < 43 && !maybe_live.empty()) {
            const std::size_t pick = rng() % maybe_live.size();
            append(flow.bytes, make_cancel(maybe_live[pick]));
            maybe_live[pick] = maybe_live.back();
            maybe_live.pop_back();
        } else if (roll < 46) {
            append(flow.bytes, make_market(side, static_cast<std::uint32_t>(1 + rng() % 100)));
        } else {
            // 32-tick band overlapping the mid by 4 ticks: ~1 in 8 limit orders is marketable.
            const Price offset = static_cast<Price>(rng() % 32) - 4;
            const Price price = side == Side::Buy ? mid - offset : mid + offset;
            const OrderId id = next_id++;
            append(flow.bytes, make_add(id, side, price, static_cast<std::uint32_t>(1 + rng() % 100)));
            maybe_live.push_back(id);
        }
    }
    flow.offsets.push_back(static_cast<std::uint32_t>(flow.bytes.size()));
    return flow;
}

// Cumulative arrival offsets (ns) of a Poisson process: i.i.d. exponential gaps.
std::vector<std::uint64_t> poisson_schedule(std::size_t count, double rate_per_sec) {
    std::vector<std::uint64_t> arrivals(count);
    XorShift64 rng{0x5EEDF00DCAFEULL};
    const double mean_gap_ns = 1e9 / rate_per_sec;
    double t = 0.0;
    for (std::size_t i = 0; i < count; ++i) {
        t += -std::log(rng.uniform()) * mean_gap_ns;
        arrivals[i] = static_cast<std::uint64_t>(t);
    }
    return arrivals;
}

std::uint32_t saturate(std::uint64_t ns) noexcept {
    return ns > UINT32_MAX ? UINT32_MAX : static_cast<std::uint32_t>(ns);
}

// Runs on the matching thread. All storage is reserved up front.
class LatencyRecorder final : public EngineListener {
public:
    LatencyRecorder(std::size_t count, const std::vector<std::uint64_t>& arrivals) : arrivals_(arrivals) {
        from_insert.reserve(count);
        from_schedule.reserve(count);
        tick_to_trade.reserve(count);
    }

    void on_trade(const Trade&, const Command&) noexcept override {
        ++trades;
        traded_ = true;
    }

    void on_processed(const Command& command, const AddResult&) noexcept override {
        const std::uint64_t now = now_ns();
        const std::uint32_t since_insert = saturate(now - command.ingress_ns);
        const std::uint64_t scheduled = start_ns + arrivals_[from_insert.size()];
        from_schedule.push_back(saturate(now > scheduled ? now - scheduled : 0));
        from_insert.push_back(since_insert);
        if (traded_) {
            tick_to_trade.push_back(since_insert);
            traded_ = false;
        }
    }

    std::uint64_t start_ns{0};  // set before the first message is sent
    std::uint64_t trades{0};
    std::vector<std::uint32_t> from_insert;
    std::vector<std::uint32_t> from_schedule;
    std::vector<std::uint32_t> tick_to_trade;

private:
    const std::vector<std::uint64_t>& arrivals_;
    bool traded_{false};
};

void report(const char* label, std::vector<std::uint32_t>& samples) {
    if (samples.empty()) {
        std::printf("  %-14s (no samples)\n", label);
        return;
    }
    std::sort(samples.begin(), samples.end());
    const auto at = [&](double q) {
        return samples[static_cast<std::size_t>(q * static_cast<double>(samples.size() - 1))];
    };
    std::printf("  %-14s n=%-8zu p50=%-6u p90=%-6u p99=%-7u p99.9=%-8u p99.99=%-8u max=%u\n", label,
                samples.size(), at(0.50), at(0.90), at(0.99), at(0.999), at(0.9999), samples.back());
}

const BookConfig kBook{.max_orders = 1U << 22, .max_levels = 1U << 12};

void run_poisson(const Flow& flow, std::size_t count, double rate) {
    const std::vector<std::uint64_t> arrivals = poisson_schedule(count, rate);
    LatencyRecorder recorder(count, arrivals);
    Engine engine({.book = kBook, .queue_capacity = 1U << 16, .idle_spins_before_yield = UINT32_MAX},
                  &recorder);
    engine.start();

    const std::span<const std::byte> bytes(flow.bytes);
    const std::uint64_t start = now_ns() + 1'000'000;  // let the matching thread settle into its spin
    recorder.start_ns = start;
    for (std::size_t i = 0; i < count; ++i) {
        const std::uint64_t due = start + arrivals[i];
        while (now_ns() < due) {
            cpu_relax();
        }
        engine.ingest(bytes.subspan(flow.offsets[i], flow.offsets[i + 1] - flow.offsets[i]));
    }
    const std::uint64_t sent = now_ns();
    engine.stop();

    const double seconds = static_cast<double>(sent - start) / 1e9;
    std::printf("Poisson arrivals: target %.0f msg/s, achieved %.0f msg/s over %.2f s\n", rate,
                static_cast<double>(count) / seconds, seconds);
    std::printf("  messages=%zu trades=%llu backpressure_events=%llu resting_orders=%zu  (latencies in ns)\n",
                count, static_cast<unsigned long long>(recorder.trades),
                static_cast<unsigned long long>(engine.backpressure_events()), engine.book().order_count());
    report("queue+match", recorder.from_insert);
    report("tick-to-trade", recorder.tick_to_trade);
    report("vs schedule", recorder.from_schedule);
}

void run_saturation(const Flow& flow, std::size_t count) {
    Engine engine({.book = kBook, .queue_capacity = 1U << 16, .idle_spins_before_yield = UINT32_MAX,
                   .stamp_ingress = false});
    engine.start();
    const std::uint64_t start = now_ns();
    const IngestResult result = engine.ingest(flow.bytes);
    engine.stop();  // returns once the matching thread has drained the queue
    const double seconds = static_cast<double>(now_ns() - start) / 1e9;

    std::printf("Saturation (unthrottled, no timestamps): %zu msgs in %.3f s -> %.2f M msg/s end-to-end, "
                "%llu backpressure events\n",
                result.messages, seconds, static_cast<double>(count) / seconds / 1e6,
                static_cast<unsigned long long>(engine.backpressure_events()));
    if (result.messages != count || engine.processed() != count || !engine.book().check_invariants()) {
        std::fprintf(stderr, "pipeline integrity check FAILED\n");
        std::exit(1);
    }
}

}  // namespace

int main(int argc, char** argv) {
    const std::size_t count = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 2'000'000;
    const double rate = argc > 2 ? std::strtod(argv[2], nullptr) : 1'000'000.0;
    if (count == 0 || !(rate > 0.0)) {
        std::fprintf(stderr, "usage: %s [messages > 0] [rate_per_sec > 0]\n", argv[0]);
        return 2;
    }

    const Flow flow = generate_flow(count);
    std::printf("AetherLOB pipeline bench: %zu wire messages, %zu bytes\n", count, flow.bytes.size());
    run_poisson(flow, count, rate);
    run_saturation(flow, count);
    return 0;
}
