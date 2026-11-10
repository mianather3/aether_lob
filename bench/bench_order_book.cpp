// AetherLOB latency benchmark: synthetic add/cancel/match flow.
//
// The whole order stream is generated before timing starts, so the measured
// region contains only book operations plus two clock reads per operation.
// Per-op figures therefore include clock overhead and are quantised to the
// platform timer tick (~41 ns on Apple Silicon).
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "order_book.hpp"

using namespace aether;

namespace {

struct Op {
    OrderId id;
    Price price;
    Qty qty;
    Side side;
    bool is_cancel;
};

struct XorShift64 {
    std::uint64_t state;
    std::uint64_t operator()() noexcept {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    }
};

std::vector<Op> generate_flow(std::size_t count) {
    std::vector<Op> ops;
    ops.reserve(count);
    std::vector<OrderId> maybe_live;  // ids the generator believes may still rest
    maybe_live.reserve(count);

    XorShift64 rng{0xA57E12B00C0FFEEULL};
    const Price mid = 100'000;
    OrderId next_id = 1;

    for (std::size_t i = 0; i < count; ++i) {
        if (!maybe_live.empty() && rng() % 100 < 45) {
            const std::size_t pick = rng() % maybe_live.size();
            ops.push_back({maybe_live[pick], 0, 0, Side::Buy, true});
            maybe_live[pick] = maybe_live.back();
            maybe_live.pop_back();
        } else {
            const Side side = (rng() & 1) != 0 ? Side::Buy : Side::Sell;
            // 32-tick band overlapping the mid by 4 ticks: ~1 in 8 orders is marketable.
            const Price offset = static_cast<Price>(rng() % 32) - 4;
            const Price price = side == Side::Buy ? mid - offset : mid + offset;
            const OrderId id = next_id++;
            ops.push_back({id, price, 1 + rng() % 100, side, false});
            maybe_live.push_back(id);
        }
    }
    return ops;
}

void report(const char* label, std::vector<std::uint32_t>& samples) {
    if (samples.empty()) return;
    std::sort(samples.begin(), samples.end());
    const auto at = [&](double q) {
        return samples[static_cast<std::size_t>(q * static_cast<double>(samples.size() - 1))];
    };
    std::printf("  %-8s n=%-9zu p50=%-5u p90=%-5u p99=%-5u p99.9=%-6u max=%u  (ns)\n", label,
                samples.size(), at(0.50), at(0.90), at(0.99), at(0.999), samples.back());
}

}  // namespace

int main(int argc, char** argv) {
    const std::size_t count = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 5'000'000;
    if (count == 0) {
        std::fprintf(stderr, "usage: %s [op_count > 0]\n", argv[0]);
        return 2;
    }

    const std::vector<Op> ops = generate_flow(count);
    OrderBook book({.max_orders = 1U << 22, .max_levels = 1U << 12});

    std::vector<std::uint32_t> add_ns;
    std::vector<std::uint32_t> cancel_ns;
    add_ns.reserve(count);
    cancel_ns.reserve(count);

    using Clock = std::chrono::steady_clock;
    const auto ns_between = [](Clock::time_point a, Clock::time_point b) {
        return static_cast<std::uint32_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count());
    };

    std::uint64_t trades = 0;
    std::uint64_t traded_qty = 0;
    std::uint64_t rejects = 0;
    const auto on_trade = [&](const Trade& t) noexcept {
        ++trades;
        traded_qty += t.qty;
    };

    const auto start = Clock::now();
    for (const Op& op : ops) {
        const auto t0 = Clock::now();
        if (op.is_cancel) {
            book.cancel(op.id);  // NotFound is expected when the order already traded
            cancel_ns.push_back(ns_between(t0, Clock::now()));
        } else {
            const AddResult r = book.add_limit(op.id, op.side, op.price, op.qty, on_trade);
            add_ns.push_back(ns_between(t0, Clock::now()));
            rejects += r.status != Status::Ok;
        }
    }
    const auto stop = Clock::now();

    const double seconds = std::chrono::duration<double>(stop - start).count();
    std::printf("AetherLOB bench: %zu ops in %.3f s  ->  %.2f M ops/s (incl. timing overhead)\n",
                count, seconds, static_cast<double>(count) / seconds / 1e6);
    std::printf("  trades=%llu traded_qty=%llu rejects=%llu resting_orders=%zu levels=%zu\n",
                static_cast<unsigned long long>(trades), static_cast<unsigned long long>(traded_qty),
                static_cast<unsigned long long>(rejects), book.order_count(), book.level_count());
    report("add", add_ns);
    report("cancel", cancel_ns);

    if (!book.check_invariants()) {
        std::fprintf(stderr, "invariant check FAILED after benchmark\n");
        return 1;
    }
    return 0;
}
