// AetherLOB unit, invariant and differential matching tests.
// Dependency-free: a failed CHECK prints its location and fails the binary.
#include <cstdint>
#include <cstdio>
#include <deque>
#include <functional>
#include <map>
#include <unordered_map>
#include <vector>

#include "order_book.hpp"
#include "test_common.hpp"

using namespace aether;

namespace {

bool same(const Trade& a, const Trade& b) {
    return a.maker_id == b.maker_id && a.taker_id == b.taker_id && a.price == b.price &&
           a.qty == b.qty && a.taker_side == b.taker_side;
}

void test_layout() {
    CHECK(sizeof(Order) == 64);
    CHECK(alignof(Order) == 64);
    CHECK(sizeof(PriceLevel) == 64);

    OrderBook book({.max_orders = 64, .max_levels = 16});
    for (OrderId id = 1; id <= 8; ++id) {
        book.add_limit(id, Side::Buy, 100 - static_cast<Price>(id), 1);
        CHECK(reinterpret_cast<std::uintptr_t>(book.find(id)) % 64 == 0);
    }
}

void test_empty_and_rejects() {
    OrderBook book({.max_orders = 64, .max_levels = 16});
    CHECK(book.best_bid() == nullptr && book.best_ask() == nullptr);
    CHECK(book.cancel(42) == Status::NotFound);
    CHECK(book.add_limit(1, Side::Buy, 100, 0).status == Status::InvalidQty);
    CHECK(book.add_limit(1, Side::Buy, 100, 5).status == Status::Ok);
    CHECK(book.add_limit(1, Side::Sell, 200, 5).status == Status::DuplicateId);
    CHECK(book.order_count() == 1);
    CHECK(book.check_invariants());
}

void test_price_priority_and_level_order() {
    OrderBook book({.max_orders = 64, .max_levels = 16});
    book.add_limit(1, Side::Buy, 100, 10);
    book.add_limit(2, Side::Buy, 102, 10);
    book.add_limit(3, Side::Buy, 101, 10);
    book.add_limit(4, Side::Sell, 110, 10);
    book.add_limit(5, Side::Sell, 108, 10);
    book.add_limit(6, Side::Sell, 109, 10);

    CHECK(book.best_bid()->price == 102);
    CHECK(book.best_bid()->next->price == 101);
    CHECK(book.best_bid()->next->next->price == 100);
    CHECK(book.best_ask()->price == 108);
    CHECK(book.best_ask()->next->price == 109);
    CHECK(book.level_count() == 6);
    CHECK(book.check_invariants());

    // An aggressive sell sweeps bids best price first, at the makers' prices.
    std::vector<Trade> trades;
    const AddResult r = book.add_limit(7, Side::Sell, 100, 25, [&](const Trade& t) { trades.push_back(t); });
    CHECK(r.status == Status::Ok && r.filled == 25 && r.resting == 0);
    CHECK(trades.size() == 3);
    CHECK(same(trades[0], Trade{2, 7, 102, 10, Side::Sell}));
    CHECK(same(trades[1], Trade{3, 7, 101, 10, Side::Sell}));
    CHECK(same(trades[2], Trade{1, 7, 100, 5, Side::Sell}));
    CHECK(book.best_bid()->price == 100 && book.best_bid()->total_qty == 5);
    CHECK(book.find(7) == nullptr);
    CHECK(book.check_invariants());
}

void test_fifo_time_priority() {
    OrderBook book({.max_orders = 64, .max_levels = 16});
    book.add_limit(1, Side::Sell, 100, 10);
    book.add_limit(2, Side::Sell, 100, 10);
    book.add_limit(3, Side::Sell, 100, 10);
    CHECK(book.best_ask()->order_count == 3 && book.best_ask()->total_qty == 30);

    // Cancelling the middle order must not disturb the others' queue position.
    CHECK(book.cancel(2) == Status::Ok);
    CHECK(book.cancel(2) == Status::NotFound);

    std::vector<Trade> trades;
    book.add_limit(4, Side::Buy, 100, 15, [&](const Trade& t) { trades.push_back(t); });
    CHECK(trades.size() == 2);
    CHECK(same(trades[0], Trade{1, 4, 100, 10, Side::Buy}));
    CHECK(same(trades[1], Trade{3, 4, 100, 5, Side::Buy}));

    // A partially filled maker keeps its place at the front of the queue.
    book.add_limit(5, Side::Sell, 100, 10);
    trades.clear();
    book.add_limit(6, Side::Buy, 100, 6, [&](const Trade& t) { trades.push_back(t); });
    CHECK(trades.size() == 2);
    CHECK(same(trades[0], Trade{3, 6, 100, 5, Side::Buy}));
    CHECK(same(trades[1], Trade{5, 6, 100, 1, Side::Buy}));
    CHECK(book.find(5)->qty == 9);
    CHECK(book.check_invariants());
}

void test_partial_fill_rests_remainder() {
    OrderBook book({.max_orders = 64, .max_levels = 16});
    book.add_limit(1, Side::Sell, 100, 10);
    const AddResult r = book.add_limit(2, Side::Buy, 101, 25);
    CHECK(r.status == Status::Ok && r.filled == 10 && r.resting == 15);
    CHECK(book.best_ask() == nullptr);
    CHECK(book.best_bid()->price == 101 && book.best_bid()->total_qty == 15);
    CHECK(book.find(2) != nullptr && book.find(2)->qty == 15);
    CHECK(book.check_invariants());

    // Non-crossing orders never trade.
    const AddResult s = book.add_limit(3, Side::Sell, 102, 5);
    CHECK(s.filled == 0 && s.resting == 5);
    CHECK(book.check_invariants());
}

void test_arena_exhaustion_and_reuse() {
    OrderBook book({.max_orders = 4, .max_levels = 3});
    CHECK(book.add_limit(1, Side::Buy, 100, 1).status == Status::Ok);
    CHECK(book.add_limit(2, Side::Buy, 100, 1).status == Status::Ok);
    CHECK(book.add_limit(3, Side::Buy, 100, 1).status == Status::Ok);
    CHECK(book.add_limit(4, Side::Buy, 100, 1).status == Status::Ok);
    CHECK(book.add_limit(5, Side::Buy, 100, 1).status == Status::PoolExhausted);
    CHECK(book.order_count() == 4);

    // Released nodes are recycled indefinitely with no growth.
    for (OrderId id = 10; id < 10'000; ++id) {
        CHECK(book.cancel(id == 10 ? 1 : id - 1) == Status::Ok);
        CHECK(book.add_limit(id, Side::Buy, 100, 1).status == Status::Ok);
    }
    CHECK(book.order_count() == 4);

    // Level arena: 1 bid level + 2 ask levels fills it.
    CHECK(book.cancel(2) == Status::Ok);
    CHECK(book.cancel(3) == Status::Ok);
    CHECK(book.add_limit(20'000, Side::Sell, 200, 1).status == Status::Ok);
    CHECK(book.add_limit(20'001, Side::Sell, 201, 1).status == Status::Ok);
    CHECK(book.level_count() == 3);
    CHECK(book.add_limit(20'002, Side::Sell, 202, 1).status == Status::PoolExhausted);
    CHECK(book.check_invariants());
}

// Straightforward reference matcher built on std containers.
class ReferenceBook {
public:
    std::vector<Trade> add(OrderId id, Side side, Price price, Qty qty) {
        std::vector<Trade> trades;
        auto match = [&](auto& levels) {
            while (qty != 0 && !levels.empty()) {
                auto it = levels.begin();
                if (side == Side::Buy ? price < it->first : price > it->first) break;
                auto& [maker_id, maker_qty] = it->second.front();
                const Qty fill = qty < maker_qty ? qty : maker_qty;
                trades.push_back({maker_id, id, it->first, fill, side});
                qty -= fill;
                maker_qty -= fill;
                if (maker_qty == 0) {
                    live_.erase(maker_id);
                    it->second.pop_front();
                    if (it->second.empty()) levels.erase(it);
                }
            }
        };
        if (side == Side::Buy) {
            match(asks_);
            if (qty != 0) bids_[price].emplace_back(id, qty);
        } else {
            match(bids_);
            if (qty != 0) asks_[price].emplace_back(id, qty);
        }
        if (qty != 0) live_[id] = {side, price};
        return trades;
    }

    bool cancel(OrderId id) {
        const auto it = live_.find(id);
        if (it == live_.end()) return false;
        const auto [side, price] = it->second;
        auto erase_from = [&](auto& levels) {
            auto& queue = levels[price];
            for (auto q = queue.begin(); q != queue.end(); ++q) {
                if (q->first == id) {
                    queue.erase(q);
                    break;
                }
            }
            if (queue.empty()) levels.erase(price);
        };
        if (side == Side::Buy) erase_from(bids_); else erase_from(asks_);
        live_.erase(it);
        return true;
    }

    bool has(OrderId id) const { return live_.count(id) != 0; }
    std::size_t order_count() const { return live_.size(); }
    std::size_t level_count() const { return bids_.size() + asks_.size(); }

    using Queue = std::deque<std::pair<OrderId, Qty>>;
    std::map<Price, Queue, std::greater<Price>> bids_;
    std::map<Price, Queue> asks_;

private:
    std::unordered_map<OrderId, std::pair<Side, Price>> live_;
};

void test_differential_against_reference() {
    // Sized above the 300k-step flow so the arena can never be the limiting factor here.
    OrderBook book({.max_orders = 1U << 19, .max_levels = 1U << 10});
    ReferenceBook ref;

    std::uint64_t state = 0x9E3779B97F4A7C15ULL;
    auto rng = [&state]() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    };

    std::vector<Trade> trades;
    OrderId next_id = 1;
    std::size_t total_trades = 0;
    bool ok = true;

    for (int step = 0; step < 300'000 && ok; ++step) {
        if (rng() % 100 < 35 && next_id > 1) {
            const OrderId id = 1 + rng() % (next_id - 1);
            const bool expected = ref.cancel(id);
            ok = ok && ((book.cancel(id) == Status::Ok) == expected);
        } else {
            const OrderId id = next_id++;
            const Side side = (rng() & 1) != 0 ? Side::Buy : Side::Sell;
            const Price price = 1000 + static_cast<Price>(rng() % 40) - 20;
            const Qty qty = 1 + rng() % 50;

            trades.clear();
            const AddResult r = book.add_limit(id, side, price, qty, [&](const Trade& t) { trades.push_back(t); });
            const std::vector<Trade> expected = ref.add(id, side, price, qty);

            ok = ok && r.status == Status::Ok && trades.size() == expected.size();
            Qty filled = 0;
            for (std::size_t i = 0; ok && i < trades.size(); ++i) {
                ok = same(trades[i], expected[i]);
                filled += trades[i].qty;
            }
            ok = ok && r.filled == filled && r.filled + r.resting == qty;
            ok = ok && (book.find(id) != nullptr) == ref.has(id);
            total_trades += trades.size();
        }

        ok = ok && book.order_count() == ref.order_count() && book.level_count() == ref.level_count();
        if (ok && !ref.bids_.empty()) {
            ok = book.best_bid() != nullptr && book.best_bid()->price == ref.bids_.begin()->first;
        }
        if (ok && !ref.asks_.empty()) {
            ok = book.best_ask() != nullptr && book.best_ask()->price == ref.asks_.begin()->first;
        }
        if (ok && step % 1024 == 0) ok = book.check_invariants();
        if (!ok) std::printf("  divergence at step %d\n", step);
    }

    CHECK(ok);
    CHECK(book.check_invariants());
    CHECK(total_trades > 10'000);  // the flow genuinely exercised matching
    std::printf("    (%zu trades, %zu resting orders, %zu levels at end)\n", total_trades,
                book.order_count(), book.level_count());
}

void test_market_order() {
    OrderBook book({.max_orders = 64, .max_levels = 16});
    CHECK(book.add_market(Side::Buy, 0).status == Status::InvalidQty);

    // Against an empty side nothing fills and nothing rests.
    AddResult r = book.add_market(Side::Buy, 10);
    CHECK(r.status == Status::Ok && r.filled == 0 && r.resting == 0);
    CHECK(book.order_count() == 0);

    book.add_limit(1, Side::Sell, 100, 5);
    book.add_limit(2, Side::Sell, 100, 5);
    book.add_limit(3, Side::Sell, 105, 5);
    book.add_limit(4, Side::Buy, 90, 7);

    // Sweeps price levels best-first, FIFO within a level, at the makers' prices.
    std::vector<Trade> trades;
    r = book.add_market(Side::Buy, 12, [&](const Trade& t) { trades.push_back(t); });
    CHECK(r.status == Status::Ok && r.filled == 12 && r.resting == 0);
    CHECK(trades.size() == 3);
    CHECK(same(trades[0], Trade{1, kMarketTakerId, 100, 5, Side::Buy}));
    CHECK(same(trades[1], Trade{2, kMarketTakerId, 100, 5, Side::Buy}));
    CHECK(same(trades[2], Trade{3, kMarketTakerId, 105, 2, Side::Buy}));
    CHECK(book.best_ask()->price == 105 && book.best_ask()->total_qty == 3);

    // Exhausting the book cancels the unfilled remainder instead of resting it.
    r = book.add_market(Side::Sell, 100);
    CHECK(r.status == Status::Ok && r.filled == 7 && r.resting == 0);
    CHECK(book.best_bid() == nullptr);
    CHECK(book.order_count() == 1);
    CHECK(book.check_invariants());
}

}  // namespace

int main() {
    using aether_test::run;
    run("layout", test_layout);
    run("market_order", test_market_order);
    run("empty_and_rejects", test_empty_and_rejects);
    run("price_priority_and_level_order", test_price_priority_and_level_order);
    run("fifo_time_priority", test_fifo_time_priority);
    run("partial_fill_rests_remainder", test_partial_fill_rests_remainder);
    run("arena_exhaustion_and_reuse", test_arena_exhaustion_and_reuse);
    run("differential_against_reference", test_differential_against_reference);
    return aether_test::summary();
}
