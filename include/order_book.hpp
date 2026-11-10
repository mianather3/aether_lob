// AetherLOB - deterministic L3 limit order book with price-time (FIFO) priority.
//
// Layout
//   Order       one cache line; intrusive node in its price level's FIFO queue.
//   PriceLevel  one cache line; owns the FIFO queue and is itself an intrusive
//               node in its side's best-first sorted level list.
//   Pool<T>     pre-allocated arenas for both node types.
//   FlatMap<T>  pre-allocated indices: order id -> Order, price -> PriceLevel.
//
// Complexity (single-threaded, no allocation after construction)
//   cancel                      O(1)
//   execute (per maker fill)    O(1)
//   add at an existing level    O(1)
//   add creating a new level    O(k), k = levels better than the new price
//
// The book is single-writer by design: one engine thread owns it.
#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "arena.hpp"
#include "flat_map.hpp"
#include "intrusive_list.hpp"

namespace aether {

using OrderId = std::uint64_t;
using Price = std::int64_t;  // integer ticks; no floating point anywhere
using Qty = std::uint64_t;

enum class Side : std::uint8_t { Buy = 0, Sell = 1 };

enum class Status : std::uint8_t {
    Ok,
    InvalidQty,     // qty == 0
    DuplicateId,    // order id already resting
    PoolExhausted,  // order or level arena is full; order rejected untouched
    NotFound,       // cancel of an unknown / already-filled id
};

struct PriceLevel;

struct alignas(kCacheLine) Order {
    Order* prev{nullptr};  // older order at the same price
    Order* next{nullptr};  // newer order at the same price
    PriceLevel* level{nullptr};
    OrderId id{0};
    Price price{0};
    Qty qty{0};  // open (unfilled) quantity
    Side side{Side::Buy};
};

struct alignas(kCacheLine) PriceLevel {
    PriceLevel* prev{nullptr};  // better-priced level
    PriceLevel* next{nullptr};  // worse-priced level
    IntrusiveList<Order> orders;  // FIFO: front() has time priority
    Price price{0};
    Qty total_qty{0};
    std::uint32_t order_count{0};
};

static_assert(sizeof(Order) == kCacheLine && alignof(Order) == kCacheLine);
static_assert(sizeof(PriceLevel) == kCacheLine && alignof(PriceLevel) == kCacheLine);
static_assert(std::is_trivially_destructible_v<Order> && std::is_standard_layout_v<Order>);
static_assert(std::is_trivially_destructible_v<PriceLevel> && std::is_standard_layout_v<PriceLevel>);

struct Trade {
    OrderId maker_id;
    OrderId taker_id;
    Price price;  // always the maker's (resting) price
    Qty qty;
    Side taker_side;
};

struct AddResult {
    Status status;
    Qty filled;   // quantity executed immediately
    Qty resting;  // quantity left on the book
};

struct BookConfig {
    std::uint32_t max_orders = 1U << 20;  // simultaneously resting orders
    std::uint32_t max_levels = 1U << 16;  // simultaneously live price levels (both sides)
};

class OrderBook {
public:
    explicit OrderBook(const BookConfig& config = {});

    OrderBook(const OrderBook&) = delete;
    OrderBook& operator=(const OrderBook&) = delete;

    // Matches against the opposite side, then rests any remainder.
    // `on_trade(const Trade&)` is invoked synchronously once per fill, in
    // execution order. It must not call back into this book.
    template <class OnTrade>
    AddResult add_limit(OrderId id, Side side, Price price, Qty qty, OnTrade&& on_trade) {
        if (qty == 0) [[unlikely]] {
            return {Status::InvalidQty, 0, 0};
        }
        // Reserve up front so an accepted order can never fail halfway.
        if (orders_.full() || levels_.full()) [[unlikely]] {
            return {Status::PoolExhausted, 0, 0};
        }
        if (order_index_.find(id) != nullptr) [[unlikely]] {
            return {Status::DuplicateId, 0, 0};
        }

        BookSide& opposite = sides_[1 - index_of(side)];
        Qty remaining = qty;
        while (remaining != 0) {
            PriceLevel* best = opposite.levels.front();
            if (best == nullptr || !crosses(side, price, best->price)) {
                break;
            }
            Order* maker = best->orders.front();
            const Qty fill = remaining < maker->qty ? remaining : maker->qty;
            const Trade trade{maker->id, id, best->price, fill, side};
            maker->qty -= fill;
            best->total_qty -= fill;
            remaining -= fill;
            if (maker->qty == 0) {
                remove_order(maker, opposite);
            }
            on_trade(trade);
        }

        if (remaining != 0) {
            rest(id, side, price, remaining);
        }
        return {Status::Ok, qty - remaining, remaining};
    }

    AddResult add_limit(OrderId id, Side side, Price price, Qty qty) {
        return add_limit(id, side, price, qty, [](const Trade&) noexcept {});
    }

    Status cancel(OrderId id) noexcept {
        Order* order = order_index_.find(id);
        if (order == nullptr) {
            return Status::NotFound;
        }
        remove_order(order, sides_[index_of(order->side)]);
        return Status::Ok;
    }

    [[nodiscard]] const PriceLevel* best_bid() const noexcept { return sides_[0].levels.front(); }
    [[nodiscard]] const PriceLevel* best_ask() const noexcept { return sides_[1].levels.front(); }
    [[nodiscard]] const PriceLevel* best(Side side) const noexcept {
        return sides_[index_of(side)].levels.front();
    }
    [[nodiscard]] const Order* find(OrderId id) const noexcept { return order_index_.find(id); }
    [[nodiscard]] std::size_t order_count() const noexcept { return orders_.in_use(); }
    [[nodiscard]] std::size_t level_count() const noexcept { return levels_.in_use(); }

    // Full structural audit (cold path, O(orders)). For tests and debugging.
    [[nodiscard]] bool check_invariants() const noexcept;

private:
    struct BookSide {
        explicit BookSide(std::size_t max_levels) : index(max_levels) {}
        IntrusiveList<PriceLevel> levels;  // sorted best-first
        FlatMap<PriceLevel> index;         // price -> level
    };

    static constexpr std::size_t index_of(Side side) noexcept {
        return static_cast<std::size_t>(side);
    }
    static constexpr std::uint64_t key_of(Price price) noexcept {
        return static_cast<std::uint64_t>(price);
    }
    // True if a taker on `side` limited at `limit` can trade with a maker at `maker_price`.
    static constexpr bool crosses(Side side, Price limit, Price maker_price) noexcept {
        return side == Side::Buy ? limit >= maker_price : limit <= maker_price;
    }
    // True if price `a` has priority over price `b` on `side`.
    static constexpr bool better(Side side, Price a, Price b) noexcept {
        return side == Side::Buy ? a > b : a < b;
    }

    void rest(OrderId id, Side side, Price price, Qty qty) noexcept {
        BookSide& own = sides_[index_of(side)];
        PriceLevel* level = own.index.find(key_of(price));
        if (level == nullptr) {
            level = levels_.acquire();
            level->price = price;
            PriceLevel* pos = own.levels.front();
            while (pos != nullptr && better(side, pos->price, price)) {
                pos = pos->next;
            }
            own.levels.insert_before(pos, level);
            own.index.insert(key_of(price), level);
        }

        Order* order = orders_.acquire();
        order->level = level;
        order->id = id;
        order->price = price;
        order->qty = qty;
        order->side = side;

        level->orders.push_back(order);
        level->total_qty += qty;
        ++level->order_count;
        order_index_.insert(id, order);
    }

    void remove_order(Order* order, BookSide& side) noexcept {
        PriceLevel* level = order->level;
        level->orders.erase(order);
        level->total_qty -= order->qty;
        --level->order_count;
        if (level->orders.empty()) {
            side.levels.erase(level);
            side.index.erase(key_of(level->price));
            levels_.release(level);
        }
        order_index_.erase(order->id);
        orders_.release(order);
    }

    Pool<Order> orders_;
    Pool<PriceLevel> levels_;
    FlatMap<Order> order_index_;
    BookSide sides_[2];  // [0] = bids, [1] = asks
};

}  // namespace aether
