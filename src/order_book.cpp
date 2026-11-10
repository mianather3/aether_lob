// Cold path: construction and structural audit of the order book.
#include "order_book.hpp"

#include <initializer_list>

namespace aether {

OrderBook::OrderBook(const BookConfig& config)
    : orders_(config.max_orders),
      levels_(config.max_levels),
      order_index_(config.max_orders),
      sides_{BookSide(config.max_levels), BookSide(config.max_levels)} {}

bool OrderBook::check_invariants() const noexcept {
    std::size_t orders_seen = 0;
    std::size_t levels_seen = 0;

    for (const Side side : {Side::Buy, Side::Sell}) {
        const BookSide& book_side = sides_[index_of(side)];
        const PriceLevel* prev_level = nullptr;

        for (const PriceLevel* level = book_side.levels.front(); level != nullptr;
             prev_level = level, level = level->next) {
            ++levels_seen;
            if (!levels_.owns(level) || level->prev != prev_level) return false;
            // Strictly best-first: no duplicate or out-of-order prices.
            if (prev_level != nullptr && !better(side, prev_level->price, level->price)) return false;
            if (book_side.index.find(key_of(level->price)) != level) return false;
            if (level->orders.empty()) return false;  // empty levels must be reclaimed

            Qty qty_sum = 0;
            std::uint32_t count = 0;
            const Order* prev_order = nullptr;
            for (const Order* order = level->orders.front(); order != nullptr;
                 prev_order = order, order = order->next) {
                if (!orders_.owns(order) || order->prev != prev_order) return false;
                if (order->level != level || order->side != side) return false;
                if (order->price != level->price || order->qty == 0) return false;
                if (order_index_.find(order->id) != order) return false;
                qty_sum += order->qty;
                ++count;
            }
            if (level->orders.back() != prev_order) return false;
            if (qty_sum != level->total_qty || count != level->order_count) return false;
            orders_seen += count;
        }
        if (book_side.levels.back() != prev_level) return false;
    }

    if (orders_seen != orders_.in_use() || orders_seen != order_index_.size()) return false;
    if (levels_seen != levels_.in_use()) return false;
    if (levels_seen != sides_[0].index.size() + sides_[1].index.size()) return false;

    // A resting book is never crossed or locked.
    const PriceLevel* bid = best_bid();
    const PriceLevel* ask = best_ask();
    return bid == nullptr || ask == nullptr || bid->price < ask->price;
}

}  // namespace aether
