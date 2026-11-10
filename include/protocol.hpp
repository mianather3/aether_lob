// AetherLOB - binary wire protocol (ITCH-style framing, SBE-style encoding).
//
// Every message starts with a one-byte type tag that fixes its length:
//
//   'A' AddOrderMsg     type(1) order_id(8) side(1) price(8) qty(4)   = 22 bytes
//   'X' CancelOrderMsg  type(1) order_id(8)                           =  9 bytes
//   'M' MarketOrderMsg  type(1) side(1) qty(4)                        =  6 bytes
//
// Integers are little-endian on the wire. Every multi-byte field is an LE<T>,
// a byte array read and written with shifts, so the structs are packed by
// construction, have no alignment requirement (safe to overlay on any buffer
// offset) and decode identically on big- and little-endian hosts. On
// little-endian targets the shifts compile down to plain loads and stores.
// Side is the ASCII byte 'B' or 'S'.
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <type_traits>
#include <vector>

#include "order_book.hpp"

namespace aether {

// Little-endian integer stored as raw bytes.
template <class T>
    requires std::is_integral_v<T>
struct LE {
    unsigned char bytes[sizeof(T)]{};

    constexpr LE() noexcept = default;
    constexpr LE(T value) noexcept { set(value); }  // NOLINT: implicit by design

    constexpr void set(T value) noexcept {
        auto bits = static_cast<std::make_unsigned_t<T>>(value);
        for (std::size_t i = 0; i < sizeof(T); ++i) {
            bytes[i] = static_cast<unsigned char>(bits >> (8 * i));
        }
    }
    [[nodiscard]] constexpr T get() const noexcept {
        std::make_unsigned_t<T> bits = 0;
        for (std::size_t i = 0; i < sizeof(T); ++i) {
            bits = static_cast<std::make_unsigned_t<T>>(
                bits | static_cast<std::make_unsigned_t<T>>(bytes[i]) << (8 * i));
        }
        return static_cast<T>(bits);
    }
};

inline constexpr std::uint8_t kWireBuy = 'B';
inline constexpr std::uint8_t kWireSell = 'S';

constexpr std::uint8_t to_wire(Side side) noexcept {
    return side == Side::Buy ? kWireBuy : kWireSell;
}

#pragma pack(push, 1)

struct AddOrderMsg {
    static constexpr std::uint8_t kType = 'A';
    std::uint8_t type{kType};
    LE<std::uint64_t> order_id;
    std::uint8_t side{kWireBuy};
    LE<std::int64_t> price;
    LE<std::uint32_t> qty;
};

struct CancelOrderMsg {
    static constexpr std::uint8_t kType = 'X';
    std::uint8_t type{kType};
    LE<std::uint64_t> order_id;
};

struct MarketOrderMsg {
    static constexpr std::uint8_t kType = 'M';
    std::uint8_t type{kType};
    std::uint8_t side{kWireBuy};
    LE<std::uint32_t> qty;
};

#pragma pack(pop)

static_assert(sizeof(AddOrderMsg) == 22 && alignof(AddOrderMsg) == 1);
static_assert(sizeof(CancelOrderMsg) == 9 && alignof(CancelOrderMsg) == 1);
static_assert(sizeof(MarketOrderMsg) == 6 && alignof(MarketOrderMsg) == 1);
static_assert(std::is_trivially_copyable_v<AddOrderMsg> && std::is_standard_layout_v<AddOrderMsg>);
static_assert(std::is_trivially_copyable_v<CancelOrderMsg> && std::is_standard_layout_v<CancelOrderMsg>);
static_assert(std::is_trivially_copyable_v<MarketOrderMsg> && std::is_standard_layout_v<MarketOrderMsg>);

inline constexpr std::size_t kMaxWireMsgSize = sizeof(AddOrderMsg);

constexpr AddOrderMsg make_add(OrderId id, Side side, Price price, std::uint32_t qty) noexcept {
    AddOrderMsg msg;
    msg.order_id.set(id);
    msg.side = to_wire(side);
    msg.price.set(price);
    msg.qty.set(qty);
    return msg;
}
constexpr CancelOrderMsg make_cancel(OrderId id) noexcept {
    CancelOrderMsg msg;
    msg.order_id.set(id);
    return msg;
}
constexpr MarketOrderMsg make_market(Side side, std::uint32_t qty) noexcept {
    MarketOrderMsg msg;
    msg.side = to_wire(side);
    msg.qty.set(qty);
    return msg;
}

// Host-order command: what the gateway hands to the matching thread.
enum class CommandType : std::uint8_t { Add, Cancel, Market };

struct Command {
    OrderId order_id{0};
    Price price{0};
    Qty qty{0};
    // Hardware timestamps in tsc::now() ticks (see tsc_timer.hpp); 0 = unstamped.
    std::uint64_t rx_ticks{0};       // the gateway received the packet carrying this message
    std::uint64_t enqueue_ticks{0};  // the gateway pushed this command to the engine queue
    CommandType type{CommandType::Add};
    Side side{Side::Buy};
};
static_assert(std::is_trivially_copyable_v<Command> && sizeof(Command) == 48);

enum class DecodeStatus : std::uint8_t {
    Ok,
    NeedMoreData,  // buffer ends inside a message; retry once more bytes arrive
    UnknownType,   // unrecognised type tag: the stream cannot be resynchronised
    BadSide,       // side byte is neither 'B' nor 'S'
};

struct DecodeResult {
    DecodeStatus status;
    std::size_t consumed;  // bytes used; 0 unless status == Ok
};

namespace detail {
constexpr bool side_from_wire(std::uint8_t wire, Side& out) noexcept {
    if (wire == kWireBuy) {
        out = Side::Buy;
        return true;
    }
    if (wire == kWireSell) {
        out = Side::Sell;
        return true;
    }
    return false;
}
}  // namespace detail

// Decodes one message from the front of `in`. Bounds-checked: never reads past
// in.size(). On anything other than Ok, `out` is left unspecified.
[[nodiscard]] inline DecodeResult decode(std::span<const std::byte> in, Command& out) noexcept {
    if (in.empty()) {
        return {DecodeStatus::NeedMoreData, 0};
    }
    switch (static_cast<std::uint8_t>(in[0])) {
        case AddOrderMsg::kType: {
            if (in.size() < sizeof(AddOrderMsg)) return {DecodeStatus::NeedMoreData, 0};
            AddOrderMsg msg;
            std::memcpy(&msg, in.data(), sizeof msg);
            if (!detail::side_from_wire(msg.side, out.side)) return {DecodeStatus::BadSide, 0};
            out.type = CommandType::Add;
            out.order_id = msg.order_id.get();
            out.price = msg.price.get();
            out.qty = msg.qty.get();
            return {DecodeStatus::Ok, sizeof msg};
        }
        case CancelOrderMsg::kType: {
            if (in.size() < sizeof(CancelOrderMsg)) return {DecodeStatus::NeedMoreData, 0};
            CancelOrderMsg msg;
            std::memcpy(&msg, in.data(), sizeof msg);
            out.type = CommandType::Cancel;
            out.side = Side::Buy;
            out.order_id = msg.order_id.get();
            out.price = 0;
            out.qty = 0;
            return {DecodeStatus::Ok, sizeof msg};
        }
        case MarketOrderMsg::kType: {
            if (in.size() < sizeof(MarketOrderMsg)) return {DecodeStatus::NeedMoreData, 0};
            MarketOrderMsg msg;
            std::memcpy(&msg, in.data(), sizeof msg);
            if (!detail::side_from_wire(msg.side, out.side)) return {DecodeStatus::BadSide, 0};
            out.type = CommandType::Market;
            out.order_id = kMarketTakerId;
            out.price = 0;
            out.qty = msg.qty.get();
            return {DecodeStatus::Ok, sizeof msg};
        }
        default:
            return {DecodeStatus::UnknownType, 0};
    }
}

// Cold-path helper for building streams in tests, benchmarks and simulators.
template <class Msg>
void append(std::vector<std::byte>& stream, const Msg& msg) {
    const auto* first = reinterpret_cast<const std::byte*>(&msg);
    stream.insert(stream.end(), first, first + sizeof(Msg));
}

}  // namespace aether
