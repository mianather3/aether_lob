#include "market_snapshot.hpp"

#include <cstdarg>
#include <cstring>

#include "protocol.hpp"  // LE<T>: endian-safe fixed-width fields

namespace aether {

namespace {

std::uint32_t capture_side(const PriceLevel* level, std::uint32_t depth, L2Level* out) noexcept {
    std::uint32_t n = 0;
    Qty cumulative = 0;
    for (; level != nullptr && n < depth; level = level->next, ++n) {
        cumulative += level->total_qty;
        out[n] = {level->price, level->total_qty, cumulative, level->order_count};
    }
    return n;
}

// Bounded formatter with snprintf semantics: keeps counting past the end.
class TextSink {
public:
    TextSink(char* out, std::size_t capacity) noexcept : out_(out), capacity_(capacity) {
        if (capacity_ != 0) out_[0] = '\0';
    }
#if defined(__GNUC__)
    __attribute__((format(printf, 2, 3)))
#endif
    void put(const char* format, ...) noexcept {
        va_list args;
        va_start(args, format);
        const std::size_t room = length_ < capacity_ ? capacity_ - length_ : 0;
        const int n = std::vsnprintf(room != 0 ? out_ + length_ : nullptr, room, format, args);
        va_end(args);
        if (n > 0) length_ += static_cast<std::size_t>(n);
    }
    [[nodiscard]] std::size_t length() const noexcept { return length_; }

private:
    char* out_;
    std::size_t capacity_;
    std::size_t length_{0};
};

unsigned long long ull(std::uint64_t v) noexcept { return static_cast<unsigned long long>(v); }

void json_levels(TextSink& sink, const char* name, const L2Level* levels, std::uint32_t count) noexcept {
    sink.put("\"%s\":[", name);
    for (std::uint32_t i = 0; i < count; ++i) {
        sink.put("%s{\"price\":%lld,\"qty\":%llu,\"cum_qty\":%llu,\"orders\":%u}", i == 0 ? "" : ",",
                 static_cast<long long>(levels[i].price), ull(levels[i].qty), ull(levels[i].cumulative_qty),
                 levels[i].orders);
    }
    sink.put("]");
}

void json_histogram(TextSink& sink, const char* name, const HistogramSummary& h, bool last) noexcept {
    sink.put("\"%s\":{\"count\":%llu,\"min\":%llu,\"mean\":%.1f,\"p50\":%llu,\"p90\":%llu,\"p99\":%llu,"
             "\"p99.9\":%llu,\"p99.99\":%llu,\"max\":%llu}%s",
             name, ull(h.count), ull(h.min), h.mean, ull(h.p50), ull(h.p90), ull(h.p99), ull(h.p999),
             ull(h.p9999), ull(h.max), last ? "" : ",");
}

#pragma pack(push, 1)
struct WireHeader {
    char magic[4];
    LE<std::uint16_t> version;
    LE<std::uint16_t> reserved;
    LE<std::uint64_t> sequence;
    LE<std::uint64_t> resting_orders;
    LE<std::uint64_t> level_count;
    LE<std::uint32_t> bid_levels;
    LE<std::uint32_t> ask_levels;
};
struct WireLevel {
    LE<std::int64_t> price;
    LE<std::uint64_t> qty;
    LE<std::uint32_t> orders;
};
#pragma pack(pop)
static_assert(sizeof(WireHeader) == 40 && sizeof(WireLevel) == 20);

constexpr char kMagic[4] = {'A', 'L', '2', 'S'};
constexpr std::uint16_t kVersion = 1;

}  // namespace

void capture_l2(const OrderBook& book, std::uint32_t depth, std::uint64_t sequence, L2Snapshot& out) noexcept {
    if (depth > kMaxSnapshotDepth) depth = kMaxSnapshotDepth;
    out.sequence = sequence;
    out.resting_orders = book.order_count();
    out.level_count = book.level_count();
    out.bid_levels = capture_side(book.best_bid(), depth, out.bids);
    out.ask_levels = capture_side(book.best_ask(), depth, out.asks);
}

void SnapshotChannel::serve(const OrderBook& book, std::uint32_t depth, std::uint64_t sequence) noexcept {
    // Read the ticket first: a request that arrives during the capture is
    // simply served by the next one.
    const std::uint64_t ticket = requested_.load(std::memory_order_acquire);
    const std::uint32_t back = 1U - front_.load(std::memory_order_acquire);
    capture_l2(book, depth, sequence, buffers_[back]);
    front_.store(back, std::memory_order_release);
    served_local_ = ticket;
    served_.store(ticket, std::memory_order_release);  // publishes the buffer contents too
}

std::size_t write_json(const L2Snapshot& s, char* out, std::size_t capacity) noexcept {
    TextSink sink(out, capacity);
    sink.put("{\"sequence\":%llu,\"resting_orders\":%llu,\"levels\":%llu,", ull(s.sequence), ull(s.resting_orders),
             ull(s.level_count));
    json_levels(sink, "bids", s.bids, s.bid_levels);
    sink.put(",");
    json_levels(sink, "asks", s.asks, s.ask_levels);
    sink.put("}");
    return sink.length();
}

std::size_t write_binary(const L2Snapshot& s, std::span<std::byte> out) noexcept {
    if (s.bid_levels > kMaxSnapshotDepth || s.ask_levels > kMaxSnapshotDepth) return 0;
    const std::size_t size = binary_size(s.bid_levels, s.ask_levels);
    if (out.size() < size) return 0;

    WireHeader header{};
    std::memcpy(header.magic, kMagic, sizeof kMagic);
    header.version.set(kVersion);
    header.sequence.set(s.sequence);
    header.resting_orders.set(s.resting_orders);
    header.level_count.set(s.level_count);
    header.bid_levels.set(s.bid_levels);
    header.ask_levels.set(s.ask_levels);
    std::byte* cursor = out.data();
    std::memcpy(cursor, &header, sizeof header);
    cursor += sizeof header;

    const auto put_levels = [&cursor](const L2Level* levels, std::uint32_t count) {
        for (std::uint32_t i = 0; i < count; ++i) {
            WireLevel wire;
            wire.price.set(levels[i].price);
            wire.qty.set(levels[i].qty);
            wire.orders.set(levels[i].orders);
            std::memcpy(cursor, &wire, sizeof wire);
            cursor += sizeof wire;
        }
    };
    put_levels(s.bids, s.bid_levels);
    put_levels(s.asks, s.ask_levels);
    return size;
}

bool read_binary(std::span<const std::byte> in, L2Snapshot& out) noexcept {
    WireHeader header;
    if (in.size() < sizeof header) return false;
    std::memcpy(&header, in.data(), sizeof header);
    if (std::memcmp(header.magic, kMagic, sizeof kMagic) != 0 || header.version.get() != kVersion) return false;
    const std::uint32_t bids = header.bid_levels.get();
    const std::uint32_t asks = header.ask_levels.get();
    if (bids > kMaxSnapshotDepth || asks > kMaxSnapshotDepth || in.size() < binary_size(bids, asks)) return false;

    out.sequence = header.sequence.get();
    out.resting_orders = header.resting_orders.get();
    out.level_count = header.level_count.get();
    out.bid_levels = bids;
    out.ask_levels = asks;
    const std::byte* cursor = in.data() + sizeof header;
    const auto get_levels = [&cursor](L2Level* levels, std::uint32_t count) {
        Qty cumulative = 0;
        for (std::uint32_t i = 0; i < count; ++i) {
            WireLevel wire;
            std::memcpy(&wire, cursor, sizeof wire);
            cursor += sizeof wire;
            cumulative += wire.qty.get();  // derived, so not carried on the wire
            levels[i] = {wire.price.get(), wire.qty.get(), cumulative, wire.orders.get()};
        }
    };
    get_levels(out.bids, bids);
    get_levels(out.asks, asks);
    return true;
}

std::size_t write_metrics_json(const TelemetrySnapshot& m, char* out, std::size_t capacity) noexcept {
    TextSink sink(out, capacity);
    sink.put("{\"messages\":{\"received\":%llu,\"accepted\":%llu,\"rejected\":%llu,\"dropped_queue_full\":%llu,"
             "\"invalid_bytes\":%llu,\"checksum_errors\":%llu,\"packets\":%llu,\"bytes\":%llu,"
             "\"backpressure_waits\":%llu},",
             ull(m.received), ull(m.accepted), ull(m.rejected), ull(m.dropped_queue_full), ull(m.invalid_bytes),
             ull(m.checksum_errors), ull(m.packets), ull(m.bytes), ull(m.backpressure_waits));
    sink.put("\"execution\":{\"trades\":%llu,\"volume\":%llu,\"resting_orders\":%llu,\"levels\":%llu},",
             ull(m.trades), ull(m.volume), ull(m.resting_orders), ull(m.levels));
    sink.put("\"latency_ns\":{");
    json_histogram(sink, "ingest_to_queue", m.ingest_to_queue, false);
    json_histogram(sink, "queue_to_match", m.queue_to_match, false);
    json_histogram(sink, "match_service", m.match_service, false);
    json_histogram(sink, "tick_to_trade", m.tick_to_trade, true);
    sink.put("}}");
    return sink.length();
}

void print_metrics(std::FILE* out, const TelemetrySnapshot& m) noexcept {
    std::fprintf(out, "  messages   received=%llu accepted=%llu rejected=%llu dropped(queue full)=%llu\n",
                 ull(m.received), ull(m.accepted), ull(m.rejected), ull(m.dropped_queue_full));
    std::fprintf(out, "             invalid_bytes=%llu checksum_errors=%llu packets=%llu bytes=%llu backpressure_waits=%llu\n",
                 ull(m.invalid_bytes), ull(m.checksum_errors), ull(m.packets), ull(m.bytes), ull(m.backpressure_waits));
    std::fprintf(out, "  execution  trades=%llu volume=%llu resting_orders=%llu levels=%llu\n", ull(m.trades),
                 ull(m.volume), ull(m.resting_orders), ull(m.levels));
    std::fprintf(out, "  | %-16s | %9s | %7s | %7s | %7s | %8s | %8s | %9s |\n", "latency (ns)", "samples", "p50",
                 "p90", "p99", "p99.9", "p99.99", "max");
    std::fprintf(out, "  |------------------|-----------|---------|---------|---------|----------|----------|-----------|\n");
    const auto row = [out](const char* name, const HistogramSummary& h) {
        std::fprintf(out, "  | %-16s | %9llu | %7llu | %7llu | %7llu | %8llu | %8llu | %9llu |\n", name, ull(h.count),
                     ull(h.p50), ull(h.p90), ull(h.p99), ull(h.p999), ull(h.p9999), ull(h.max));
    };
    row("ingest->queue", m.ingest_to_queue);
    row("queue->match", m.queue_to_match);
    row("match service", m.match_service);
    row("order-processing latency", m.tick_to_trade);
}

void print_snapshot(std::FILE* out, const L2Snapshot& s, std::uint32_t max_levels) noexcept {
    std::fprintf(out, "  L2 @ sequence %llu: %llu resting orders on %llu levels\n", ull(s.sequence),
                 ull(s.resting_orders), ull(s.level_count));
    std::fprintf(out, "    %10s %10s %7s | %-10s %10s %10s %7s\n", "bid cum", "bid qty", "orders", "bid", "ask",
                 "ask qty", "ask cum");
    const std::uint32_t rows = s.bid_levels > s.ask_levels ? s.bid_levels : s.ask_levels;
    for (std::uint32_t i = 0; i < rows && i < max_levels; ++i) {
        if (i < s.bid_levels) {
            std::fprintf(out, "    %10llu %10llu %7u | %-10lld", ull(s.bids[i].cumulative_qty), ull(s.bids[i].qty),
                         s.bids[i].orders, static_cast<long long>(s.bids[i].price));
        } else {
            std::fprintf(out, "    %10s %10s %7s | %-10s", "", "", "", "");
        }
        if (i < s.ask_levels) {
            std::fprintf(out, " %10lld %10llu %7llu\n", static_cast<long long>(s.asks[i].price), ull(s.asks[i].qty),
                         ull(s.asks[i].cumulative_qty));
        } else {
            std::fprintf(out, "\n");
        }
    }
}

}  // namespace aether
