// SPSC ring buffer tests: capacity/overflow semantics, backpressure, and a
// two-thread 5,000,000-message integrity run. Built to run under TSan/ASan.
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>

#include "spsc_queue.hpp"
#include "test_common.hpp"

using namespace aether;

namespace {

// 32-byte payload whose every field is derived from the sequence number, so a
// lost, duplicated, reordered or torn message is detectable by the consumer.
struct Msg {
    std::uint64_t seq;
    std::uint64_t a;
    std::uint64_t b;
    std::uint64_t checksum;
};

constexpr std::uint64_t mix(std::uint64_t x) noexcept {
    x ^= x >> 33;
    x *= 0xFF51AFD7ED558CCDULL;
    x ^= x >> 33;
    return x;
}

constexpr Msg make_msg(std::uint64_t seq) noexcept {
    return {seq, seq * 0x9E3779B97F4A7C15ULL, ~seq, mix(seq)};
}

constexpr bool intact(const Msg& m, std::uint64_t expected_seq) noexcept {
    return m.seq == expected_seq && m.a == expected_seq * 0x9E3779B97F4A7C15ULL &&
           m.b == ~expected_seq && m.checksum == mix(expected_seq);
}

void test_layout_and_capacity() {
    // Producer and consumer state must live on different cache lines.
    CHECK(alignof(SpscQueue<Msg>) == 64);
    CHECK(sizeof(SpscQueue<Msg>) % 64 == 0 && sizeof(SpscQueue<Msg>) >= 3 * 64);

    CHECK(SpscQueue<Msg>(0).capacity() == 2);
    CHECK(SpscQueue<Msg>(8).capacity() == 8);
    CHECK(SpscQueue<Msg>(9).capacity() == 16);
    CHECK(SpscQueue<Msg>(1000).capacity() == 1024);

    SpscQueue<Msg> q(8);
    Msg out{};
    CHECK(!q.try_pop(out));
    CHECK(q.size_approx() == 0);
}

void test_overflow_is_rejected_not_overwritten() {
    SpscQueue<Msg> q(8);
    for (std::uint64_t i = 0; i < 8; ++i) {
        CHECK(q.try_push(make_msg(i)));
    }
    CHECK(q.size_approx() == 8);

    // Full: pushes fail and must leave the queued data untouched.
    for (std::uint64_t i = 100; i < 110; ++i) {
        CHECK(!q.try_push(make_msg(i)));
    }
    CHECK(q.size_approx() == 8);

    // Freeing one slot admits exactly one more item.
    Msg out{};
    CHECK(q.try_pop(out) && intact(out, 0));
    CHECK(q.try_push(make_msg(8)));
    CHECK(!q.try_push(make_msg(9)));

    for (std::uint64_t i = 1; i <= 8; ++i) {
        CHECK(q.try_pop(out) && intact(out, i));
    }
    CHECK(!q.try_pop(out));
    CHECK(q.size_approx() == 0);
}

void test_wraparound_single_thread() {
    SpscQueue<Msg> q(4);
    std::uint64_t pushed = 0;
    std::uint64_t popped = 0;
    bool ok = true;
    Msg out{};
    // Varying burst sizes walk the indices around the ring many thousands of times.
    for (int round = 0; round < 100'000 && ok; ++round) {
        const int burst = 1 + round % 4;
        for (int i = 0; i < burst; ++i) {
            ok = ok && q.try_push(make_msg(pushed++));
        }
        for (int i = 0; i < burst; ++i) {
            ok = ok && q.try_pop(out) && intact(out, popped++);
        }
    }
    CHECK(ok);
    CHECK(pushed == popped && q.size_approx() == 0);
}

void test_backpressure_slow_consumer() {
    constexpr std::uint64_t kCount = 200'000;
    SpscQueue<Msg> q(64);
    std::atomic<bool> consumer_may_start{false};
    std::atomic<bool> corrupt{false};
    std::atomic<std::uint64_t> received{0};

    std::thread consumer([&] {
        while (!consumer_may_start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        Msg out{};
        std::uint64_t expected = 0;
        while (expected < kCount) {
            if (!q.try_pop(out)) {
                std::this_thread::yield();
                continue;
            }
            if (!intact(out, expected)) {
                corrupt.store(true, std::memory_order_release);
            }
            ++expected;
        }
        received.store(expected, std::memory_order_release);
    });

    // The consumer is held back until the producer has provably hit a full queue.
    std::uint64_t rejected = 0;
    for (std::uint64_t seq = 0; seq < kCount;) {
        if (q.try_push(make_msg(seq))) {
            ++seq;
            continue;
        }
        ++rejected;
        if (seq == q.capacity()) {
            consumer_may_start.store(true, std::memory_order_release);
        }
        std::this_thread::yield();
    }
    consumer.join();

    CHECK(rejected > 0);  // backpressure was really exercised
    CHECK(received.load(std::memory_order_acquire) == kCount);  // and nothing was dropped
    CHECK(!corrupt.load(std::memory_order_acquire));
    CHECK(q.size_approx() == 0);
}

void test_five_million_messages_two_threads() {
    constexpr std::uint64_t kCount = 5'000'000;
    SpscQueue<Msg> q(1024);
    std::atomic<std::uint64_t> first_bad{UINT64_MAX};
    std::atomic<std::uint64_t> received{0};
    std::atomic<std::uint64_t> consumer_sum{0};

    std::thread consumer([&] {
        Msg out{};
        std::uint64_t expected = 0;
        std::uint64_t sum = 0;
        while (expected < kCount) {
            if (!q.try_pop(out)) {
                std::this_thread::yield();
                continue;
            }
            if (!intact(out, expected) && first_bad.load(std::memory_order_acquire) == UINT64_MAX) {
                first_bad.store(expected, std::memory_order_release);
            }
            sum += out.checksum;
            ++expected;
        }
        consumer_sum.store(sum, std::memory_order_release);
        received.store(expected, std::memory_order_release);
    });

    std::uint64_t producer_sum = 0;
    std::uint64_t rejected = 0;
    for (std::uint64_t seq = 0; seq < kCount;) {
        if (q.try_push(make_msg(seq))) {
            producer_sum += mix(seq);
            ++seq;
        } else {
            ++rejected;
            std::this_thread::yield();
        }
    }
    consumer.join();

    Msg out{};
    CHECK(first_bad.load(std::memory_order_acquire) == UINT64_MAX);
    CHECK(received.load(std::memory_order_acquire) == kCount);
    CHECK(consumer_sum.load(std::memory_order_acquire) == producer_sum);
    CHECK(!q.try_pop(out));  // nothing extra was ever produced
    std::printf("    (%llu messages verified in order, %llu full-queue rejections retried)\n",
                static_cast<unsigned long long>(kCount), static_cast<unsigned long long>(rejected));
}

}  // namespace

int main() {
    using aether_test::run;
    run("layout_and_capacity", test_layout_and_capacity);
    run("overflow_is_rejected_not_overwritten", test_overflow_is_rejected_not_overwritten);
    run("wraparound_single_thread", test_wraparound_single_thread);
    run("backpressure_slow_consumer", test_backpressure_slow_consumer);
    run("five_million_messages_two_threads", test_five_million_messages_two_threads);
    return aether_test::summary();
}
