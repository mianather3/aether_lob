// AetherLOB - hardware timestamp counter access.
//
//   x86-64   `lfence; rdtsc` to open an interval and `rdtscp; lfence` to close
//            it, so that neither read can be reordered across the code being
//            timed. Requires an invariant TSC (reported by calibrate()).
//   AArch64  the virtual counter register, read directly with
//            `mrs x, cntvct_el0`. This is the counter behind
//            clock_gettime_nsec_np(CLOCK_UPTIME_RAW) / mach_absolute_time on
//            Apple Silicon and behind CLOCK_MONOTONIC's vDSO on Linux, minus
//            the call and the conversion.
//   other    std::chrono::steady_clock nanoseconds.
//
// Both counters are system-wide and monotonic, so a tick value taken on one
// core can be subtracted from one taken on another.
//
// Resolution is a property of the hardware, not of this header. A TSC ticks
// at roughly the CPU's base frequency. Apple Silicon is coarser than its
// register suggests: recent chips report the counter in 1 ns units
// (cntfrq_el0 = 1 GHz) but the value advances in steps of 41-42 ns, because
// the underlying oscillator is 24 MHz (measured on an M5; older chips report
// 24 MHz directly). Reading the register makes the *read* cheap - about 1 ns
// against a library clock call - it cannot make the step finer. Any single
// interval is therefore quantised to ~42 ns there; means and percentiles over
// many samples are not biased by it.
//
// Ticks are converted with a fixed-point multiplier that is measured once
// (calibrate()) and then only read. Call calibrate() before starting any
// thread that converts ticks; Engine's constructor does.
#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>

#if defined(__x86_64__)
#include <cpuid.h>
#include <x86intrin.h>
#endif

namespace aether::tsc {

// Opens a timed interval.
[[nodiscard]] inline std::uint64_t now() noexcept {
#if defined(__x86_64__)
    _mm_lfence();  // everything before the interval has retired
    return __rdtsc();
#elif defined(__aarch64__)
    std::uint64_t ticks;
    __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(ticks));
    return ticks;
#else
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          std::chrono::steady_clock::now().time_since_epoch())
                                          .count());
#endif
}

// Closes a timed interval: on x86 `rdtscp` waits for the timed code to retire
// and the trailing `lfence` keeps later instructions from starting early.
[[nodiscard]] inline std::uint64_t now_end() noexcept {
#if defined(__x86_64__)
    unsigned aux;
    const std::uint64_t ticks = __rdtscp(&aux);
    _mm_lfence();
    return ticks;
#else
    return now();
#endif
}

struct Calibration {
    double ns_per_tick{1.0};
    std::uint64_t multiplier{1ULL << 24};  // ns_per_tick as 40.24 fixed point
    std::uint64_t ticks_per_second{1'000'000'000};
    const char* counter{"steady_clock"};
    const char* method{"nanosecond clock, no conversion"};
    bool invariant{true};  // counter rate is independent of frequency scaling / sleep states
};

namespace detail {

inline Calibration g_calibration;
inline std::once_flag g_calibration_once;

inline constexpr unsigned kFixedPointShift = 24;

inline Calibration from_frequency(double ticks_per_second, const char* counter, const char* method,
                                  bool invariant) noexcept {
    Calibration c;
    c.ns_per_tick = 1e9 / ticks_per_second;
    c.multiplier = static_cast<std::uint64_t>(c.ns_per_tick * static_cast<double>(1ULL << kFixedPointShift) + 0.5);
    c.ticks_per_second = static_cast<std::uint64_t>(ticks_per_second + 0.5);
    c.counter = counter;
    c.method = method;
    c.invariant = invariant;
    return c;
}

inline Calibration measure() noexcept {
#if defined(__aarch64__)
    // The architecture publishes the counter frequency in a register.
    std::uint64_t frequency;
    __asm__ __volatile__("mrs %0, cntfrq_el0" : "=r"(frequency));
    if (frequency != 0) {
        return from_frequency(static_cast<double>(frequency), "cntvct_el0", "cntfrq_el0 register", true);
    }
#endif
#if defined(__x86_64__) || defined(__aarch64__)
    // Measure the counter against the OS monotonic clock. Three 20 ms windows;
    // keep the window with the least clock-read jitter (the shortest one).
    using Clock = std::chrono::steady_clock;
    double best_rate = 0.0;
    double best_window = 1e300;
    for (int attempt = 0; attempt < 3; ++attempt) {
        const Clock::time_point wall_start = Clock::now();
        const std::uint64_t tick_start = now();
        Clock::time_point wall_end;
        do {
            wall_end = Clock::now();
        } while (wall_end - wall_start < std::chrono::milliseconds(20));
        const std::uint64_t tick_end = now();
        const double seconds = std::chrono::duration<double>(wall_end - wall_start).count();
        if (seconds < best_window) {
            best_window = seconds;
            best_rate = static_cast<double>(tick_end - tick_start) / seconds;
        }
    }
    bool invariant = true;
    const char* counter = "cntvct_el0";
#if defined(__x86_64__)
    counter = "tsc";
    unsigned eax = 0, ebx = 0, ecx = 0, edx = 0;
    invariant = __get_cpuid(0x80000007U, &eax, &ebx, &ecx, &edx) != 0 && (edx & (1U << 8)) != 0;
#endif
    if (best_rate > 0.0) {
        return from_frequency(best_rate, counter, "measured against steady_clock (3 x 20 ms)", invariant);
    }
#endif
    return Calibration{};
}

}  // namespace detail

// Measures the counter once per process (thread-safe, ~60 ms on x86, instant
// on AArch64) and returns the cached result on every later call.
inline const Calibration& calibrate() noexcept {
    std::call_once(detail::g_calibration_once, [] { detail::g_calibration = detail::measure(); });
    return detail::g_calibration;
}

// Converts a tick *difference* to nanoseconds. Hot path: one multiply and one
// shift. Intervals longer than 2^32 ticks would overflow that multiply, so
// they are split into two 32-bit halves and use the same multiplier: the two
// paths are one function, continuous and monotonic across the boundary.
// (Converting the long path through the double `ns_per_tick` instead is not:
// the multiplier is rounded to 2^-24 ns, which at 2^32 ticks is up to 128 ns
// away from the double, in either direction.)
[[nodiscard]] inline std::uint64_t to_ns(std::uint64_t ticks) noexcept {
    const Calibration& c = detail::g_calibration;
    if ((ticks >> 32) != 0) [[unlikely]] {
        return (((ticks >> 32) * c.multiplier) << (32 - detail::kFixedPointShift)) +
               (((ticks & 0xFFFF'FFFFULL) * c.multiplier) >> detail::kFixedPointShift);
    }
    return (ticks * c.multiplier) >> detail::kFixedPointShift;
}

// Elapsed nanoseconds between two readings; 0 if `end` is not after `start`
// (possible by a tick or two when the readings come from different cores).
[[nodiscard]] inline std::uint64_t elapsed_ns(std::uint64_t start, std::uint64_t end) noexcept {
    return end > start ? to_ns(end - start) : 0;
}

}  // namespace aether::tsc
