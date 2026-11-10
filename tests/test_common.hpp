// Minimal dependency-free test harness shared by the AetherLOB test binaries.
// A failed CHECK prints its location and makes the binary exit non-zero.
// CHECK is for the main test thread; worker threads report through atomics.
#pragma once

#include <cstdio>

namespace aether_test {

inline int g_failures = 0;

inline void run(const char* name, void (*test)()) {
    const int before = g_failures;
    std::printf("[ RUN  ] %s\n", name);
    std::fflush(stdout);
    test();
    std::printf("[ %s ] %s\n", g_failures == before ? " OK " : "FAIL", name);
}

inline int summary() {
    std::printf("\n%s (%d failure%s)\n", g_failures == 0 ? "ALL TESTS PASSED" : "TESTS FAILED",
                g_failures, g_failures == 1 ? "" : "s");
    return g_failures == 0 ? 0 : 1;
}

}  // namespace aether_test

#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond); \
            ++aether_test::g_failures;                                    \
        }                                                                 \
    } while (false)
