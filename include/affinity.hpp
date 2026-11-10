// AetherLOB - thread-to-core placement.
//
// Header-only and called exactly once per thread, at thread start-up, so it
// adds nothing to the hot path.
//
//   Linux   pthread_setaffinity_np: a hard pin to one logical CPU. Verified by
//           reading the mask back and checking the CPU the thread is running on.
//   Darwin  thread_policy_set(THREAD_AFFINITY_POLICY). macOS has no hard
//           pinning. `core` is used as an affinity *tag*: threads with
//           different tags are placed on different L2 cache domains where the
//           kernel supports it (Intel Macs). Apple Silicon kernels reject the
//           policy with KERN_NOT_SUPPORTED; the only remaining lever there is
//           QoS, so the thread is raised to USER_INTERACTIVE to keep it on the
//           performance cluster and the result is reported as Unsupported.
//
// A core of -1 always means "leave the thread alone".
#pragma once

#include <cstdint>
#include <cstdio>

#include <pthread.h>

#if defined(__linux__)
#include <sched.h>
#include <unistd.h>
#elif defined(__APPLE__)
#include <mach/mach.h>
#include <mach/thread_policy.h>
#include <pthread/qos.h>
#endif

namespace aether {

enum class PinStatus : std::uint8_t {
    NotRequested,  // core < 0
    Pinned,        // hard pin applied and verified (Linux)
    Advisory,      // affinity tag accepted and read back; a scheduler hint (Darwin/Intel)
    Unsupported,   // the platform or CPU offers no affinity control
    Failed,        // the OS refused the request, or verification did not match
};

struct PinResult {
    PinStatus status{PinStatus::NotRequested};
    int requested{-1};
    int observed_cpu{-1};      // CPU the thread was running on after pinning (Linux only)
    int error{0};              // errno / kern_return_t on Failed or Unsupported
    bool qos_elevated{false};  // Darwin: QoS raised to USER_INTERACTIVE
};

// Pins the calling thread and verifies the result.
[[nodiscard]] inline PinResult pin_current_thread(int core) noexcept {
    PinResult result;
    result.requested = core;
    if (core < 0) {
        return result;
    }

#if defined(__linux__)
    if (core >= CPU_SETSIZE) {
        result.status = PinStatus::Failed;
        return result;
    }
    cpu_set_t wanted;
    CPU_ZERO(&wanted);
    CPU_SET(core, &wanted);
    if (const int rc = ::pthread_setaffinity_np(::pthread_self(), sizeof wanted, &wanted); rc != 0) {
        result.status = PinStatus::Failed;
        result.error = rc;
        return result;
    }
    // Verify: the mask must be exactly {core} and the thread must be on it.
    cpu_set_t actual;
    CPU_ZERO(&actual);
    const int rc = ::pthread_getaffinity_np(::pthread_self(), sizeof actual, &actual);
    result.observed_cpu = ::sched_getcpu();
    const bool verified = rc == 0 && CPU_COUNT(&actual) == 1 && CPU_ISSET(core, &actual) &&
                          result.observed_cpu == core;
    result.status = verified ? PinStatus::Pinned : PinStatus::Failed;
    result.error = rc;
#elif defined(__APPLE__)
    result.qos_elevated = ::pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0) == 0;

    // pthread_mach_thread_np names the same port as mach_thread_self() but
    // without taking a send right that would then have to be deallocated.
    const thread_act_t thread = ::pthread_mach_thread_np(::pthread_self());
    thread_affinity_policy_data_t policy{core + 1};  // tag 0 means "no affinity"
    const kern_return_t kr = ::thread_policy_set(thread, THREAD_AFFINITY_POLICY,
                                                 reinterpret_cast<thread_policy_t>(&policy),
                                                 THREAD_AFFINITY_POLICY_COUNT);
    if (kr == KERN_SUCCESS) {
        thread_affinity_policy_data_t readback{0};
        mach_msg_type_number_t count = THREAD_AFFINITY_POLICY_COUNT;
        boolean_t get_default = FALSE;
        const kern_return_t got = ::thread_policy_get(thread, THREAD_AFFINITY_POLICY,
                                                      reinterpret_cast<thread_policy_t>(&readback),
                                                      &count, &get_default);
        const bool verified = got == KERN_SUCCESS && get_default == FALSE &&
                              readback.affinity_tag == policy.affinity_tag;
        result.status = verified ? PinStatus::Advisory : PinStatus::Failed;
        result.error = got;
    } else {
        result.status = kr == KERN_NOT_SUPPORTED ? PinStatus::Unsupported : PinStatus::Failed;
        result.error = kr;
    }
#else
    result.status = PinStatus::Unsupported;
#endif
    return result;
}

#if defined(__linux__)
namespace detail {
inline long cpu_topology_value(int cpu, const char* leaf) noexcept {
    char path[96];
    std::snprintf(path, sizeof path, "/sys/devices/system/cpu/cpu%d/topology/%s", cpu, leaf);
    long value = -1;
    if (std::FILE* file = std::fopen(path, "r")) {
        if (std::fscanf(file, "%ld", &value) != 1) {
            value = -1;
        }
        std::fclose(file);
    }
    return value;
}
}  // namespace detail
#endif

// True if the two ids cannot give the threads separate physical cores: they
// are the same id or, on Linux, SMT siblings of one core.
[[nodiscard]] inline bool same_physical_core(int a, int b) noexcept {
    if (a == b) {
        return true;
    }
#if defined(__linux__)
    const long core_a = detail::cpu_topology_value(a, "core_id");
    const long core_b = detail::cpu_topology_value(b, "core_id");
    return core_a >= 0 && core_a == core_b &&
           detail::cpu_topology_value(a, "physical_package_id") ==
               detail::cpu_topology_value(b, "physical_package_id");
#else
    return false;
#endif
}

struct CorePlan {
    int gateway_core{-1};
    int matching_core{-1};
};

// A default placement that puts the two pipeline threads on different
// physical cores. Linux: the two highest-numbered CPUs this process may use
// that are not SMT siblings (high CPUs are the conventional isolcpus range and
// stay clear of CPU 0's interrupt load). Darwin: two distinct affinity tags.
// Returns {-1, -1} if the machine cannot offer two separate cores.
[[nodiscard]] inline CorePlan default_core_plan() noexcept {
#if defined(__linux__)
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    if (::sched_getaffinity(0, sizeof allowed, &allowed) != 0) {
        return {};
    }
    int matching = -1;
    for (int cpu = CPU_SETSIZE - 1; cpu >= 0; --cpu) {
        if (!CPU_ISSET(cpu, &allowed)) {
            continue;
        }
        if (matching < 0) {
            matching = cpu;
        } else if (!same_physical_core(cpu, matching)) {
            return {cpu, matching};
        }
    }
    return {};
#elif defined(__APPLE__)
    return {0, 1};
#else
    return {};
#endif
}

// One-line start-up report (cold path).
inline void log_pin(std::FILE* out, const char* role, const PinResult& r) noexcept {
    switch (r.status) {
        case PinStatus::NotRequested:
            std::fprintf(out, "[affinity] %s: no core requested, thread left unpinned\n", role);
            break;
        case PinStatus::Pinned:
            std::fprintf(out, "[affinity] %s: pinned to cpu %d (verified: mask={%d}, running on cpu %d)\n",
                         role, r.requested, r.requested, r.observed_cpu);
            break;
        case PinStatus::Advisory:
            std::fprintf(out,
                         "[affinity] %s: Mach affinity tag %d applied and read back "
                         "(scheduler hint; Darwin has no hard pinning)%s\n",
                         role, r.requested + 1, r.qos_elevated ? ", QoS=USER_INTERACTIVE" : "");
            break;
        case PinStatus::Unsupported:
            std::fprintf(out,
                         "[affinity] %s: NOT pinned - this platform rejects thread affinity (code %d)%s\n",
                         role, r.error,
                         r.qos_elevated ? "; QoS raised to USER_INTERACTIVE to stay on performance cores" : "");
            break;
        case PinStatus::Failed:
            std::fprintf(out, "[affinity] %s: FAILED to pin to core %d (code %d, running on cpu %d)\n",
                         role, r.requested, r.error, r.observed_cpu);
            break;
    }
}

}  // namespace aether
