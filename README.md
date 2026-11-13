# AetherLOB

A deterministic L3 limit order book and matching engine in C++20, built for the
nanosecond regime: price-time priority, a binary wire protocol, a non-blocking
UDP/TCP gateway, a lock-free hand-off to a single matching thread, and no
allocation, lock or system call on the path between a packet arriving and its
last fill.

| | |
|---|---|
| **Median order-processing** | **125 ns** (packet received → last fill done, through the queue) |
| **p99 order-processing** | **~375 ns** at 1M msg/s (median of 12 runs) |
| **Peak throughput** | **~20–21.6M msg/s** end to end (wire bytes → decode → queue → match) |
| **Matching core alone** | ~25 ns per command, ~20 ns (~85 cycles) per fill |
| **Hot-path allocations** | none, by construction: every byte is reserved and pre-faulted at start-up |
| **Verification** | 6 test suites clean under ASan + UBSan and TSan on macOS and Linux; 13M-payload differential fuzz against an independent reference engine, zero divergences |

Numbers are from an Apple M5 laptop, unpinned (macOS has no hard thread
affinity). [How they were measured, and what they do not show](#performance).

## Architecture

```
            UDP datagrams                 TCP streams
                  │                            │
                  ▼                            ▼
      ┌────────────────────────────────────────────────────┐
      │  NetworkGateway                    gateway thread  │
      │  epoll (Linux) · kqueue (macOS/BSD) · poll         │
      │  O_NONBLOCK sockets, pre-allocated receive buffers │
      │  UDP: one datagram = one frame                     │
      │  TCP: in-place reassembly of split messages        │
      │  decode 'A' add / 'X' cancel / 'M' market          │
      │  backpressure: stop reading, never drop            │
      └──────────────────────────┬─────────────────────────┘
                                 │ Command (trivially copyable, TSC-stamped)
                                 ▼
      ┌────────────────────────────────────────────────────┐
      │  SpscQueue<Command>         lock-free, wait-free   │
      │  producer line │ consumer line │ slots (own pages) │
      └──────────────────────────┬─────────────────────────┘
                                 │
                                 ▼
      ┌────────────────────────────────────────────────────┐
      │  Matching engine                  matching thread  │
      │  sole owner of the OrderBook: no locks on the book │
      │  price-time priority, O(1) cancel, O(1) per fill   │
      └──────┬───────────────────┬───────────────────┬─────┘
             │                   │                   │
             ▼                   ▼                   ▼
      ┌──────────────┐   ┌───────────────┐   ┌────────────────┐
      │ Arena        │   │ L2 snapshots  │   │ Telemetry      │
      │ Pool<Order>  │   │ double buffer │   │ single-writer  │
      │ Pool<Level>  │   │ flipped by    │   │ counters and   │
      │ FlatMap ×3   │   │ the matcher,  │   │ log-linear     │
      │ pre-faulted  │   │ read by any   │   │ latency        │
      │ mmap regions │   │ exporter      │   │ histograms     │
      └──────────────┘   └───────────────┘   └────────────────┘
```

Two threads, one queue. The gateway thread turns bytes into `Command`s; the
matching thread applies them. They share the ring buffer and a few atomic
counters and nothing else, so the order book itself needs no synchronisation
at all.

| Component | File | What it does |
|---|---|---|
| Wire protocol | `include/protocol.hpp` | ITCH-style fixed-length messages keyed by a type byte; endian-independent, alignment-free decode |
| Gateway | `src/network_gateway.cpp` | Non-blocking UDP + TCP ingestion over epoll / kqueue / poll, per-connection stream reassembly |
| Queue | `include/spsc_queue.hpp` | Single-producer / single-consumer ring buffer |
| Engine | `src/engine.cpp` | Thread lifecycle, core pinning, backpressure, the matching loop |
| Order book | `include/order_book.hpp` | L3 book: intrusive FIFO per price level, intrusive sorted level list per side |
| Arena | `include/arena.hpp`, `include/flat_map.hpp` | Pre-faulted object pools and fixed-capacity hash indices |
| Snapshots | `src/market_snapshot.cpp` | L2 depth for exporter threads without locking the book |
| Telemetry | `include/telemetry.hpp`, `include/tsc_timer.hpp` | Hardware-counter timestamps, wait-free histograms |
| Affinity | `include/affinity.hpp` | `pthread_setaffinity_np` pinning with read-back verification; SMT-sibling detection from sysfs |

## Performance

### Method

`aether_bench_latency_profile` replays a synthetic market through the full
pipeline — wire bytes, gateway decode, SPSC queue, matching thread — with
**open-loop Poisson arrivals** at 100k, 500k, 1M and 5M messages per second.
Arrival times are fixed in advance, so a slow engine cannot slow the producer
down and hide its own latency (no coordinated omission); the producer's
lateness against its schedule is reported alongside.

The replay is 4,000,000 messages on top of 40,000 seeded resting orders:

| Flow | Share | |
|---|---|---|
| Resting limit adds | 55% | 70% within 10 ticks of the touch, 30% up to 200 ticks deep |
| Cancels | 25% | of orders that are actually resting; ~8% are deliberately stale (cancel loses the race with a fill → reject) |
| Aggressive | 20% | 15% marketable limits crossing 0–2 ticks, 5% multi-level market sweeps |

The generator runs its own copy of the book so it prices off the real touch,
and sizes taker flow in proportion to resting depth. The book therefore stays
deep for the whole run instead of draining: **38,000–40,600 resting orders
throughout, never fewer than 197 price levels on the thinner side, never fewer
than 10,000 orders within 50 ticks of the touch.** Those bounds are sampled
every 1,024 messages and the benchmark aborts if they are breached. Roughly one
message in five trades, at 2.6 fills per trade, with a third thread pulling
live L2 snapshots every 10 ms.

### Results

Apple M5, macOS 26.5, Apple clang 21, `-O3 -march=native`. One representative
run; latencies in nanoseconds.

**Order-Processing Latency** — packet received by the gateway → matching complete, for
commands that traded:

| Rate (msg/s) | p50 | p90 | p99 | p99.9 | p99.99 | max |
|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | 125 | 167 | 927 | 8,447 | 20,479 | 31,352 |
| 500,000 | 125 | 167 | 375 | 3,519 | 9,983 | 15,000 |
| 1,000,000 | 125 | 167 | 335 | 3,583 | 10,239 | 13,333 |
| 5,000,000 | 125 | 211 | 503 | 33,791 | 96,255 | 100,760 |

**Match service** — dequeued by the matching thread → matching complete, all
commands:

| Rate (msg/s) | p50 | p90 | p99 | p99.9 | p99.99 | max |
|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | 41 | 85 | 751 | 1,311 | 9,471 | 19,209 |
| 500,000 | 41 | 83 | 211 | 671 | 3,711 | 14,959 |
| 1,000,000 | 41 | 83 | 167 | 719 | 3,647 | 13,125 |
| 5,000,000 | 0 | 83 | 125 | 335 | 1,375 | 13,309 |

**Throughput and core cost**

| | |
|---|---|
| End-to-end saturation (`aether_bench_pipeline`, unthrottled, timestamps off) | 19.7–21.6M msg/s over 6 runs |
| Matching core alone, single thread, same replay | 24–27 ns per command |
| Per matched fill | 19–21 ns ≈ 85 cycles |
| Instructions per command | ~140, IPC ~1.3 |
| Messages dropped / backpressure waits at 5M msg/s | 0 / 0 |

### Reading these numbers honestly

- **Timer resolution.** Apple Silicon's counter advances at 24 MHz, one tick
  per 41.67 ns. Every figure above is a multiple of that: 125 ns is three
  ticks, 41 ns is one, and a p50 of 0 means "under one tick". On x86-64 the
  same code reads the TSC (`lfence; rdtsc` … `rdtscp; lfence`) at sub-nanosecond
  resolution.
- **The median is stable; the tail is the operating system.** p50 was 125 ns in
  every one of 12 runs at every rate. p99 at 1M msg/s was 335–591 ns in ten of
  them (median 375 ns); the other two read 799 ns and 7.7 µs because the
  scheduler preempted a pipeline thread. macOS offers no hard affinity and no
  core isolation, so the p99.9+ columns measure the laptop, not the engine.
  p99 is consistently worst at the *lowest* rate, 100k msg/s, which points the
  same way: most likely the core is allowed to idle down between messages.
- **What "Order-Processing Latency" covers.** Bytes handed to the gateway's decoder →
  last fill applied. It includes decode, the queue hop between two cores and
  matching. It excludes the kernel network stack and NIC, and there is no
  outbound order-entry leg.
- **Linux numbers are not claimed.** The Linux build is verified for
  correctness in a container (below), where the pipeline threads are pinned
  and verified, but a Docker Desktop VM is not a latency environment. On
  bare-metal Linux with `isolcpus`, run
  `perf stat -e cycles,instructions,cache-misses,branch-misses ./aether_bench_latency_profile`.

## Memory layout and mechanical sympathy

**Nothing is allocated after start-up.** `Region` (`src/arena.cpp`) maps
anonymous memory once, zeroed and pre-faulted (`MAP_POPULATE` on Linux), so the
hot path never takes a page fault. `Pool<T>` carves fixed-size nodes out of a
region and hands them back from a LIFO free stack, so the node reused next is
the one most recently touched and still in cache. The order-id and price
indices are `FlatMap`s: open addressing, linear probing at ≤ 50% load,
backward-shift deletion, no tombstones and no rehash, ever. Acquire and release
are a handful of instructions with no `malloc`, no lock and no system call.
When a pool is full the order is rejected with `PoolExhausted` before anything
is modified.

**One cache line per node.** `Order` and `PriceLevel` are `alignas(64)` and
`static_assert`ed to be exactly 64 bytes. Touching an order costs one line, and
two orders never share one, so there is no false sharing between neighbours and
no node straddles a line boundary.

**Intrusive doubly linked lists.** An `Order` carries its own `prev`/`next`
pointers and is itself the node in its price level's FIFO queue; a
`PriceLevel` is itself the node in its side's best-first level list. There is
no separate list-node allocation and no extra pointer chase to reach the
payload. Given the `Order*` from the id index, a cancel unlinks in O(1) with no
traversal; a fill pops the front of the best level in O(1). Adding at an
existing price is O(1); a new price walks only the levels better than it.

**Producer and consumer never write the same line.** The ring buffer has three
`alignas(64)` blocks, each `static_assert`ed to one line:

```
Shared    capacity, mask, slots*          written once, read by both
Producer  tail (atomic), tail_local, head_cache
Consumer  head (atomic), head_local, tail_cache
```

Each side keeps a private copy of its own index and a *cached* copy of the
other side's. The producer re-reads the consumer's `head` only when its cache
says the queue looks full; the consumer re-reads `tail` only when it looks
empty. In steady state a push is one atomic store and a pop is one atomic
store, and neither thread pulls the other's line into its cache. The slots live
in their own page-aligned mapping. The engine's counters and the telemetry
blocks follow the same rule: every block is cache-aligned and has exactly one
writer.

**Acquire/release, nothing stronger and nothing weaker.** The producer writes
the slot and then publishes it with a `memory_order_release` store of `tail`;
the consumer's `memory_order_acquire` load of `tail` is what makes the slot's
contents visible to it. The read index is handed back the same way. No
operation uses `seq_cst`, so there is no full barrier or `lock`-prefixed
instruction on x86, where a release store is a plain `mov`. No operation uses
`relaxed`, so there is no ordering argument to get wrong. Both operations are
wait-free: no CAS, no retry loop. ThreadSanitizer checks this on every run.

**Reading the book without locking it.** An exporter that wants L2 depth
raises an atomic request. The matching thread sees it on an idle spin (or at
most 256 commands later under load), copies the top N levels into the spare
half of a pre-allocated double buffer and flips the front index. Its total
cost is one atomic load per idle spin; it never waits for a reader.

**Time without system calls.** Timestamps come straight from the hardware
counter (TSC on x86-64, `cntvct_el0` on AArch64) and are converted with a
fixed-point multiply and shift. Histograms are log-linear with ≤ 3.125%
relative error, single-writer, and recorded with one load and one store.

## Verification

| Build | Platform | Compiler | Result |
|---|---|---|---|
| Release | macOS 26.5 arm64 | Apple clang 21 | 6/6 |
| Debug | macOS 26.5 arm64 | Apple clang 21 | 6/6 |
| ASan + UBSan | macOS 26.5 arm64 | Apple clang 21 | 6/6, no reports |
| TSan | macOS 26.5 arm64 | Apple clang 21 | 6/6, no reports |
| Release | Linux arm64 (ubuntu:24.04) | GCC 14.2 | 6/6 |
| Release | Linux arm64 (ubuntu:24.04) | Clang 18.1 | 6/6 |
| ASan + UBSan | Linux arm64 (ubuntu:24.04) | Clang 18.1 | 6/6, no reports |
| TSan | Linux arm64 (ubuntu:24.04) | Clang 18.1 | 6/6, no reports |
| Release | Linux x86-64 (ubuntu:24.04, emulated) | GCC 14.2 | 6/6 |

Every build uses `-Wall -Wextra -Wpedantic -Werror`. Sanitizer runs use
`halt_on_error=1` and `-fno-sanitize-recover`, so any report fails the test.

The six suites: order book semantics and invariants; SPSC queue under
two-thread stress; engine pipeline and backpressure; network gateway (UDP
framing, TCP reassembly across fragmented reads, connection limits, both
readiness backends, affinity); telemetry, timer and snapshots; and the
differential fuzzer.

On Linux the gateway test runs on the epoll backend, and the affinity test
confirms the hard pin by reading the mask back and checking `sched_getcpu()`:

```
(backend=epoll udp_port=60686 tcp_port=45597 SO_RCVBUF udp=2097152 tcp=2097152)
[affinity] matching-engine thread: pinned to cpu 9 (verified: mask={9}, running on cpu 9)
[affinity] network-gateway thread: pinned to cpu 8 (verified: mask={8}, running on cpu 8)
```

### Differential fuzzing

`aether_fuzz_pipeline` feeds every payload both to the real pipeline (gateway
→ queue → matching thread) and to a **reference engine that shares no code
with it**: its own byte parser, its own framing rules and a naive `std::map`
book written from the protocol spec. Payloads start as valid order flow and
are then truncated, bit-flipped, scrambled, overwritten, shifted by inserted
or deleted bytes, or replaced with noise. After each scenario the two must
agree exactly on every command outcome, every trade, the full L3 book in
priority order, and the gateway's error counters.

A 13M-payload campaign (Release build, seed `0xAE7E4B0B`, 85 s):

| | |
|---|---|
| Payloads | 13,001,615 in 7,181 scenarios; 3,898,777 corrupted |
| Commands verified | 390,153,561 (224,304,618 of them rejects) |
| Trades compared | 109,425,723 |
| Over real loopback sockets | 729 scenarios, epoll/kqueue and poll alternating |
| Queue saturation | 1,434 / 1,434 burst scenarios drove the queue 100% full; 47.5M backpressure events, no command lost |
| Divergences | **0** |

CTest runs the same fuzzer at 1M payloads (250k under the sanitizers) on every
build in the table above. Inputs are a pure function of the seed, so any
failure reproduces.

## Build and run

Requires CMake ≥ 3.20 and a C++20 compiler (GCC 14, Clang 18 and Apple clang
21 are tested). No third-party dependencies.

```sh
# Release (the default build type): -O3, tuned for the build machine
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j
ctest --test-dir build-release --output-on-failure

# Debug
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j && ctest --test-dir build-debug --output-on-failure

# AddressSanitizer + UndefinedBehaviorSanitizer
cmake -S . -B build-address -DAETHER_SANITIZER=address
cmake --build build-address -j && ctest --test-dir build-address --output-on-failure

# ThreadSanitizer (cannot be combined with ASan, hence its own tree)
cmake -S . -B build-thread -DAETHER_SANITIZER=thread
cmake --build build-thread -j && ctest --test-dir build-thread --output-on-failure
```

| CMake option | Default | |
|---|---|---|
| `AETHER_SANITIZER` | `none` | `address` (ASan + UBSan) or `thread` (TSan) |
| `AETHER_NATIVE_ARCH` | `ON` | `-march=native` / `-mcpu=native` |
| `AETHER_WARNINGS_AS_ERRORS` | `ON` | `-Werror` |
| `AETHER_FUZZ_PAYLOADS` | 1,000,000 (250,000 sanitized) | payloads in the CTest fuzz run |

### Benchmarks

```sh
./build-release/aether_bench_latency_profile                  # 100k, 500k, 1M, 5M msg/s
./build-release/aether_bench_latency_profile --json out.json 2000000   # one rate, machine-readable
./build-release/aether_bench_pipeline                         # Poisson run + saturation throughput
./build-release/aether_bench                                  # order book alone
./build-release/aether_fuzz_pipeline 13000000                 # long fuzz campaign: [payloads] [seed]
```

### Docker (Linux)

A multi-stage `ubuntu:24.04` image. Each build stage compiles with `-Werror`
and runs CTest, so the image only exists if the tests passed on Linux.

```sh
docker build -t aetherlob .                             # GCC 14 + Clang 18 Release, tests, runtime image
docker build --target clang-asan -t aetherlob:asan .    # ASan + UBSan build and test run
docker build --target clang-tsan -t aetherlob:tsan .    # TSan build and test run
docker build --platform linux/amd64 --target gcc-release -t aetherlob:amd64 .   # x86-64 build + tests (TSC timer path)

docker run --rm aetherlob                               # latency profile
docker run --rm aetherlob aether_test_network_gateway   # epoll + affinity checks
```

The runtime stage holds only the binaries and runs as an unprivileged user.
The image is built with `AETHER_NATIVE_ARCH=OFF` so it runs on any CPU of its
architecture; pass `--build-arg AETHER_NATIVE_ARCH=ON` when building on the
machine that will run it.

## Scope

AetherLOB is a single-instrument matching core and its ingestion path. It has
no persistence or recovery, no outbound market-data or execution-report feed,
no order types beyond limit and market, no session or risk layer, and no
kernel-bypass networking. Sockets go through the kernel; the measurements
above begin after the kernel hands the bytes over.
