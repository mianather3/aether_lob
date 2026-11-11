# syntax=docker/dockerfile:1
#
# AetherLOB - Linux build, verification and runtime image.
#
#   docker build -t aetherlob .                      # GCC 14 + Clang 18 Release builds, full test suite, runtime image
#   docker build --target clang-asan -t aetherlob:asan .   # ASan + UBSan build and test run
#   docker build --target clang-tsan -t aetherlob:tsan .   # TSan build and test run
#   docker run --rm aetherlob                        # latency profile at 100k / 500k / 1M / 5M msg/s
#
# Every build stage compiles with -Wall -Wextra -Wpedantic -Werror and runs
# CTest, so an image that builds is an image whose tests passed on Linux
# (epoll gateway, pthread_setaffinity_np pinning, sysfs core topology).
#
# AETHER_NATIVE_ARCH defaults to OFF here so the image runs on any CPU of the
# target architecture. For numbers that mean something, build on the machine
# that will run it with --build-arg AETHER_NATIVE_ARCH=ON.

# ---- Toolchain + sources -----------------------------------------------------
FROM ubuntu:24.04 AS toolchain
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        g++-14 \
        clang-18 \
        libclang-rt-18-dev \
        llvm-18 \
        cmake \
        ninja-build \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY CMakeLists.txt ./
COPY include/ include/
COPY src/ src/
COPY tests/ tests/
COPY bench/ bench/
ARG AETHER_NATIVE_ARCH=OFF
ENV AETHER_CMAKE_ARGS="-G Ninja -DAETHER_NATIVE_ARCH=${AETHER_NATIVE_ARCH}"

# ---- GCC 14, Release: the binaries that ship ----------------------------------
FROM toolchain AS gcc-release
RUN cmake -S . -B /build ${AETHER_CMAKE_ARGS} -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++-14 \
 && cmake --build /build \
 && ctest --test-dir /build --output-on-failure

# ---- Clang 18, Release ---------------------------------------------------------
FROM toolchain AS clang-release
RUN cmake -S . -B /build ${AETHER_CMAKE_ARGS} -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=clang++-18 \
 && cmake --build /build \
 && ctest --test-dir /build --output-on-failure \
 && touch /build/.verified

# ---- Clang 18, AddressSanitizer + UndefinedBehaviorSanitizer (opt-in target) ---
FROM toolchain AS clang-asan
ENV ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-18
RUN cmake -S . -B /build ${AETHER_CMAKE_ARGS} -DCMAKE_CXX_COMPILER=clang++-18 -DAETHER_SANITIZER=address \
 && cmake --build /build \
 && ctest --test-dir /build --output-on-failure

# ---- Clang 18, ThreadSanitizer (opt-in target) ---------------------------------
FROM toolchain AS clang-tsan
ENV TSAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-18
RUN cmake -S . -B /build ${AETHER_CMAKE_ARGS} -DCMAKE_CXX_COMPILER=clang++-18 -DAETHER_SANITIZER=thread \
 && cmake --build /build \
 && ctest --test-dir /build --output-on-failure

# ---- Runtime: binaries only, no compiler, unprivileged user -------------------
FROM ubuntu:24.04 AS runtime
RUN useradd --system --no-create-home aether
COPY --from=gcc-release /build/aether_* /opt/aetherlob/bin/
# Pulls the Clang stage into the default build, so the image only exists if
# both compilers built the tree warning-free and passed the tests.
COPY --from=clang-release /build/.verified /opt/aetherlob/.clang-verified
ENV PATH=/opt/aetherlob/bin:$PATH
USER aether
CMD ["aether_bench_latency_profile"]
