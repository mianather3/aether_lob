cmake_minimum_required(VERSION 3.20)

project(AetherLOB
    VERSION 0.1.0
    DESCRIPTION "Ultra-low-latency deterministic L3 limit order book and matching engine"
    LANGUAGES CXX)

# ---- Language standard: C++20, no compiler extensions (-std=c++20) ----------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ---- Build type: default to an optimised build ------------------------------
get_property(AETHER_MULTI_CONFIG GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
if(NOT AETHER_MULTI_CONFIG AND NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g -DNDEBUG")

option(AETHER_WARNINGS_AS_ERRORS "Fail the build on any compiler warning" ON)
option(AETHER_NATIVE_ARCH "Tune for the build machine's CPU (-march=native)" ON)
set(AETHER_SANITIZER "none" CACHE STRING "Sanitizer build: none, address (ASan + UBSan) or thread (TSan)")
set_property(CACHE AETHER_SANITIZER PROPERTY STRINGS none address thread)

# ---- Shared compile options --------------------------------------------------
add_library(aether_options INTERFACE)
target_compile_options(aether_options INTERFACE -Wall -Wextra -Wpedantic)
if(AETHER_WARNINGS_AS_ERRORS)
    target_compile_options(aether_options INTERFACE -Werror)
endif()

if(AETHER_NATIVE_ARCH)
    # Some toolchains (notably older arm64 clang) reject -march=native and
    # spell it -mcpu=native instead.
    include(CheckCXXCompilerFlag)
    check_cxx_compiler_flag(-march=native AETHER_HAS_MARCH_NATIVE)
    if(AETHER_HAS_MARCH_NATIVE)
        target_compile_options(aether_options INTERFACE -march=native)
    else()
        check_cxx_compiler_flag(-mcpu=native AETHER_HAS_MCPU_NATIVE)
        if(AETHER_HAS_MCPU_NATIVE)
            target_compile_options(aether_options INTERFACE -mcpu=native)
        endif()
    endif()
endif()

# ASan and TSan cannot be combined, so each gets its own build tree, e.g.
#   cmake -B build-tsan -DAETHER_SANITIZER=thread && cmake --build build-tsan
if(AETHER_SANITIZER STREQUAL "address")
    set(AETHER_SANITIZER_FLAGS -fsanitize=address,undefined -fno-sanitize-recover=undefined)
elseif(AETHER_SANITIZER STREQUAL "thread")
    set(AETHER_SANITIZER_FLAGS -fsanitize=thread)
elseif(NOT AETHER_SANITIZER STREQUAL "none")
    message(FATAL_ERROR "AETHER_SANITIZER must be none, address or thread (got '${AETHER_SANITIZER}')")
endif()
if(AETHER_SANITIZER_FLAGS)
    target_compile_options(aether_options INTERFACE ${AETHER_SANITIZER_FLAGS} -g -fno-omit-frame-pointer)
    target_link_options(aether_options INTERFACE ${AETHER_SANITIZER_FLAGS})
endif()

# ---- Core library ------------------------------------------------------------
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_library(aether_lob STATIC
    src/arena.cpp
    src/order_book.cpp
    src/engine.cpp)
add_library(aether::lob ALIAS aether_lob)
target_include_directories(aether_lob PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(aether_lob PUBLIC aether_options Threads::Threads)

# ---- Tests -------------------------------------------------------------------
include(CTest)
function(aether_add_test name source)
    add_executable(${name} ${source})
    target_link_libraries(${name} PRIVATE aether::lob)
    add_test(NAME ${name} COMMAND ${name})
    # Any sanitizer report fails the test instead of just being printed.
    set_tests_properties(${name} PROPERTIES
        ENVIRONMENT "TSAN_OPTIONS=halt_on_error=1;ASAN_OPTIONS=halt_on_error=1;UBSAN_OPTIONS=halt_on_error=1"
        TIMEOUT 600)
endfunction()

aether_add_test(aether_tests tests/test_order_book.cpp)
aether_add_test(aether_test_spsc_queue tests/test_spsc_queue.cpp)
aether_add_test(aether_test_engine tests/test_engine.cpp)

# ---- Benchmarks --------------------------------------------------------------
add_executable(aether_bench bench/bench_order_book.cpp)
target_link_libraries(aether_bench PRIVATE aether::lob)

add_executable(aether_bench_pipeline bench/bench_pipeline.cpp)
target_link_libraries(aether_bench_pipeline PRIVATE aether::lob)
