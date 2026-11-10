// Cold path: memory reservation. Runs once at start-up, never while trading.
#include "arena.hpp"

#include <sys/mman.h>
#include <unistd.h>

namespace aether {

Region::Region(std::size_t bytes) {
    if (bytes == 0) {
        throw std::invalid_argument("aether::Region size must be > 0");
    }
    const auto page = static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));
    bytes_ = (bytes + page - 1) / page * page;

    int flags = MAP_PRIVATE | MAP_ANON;
#ifdef MAP_POPULATE
    flags |= MAP_POPULATE;
#endif
    void* mem = ::mmap(nullptr, bytes_, PROT_READ | PROT_WRITE, flags, -1, 0);
    if (mem == MAP_FAILED) {
        throw std::bad_alloc();
    }
    data_ = mem;

    // Touch every page now so the hot path never takes a first-touch fault.
    auto* bytes_view = static_cast<volatile unsigned char*>(mem);
    for (std::size_t off = 0; off < bytes_; off += page) {
        bytes_view[off] = 0;
    }
}

Region::~Region() {
    if (data_ != nullptr) {
        ::munmap(data_, bytes_);
    }
}

}  // namespace aether
