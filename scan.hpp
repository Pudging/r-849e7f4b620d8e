#pragma once
#include "csv.hpp"
#include <memory>

#if defined(__linux__) && defined(__SSE2__) && !defined(SHOP_SCALAR)
#include <emmintrin.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#define SHOP_MAPPED_SCAN 1
#endif

namespace shop {
namespace mapped_csv {

#ifdef SHOP_MAPPED_SCAN
struct Mapping {
    const char* data;
    size_t size;
    std::atomic<size_t> remaining{0};
    Mapping(const std::filesystem::path& path, size_t bytes) : size(bytes) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("open failed: " + path.string());
        void* result = mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (result == MAP_FAILED) throw std::runtime_error("mmap failed: " + path.string());
        data = static_cast<const char*>(result);
    }
    void release() {
        if (data) { munmap(const_cast<char*>(data), size); data = nullptr; }
    }
    ~Mapping() { release(); }
    Mapping(const Mapping&) = delete;
};

inline const char* after_line(const char* p, const char* end) {
    auto hit = static_cast<const char*>(std::memchr(p, '\n', end - p));
    return hit ? hit + 1 : end;
}

inline bool delimiter(char c) { return c == ',' || c == '\n'; }

struct Masks { unsigned separators, quotes; };
inline Masks classify(const char* bytes) {
    auto v = _mm_loadu_si128(reinterpret_cast<const __m128i*>(bytes));
    auto commas = _mm_cmpeq_epi8(v, _mm_set1_epi8(','));
    auto lf = _mm_cmpeq_epi8(v, _mm_set1_epi8('\n'));
    auto quotes = _mm_cmpeq_epi8(v, _mm_set1_epi8('"'));
    return {unsigned(_mm_movemask_epi8(_mm_or_si128(commas, lf))),
            unsigned(_mm_movemask_epi8(quotes))};
}

template<size_t N, class F>
void scan(const char* begin, const char* end, F&& emit) {
    const char *row = begin, *field = begin, *p = begin;
    size_t column = 0;
    bool suspect = false;
    std::array<std::string_view, N> values;
    for (; end - p >= 32; p += 32) {
        auto a = classify(p), b = classify(p + 16);
        uint32_t sep = a.separators | (b.separators << 16);
        uint32_t quotes = a.quotes | (b.quotes << 16);
        uint32_t neighbors = (sep << 1) | (sep >> 1);
        if (p == begin || delimiter(p[-1])) neighbors |= 1u;
        if (end - p == 32 || delimiter(p[32])) neighbors |= 1u << 31;
        // An interior quote cannot belong to a legal field. Boundary quotes
        // still need to form a complete pair within that same field.
        uint32_t marks = sep | (quotes & ~neighbors);
        while (marks) {
            unsigned bit = unsigned(__builtin_ctz(marks));
            marks &= marks - 1;
            const char* cut = p + bit;
            if (*cut == '"') { suspect = true; continue; }
            if (column < N) {
                size_t len = cut - field;
                bool opens = len && *field == '"';
                bool closes = len && cut[-1] == '"';
                if (opens != closes || (opens && len < 2)) suspect = true;
                if (opens && closes && len >= 2) values[column] = {field + 1, len - 2};
                else values[column] = {field, len};
                ++column;
            } else {
                suspect = true;
            }
            field = cut + 1;
            if (*cut == '\n') {
                if (suspect) {
                    if (fields(std::string_view(row, cut - row), values)) emit(values);
                } else if (column == N) {
                    emit(values);
                }
                row = field;
                column = 0;
                suspect = false;
            }
        }
    }
    // The scalar parser handles each remaining complete or unterminated row.
    while (row < end) {
        auto next = after_line(row, end);
        auto stop = next - (next[-1] == '\n');
        if (fields(std::string_view(row, stop - row), values)) emit(values);
        row = next;
    }
}
#endif
} // namespace mapped_csv

template<size_t N, class F>
void parsed_records(const std::filesystem::path& dir, size_t threads, F fn) {
#ifdef SHOP_MAPPED_SCAN
    using namespace mapped_csv;
    std::vector<std::filesystem::path> paths;
    for (const auto& e : std::filesystem::directory_iterator(dir))
        if (e.is_regular_file() && e.path().extension() == ".csv") paths.push_back(e.path());
    std::sort(paths.begin(), paths.end());
    std::vector<std::unique_ptr<Mapping>> maps;
    struct Range { const char *begin, *end; Mapping* file; };
    std::vector<Range> jobs;
    constexpr size_t chunk = 2u << 20;
    for (const auto& path : paths) {
        size_t size = std::filesystem::file_size(path);
        if (!size) continue;
        maps.push_back(std::make_unique<Mapping>(path, size));
        const char* start = maps.back()->data;
        const char* end = start + size;
        start = after_line(start, end);
        while (start < end) {
            const char* next = size_t(end - start) <= chunk ? end : after_line(start + chunk, end);
            jobs.push_back({start, next, maps.back().get()});
            ++maps.back()->remaining;
            start = next;
        }
    }
    if (jobs.empty()) return;
    std::atomic<size_t> cursor{0};
    std::exception_ptr error;
    std::mutex lock;
    auto work = [&](size_t worker) {
        try {
            for (;;) {
                size_t i = cursor.fetch_add(1, std::memory_order_relaxed);
                if (i >= jobs.size()) break;
                auto [begin, end, file] = jobs[i];
                scan<N>(begin, end, [&](const auto& values) { fn(values, worker); });
                if (file->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) file->release();
            }
        } catch (...) {
            std::lock_guard guard(lock);
            if (!error) error = std::current_exception();
            cursor.store(jobs.size());
        }
    };
    std::vector<std::thread> pool;
    try {
        for (size_t i = 1; i < std::min(threads, jobs.size()); ++i) pool.emplace_back(work, i);
    } catch (...) {
        cursor.store(jobs.size());
        for (auto& t : pool) t.join();
        throw;
    }
    work(0);
    for (auto& t : pool) t.join();
    if (error) std::rethrow_exception(error);
#else
    records(dir, threads, [&](std::string_view line, size_t worker) {
        std::array<std::string_view, N> values;
        if (fields(line, values)) fn(values, worker);
    });
#endif
}
} // namespace shop
