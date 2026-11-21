#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

namespace shop {

// The released dialect permits quoted fields but no embedded CSV delimiters,
// escaped quotes, or embedded LF. A CR stays in the field even before LF.
template<std::size_t N>
inline bool fields(std::string_view line, std::array<std::string_view, N>& out) {
    static_assert(N > 0);
    if (line.empty()) return false;
    for (std::size_t i = 0; i < N; ++i) {
        const std::size_t comma = line.find(',');
        if ((comma == line.npos) != (i + 1 == N)) return false;
        auto value = line.substr(0, comma);
        if (!value.empty() && value.front() == '"') {
            if (value.size() < 2 || value.back() != '"') return false;
            value.remove_prefix(1);
            value.remove_suffix(1);
        }
        if (value.find('"') != value.npos) return false;
        out[i] = value;
        if (comma != line.npos) line.remove_prefix(comma + 1);
    }
    return true;
}

namespace csv_detail {

inline std::optional<std::int64_t> digits(std::string_view text, bool saturate) {
    if (text.empty()) return {};
    constexpr auto limit = std::numeric_limits<std::int64_t>::max();
    std::int64_t value = 0;
    // All strings of up to 18 decimal digits fit a signed 64-bit result.
    if (text.size() <= 18) {
        for (unsigned char c : text) {
            unsigned d = unsigned(c) - '0';
            if (d > 9) return {};
            value = value * 10 + d;
        }
        return value;
    }
    for (unsigned char c : text) {
        unsigned d = unsigned(c) - '0';
        if (d > 9) return {};
        if (value > (limit - d) / 10) {
            if (!saturate) return {};
            value = limit;
        } else {
            value = value * 10 + d;
        }
    }
    return value;
}

inline int two_digits(const char* text) {
    unsigned a = static_cast<unsigned char>(text[0]) - unsigned('0');
    unsigned b = static_cast<unsigned char>(text[1]) - unsigned('0');
    return a < 10 && b < 10 ? int(a * 10 + b) : -1;
}

} // namespace csv_detail

inline std::optional<std::int64_t> number(std::string_view text) {
    return csv_detail::digits(text, false);
}

inline std::optional<std::int64_t> scalar_timestamp(std::string_view text) {
    if ((text.size() != 20 && text.size() != 25) || text[4] != '-') {
        // Unrepresentably large all-digit times are valid but outside any
        // manifest window, which has calendar-form bounds.
        return csv_detail::digits(text, true);
    }
    if (text[7] != '-' || text[10] != 'T' || text[13] != ':' || text[16] != ':') return {};
    const char* s = text.data();
    using csv_detail::two_digits;
    int century = two_digits(s), year_part = two_digits(s + 2);
    int month = two_digits(s + 5), day = two_digits(s + 8);
    int hour = two_digits(s + 11), minute = two_digits(s + 14), second = two_digits(s + 17);
    if (century < 0 || year_part < 0 || month < 1 || month > 12 || day < 1 ||
        hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) return {};
    int year = century * 100 + year_part;
    constexpr int lengths[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    if (day > lengths[month - 1] + (month == 2 && leap)) return {};
    int offset = 0;
    if (text.size() == 20) {
        if (s[19] != 'Z') return {};
    } else {
        if ((s[19] != '+' && s[19] != '-') || s[22] != ':') return {};
        int oh = two_digits(s + 20), om = two_digits(s + 23);
        if (oh < 0 || oh > 23 || om < 0 || om > 59) return {};
        offset = (oh * 60 + om) * (s[19] == '+' ? 1 : -1);
    }
    // Civil date to days since 1970-01-01, using March-based Gregorian eras.
    year -= month <= 2;
    int era = (year >= 0 ? year : year - 399) / 400;
    int within = year - era * 400;
    int march_month = month + (month > 2 ? -3 : 9);
    std::int64_t days = era * 146097 + within * 365 + within / 4 - within / 100
                      + (153 * march_month + 2) / 5 + day - 1 - 719468;
    return ((days * 24 * 60 + hour * 60 + minute - offset) * 60 + second) * 1000;
}

inline std::optional<std::int64_t> timestamp(std::string_view text) {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__ && !defined(SHOP_SCALAR)
    // Cache validated dates only. A hit requires equality of all ten date bytes.
    if (text.size() == 20 && text[10] == 'T' && text[19] == 'Z') {
        struct Date { uint64_t prefix = 0; uint16_t suffix = 0; int64_t midnight = 0; bool valid = false; };
        static thread_local std::array<Date, 64> cache;
        const char* s = text.data();
        uint64_t prefix, clock;
        uint16_t suffix;
        std::memcpy(&prefix, s, 8);
        std::memcpy(&suffix, s + 8, 2);
        std::memcpy(&clock, s + 11, 8);
        auto& date = cache[(unsigned(uint8_t(s[8])) * 10 + uint8_t(s[9])) & 63];
        // XOR with "00:00:00". The colon positions must become zero. At
        // digit positions, x| (x+6) fits the low nibble exactly for 0..9.
        uint64_t x = clock ^ UINT64_C(0x30303a30303a3030);
        if ((x | (x + UINT64_C(0x0606000606000606))) & UINT64_C(0xf0f0fff0f0fff0f0)) return {};
        auto pair = [](uint64_t v) { return unsigned(v & 15) * 10 + unsigned((v >> 8) & 15); };
        unsigned hour = pair(x), minute = pair(x >> 24), second = pair(x >> 48);
        if (hour >= 24 || minute >= 60 || second >= 60) return {};
        int64_t elapsed = ((hour * 60 + minute) * 60 + second) * 1000;
        if (date.valid && date.prefix == prefix && date.suffix == suffix) return date.midnight + elapsed;
        auto result = scalar_timestamp(text);
        if (result) date = {prefix, suffix, *result - elapsed, true};
        return result;
    }
#endif
    return scalar_timestamp(text);
}

// Visit each shard's data lines. A record belongs to the chunk containing its
// first byte. Ordinary rows remain views into the worker's read buffer. Only a
// record crossing a chunk boundary needs a temporary string.
template<class Fn>
void records(const std::filesystem::path& source, std::size_t threads, Fn fn) {
    namespace fs = std::filesystem;
    constexpr std::size_t chunk_bytes = 2U << 20;
    struct Range {
        std::size_t file;
        std::uintmax_t begin, end, size;
    };
    std::vector<fs::path> paths;
    if (fs::is_directory(source)) {
        for (const auto& e : fs::directory_iterator(source)) {
            if (e.path().extension() == ".csv" && e.is_regular_file()) paths.push_back(e.path());
        }
    } else {
        paths.push_back(source);
    }
    std::sort(paths.begin(), paths.end());
    std::vector<Range> jobs;
    for (std::size_t i = 0; i < paths.size(); ++i) {
        const auto size = fs::file_size(paths[i]);
        for (std::uintmax_t start = 0; start < size; start += chunk_bytes) {
            jobs.push_back({i, start, std::min<std::uintmax_t>(size, start + chunk_bytes), size});
        }
    }
    if (jobs.empty()) return;
    const auto workers = std::min(jobs.size(), std::max<std::size_t>(1, threads));
    std::atomic<std::size_t> next{0};
    std::atomic<bool> failed{false};
    std::exception_ptr error;
    std::mutex error_mutex;
    auto run = [&](std::size_t worker) {
        try {
            std::vector<char> buffer(chunk_bytes + 1);
            std::ifstream file;
            std::size_t open_file = paths.size();
            auto emit = [&](std::string_view line) {
                if constexpr (std::is_invocable_v<Fn&, std::string_view, std::size_t>) fn(line, worker);
                else fn(line);
            };
            while (!failed.load(std::memory_order_relaxed)) {
                auto idx = next.fetch_add(1, std::memory_order_relaxed);
                if (idx >= jobs.size()) break;
                auto range = jobs[idx];
                if (open_file != range.file) {
                    if (file.is_open()) file.close();
                    file.open(paths[range.file], std::ios::binary);
                    if (!file) throw std::runtime_error("cannot open " + paths[range.file].string());
                    open_file = range.file;
                }
                auto prefix = std::size_t(range.begin != 0);
                auto count = static_cast<std::size_t>(range.end - range.begin);
                file.clear();
                file.seekg(static_cast<std::streamoff>(range.begin - prefix));
                if (!file.read(buffer.data(), static_cast<std::streamsize>(count + prefix))) {
                    throw std::runtime_error("short read in " + paths[range.file].string());
                }
                std::string_view bytes(buffer.data() + prefix, count);
                if (!prefix || buffer[0] != '\n') {
                    auto end = bytes.find('\n');
                    if (end == bytes.npos) continue;
                    bytes.remove_prefix(end + 1);
                }
                while (!bytes.empty()) {
                    auto end = bytes.find('\n');
                    if (end == bytes.npos) {
                        if (range.end == range.size) {
                            emit(bytes);
                        } else {
                            std::string tail;
                            std::getline(file, tail);
                            if (file.bad()) throw std::runtime_error("read failed in " + paths[range.file].string());
                            std::string line(bytes);
                            line += tail;
                            emit(line);
                        }
                        break;
                    }
                    emit(bytes.substr(0, end));
                    bytes.remove_prefix(end + 1);
                }
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(error_mutex);
            if (!error) error = std::current_exception();
            failed.store(true, std::memory_order_relaxed);
        }
    };
    std::vector<std::thread> pool;
    try {
        for (std::size_t i = 1; i < workers; ++i) pool.emplace_back(run, i);
    } catch (...) {
        failed.store(true);
        for (auto& t : pool) t.join();
        throw;
    }
    run(0);
    for (auto& t : pool) t.join();
    if (error) std::rethrow_exception(error);
}

} // namespace shop
