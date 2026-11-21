#pragma once

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace shop {

struct UserRow {
    size_t person;
    int32_t day;
    int64_t sessions, events, revenue;
    uint32_t category;
};

struct CampaignRow {
    std::string id, channel;
    int32_t day;
    int64_t sessions, purchases, revenue, assists;
};

namespace parquet_detail {

using Bytes = std::string;

inline void le32(Bytes& dest, uint32_t value) {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    dest.append(reinterpret_cast<const char*>(&value), sizeof(value));
#else
    for (unsigned i = 0; i < 4; ++i) dest.push_back(static_cast<char>(value >> (8 * i)));
#endif
}

inline void le64(Bytes& dest, uint64_t value) {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    dest.append(reinterpret_cast<const char*>(&value), sizeof(value));
#else
    for (unsigned i = 0; i < 8; ++i) dest.push_back(static_cast<char>(value >> (8 * i)));
#endif
}

inline void varint(Bytes& dest, uint64_t value) {
    while (value >= 128) {
        dest.push_back(static_cast<char>((value & 127) | 128));
        value >>= 7;
    }
    dest.push_back(static_cast<char>(value));
}

// The footer and page headers use Thrift Compact Protocol. Every nested
// struct starts a fresh field-number delta sequence and ends with STOP.
enum Wire { STOP = 0, I32 = 5, I64 = 6, BINARY = 8, LIST = 9, STRUCT = 12 };
inline void integer(Bytes& dest, int64_t value) {
    varint(dest, (static_cast<uint64_t>(value) << 1) ^ (value < 0 ? ~uint64_t(0) : 0));
}
inline void text(Bytes& dest, const std::string& value) {
    varint(dest, value.size());
    dest.append(value);
}
inline void list(Bytes& dest, size_t count, Wire element_type) {
    dest.push_back(static_cast<char>((std::min<size_t>(15, count) << 4) | element_type));
    if (count >= 15) varint(dest, count);
}

struct Struct {
    Bytes& dest;
    unsigned previous = 0;
    void field(unsigned id, Wire type) {
        dest.push_back(static_cast<char>(((id - previous) << 4) | type));
        previous = id;
    }
    void number(unsigned id, Wire type, int64_t value) {
        field(id, type);
        integer(dest, value);
    }
    void string(unsigned id, const std::string& value) {
        field(id, BINARY);
        text(dest, value);
    }
    void finish() { dest.push_back(STOP); }
};

enum Physical { INT32 = 1, INT64 = 2, BYTE_ARRAY = 6 };
enum Converted { NONE = -1, UTF8 = 0, DATE = 6 };
struct Column { const char* name; Physical type; Converted annotation; bool optional = false; };
struct Chunk { int64_t start, size; };
struct Group { int64_t rows; std::vector<Chunk> chunks; };

inline void schema(Bytes& dest, const Column& column) {
    Struct element{dest};
    element.number(1, I32, column.type);
    element.number(3, I32, column.optional ? 1 : 0);
    element.string(4, column.name);
    if (column.annotation != NONE) element.number(6, I32, column.annotation);
    element.finish();
}

inline Bytes page_header(int64_t rows, size_t bytes) {
    if (bytes > static_cast<size_t>(std::numeric_limits<int32_t>::max()))
        throw std::length_error("Parquet page exceeds INT32");
    Bytes dest;
    Struct page{dest};
    page.number(1, I32, 0); // DATA_PAGE
    page.number(2, I32, bytes);
    page.number(3, I32, bytes);
    page.field(5, STRUCT);
    Struct data{dest};
    data.number(1, I32, rows);
    data.number(2, I32, 0); // PLAIN
    data.number(3, I32, 3); // RLE/bit-packed hybrid definition levels
    data.number(4, I32, 3);
    data.finish();
    page.finish();
    return dest;
}

inline void chunk_metadata(Bytes& dest, const Column& column, const Chunk& chunk, int64_t rows) {
    Struct wrapper{dest};
    wrapper.number(2, I64, chunk.start);
    wrapper.field(3, STRUCT);
    Struct meta{dest};
    meta.number(1, I32, column.type);
    meta.field(2, LIST);
    list(dest, column.optional ? 2 : 1, I32);
    integer(dest, 0);
    if (column.optional) integer(dest, 3);
    meta.field(3, LIST);
    list(dest, 1, BINARY);
    text(dest, column.name);
    meta.number(4, I32, 0); // UNCOMPRESSED
    meta.number(5, I64, rows);
    meta.number(6, I64, chunk.size);
    meta.number(7, I64, chunk.size);
    meta.number(9, I64, chunk.start);
    meta.finish();
    wrapper.finish();
}

template<size_t N>
Bytes footer(const char* table, const Column (&columns)[N], size_t rows, const std::vector<Group>& groups) {
    Bytes dest;
    Struct file{dest};
    file.number(1, I32, 1);
    file.field(2, LIST);
    list(dest, N + 1, STRUCT);
    Struct root{dest};
    root.string(4, table);
    root.number(5, I32, N);
    root.finish();
    for (auto& column : columns) schema(dest, column);
    file.number(3, I64, rows);
    file.field(4, LIST);
    list(dest, groups.size(), STRUCT);
    for (auto& group : groups) {
        Struct rg{dest};
        rg.field(1, LIST);
        list(dest, N, STRUCT);
        int64_t total = 0;
        for (size_t i = 0; i < N; ++i) {
            chunk_metadata(dest, columns[i], group.chunks[i], group.rows);
            total += group.chunks[i].size;
        }
        rg.number(2, I64, total);
        rg.number(3, I64, group.rows);
        rg.finish();
    }
    file.finish();
    return dest;
}

inline void string_value(Bytes& page, const std::string& value) {
    if (value.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max()))
        throw std::length_error("Parquet BYTE_ARRAY exceeds INT32");
    le32(page, static_cast<uint32_t>(value.size()));
    page.append(value);
}

template<size_t N, class Fill>
void write(const std::string& path, const char* table, const Column (&columns)[N],
           size_t count, Fill fill) {
    std::ofstream output;
    output.exceptions(std::ios::badbit | std::ios::failbit);
    output.open(path, std::ios::binary);
    output.write("PAR1", 4);
    int64_t position = 4;
    std::vector<Group> groups;
    Bytes page;
    constexpr size_t group_size = 32768;
    for (size_t first = 0; first < count; first += group_size) {
        const size_t end = std::min(count, first + group_size);
        Group group{static_cast<int64_t>(end - first), {}};
        group.chunks.reserve(N);
        for (size_t i = 0; i < N; ++i) {
            page.clear();
            fill(page, i, first, end);
            auto header = page_header(group.rows, page.size());
            output.write(header.data(), header.size());
            output.write(page.data(), page.size());
            const auto size = static_cast<int64_t>(page.size() + header.size());
            group.chunks.push_back({position, size});
            position += size;
        }
        groups.push_back(std::move(group));
    }
    auto meta = footer(table, columns, count, groups);
    Bytes trailer;
    le32(trailer, static_cast<uint32_t>(meta.size()));
    output.write(meta.data(), meta.size());
    output.write(trailer.data(), trailer.size());
    output.write("PAR1", 4);
    output.close();
}

inline void category_page(Bytes& page, const std::vector<UserRow>& rows, const std::vector<std::string>& categories, size_t first, size_t end) {
    // An optional primitive has a one-bit definition level. Write a single
    // bit-packed run rounded to octets, then PLAIN values for present rows.
    const size_t octets = (end - first + 7) / 8;
    Bytes header;
    varint(header, (octets << 1) | 1);
    le32(page, static_cast<uint32_t>(header.size() + octets));
    page += header;
    const size_t bitmap = page.size();
    page.resize(bitmap + octets, 0);
    for (size_t i = first; i < end; ++i) {
        if (rows[i].category!=std::numeric_limits<uint32_t>::max()) {
            page[bitmap + (i-first)/8] |= static_cast<char>(1u << ((i-first)%8));
            string_value(page, categories[rows[i].category]);
        }
    }
}
} // namespace parquet_detail

inline void write_users(const std::string& file, const std::vector<UserRow>& rows,
                        const std::vector<std::string>& names,const std::vector<std::string>& categories) {
    using namespace parquet_detail;
    Column columns[] = {
        {"user_id", BYTE_ARRAY, UTF8}, {"day", INT32, DATE}, {"sessions", INT64, NONE},
        {"events", INT64, NONE}, {"revenue_cents", INT64, NONE}, {"top_category", BYTE_ARRAY, UTF8, true}
    };
    // The output contract accepts any integer width. Select a narrower page
    // only after checking every value in the partition.
    int64_t maxima[3]={};
    for (const auto& r:rows) {
        maxima[0]=std::max(maxima[0],r.sessions);
        maxima[1]=std::max(maxima[1],r.events);
        maxima[2]=std::max(maxima[2],r.revenue);
    }
    for (size_t i=0;i<3;++i) if (maxima[i]<=std::numeric_limits<int32_t>::max())
        columns[i+2].type=INT32;
    write(file, "user_daily", columns, rows.size(), [&](Bytes& page, size_t col, size_t first, size_t end) {
        switch (col) {
        case 0: for (size_t i=first; i<end; ++i) string_value(page, names[rows[i].person]); break;
        case 1: for (size_t i=first; i<end; ++i) le32(page, rows[i].day); break;
        case 2:
            if (columns[col].type==INT32) for (size_t i=first;i<end;++i) le32(page,rows[i].sessions);
            else for (size_t i=first;i<end;++i) le64(page,rows[i].sessions);
            break;
        case 3:
            if (columns[col].type==INT32) for (size_t i=first;i<end;++i) le32(page,rows[i].events);
            else for (size_t i=first;i<end;++i) le64(page,rows[i].events);
            break;
        case 4:
            if (columns[col].type==INT32) for (size_t i=first;i<end;++i) le32(page,rows[i].revenue);
            else for (size_t i=first;i<end;++i) le64(page,rows[i].revenue);
            break;
        case 5: category_page(page, rows, categories, first, end); break;
        }
    });
}

inline void write_campaigns(const std::string& file, const std::vector<CampaignRow>& rows) {
    using namespace parquet_detail;
    constexpr Column columns[] = {
        {"campaign_id", BYTE_ARRAY, UTF8}, {"channel", BYTE_ARRAY, UTF8}, {"day", INT32, DATE},
        {"sessions", INT64, NONE}, {"purchasing_sessions", INT64, NONE},
        {"revenue_cents", INT64, NONE}, {"assisted_sessions", INT64, NONE}
    };
    write(file, "campaign_daily", columns, rows.size(), [&](Bytes& page, size_t col, size_t first, size_t end) {
        switch (col) {
        case 0: for (size_t i=first; i<end; ++i) string_value(page, rows[i].id); break;
        case 1: for (size_t i=first; i<end; ++i) string_value(page, rows[i].channel); break;
        case 2: for (size_t i=first; i<end; ++i) le32(page, rows[i].day); break;
        case 3: for (size_t i=first; i<end; ++i) le64(page, rows[i].sessions); break;
        case 4: for (size_t i=first; i<end; ++i) le64(page, rows[i].purchases); break;
        case 5: for (size_t i=first; i<end; ++i) le64(page, rows[i].revenue); break;
        case 6: for (size_t i=first; i<end; ++i) le64(page, rows[i].assists); break;
        }
    });
}
} // namespace shop
