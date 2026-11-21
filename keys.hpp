#pragma once
#include <array>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace shop {
using Key = uint64_t;
inline uint64_t mix(uint64_t k) {
    k ^= k >> 30;
    k *= 0xbf58476d1ce4e5b9ULL;
    k ^= k >> 27;
    k *= 0x94d049bb133111ebULL;
    return k ^ (k >> 31);
}

// Common IDs have a reversible representation ordered like the original bytes.
// Intern uncommon strings by exact equality. Neither scheme truncates user data.
class Keys {
    static constexpr std::string_view alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    static constexpr Key marker = Key(1) << 63;
    std::array<unsigned char,256> codes{};
    std::array<uint16_t,65536> pairs{};
    std::unordered_map<std::string,Key> others;
    std::vector<std::string> values;
    mutable std::shared_mutex lock;
    Key intern(std::string_view v) {
        std::string name(v);
        {
            std::shared_lock read(lock);
            auto it = others.find(name);
            if (it != others.end()) return it->second;
        }
        std::unique_lock write(lock);
        auto [it, inserted] = others.emplace(std::move(name), marker | values.size());
        if (inserted) values.push_back(it->first);
        return it->second;
    }
public:
    Keys() {
        for (size_t i=0; i<alphabet.size(); ++i)
            codes[static_cast<unsigned char>(alphabet[i])] = i+1;
        pairs.fill(0x8000);
        for (unsigned char a : alphabet) for (unsigned char b : alphabet) {
            char bytes[2] = {char(a), char(b)};
            uint16_t index;
            std::memcpy(&index, bytes, 2);
            pairs[index] = uint16_t(codes[a] * 64 + codes[b]);
        }
    }
    Key encode(std::string_view v) {
        if (v.size() > 10) return intern(v);
        // Four independent two-byte lookups pack the common widths. The
        // marker bit detects every pair containing an unsupported byte.
        if (v.size() >= 8) {
            auto lookup = [&](size_t pos) -> Key {
                uint16_t bytes;
                std::memcpy(&bytes, v.data() + pos, 2);
                return pairs[bytes];
            };
            Key a = lookup(0), b = lookup(2), c = lookup(4), d = lookup(6);
            if ((a | b | c | d) & 0x8000) return intern(v);
            Key result = (a << 36) | (b << 24) | (c << 12) | d;
            if (v.size() == 8) return result << 12;
            if (v.size() == 10) {
                Key last = lookup(8);
                return last & 0x8000 ? intern(v) : (result << 12) | last;
            }
            Key last = codes[static_cast<unsigned char>(v[8])];
            return last ? (result << 12) | (last << 6) : intern(v);
        }
        Key result = 0;
        for (unsigned char c : v) {
            unsigned char n = codes[c];
            if (!n) return intern(v);
            result = (result << 6) | n;
        }
        return result << (6*(10-v.size()));
    }
    std::string decode(Key k) const {
        if (k & marker) {
            std::shared_lock read(lock);
            return values.at(k ^ marker);
        }
        std::string s;
        for (int shift=54; shift>=0; shift-=6) {
            auto i = (k >> shift) & 63;
            if (!i) break;
            s.push_back(alphabet[i-1]);
        }
        return s;
    }
    bool less(Key a, Key b) const {
        if (!((a|b) & marker)) return a<b;
        return decode(a)<decode(b);
    }
};

// Nonempty IDs never encode to zero. The table has no deletions and freezes
// before parallel reads. Collision resolution compares the complete key.
template<unsigned PartitionBits>
class KeyMap {
    struct Slot { Key k=0, value=0; };
    std::vector<Slot> table;
    size_t used=0;
    size_t slot(Key key) const {
        size_t mask = table.size()-1, i=(mix(key)>>PartitionBits)&mask;
        while (table[i].k && table[i].k!=key) i=(i+1)&mask;
        return i;
    }
    void grow() {
        auto old = std::move(table);
        table.resize(old.empty() ? 32 : old.size()*2);
        for (const auto& s:old) if (s.k) table[slot(s.k)]=s;
    }
public:
    void reserve(size_t estimated_entries) {
        if (!table.empty()) return;
        size_t capacity=32;
        while (estimated_entries>capacity*7/10) capacity*=2;
        table.resize(capacity);
    }
    Key insert_or_get_hashed(Key k,Key proposed,Key hash) {
        if (!k) throw std::logic_error("empty dictionary key");
        if (table.empty() || (used+1)*10>table.size()*7) grow();
        size_t mask=table.size()-1;
        size_t i=(hash>>PartitionBits)&mask;
        while (table[i].k && table[i].k!=k) i=(i+1)&mask;
        auto& s=table[i];
        if (!s.k) { s={k,proposed}; ++used; }
        return s.value;
    }
    Key insert_or_get(Key k,Key proposed) { return insert_or_get_hashed(k,proposed,mix(k)); }
    Key get(Key k, Key fallback) const {
        if (!k || table.empty()) return fallback;
        auto& s=table[slot(k)];
        return s.k ? s.value : fallback;
    }
    void prefetch(uint64_t hash) const {
        if (!table.empty()) __builtin_prefetch(&table[(hash>>PartitionBits)&(table.size()-1)],0,1);
    }
    Key lookup_hashed(Key key,uint64_t hash) const {
        if (table.empty()) return 0;
        const auto mask=table.size()-1;
        for (size_t i=(hash>>PartitionBits)&mask;;i=(i+1)&mask) {
            const auto& cell=table[i];
            if (cell.k==key) return cell.value;
            if (!cell.k) return 0;
        }
    }
    void put(Key k, Key v) {
        if (!k) throw std::logic_error("empty dictionary key");
        if (table.empty() || (used+1)*10>table.size()*7) grow();
        auto& s=table[slot(k)];
        used+=s.k==0;
        s={k,v};
    }
    size_t size() const { return used; }
    template<class F> void visit(F fn) const {
        for (const auto& entry:table) if (entry.k) fn(entry.k,entry.value);
    }
    template<class F> void change_values(F f) {
        for (auto &s:table) if (s.k) s.value=f(s.value);
    }
};
using FlatMap=KeyMap<0>;
}
