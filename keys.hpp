#pragma once
#include <array>
#include <cstdint>
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
    }
    Key encode(std::string_view v) {
        if (v.size() > 10) return intern(v);
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
class FlatMap {
    struct Slot { Key k=0, value=0; };
    std::vector<Slot> table;
    size_t used=0;
    size_t slot(Key key) const {
        size_t mask = table.size()-1, i=mix(key)&mask;
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
    Key insert_or_get(Key k, Key proposed) {
        if (!k) throw std::logic_error("empty dictionary key");
        if (table.empty() || (used+1)*10>table.size()*7) grow();
        auto& s=table[slot(k)];
        if (!s.k) {
            s={k,proposed};
            ++used;
        }
        return s.value;
    }
    Key get(Key k, Key fallback) const {
        if (!k || table.empty()) return fallback;
        auto& s=table[slot(k)];
        return s.k ? s.value : fallback;
    }
    void put(Key k, Key v) {
        if (!k) throw std::logic_error("empty dictionary key");
        if (table.empty() || (used+1)*10>table.size()*7) grow();
        auto& s=table[slot(k)];
        used+=s.k==0;
        s={k,v};
    }
    template<class F> void change_values(F f) {
        for (auto &s:table) if (s.k) s.value=f(s.value);
    }
};
}
