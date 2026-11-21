#include "csv.hpp"
#include "scan.hpp"
#include "keys.hpp"
#include "parquet.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <regex>
#include <thread>

namespace fs = std::filesystem;
namespace shop {
constexpr size_t partitions = 64;
constexpr int64_t day_ms=86'400'000, week_ms=7*day_ms;
constexpr int64_t invalid_time=std::numeric_limits<int64_t>::min();
using Index=uint32_t;
constexpr size_t part(Key k) { return k % partitions; }
inline size_t bucket(Key k) { return part(mix(k)); }
template<class T> using Bins=std::array<std::vector<T>,partitions>;

template<class Fn> void parallel(size_t count,size_t threads, Fn fn) {
    std::atomic<size_t> cursor{0};
    std::exception_ptr error;
    std::mutex lock;
    auto work=[&] {
        try {
            for (size_t i;(i=cursor.fetch_add(1))<count;) fn(i);
        } catch (...) {
            std::lock_guard guard(lock);
            error=std::current_exception();
            cursor.store(count);
        }
    };
    std::vector<std::thread> group;
    for (size_t i=1;i<std::min(threads,count);++i) group.emplace_back(work);
    work();
    for (auto& t:group) t.join();
    if (error) std::rethrow_exception(error);
}

// Group by a byte from the middle of the reversible key before comparison
// sorting. Fixed prefixes and zero padding are frequent at the ends of IDs.
inline size_t stripe(Key person) { return (person >> 18) & 255; }
inline bool person_before(Key a,Key b) {
    return stripe(a)==stripe(b) ? a<b : stripe(a)<stripe(b);
}

template<class T,class Groups,class Less>
std::vector<T> sorted_partition(Groups& groups,size_t p,Less less) {
    std::array<size_t,256> ends{},cursor{};
    for (const auto& g:groups) for (const T& row:g[p]) ++ends[stripe(row.person)];
    size_t total=0;
    for (size_t i=0;i<ends.size();++i) {
        cursor[i]=total;
        total+=ends[i];
        ends[i]=total;
    }
    std::vector<T> result(total);
    for (auto& g:groups) {
        auto &source=g[p];
        for (const T& row:source) result[cursor[stripe(row.person)]++]=row;
        std::vector<T>().swap(source);
    }
    size_t begin=0;
    for (size_t end:ends) {
        std::sort(result.begin()+begin,result.begin()+end,less);
        begin=end;
    }
    return result;
}

struct Catalog {
    FlatMap products, campaign_index;
    std::vector<Key> categories;
    std::vector<std::pair<Key,Key>> campaigns;
    int64_t start, end;
    Catalog(const fs::path& input, Keys& keys) {
        std::ifstream manifest(input/"manifest.json");
        std::string json((std::istreambuf_iterator<char>(manifest)),{});
        auto bound=[&](const char* name) {
            std::regex field(std::string("\"")+name+"\"\\s*:\\s*\"([^\"]+)\"");
            std::smatch m;
            if (!std::regex_search(json,m,field)) throw std::runtime_error("missing manifest window");
            return timestamp(m[1].str()).value();
        };
        start=bound("window_start");
        end=bound("window_end");
        Key organic=keys.encode("organic");
        campaigns.emplace_back(organic,organic);
        records(input/"campaigns.csv",1,[&](std::string_view line){
            std::array<std::string_view,2> v;
            if (!fields(line,v)) return;
            Key id=keys.encode(v[0]);
            campaign_index.put(id,campaigns.size());
            campaigns.emplace_back(id, keys.encode(v[1]));
        });
        categories.push_back(keys.encode("unknown"));
        std::unordered_map<Key,Index> category_ids;
        records(input/"products.csv",1,[&](std::string_view line){
            std::array<std::string_view,2> v;
            if (!fields(line,v)) return;
            Key category=keys.encode(v[1]);
            auto [it,fresh]=category_ids.emplace(category,categories.size());
            if (fresh) categories.push_back(category);
            products.put(keys.encode(v[0]),it->second);
        });
    }
};

FlatMap identities(const fs::path& dir,size_t threads,Keys& keys) {
    FlatMap index;
    std::vector<Index> parent,sizes;
    std::vector<Key> names;
    std::vector<std::vector<std::pair<Key,Key>>> edges(threads);
    parsed_records<2>(dir,threads,[&](const auto& v,size_t worker) {
        if (v[0].empty() || v[1].empty() || v[0]==v[1]) return;
        edges[worker].emplace_back(keys.encode(v[0]),keys.encode(v[1]));
    });
    size_t edge_count=0;
    for (const auto& batch:edges) edge_count+=batch.size();
    // Size hints reduce reallocation. They never bound the number of names.
    size_t hint=edge_count+edge_count/2;
    index.reserve(hint);
    parent.reserve(hint); sizes.reserve(hint); names.reserve(hint);
    auto find=[&](Index i) {
        while (parent[i]!=i) { parent[i]=parent[parent[i]]; i=parent[i]; }
        return i;
    };
    auto add=[&](Key k) -> Index {
        Index next=parent.size();
        Index i=index.insert_or_get(k,next);
        if (i==next) {
            parent.push_back(i);
            sizes.push_back(1);
            names.push_back(k);
        }
        return find(i);
    };
    for (auto &batch:edges) for (auto [left,right]:batch) {
        Index a=add(left), b=add(right);
        if (a==b) continue;
        if (sizes[a]<sizes[b]) std::swap(a,b);
        parent[b]=a; sizes[a]+=sizes[b];
        if (keys.less(names[b],names[a])) names[a]=names[b];
    }
    index.change_values([&](Key i){return names[find(i)];});
    return index;
}

struct Event { Key person=0; int64_t time=invalid_time; uint32_t price=0, category=0; };
struct Raw { Key id; int64_t ingest; Event e; };
struct Click { Key person, id; int64_t time; Index campaign; };
using EventGroups=std::array<Bins<Event>,partitions>;

std::vector<Bins<Raw>> parse_events(const fs::path& dir,size_t workers,const Catalog& cat, Keys& keys) {
    std::vector<Bins<Raw>> raw(workers);
    parsed_records<7>(dir,workers,[&](const auto& v,size_t worker){
        if (v[0].empty()) return;
        auto ingest=timestamp(v[3]);
        if (!ingest) return;
        Raw row{keys.encode(v[0]),*ingest,{}};
        auto time=timestamp(v[2]);
        if (!v[1].empty() && !v[5].empty() && time && *time>=cat.start && *time<cat.end
                && *time>=*ingest-day_ms) {
            auto amount = v[4]=="purchase" ? number(v[6]) : std::nullopt;
            if (v[4]=="view" || v[4]=="cart" || (amount && *amount>=1 && *amount<=10'000'000)) {
                row.e.person=keys.encode(v[1]); row.e.time=*time;
                if (amount) {
                    row.e.price=*amount;
                    row.e.category=cat.products.get(keys.encode(v[5]),0);
                }
            }
        }
        // Retain the ID and arrival even if the event is invalid. An early
        // invalid event must suppress its later valid duplicates.
        raw[worker][bucket(row.id)].push_back(row);
    });
    return raw;
}

EventGroups deduplicate(std::vector<Bins<Raw>>& raw,size_t threads,const FlatMap& canon) {
    EventGroups clean;
    parallel(partitions,threads,[&](size_t p){
        size_t n=0;
        for (const auto& batch:raw) n+=batch[p].size();
        size_t words=1;
        while (words<n/4) words*=2;
        std::vector<uint64_t> seen(words),repeated(words);
        // Low six hash bits already select this partition. The next bits
        // only identify possible repeats; collisions always get exact dedup.
        auto fingerprint=[&](Key id) {
            uint64_t h=mix(id)>>6;
            return std::pair<size_t,uint64_t>{(h>>6)&(words-1),uint64_t(1)<<(h&63)};
        };
        for (auto &batch:raw) for (const auto &r:batch[p]) {
            auto [word,bit]=fingerprint(r.id);
            repeated[word]|=seen[word]&bit;
            seen[word]|=bit;
        }
        auto emit=[&](Event e) {
            if (e.time==invalid_time) return;
            e.person=canon.get(e.person,e.person);
            clean[p][bucket(e.person)].push_back(e);
        };
        std::vector<Raw> exact;
        for (auto &batch:raw) {
            auto& rows=batch[p];
            for (const auto& r:rows) {
                auto [word,bit]=fingerprint(r.id);
                if (repeated[word]&bit) exact.push_back(r);
                else emit(r.e);
            }
            std::vector<Raw>().swap(rows);
        }
        std::sort(exact.begin(),exact.end(),[](auto& a,auto& b){return a.id<b.id;});
        for (size_t i=0;i<exact.size();) {
            size_t next=i+1,best=i;
            while (next<exact.size() && exact[next].id==exact[i].id) {
                if (exact[next].ingest<exact[best].ingest) best=next;
                ++next;
            }
            emit(exact[best].e);
            i=next;
        }
    });
    return clean;
}

std::vector<Bins<Click>> read_clicks(const fs::path& dir,size_t threads,const Catalog& cat, Keys& keys,const FlatMap& canon) {
    std::vector<Bins<Click>> result(threads);
    parsed_records<4>(dir,threads,[&](const auto& v,size_t worker){
        if (v[0].empty() || v[1].empty()) return;
        auto time=timestamp(v[3]);
        if (!time || *time<cat.start-week_ms || *time>=cat.end) return;
        Index campaign=cat.campaign_index.get(keys.encode(v[2]),0);
        if (!campaign) return;
        Key user=keys.encode(v[1]), person=canon.get(user,user);
        result[worker][bucket(person)].push_back({person,keys.encode(v[0]),*time,campaign});
    });
    return result;
}

using Totals=std::array<int64_t,4>;
using CampaignCounts=std::unordered_map<uint64_t,Totals>;
inline uint64_t campaign_day(uint32_t c, int32_t date) {
    return (uint64_t(c)<<32)|uint32_t(date);
}

void user_days(const std::vector<Event>& es,size_t first,size_t stop,
               const std::vector<Click>& cs,size_t left,size_t click_end,
               const Catalog& cat,const Keys& keys,CampaignCounts& counts,std::vector<UserRow>& rows) {
    std::string name=keys.decode(es[first].person);
    size_t right=left;
    std::unordered_map<Index,size_t> active;
    std::vector<std::pair<Index,int64_t>> sums;
    UserRow day{};
    auto finish=[&] {
        if (!day.sessions) return;
        if (!sums.empty()) {
            auto best=std::max_element(sums.begin(),sums.end(),[&](auto& a,auto& b) {
                return a.second!=b.second ? a.second<b.second : keys.less(cat.categories[b.first],cat.categories[a.first]);
            });
            day.category=keys.decode(cat.categories[best->first]);
        }
        rows.push_back(std::move(day));
        day={};
        sums.clear();
    };
    while (first<stop) {
        int64_t start=es[first].time;
        int32_t date=start/day_ms-(start%day_ms<0);
        if (day.sessions && day.day!=date) finish();
        if (!day.sessions) {day.user_id=name; day.day=date;}
        while (right<click_end && cs[right].time<=start) ++active[cs[right++].campaign];
        while (left<right && cs[left].time<start-week_ms) {
            auto it=active.find(cs[left++].campaign);
            if (!--it->second) active.erase(it);
        }
        Index credit=left==right ? 0 : cs[right-1].campaign;
        size_t next=first;
        int64_t revenue=0;
        do {
            auto &e=es[next++];
            if (e.price) {
                revenue+=e.price;
                auto it=std::find_if(sums.begin(),sums.end(),[&](auto& x){return x.first==e.category;});
                if (it==sums.end()) sums.emplace_back(e.category,e.price);
                else it->second+=e.price;
            }
        } while (next<stop && es[next].time-es[next-1].time<=1'800'000);
        ++day.sessions; day.events+=next-first; day.revenue+=revenue;
        auto &v=counts[campaign_day(credit,date)];
        ++v[0]; v[1]+=revenue>0; v[2]+=revenue;
        for (auto [campaign,n]:active) ++counts[campaign_day(campaign,date)][3];
        first=next;
    }
    finish();
}

void run(const fs::path& input,const fs::path& output,size_t threads) {
    auto start=std::chrono::steady_clock::now();
    auto progress=[&](const char* phase) {
        double seconds=std::chrono::duration<double>(std::chrono::steady_clock::now()-start).count();
        std::cerr<<phase<<" elapsed seconds "<<seconds<<'\n';
    };
    fs::create_directories(output/"user_daily");
    fs::create_directories(output/"campaign_daily");
    Keys keys;
    Catalog catalog(input,keys);
    auto links=std::async(threads>1 ? std::launch::async : std::launch::deferred,
                          [&]{return identities(input/"links",1,keys);});
    auto raw=parse_events(input/"events",threads>1 ? threads-1 : 1,catalog,keys);
    auto canon=links.get();
    progress("catalog, identities and event parse");
    auto events=deduplicate(raw,threads,canon);
    progress("event deduplication");
    auto clicks=read_clicks(input/"clicks",threads,catalog,keys,canon);
    canon=FlatMap{};
    progress("click parse");
    std::array<CampaignCounts,partitions> partial;
    parallel(partitions,threads,[&](size_t p){
        auto es=sorted_partition<Event>(events,p,[](auto& a,auto& b){
            return a.person==b.person ? a.time<b.time : a.person<b.person;
        });
        auto cs=sorted_partition<Click>(clicks,p,[&](auto& a,auto& b){
            if (a.person!=b.person) return a.person<b.person;
            if (a.time!=b.time) return a.time<b.time;
            return keys.less(b.id,a.id);
        });
        cs.erase(std::unique(cs.begin(),cs.end(),[](auto& a,auto& b){return a.id==b.id;}),cs.end());
        std::vector<UserRow> rows;
        size_t j=0;
        for (size_t i=0;i<es.size();) {
            Key person=es[i].person;
            size_t next=i+1;
            while (next<es.size() && es[next].person==person) ++next;
            while (j<cs.size() && person_before(cs[j].person,person)) ++j;
            size_t left=j;
            while (j<cs.size() && cs[j].person==person) ++j;
            user_days(es,i,next,cs,left,j,catalog,keys,partial[p],rows);
            i=next;
        }
        if (p==0 || !rows.empty())
            write_users((output/"user_daily"/("part-"+std::to_string(p)+".parquet")).string(),rows);
    });
    CampaignCounts merged;
    for (auto& part:partial) for (auto& [k,arr]:part) {
        auto& v=merged[k];
        for (size_t j=0;j<4;++j) v[j]+=arr[j];
    }
    std::vector<CampaignRow> rows;
    for (auto& [k,v]:merged) {
        auto [id,channel]=catalog.campaigns[k>>32];
        rows.push_back({keys.decode(id),keys.decode(channel),static_cast<int32_t>(k),v[0],v[1],v[2],v[3]});
    }
    write_campaigns((output/"campaign_daily"/"part-0.parquet").string(),rows);
    progress("all outputs");
}
}

int main(int argc,char** argv) {
    try {
        auto option=[](const char* name,const char* fallback) {
            auto s=std::getenv(name); return s?s:fallback;
        };
        size_t threads=std::clamp(std::stoi(option("NCPU","8")),1,64);
        shop::run(argc>1 ? argv[1]:option("DATA_DIR","/data"),
                  argc>2 ? argv[2]:option("OUTPUT_DIR","/output"),threads);
    } catch (const std::exception& e) {
        std::cerr<<e.what()<<'\n';
        return 1;
    }
}
