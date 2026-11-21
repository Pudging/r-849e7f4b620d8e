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

struct Catalog {
    FlatMap products, campaign_index;
    std::vector<Key> categories;
    std::vector<std::string> category_names;
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
        for (Key category:categories) category_names.push_back(keys.decode(category));
    }
};

struct Identities {
    FlatMap routes;
    Bins<Key> names;
    // Packed text uses at most 60 bits. Interned keys always have bit 63 set.
    // The remaining 01 prefix can therefore carry a dense group ordinal.
    static constexpr Key tag=Key(1)<<62;
    static bool linked(Key token) { return (token>>62)==1; }
    struct Destination { Key person; size_t bin; };
    Destination locate(Key source,uint64_t hash) const {
        Key route=routes.lookup_hashed(source,hash);
        return route ? Destination{tag|((route^tag)/partitions),part(route)}
                     : Destination{source,part(hash)};
    }
};
Identities identities(const fs::path& dir,size_t threads,Keys& keys) {
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
    Identities result;
    for (size_t i=0;i<parent.size();++i) if (parent[i]==i) {
        Key name=names[i];
        size_t bin=bucket(name);
        names[i]=Identities::tag | (result.names[bin].size()*partitions+bin);
        result.names[bin].push_back(name);
    }
    index.change_values([&](Key i){return names[find(i)];});
    result.routes=std::move(index);
    return result;
}

// The common representation is selected from the actual catalog and manifest.
struct WindowValue {
    static constexpr uint32_t invalid=UINT32_MAX;
    uint32_t time, purchase;
    WindowValue() = default;
    WindowValue(int64_t t, uint32_t price, uint32_t category,const Catalog& cat)
        : time(uint32_t(t-cat.start)), purchase((category<<24)|price) {}
    int64_t absolute(const Catalog& cat) const { return cat.start+time; }
    uint32_t price() const { return purchase&0xffffff; }
    uint32_t category() const { return purchase>>24; }
};
struct WideValue {
    static constexpr int64_t invalid=invalid_time;
    int64_t time;
    uint32_t amount, product_category;
    WideValue() = default;
    WideValue(int64_t t,uint32_t price,uint32_t category,const Catalog&)
        : time(t),amount(price),product_category(category) {}
    int64_t absolute(const Catalog&) const { return time; }
    uint32_t price() const { return amount; }
    uint32_t category() const { return product_category; }
};

template<class Value>
struct Engine {
struct Event { Key person; Value value; };
struct Reference { Key id; Event* record; int64_t ingest; };

// Input workers own fixed pages partitioned by full event-ID hash. The same
// page holds the identity, event value and arrival time throughout arbitration.
struct EventPage {
    static constexpr size_t capacity=2048;
    Key hashes[capacity];
    uint32_t arrivals[capacity];
    Event events[capacity];
    int64_t base;
    std::unique_ptr<int64_t[]> exceptions;
    explicit EventPage(int64_t first)
        :base(first<INT64_MIN+INT32_MAX ? INT64_MIN : first-INT32_MAX) {}
    int64_t ingest(size_t pos) const {
        return arrivals[pos]==UINT32_MAX ? exceptions[pos] : base+arrivals[pos];
    }
    Event& set(size_t pos,Key hash,int64_t time) {
        hashes[pos]=hash;
        uint64_t delta=uint64_t(time)-uint64_t(base);
        if (time>=base && delta<UINT32_MAX) arrivals[pos]=uint32_t(delta);
        else {
            arrivals[pos]=UINT32_MAX;
            if (!exceptions) exceptions.reset(new int64_t[capacity]);
            exceptions[pos]=time;
        }
        return events[pos];
    }
};
struct alignas(64) EventBucket {
    static constexpr size_t buffer_size=8;
    struct Staged { Key hash; int64_t ingest; Event event; };
    Staged pending[buffer_size];
    size_t count=0, buffered=0;
    std::vector<std::unique_ptr<EventPage>> pages;
    void flush() {
        if (!buffered) return;
        size_t pos=(count-buffered)%EventPage::capacity;
        if (!pos) pages.emplace_back(new EventPage(pending[0].ingest));
        auto& page=*pages.back();
        for (size_t i=0;i<buffered;++i) {
            const auto& src=pending[i];
            page.set(pos+i,src.hash,src.ingest)=src.event;
        }
        buffered=0;
    }
    Event& append(Key hash,int64_t ingest) {
        if (buffered==buffer_size) flush();
        ++count;
        auto& slot=pending[buffered++];
        slot.hash=hash;
        slot.ingest=ingest;
        return slot.event;
    }
    template<class Fn> void visit(Fn fn) {
        size_t left=count;
        for (auto& page:pages) {
            size_t take=std::min(left,EventPage::capacity);
            fn(*page,take);
            left-=take;
        }
    }
};
using EventBatch=std::array<EventBucket,partitions>;

// A click can precede the event window by one week. Compact engines store
// offsets from that earlier bound; wide engines retain absolute milliseconds.
using ClickTime=std::conditional_t<std::is_same_v<Value,WindowValue>,uint32_t,int64_t>;
static ClickTime encode_click(int64_t t,const Catalog& cat) {
    if constexpr (std::is_same_v<Value,WindowValue>) return uint32_t(t-(cat.start-week_ms));
    return t;
}
static int64_t click_time(ClickTime t,const Catalog& cat) {
    if constexpr (std::is_same_v<Value,WindowValue>) return (cat.start-week_ms)+t;
    return t;
}
struct ClickValue { Key id; ClickTime time; Index campaign; };
struct Click { Key person; ClickValue value; };
// Routing writes to many bins concurrently. Fixed pages let each bin grow
// without reallocating and copying previously routed rows.
template<class T> struct Payloads {
    static constexpr size_t capacity=2048;
    size_t count=0;
    std::vector<std::unique_ptr<T[]>> pages;
    void append(const T& row) {
        size_t i=count++%capacity;
        if (!i) pages.emplace_back(new T[capacity]);
        pages.back()[i]=row;
    }
    template<class F> void visit(F fn) {
        size_t left=count;
        for (auto& page:pages) {
            const size_t take=std::min(left,capacity);
            for (size_t i=0;i<take;++i) fn(page[i]);
            left-=take;
        }
    }
};
template<class T> using Routed=std::vector<std::array<Payloads<T>,partitions>>;
using EventGroups=Routed<Event>;

std::vector<EventBatch> parse_events(const fs::path& dir,size_t workers,const Catalog& cat, Keys& keys) {
    std::vector<EventBatch> raw(workers);
    parsed_records<7>(dir,workers,[&](const auto& v,size_t worker){
        if (v[0].empty()) return;
        auto ingest=timestamp(v[3]);
        if (!ingest) return;
        Key hash=mix(keys.encode(v[0]));
        auto& batch=raw[worker];
        Event& row=batch[part(hash)].append(hash,*ingest);
        row={};
        row.value.time=Value::invalid;
        auto time=timestamp(v[2]);
        if (!v[1].empty() && !v[5].empty() && time && *time>=cat.start && *time<cat.end
                && *time>=*ingest-day_ms) {
            auto amount = v[4]=="purchase" ? number(v[6]) : std::nullopt;
            if (v[4]=="view" || v[4]=="cart" || (amount && *amount>=1 && *amount<=10'000'000)) {
                row.person=keys.encode(v[1]);
                const uint32_t category=amount ? cat.products.get(keys.encode(v[5]),0) : 0;
                row.value=Value(*time,amount.value_or(0),category,cat);
            }
        }
        // Retain the ID and arrival even if the event is invalid. An early
        // invalid event must suppress its later valid duplicates.
        // mix is a permutation on 64-bit words. Comparing full hashes is exact.

    });
    for (auto& batch:raw) for (auto& bin:batch) bin.flush();
    return raw;
}

EventGroups deduplicate(std::vector<EventBatch>& raw,size_t threads,const Identities& canon) {
    EventGroups clean(threads);
    std::atomic<size_t> next{0};
    parallel(threads,threads,[&](size_t worker) {
        for (size_t p;(p=next.fetch_add(1,std::memory_order_relaxed))<partitions;) {
            size_t n=0;
            for (auto& batch:raw) n+=batch[p].count;
            if (!n) continue;
            size_t words=1;
            while (words<n/4) words*=2;
            std::vector<uint64_t> seen(words),repeated(words);
            for (auto& batch:raw) batch[p].visit([&](EventPage& page,size_t size) {
                for (size_t i=0;i<size;++i) {
                    Key hash=page.hashes[i];
                    auto word=(hash>>12)&(words-1);
                    uint64_t bit=uint64_t(1)<<((hash>>6)&63);
                    repeated[word]|=seen[word]&bit;
                    seen[word]|=bit;
                }
            });
            size_t owners=0;
            for (size_t i=0;i<words;++i) {
                seen[i]=owners;
                owners+=__builtin_popcountll(repeated[i]);
            }
            std::vector<Reference> first(owners),exact;
            if (owners) for (auto& batch:raw) batch[p].visit([&](EventPage& page,size_t size) {
                for (size_t i=0;i<size;++i) {
                    Key hash=page.hashes[i];
                    auto word=(hash>>12)&(words-1);
                    uint64_t bit=uint64_t(1)<<((hash>>6)&63);
                    auto mask=repeated[word];
                    if (!(mask&bit)) continue;
                    Reference entry{hash,&page.events[i],page.ingest(i)};
                    auto& owner=first[seen[word]+__builtin_popcountll(mask&(bit-1))];
                    if (!owner.record) owner=entry;
                    else if (owner.id!=hash) exact.push_back(entry);
                    else {
                        if (entry.ingest<owner.ingest) std::swap(entry,owner);
                        entry.record->value.time=Value::invalid;
                    }
                }
            });
            std::sort(exact.begin(),exact.end(),[](auto a,auto b){return a.id<b.id;});
            for (size_t i=0;i<exact.size();) {
                size_t stop=i+1;
                auto best=exact[i];
                while (stop<exact.size() && exact[stop].id==exact[i].id) {
                    auto other=exact[stop++];
                    if (other.ingest<best.ingest) std::swap(other,best);
                    other.record->value.time=Value::invalid;
                }
                i=stop;
            }
            for (auto& batch:raw) {
                batch[p].visit([&](EventPage& page,size_t size){
                    constexpr size_t width=48;
                    for (size_t i=0;i<size;i+=width) {
                        size_t take=std::min(width,size-i);
                        uint64_t hashes[width];
                        for (size_t k=0;k<take;++k) {
                            hashes[k]=mix(page.events[i+k].person);
                            canon.routes.prefetch(hashes[k]);
                        }
                        for (size_t k=0;k<take;++k) {
                            auto& e=page.events[i+k];
                            if (e.value.time==Value::invalid) continue;
                            auto dest=canon.locate(e.person,hashes[k]);
                            clean[worker][dest.bin].append({dest.person,e.value});
                        }
                    }
                });
                batch[p].pages.clear();
            }
        }
    });
    return clean;
}

Routed<Click> read_clicks(const fs::path& dir,size_t threads,const Catalog& cat, Keys& keys,const Identities& canon) {
    Routed<Click> result(threads);
    constexpr size_t batch_size=48;
    struct alignas(64) Batch {
        Click rows[batch_size];
        uint64_t hashes[batch_size];
        size_t count=0;
    };
    std::vector<Batch> batches(threads);
    auto flush=[&](size_t w) {
        auto& batch=batches[w];
        for (size_t i=0;i<batch.count;++i) {
            Click& c=batch.rows[i];
            auto dest=canon.locate(c.person,batch.hashes[i]);
            c.person=dest.person;
            result[w][dest.bin].append(c);
        }
        batch.count=0;
    };
    parsed_records<4>(dir,threads,[&](const auto& v,size_t worker){
        if (v[0].empty() || v[1].empty()) return;
        auto time=timestamp(v[3]);
        if (!time || *time<cat.start-week_ms || *time>=cat.end) return;
        Index campaign=cat.campaign_index.get(keys.encode(v[2]),0);
        if (!campaign) return;
        auto& batch=batches[worker];
        size_t i=batch.count++;
        Key person=keys.encode(v[1]);
        batch.hashes[i]=mix(person);
        canon.routes.prefetch(batch.hashes[i]);
        batch.rows[i]={person,{keys.encode(v[0]),encode_click(*time,cat),campaign}};
        if (batch.count==batch_size) flush(worker);
    });
    for (size_t w=0;w<threads;++w) flush(w);
    return result;
}

using Totals=std::array<int64_t,4>;
using SparseCounts=std::unordered_map<uint64_t,Totals>;
static uint64_t campaign_day(uint32_t c, int32_t date) {
    return (uint64_t(c)<<32)|uint32_t(date);
}

struct CampaignCounts {
    int32_t first_day;
    size_t days;
    std::vector<Totals> dense;
    SparseCounts sparse;
    explicit CampaignCounts(const Catalog& cat) {
        auto day=[](int64_t t){return t/day_ms-(t%day_ms<0);};
        first_day=day(cat.start);
        days=size_t(day(cat.end-1)-first_day+1);
        // Use bounded direct addressing for small catalogs and short windows.
        if (days && cat.campaigns.size()<=65536/days) dense.resize(days*cat.campaigns.size());
    }
    Totals& at(Index campaign,int32_t date) {
        if (dense.empty()) return sparse[campaign_day(campaign,date)];
        return dense[size_t(campaign)*days+size_t(date-first_day)];
    }
    void merge(SparseCounts& target) const {
        auto add=[&](uint64_t k,const Totals& v) {
            auto& out=target[k];
            for (size_t i=0;i<v.size();++i) out[i]+=v[i];
        };
        for (size_t i=0;i<dense.size();++i) {
            if (dense[i][0] || dense[i][3]) add(campaign_day(i/days,first_day+i%days),dense[i]);
        }
        for (const auto& [k,v]:sparse) add(k,v);
    }
};

struct CampaignWindow {
    std::vector<size_t> count, position;
    std::vector<Index> active;
    explicit CampaignWindow(size_t campaigns):count(campaigns),position(campaigns) {}
    void add(Index c) {
        if (count[c]++==0) {
            position[c]=active.size();
            active.push_back(c);
        }
    }
    void remove(Index c) {
        if (--count[c]==0) {
            auto last=active.back();
            active[position[c]]=last;
            position[last]=position[c];
            active.pop_back();
        }
    }
    void reset() {
        for (Index c:active) count[c]=0;
        active.clear();
    }
};

using PurchaseEvent = Value;

void user_days(const std::string& person,CampaignWindow& window,const std::vector<PurchaseEvent>& es,size_t first,size_t stop,
               const std::vector<ClickValue>& cs,size_t left,size_t click_end,
               const Catalog& cat,const Keys& keys,CampaignCounts& counts,UserWriter& writer,
               std::vector<std::pair<Index,int64_t>>& sums) {
    size_t right=left;
    window.reset();
    bool first_day=true;
    UserRow day{};
    auto finish=[&] {
        if (!day.sessions) return;
        if (!sums.empty()) {
            auto best=std::max_element(sums.begin(),sums.end(),[&](auto& a,auto& b) {
                return a.second!=b.second ? a.second<b.second : keys.less(cat.categories[b.first],cat.categories[a.first]);
            });
            day.category=best->first;
        }
        writer.append(day,person,first_day);
        first_day=false;
        day={};
        sums.clear();
    };
    while (first<stop) {
        int64_t start=es[first].absolute(cat);
        int32_t date=start/day_ms-(start%day_ms<0);
        if (day.sessions && day.day!=date) finish();
        if (!day.sessions) {day.day=date; day.category=std::numeric_limits<Index>::max();}
        while (right<click_end && click_time(cs[right].time,cat)<=start) window.add(cs[right++].campaign);
        while (left<right && click_time(cs[left].time,cat)<start-week_ms) {
            window.remove(cs[left++].campaign);
        }
        Index credit=left==right ? 0 : cs[right-1].campaign;
        size_t next=first;
        int64_t revenue=0;
        do {
            auto &e=es[next++];
            if (e.price()) {
                revenue+=e.price();
                auto it=std::find_if(sums.begin(),sums.end(),[&](auto& x){return x.first==e.category();});
                if (it==sums.end()) sums.emplace_back(e.category(),e.price());
                else it->second+=e.price();
            }
        } while (next<stop && es[next].time-es[next-1].time<=1'800'000);
        ++day.sessions; day.events+=next-first; day.revenue+=revenue;
        auto &v=counts.at(credit,date);
        ++v[0]; v[1]+=revenue>0; v[2]+=revenue;
        for (Index campaign:window.active) ++counts.at(campaign,date)[3];
        first=next;
    }
    finish();
}

// Count exact canonical IDs first. Within one person's range timestamp is
// the sole event sort key. Person ranges also index the two sides of the join.
struct PersonGroup {
    size_t events=0,clicks=0;
};
// Three stable counting passes sort large compact groups. Small groups and
// full-width timestamp groups use comparison sorting.
static void sort_events(std::vector<Value>& events,size_t first,size_t count,
                        std::vector<Value>& scratch) {
    auto begin=events.begin()+first;
    if constexpr (std::is_same_v<Value,WindowValue>) {
        if (count>=2048) {
            std::array<std::array<size_t,2048>,3> offsets{};
            for (size_t i=0;i<count;++i) {
                auto time=begin[i].time;
                ++offsets[0][time&2047];
                ++offsets[1][(time>>11)&2047];
                ++offsets[2][time>>22];
            }
            scratch.resize(count);
            Value* src=events.data()+first;
            Value* dst=scratch.data();
            for (unsigned pass=0;pass<3;++pass) {
                size_t total=0;
                auto& pos=offsets[pass];
                for (auto& end:pos) {
                    size_t size=end;
                    end=total;
                    total+=size;
                }
                for (size_t i=0;i<count;++i) {
                    auto value=src[i];
                    dst[pos[(value.time>>(11*pass))&2047]++]=value;
                }
                std::swap(src,dst);
            }
            std::copy_n(src,count,dst);
            return;
        }
    }
    std::sort(begin,begin+count,[](auto a,auto b){return a.time<b.time;});
}
void reduce_partition(size_t p,EventGroups& events,Routed<Click>& clicks,std::vector<Key>& names,
                      const Catalog& cat,const Keys& keys,CampaignCounts& counts,
                      const fs::path& output,bool narrow_counts) {
    // Only singleton keys require a dictionary probe. Annotate them once
    // so that all event payloads share the same direct-index scatter.
    static_assert(partitions==64);
    KeyMap<6> singletons;
    std::vector<PersonGroup> groups(names.size());
    for (auto& batch:events) batch[p].visit([&](Event& e) {
        if (!Identities::linked(e.person)) {
            size_t i=singletons.insert_or_get(e.person,names.size());
            if (i==names.size()) { names.push_back(e.person); groups.emplace_back(); }
            e.person=Identities::tag|i;
        }
        ++groups[e.person^Identities::tag].events;
    });
    constexpr Key missing=UINT64_MAX;
    for (auto& batch:clicks) batch[p].visit([&](Click& c) {
        if (!Identities::linked(c.person)) {
            size_t i=singletons.get(c.person,missing);
            if (i==missing) { c.person=missing; return; }
            c.person=Identities::tag|i;
        }
        ++groups[c.person^Identities::tag].clicks;
    });
    singletons={};
    size_t ne=0,nc=0;
    for (auto& g:groups) {
        ne+=g.events; g.events=ne;
        nc+=g.clicks; g.clicks=nc;
    }
    std::vector<Value> es(ne);
    std::vector<ClickValue> cs(nc);
    for (auto& batch:events) {
        batch[p].visit([&](const Event& e){es[--groups[e.person^Identities::tag].events]=e.value;});
        batch[p]={};
    }
    for (auto& batch:clicks) {
        batch[p].visit([&](const Click& c) {
            if (c.person!=missing) cs[--groups[c.person^Identities::tag].clicks]=c.value;
        });
        batch[p]={};
    }
    if (!ne && p!=0) return;
    UserWriter writer((output/("part-"+std::to_string(p)+".parquet")).string(),cat.category_names,narrow_counts);
    groups.push_back({ne,nc});
    CampaignWindow window(cat.campaigns.size());
    std::vector<Value> scratch;
    std::vector<std::pair<Index,int64_t>> sums;
    for (size_t i=0;i<names.size();++i) {
        const auto first=groups[i].events,stop=groups[i+1].events;
        if (first==stop) continue;
        sort_events(es,first,stop-first,scratch);
        auto left=groups[i].clicks;
        auto begin=cs.begin()+left,end=cs.begin()+groups[i+1].clicks;
        std::sort(begin,end,[&](const auto& a,const auto& b){
            return a.time==b.time ? keys.less(b.id,a.id) : a.time<b.time;
        });
        end=std::unique(begin,end,[](const auto& a,const auto& b){return a.id==b.id;});
        user_days(keys.decode(names[i]),window,es,first,stop,cs,left,end-cs.begin(),cat,keys,counts,writer,sums);
    }
    writer.close();
}

void run(const fs::path& input,const fs::path& output,size_t threads,Keys& keys,const Catalog& catalog,
         std::chrono::steady_clock::time_point start) {
    auto progress=[&](const char* phase) {
        double seconds=std::chrono::duration<double>(std::chrono::steady_clock::now()-start).count();
        std::cerr<<phase<<" elapsed seconds "<<seconds<<'\n';
    };
    fs::create_directories(output/"user_daily");
    fs::create_directories(output/"campaign_daily");

    auto links=std::async(threads>1 ? std::launch::async : std::launch::deferred,
                          [&]{return identities(input/"links",1,keys);});
    auto raw=parse_events(input/"events",threads>1 ? threads-1 : 1,catalog,keys);
    auto canon=links.get();
    uint64_t physical_rows=0;
    for (const auto& batch:raw) for (const auto& bin:batch) physical_rows+=bin.count;
    progress("catalog, identities and event parse");
    auto events=deduplicate(raw,threads,canon);
    progress("event deduplication");
    auto clicks=read_clicks(input/"clicks",threads,catalog,keys,canon);
    canon.routes=FlatMap{};
    progress("click parse");
    std::vector<std::unique_ptr<CampaignCounts>> partial(threads);
    std::atomic<size_t> next{0};
    parallel(threads,threads,[&](size_t worker){
        partial[worker]=std::make_unique<CampaignCounts>(catalog);
        for (size_t p;(p=next.fetch_add(1))<partitions;)
            reduce_partition(p,events,clicks,canon.names[p],catalog,keys,*partial[worker],output/"user_daily",
                             physical_rows<=INT32_MAX);
    });
    SparseCounts merged;
    for (const auto& group:partial) group->merge(merged);
    std::vector<CampaignRow> rows;
    for (auto& [k,v]:merged) {
        auto [id,channel]=catalog.campaigns[k>>32];
        rows.push_back({keys.decode(id),keys.decode(channel),static_cast<int32_t>(k),v[0],v[1],v[2],v[3]});
    }
    write_campaigns((output/"campaign_daily"/"part-0.parquet").string(),rows);
    progress("all outputs");
}
};

void run(const fs::path& input,const fs::path& output,size_t threads) {
    auto start=std::chrono::steady_clock::now();
    Keys keys;
    Catalog catalog(input,keys);
    if (catalog.end>catalog.start && uint64_t(catalog.end-catalog.start)<=UINT32_MAX-week_ms &&
        catalog.categories.size()<=256) {
        Engine<WindowValue>{}.run(input,output,threads,keys,catalog,start);
    } else {
        Engine<WideValue>{}.run(input,output,threads,keys,catalog,start);
    }
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
