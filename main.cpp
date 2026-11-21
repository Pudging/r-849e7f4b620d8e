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
    Destination locate(Key source) const {
        Key token=routes.get(source,source);
        if (linked(token)) return {tag | ((token^tag)/partitions),part(token)};
        return {source,bucket(source)};
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

// Most successive rows in one hash bucket are nearby in the input order.
struct Positions {
    // Fixed pages keep the two append-only streams together without copying
    // millions of entries into progressively larger vector allocations.
    static constexpr size_t page_size=2048;
    struct Page {
        Key hashes[page_size];
        uint8_t distances[page_size];
    };
    std::vector<std::unique_ptr<Page>> pages;
    std::vector<size_t> large;
    size_t previous=0,count=0;

    void append(Key hash,size_t position) {
        const size_t distance=position-previous;
        const size_t i=count++%page_size;
        if (!i) pages.emplace_back(new Page);
        Page& page=*pages.back();
        page.hashes[i]=hash;
        if (distance<255) page.distances[i]=uint8_t(distance);
        else { page.distances[i]=255; large.push_back(position); }
        previous=position;
    }
};

// Keep ingestion times beside the payload. A block base represents nearby
// arrivals exactly in 32 bits, including out-of-order arrivals. Rare values
// outside the range use a full-width entry at the same position.
struct EventBatch {
    static constexpr size_t block_size = 8192;
    struct Block {
        Event events[block_size];
        uint32_t arrivals[block_size];
        int64_t base;
        std::unique_ptr<int64_t[]> exceptions;
        explicit Block(int64_t first)
            :base(first<INT64_MIN+INT32_MAX ? INT64_MIN : first-INT32_MAX) {}
        int64_t ingest(size_t pos) const {
            return arrivals[pos]==UINT32_MAX ? exceptions[pos] : base+arrivals[pos];
        }
        Event& set(size_t pos,int64_t time) {
            const uint64_t offset=uint64_t(time)-uint64_t(base);
            if (time>=base && offset<UINT32_MAX) arrivals[pos]=uint32_t(offset);
            else {
                arrivals[pos]=UINT32_MAX;
                if (!exceptions) exceptions.reset(new int64_t[block_size]);
                exceptions[pos]=time;
            }
            return events[pos];
        }
    };
    size_t count=0;
    std::vector<std::unique_ptr<Block>> blocks;
    std::array<Positions,partitions> ids;
    Event& append(int64_t ingest) {
        const size_t pos=count++%block_size;
        if (!pos) blocks.emplace_back(new Block(ingest));
        return blocks.back()->set(pos,ingest);
    }
};
struct ClickValue { Key id; int64_t time; Index campaign; };
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
        Event& row=batch.append(*ingest);
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
        batch.ids[part(hash)].append(hash,batch.count-1);
    });
    return raw;
}

EventGroups deduplicate(std::vector<EventBatch>& raw,size_t threads,const Identities& canon) {
    parallel(partitions,threads,[&](size_t p) {
        size_t n=0;
        for (const auto& batch:raw) n+=batch.ids[p].count;
        if (!n) return;
        size_t words=1;
        while (words<n/4) words*=2;
        std::vector<uint64_t> seen(words),repeated(words);
        auto fingerprint=[&](Key hash) {
            return std::pair<size_t,uint64_t>{(hash>>12)&(words-1),uint64_t(1)<<((hash>>6)&63)};
        };
        for (const auto& batch:raw) {
            const auto& ids=batch.ids[p];
            size_t left=ids.count;
            for (const auto& page:ids.pages) {
                const size_t take=std::min(left,Positions::page_size);
                for (size_t i=0;i<take;++i) {
                    auto [word,bit]=fingerprint(page->hashes[i]);
                    repeated[word]|=seen[word]&bit;
                    seen[word]|=bit;
                }
                left-=take;
            }
        }
        // Rank gives each occupied repeated bit one owner. All collisions
        // with the original owner spill to an exact sort. A spill key can
        // never match an owner key from a different fingerprint.
        size_t owners=0;
        for (size_t i=0;i<words;++i) {
            seen[i]=owners;
            owners+=__builtin_popcountll(repeated[i]);
        }
        std::vector<Reference> first(owners),exact;
        for (auto& batch:raw) {
            auto& ids=batch.ids[p];
            size_t position=0,escape=0;
            size_t left=ids.count;
            for (const auto& page:ids.pages) {
                const size_t take=std::min(left,Positions::page_size);
                for (size_t i=0;i<take;++i) {
                    const auto distance=page->distances[i];
                    position=distance<255 ? position+distance : ids.large[escape++];
                    const auto hash=page->hashes[i];
                    auto [word,bit]=fingerprint(hash);
                    const auto mask=repeated[word];
                    if (!(mask&bit)) continue;
                    auto& block=*batch.blocks[position/EventBatch::block_size];
                    size_t offset=position%EventBatch::block_size;
                    Reference entry{hash,&block.events[offset],block.ingest(offset)};
                    auto& owner=first[seen[word]+__builtin_popcountll(mask&(bit-1))];
                    if (!owner.record) owner=entry;
                    else if (owner.id!=hash) exact.push_back(entry);
                    else {
                        if (entry.ingest<owner.ingest) std::swap(entry,owner);
                        entry.record->value.time=Value::invalid;
                    }
                }
                left-=take;
            }
            ids=Positions{};
        }
        std::sort(exact.begin(),exact.end(),[](auto a,auto b){return a.id<b.id;});
        for (size_t i=0;i<exact.size();) {
            size_t next=i+1;
            auto best=exact[i];
            while (next<exact.size() && exact[next].id==exact[i].id) {
                auto other=exact[next++];
                if (other.ingest<best.ingest) std::swap(other,best);
                other.record->value.time=Value::invalid;
            }
            i=next;
        }
    });

    EventGroups clean(raw.size());
    parallel(raw.size(),threads,[&](size_t w) {
        struct Cached {Key source=0, person=0; size_t bin=0;};
        std::array<Cached,256> cached{};
        auto& batch=raw[w];
        size_t remaining=batch.count;
        for (auto& block:batch.blocks) {
            size_t count=std::min(remaining,EventBatch::block_size);
            for (size_t i=0;i<count;++i) {
                if (block->events[i].value.time==Value::invalid) continue;
                Event e=block->events[i];
                auto& slot=cached[(e.person>>12)&255];
                if (slot.source!=e.person) {
                    auto dest=canon.locate(e.person);
                    slot={e.person,dest.person,dest.bin};
                }
                e.person=slot.person;
                clean[w][slot.bin].append(e);
            }
            block.reset();
            remaining-=count;
        }
    });
    return clean;
}

Routed<Click> read_clicks(const fs::path& dir,size_t threads,const Catalog& cat, Keys& keys,const Identities& canon) {
    Routed<Click> result(threads);
    parsed_records<4>(dir,threads,[&](const auto& v,size_t worker){
        if (v[0].empty() || v[1].empty()) return;
        auto time=timestamp(v[3]);
        if (!time || *time<cat.start-week_ms || *time>=cat.end) return;
        Index campaign=cat.campaign_index.get(keys.encode(v[2]),0);
        if (!campaign) return;
        auto dest=canon.locate(keys.encode(v[1]));
        result[worker][dest.bin].append({dest.person,{keys.encode(v[0]),*time,campaign}});
    });
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
        while (right<click_end && cs[right].time<=start) window.add(cs[right++].campaign);
        while (left<right && cs[left].time<start-week_ms) {
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
    for (const auto& batch:raw) physical_rows+=batch.count;
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
    if (catalog.end>catalog.start && uint64_t(catalog.end-catalog.start)<=UINT32_MAX &&
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
