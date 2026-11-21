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
struct TimedEvent { Event event; int64_t ingest; };
struct Reference { Key id; TimedEvent* record; };

// Payload blocks stay in input order; pointers remain valid until all ID
// partitions have finished choosing their earliest arrivals.
struct EventBatch {
    static constexpr size_t block_size = 8192;
    size_t count = 0;
    std::vector<std::unique_ptr<TimedEvent[]>> blocks;
    Bins<Reference> ids;
    TimedEvent* append() {
        if (count % block_size == 0) blocks.emplace_back(new TimedEvent[block_size]);
        return &blocks.back()[count++ % block_size];
    }
};
struct Click { Key person, id; int64_t time; Index campaign; };
using EventGroups=std::vector<Bins<Event>>;

std::vector<EventBatch> parse_events(const fs::path& dir,size_t workers,const Catalog& cat, Keys& keys) {
    std::vector<EventBatch> raw(workers);
    parsed_records<7>(dir,workers,[&](const auto& v,size_t worker){
        if (v[0].empty()) return;
        auto ingest=timestamp(v[3]);
        if (!ingest) return;
        Key id=keys.encode(v[0]);
        auto& batch=raw[worker];
        auto* row=batch.append();
        row->ingest=*ingest;
        auto time=timestamp(v[2]);
        if (!v[1].empty() && !v[5].empty() && time && *time>=cat.start && *time<cat.end
                && *time>=*ingest-day_ms) {
            auto amount = v[4]=="purchase" ? number(v[6]) : std::nullopt;
            if (v[4]=="view" || v[4]=="cart" || (amount && *amount>=1 && *amount<=10'000'000)) {
                row->event.person=keys.encode(v[1]); row->event.time=*time;
                if (amount) {
                    row->event.price=*amount;
                    row->event.category=cat.products.get(keys.encode(v[5]),0);
                }
            }
        }
        // Retain the ID and arrival even if the event is invalid. An early
        // invalid event must suppress its later valid duplicates.
        batch.ids[bucket(id)].push_back({id,row});
    });
    return raw;
}

EventGroups deduplicate(std::vector<EventBatch>& raw,size_t threads,const FlatMap& canon) {
    parallel(partitions,threads,[&](size_t p) {
        size_t n=0;
        for (const auto& batch:raw) n+=batch.ids[p].size();
        size_t words=1;
        while (words<n/4) words*=2;
        std::vector<uint64_t> seen(words),repeated(words);
        auto fingerprint=[&](Key id) {
            uint64_t h=mix(id)>>6;
            return std::pair<size_t,uint64_t>{(h>>6)&(words-1),uint64_t(1)<<(h&63)};
        };
        for (const auto& batch:raw) for (auto r:batch.ids[p]) {
            auto [word,bit]=fingerprint(r.id);
            repeated[word]|=seen[word]&bit;
            seen[word]|=bit;
        }
        std::vector<Reference> exact;
        for (auto& batch:raw) {
            for (auto r:batch.ids[p]) {
                auto [word,bit]=fingerprint(r.id);
                if (repeated[word]&bit) exact.push_back(r);
            }
            std::vector<Reference>().swap(batch.ids[p]);
        }
        std::sort(exact.begin(),exact.end(),[](auto a,auto b){return a.id<b.id;});
        for (size_t i=0;i<exact.size();) {
            size_t next=i+1;
            auto* best=exact[i].record;
            while (next<exact.size() && exact[next].id==exact[i].id) {
                auto* other=exact[next++].record;
                if (other->ingest<best->ingest) std::swap(other,best);
                other->event.time=invalid_time;
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
                Event e=block[i].event;
                if (e.time==invalid_time) continue;
                auto& slot=cached[(e.person>>12)&255];
                if (slot.source!=e.person) {
                    Key person=canon.get(e.person,e.person);
                    slot={e.person,person,bucket(person)};
                }
                e.person=slot.person;
                clean[w][slot.bin].push_back(e);
            }
            block.reset();
            remaining-=count;
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
using SparseCounts=std::unordered_map<uint64_t,Totals>;
inline uint64_t campaign_day(uint32_t c, int32_t date) {
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

struct PurchaseEvent { int64_t time; uint32_t price,category; };

void user_days(size_t person,CampaignWindow& window,const std::vector<PurchaseEvent>& es,size_t first,size_t stop,
               const std::vector<Click>& cs,size_t left,size_t click_end,
               const Catalog& cat,const Keys& keys,CampaignCounts& counts,std::vector<UserRow>& rows) {
    size_t right=left;
    window.reset();
    std::vector<std::pair<Index,int64_t>> sums;
    UserRow day{};
    auto finish=[&] {
        if (!day.sessions) return;
        if (!sums.empty()) {
            auto best=std::max_element(sums.begin(),sums.end(),[&](auto& a,auto& b) {
                return a.second!=b.second ? a.second<b.second : keys.less(cat.categories[b.first],cat.categories[a.first]);
            });
            day.category=best->first;
        }
        rows.push_back(day);
        day={};
        sums.clear();
    };
    while (first<stop) {
        int64_t start=es[first].time;
        int32_t date=start/day_ms-(start%day_ms<0);
        if (day.sessions && day.day!=date) finish();
        if (!day.sessions) {day.person=person; day.day=date; day.category=std::numeric_limits<Index>::max();}
        while (right<click_end && cs[right].time<=start) window.add(cs[right++].campaign);
        while (left<right && cs[left].time<start-week_ms) {
            window.remove(cs[left++].campaign);
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
    Key person;
    size_t events=0,clicks=0,event_end=0,click_end=0;
};
void reduce_partition(size_t p,EventGroups& events,std::vector<Bins<Click>>& clicks,
                      const Catalog& cat,const Keys& keys,CampaignCounts& counts,
                      std::vector<UserRow>& rows,std::vector<std::string>& names) {
    std::vector<PersonGroup> groups;
    FlatMap people;
    Key last=0;
    size_t group=0;
    for (const auto& batch:events) for (const auto& e:batch[p]) {
        if (e.person!=last) {
            group=people.insert_or_get(e.person,groups.size());
            if (group==groups.size()) groups.push_back({e.person});
            last=e.person;
        }
        ++groups[group].events;
    }
    constexpr size_t missing=std::numeric_limits<size_t>::max();
    auto locate=[&](Key person) {
        if (last!=person) { last=person; group=people.get(person,missing); }
        return group;
    };
    for (const auto& batch:clicks) for (const auto& c:batch[p]) {
        auto i=locate(c.person);
        if (i!=missing) ++groups[i].clicks;
    }
    size_t ne=0,nc=0;
    for (auto& g:groups) {
        g.event_end=ne; ne+=g.events;
        g.click_end=nc; nc+=g.clicks;
    }
    std::vector<PurchaseEvent> es(ne);
    std::vector<Click> cs(nc);
    for (auto& batch:events) {
        for (const auto& e:batch[p]) {
            auto& g=groups[locate(e.person)];
            es[g.event_end++]={e.time,e.price,e.category};
        }
        std::vector<Event>().swap(batch[p]);
    }
    for (auto& batch:clicks) {
        for (const auto& c:batch[p]) {
            auto i=locate(c.person);
            if (i!=missing) cs[groups[i].click_end++]=c;
        }
        std::vector<Click>().swap(batch[p]);
    }
    names.reserve(groups.size());
    CampaignWindow window(cat.campaigns.size());
    for (const auto& g:groups) {
        size_t first=g.event_end-g.events, left=g.click_end-g.clicks;
        std::sort(es.begin()+first,es.begin()+g.event_end,[](auto a,auto b){return a.time<b.time;});
        auto begin=cs.begin()+left, end=cs.begin()+g.click_end;
        std::sort(begin,end,[&](const auto& a,const auto& b){
            return a.time==b.time ? keys.less(b.id,a.id) : a.time<b.time;
        });
        end=std::unique(begin,end,[](const auto& a,const auto& b){return a.id==b.id;});
        names.push_back(keys.decode(g.person));
        user_days(names.size()-1,window,es,first,g.event_end,cs,left,end-cs.begin(),cat,keys,counts,rows);
    }
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
    std::vector<std::unique_ptr<CampaignCounts>> partial(partitions);
    parallel(partitions,threads,[&](size_t p){
        std::vector<UserRow> rows;
        std::vector<std::string> names;
        partial[p]=std::make_unique<CampaignCounts>(catalog);
        reduce_partition(p,events,clicks,catalog,keys,*partial[p],rows,names);
        if (p==0 || !rows.empty())
            write_users((output/"user_daily"/("part-"+std::to_string(p)+".parquet")).string(),rows,names,catalog.category_names);
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
