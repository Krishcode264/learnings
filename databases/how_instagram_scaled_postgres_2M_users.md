# How Instagram Scaled Postgres to 2 Billion Users

> 🎥 **Video:** [How Instagram Scaled Postgres to 2 Billion Users](https://youtu.be/YLoYcwnqVzM)  
> 📅 **Watched:** June 2026  
> 🏷️ **Tags:** `system-design` `databases` `postgres` `sharding` `scaling` `backend`

## ⚡ TL;DR (Quick Revision)

Instagram ran on a **single Postgres database** until 27 million users. When they hit limits, instead of switching to NoSQL, they scaled Postgres itself using three key techniques:

1. **PgBouncer** — connection pooling proxy to stop RAM being wasted on idle connections
2. **Logical-to-physical sharding** — shard by user ID, but decouple logical shards from physical machines so scaling = just a config change, not a data rewrite
3. **Snowflake-style IDs in Postgres** — 64-bit IDs encoding timestamp + shard ID + sequence, generated inside Postgres itself (no external service)

**Core lesson:** The database wasn't the bottleneck. The architecture around it was. Understand what you have before reaching for something new.

---

## 📖 Full Notes

### 1. Background — Instagram in 2010

- 3 engineers, one single Postgres database on AWS EC2
- Photos → S3, everything else (users, metadata, comments, likes, follow graph) → Postgres
- No microservices, no Kafka, nothing fancy
- By 2012 (Facebook acquisition): **27 million users on a 2 TB Postgres DB**

> **Key insight:** Boring technology + good engineering beats new technology every time

---

### 2. Problem 1 — Connection Exhaustion

**What broke:**  
Each Django app server maintained its own connection pool. With 50 servers × 30 connections = **1,500 connections × 1.3 MB each ≈ 2 GB RAM** — just sitting idle, before the DB did any real work.

**Fix: PgBouncer**

```
[App Servers] → [PgBouncer] → [Postgres]
```

- PgBouncer is a lightweight proxy between the app and Postgres
- It **multiplexes** thousands of app connections onto a small pool of real DB connections (e.g. 1500 → 30)
- DB memory is now free for actual work: caching pages, query planning, sorting

> 💡 **Interview tip:** If you're running Postgres at scale without a connection pooler (PgBouncer or pgpool-II), that's your #1 quick win. This is the **first invisible bottleneck** most teams hit.

---

### 3. Problem 2 — Hitting the Vertical Scaling Ceiling

- DB grew to 2 TB, maxed out the biggest EC2 instance available
- RAM maxed, disk I/O saturated — nowhere left to go vertically

**The decision everyone expected:** Switch to Cassandra or MongoDB  
**What Instagram actually did:** Stayed on Postgres and sharded it

**Why they didn't switch:**
- The problems (connection limits, memory, disk IO) were **scale problems, not Postgres problems** — any DB would hit them
- Switching to Cassandra doesn't make horizontal partitioning disappear, it just hides it behind a different abstraction
- Once you switch, you can't easily go back ("burned the boats")

---

### 4. Sharding — The Approach

#### Shard Key Choice: User ID

- All data for user X lives on the same shard
- Fast for single-user queries: hash userID → know the shard → query it

**Trade-off:**  
Cross-user queries (e.g. "show me my feed from 200 people I follow") = hitting 50+ different shards, merging results in the app layer. Instagram accepted this and solved it with caching + precomputed feeds.

> ⚠️ **The shard key decision is permanent.** Resharding means rewriting every row. Pick carefully, once.

---

### 5. Logical vs Physical Sharding (The Clever Part)

**What most people think sharding means:**  
Split data across N physical machines. Want to grow? Add machines, reshuffle data. Painful.

**What Instagram did:**  
Created **thousands of logical shards** (each is a Postgres `schema` — a namespaced collection of tables), completely independent of physical hardware.

```
Logical shards: shard_0, shard_1, ... shard_4000  (fixed forever)
         ↓  mapping table  ↓
Physical nodes: node_1, node_2, ... node_N  (can change)
```

**How scaling works with this design:**

1. Physical node 7 hits 95% capacity
2. Spin up new physical node 16
3. Use Postgres **streaming replication** to copy some logical shards from node 7 → node 16
4. Once in sync, update the **mapping table**: "shard 1920–2047 now lives on node 16"
5. Done. Data unchanged. Schema unchanged. App code unchanged. Only the mapping changed.

> 💡 **The mapping table IS the architecture.** If you hardcode "16 shards" in your app, growth is painful. If you have 4000 logical shards mapped onto N physical nodes, growth is just a config change.

**Key principle:** Decouple **how data is partitioned** from **where it physically lives**

---

### 6. ID Generation Problem

**The problem:**  
After sharding, auto-increment IDs break immediately. Shard 0 and Shard 1 both generate ID `1`, `2`, `3`... → collision → two different photos with same ID → app can't tell them apart.

**Options evaluated:**

| Approach | Problem |
|---|---|
| UUID (128-bit random) | Huge size, no ordering → can't sort by recency using ID |
| Ticket server (Flickr's approach) | Single point of failure — if it goes down, nothing can insert |
| Twitter Snowflake | Required running a separate external service |

**What Instagram built: Snowflake-style IDs inside Postgres**

A 64-bit number split into 3 parts:

```
[ 41 bits: timestamp (ms since custom epoch) ][ 13 bits: shard ID ][ 10 bits: sequence ]
```

- **41 bits timestamp** → 41 years of millisecond precision
- **13 bits shard ID** → up to 8,192 possible shards
- **10 bits sequence** → 1,024 unique IDs per millisecond per shard = ~1M IDs/sec/shard

**Implemented as a Postgres function** — no external service, no coordination between shards, no single point of failure.

**Bonus:** Because timestamp is in the high-order bits, IDs are **sortable by time**.  
"Show me latest 20 photos" = `ORDER BY id DESC LIMIT 20` — no separate timestamp index needed.

> 💡 This pattern is now industry-standard. Discord, Slack, and many others copied it.

---

### 7. Three Underrated Postgres Features Instagram Used

#### Partial Indexes
```sql
-- Normal index: 1 billion entries for 1 billion rows
CREATE INDEX idx_photos_created ON photos(created_at);

-- Partial index: only index recent rows
CREATE INDEX idx_photos_recent ON photos(created_at)
WHERE created_at > NOW() - INTERVAL '30 days';
```
Result: index stays ~100M entries instead of 1B → 10x faster to scan, naturally stays small as data ages.

#### Functional Indexes
```sql
-- Instead of indexing the full 64-char token (wasteful):
CREATE INDEX idx_token_prefix ON tokens(substr(token, 1, 8));
```
Index only the first 8 characters of a long string token. Most 8-char prefixes are unique enough for fast lookups, index is ~10x smaller.

#### Logical Replication
Postgres can stream every INSERT/UPDATE/DELETE on a table to downstream systems in real time:

```
Primary DB (source of truth)
    ↓ logical replication stream
    ├── Search index (auto-updates)
    ├── Cache layer (auto-invalidates)
    └── Analytics warehouse (real-time copy)
```

Teams often build this manually with Kafka + dual writes. Postgres has it built in.

---

### 8. "Boring Technology is a Superpower"

Every team has a **finite complexity budget**. Every new piece of infrastructure you adopt eats into it:
- Learn its quirks
- Build operational expertise
- Train the team
- Hire for it

These costs are real even if they don't show on a balance sheet.

**Boring tech (Postgres, Linux, Nginx, Memcached):**
- Team already knows it
- Failure modes are well documented
- Operational tooling has existed for 15+ years
- Internet is full of answers to your specific problems

**When to actually reach for specialized tech:**
- Vector search at scale
- Real-time time series analytics
- Graph traversal at planet scale

> Having a lot of users is NOT a new problem. Postgres can handle it. Instagram, Notion, Reddit, GitHub, Stripe all prove it.

---

## 📌 Takeaways / Interview Cheat Sheet

| # | Takeaway |
|---|---|
| 1 | Don't shard until you absolutely have to (Instagram waited until 27M users) |
| 2 | When you shard: separate logical shards from physical nodes |
| 3 | Use Snowflake-style IDs from day one, even if not sharded yet (migrating later is painful) |
| 4 | Add PgBouncer early — it's the first invisible bottleneck every Postgres deployment hits |
| 5 | Adopt new infrastructure only when you have a genuinely new problem |

---

## ❓ Interview Questions This Covers

- *"How would you scale a relational database?"*
- *"What is sharding and what are the trade-offs?"*
- *"How do you generate unique IDs in a distributed system?"*
- *"Why not just use NoSQL when Postgres gets slow?"*
- *"What is connection pooling and why does it matter?"*
- *"What are some underused Postgres features?"*

---

## 🔗 Related Topics to Explore

- [ ] Consistent hashing (alternative sharding strategy)
- [ ] Read replicas in Postgres
- [ ] CAP theorem and where Postgres sits
- [ ] TAO — Meta's distributed graph store (what Instagram uses today)
- [ ] Kafka vs Postgres logical replication for CDC (change data capture)
