# Data Management & Databases: Reference Card and Cheat Sheet

> Scope: RDBMS, NoSQL, Vector DBs, Data Modeling & Architecture, Data Governance & Quality.
> Versions/features evolve quickly. Verify specifics against official docs before production use.

---

## Table of Contents

1. [Big Picture & Selection Matrix](#1-big-picture--selection-matrix)
2. [Core Concepts (ACID, BASE, CAP, Indexing)](#2-core-concepts)
3. [Relational Databases](#3-relational-databases-rdbms): PostgreSQL, MySQL, SQL Server
4. [NoSQL Databases](#4-nosql-databases): MongoDB, Redis, Cassandra, Neo4j
5. [Vector Databases](#5-vector-databases): Pinecone, Milvus, Qdrant
6. [Data Modeling & Architecture](#6-data-modeling--architecture): 3NF, Star/Snowflake, Data Vault 2.0, OBT
7. [Data Governance & Quality](#7-data-governance--quality): Catalogs, Lineage, MDM
8. [Cross-Cutting Best Practices](#8-cross-cutting-best-practices)
9. [Decision Cheat Sheet](#9-decision-cheat-sheet)

---

## 1. Big Picture & Selection Matrix

| Category | Examples | Data Shape | Consistency | Scaling | Sweet Spot |
|---|---|---|---|---|---|
| Relational | PostgreSQL, MySQL, SQL Server | Tables, rows, FKs | Strong (ACID) | Vertical, read replicas, some sharding | Transactions, structured data, reporting |
| Document | MongoDB | JSON/BSON docs | Tunable | Horizontal (sharding) | Flexible schema, content, catalogs |
| Key-Value | Redis | key -> value/structure | Mostly strong on single node | Cluster sharding | Caching, sessions, queues, counters |
| Wide-Column | Cassandra | Partitioned rows, column families | Tunable (eventual default) | Linear horizontal | Massive write throughput, time series, IoT |
| Graph | Neo4j | Nodes, relationships, properties | ACID | Vertical + clustering/fabric | Connected data, fraud, recommendations |
| Vector | Pinecone, Milvus, Qdrant | High-dim embeddings + metadata | Mostly eventual | Horizontal | Semantic search, RAG, recommendations |
| Analytical (OLAP) | Snowflake, BigQuery, Redshift, ClickHouse | Columnar facts/dims | Varies | Massive horizontal | BI, aggregates, data warehouse |

**Workload types**

| Type | Characteristics | Typical Model |
|---|---|---|
| OLTP | Many small reads/writes, low latency, high concurrency | 3NF |
| OLAP | Few large scans/aggregations, read heavy | Star/Snowflake, OBT |
| Hybrid (HTAP) | Both on same engine | e.g. SQL Server columnstore, TiDB, SingleStore |
| Streaming/Event | Append-only, ordered events | Kafka + Cassandra / lakehouse |
| AI/Retrieval | Similarity search on embeddings | Vector DB or pgvector |

**Polyglot persistence**: Use several specialized stores in one architecture (e.g. PostgreSQL as system of record, Redis as cache, Vector DB for RAG, warehouse for analytics).

---

## 2. Core Concepts

### ACID (transactions)
- **A**tomicity: all or nothing
- **C**onsistency: constraints always hold after a transaction
- **I**solation: concurrent transactions do not corrupt each other
- **D**urability: committed data survives crashes (WAL/redo log)

### Isolation levels (SQL standard)

| Level | Dirty Read | Non-repeatable Read | Phantom |
|---|---|---|---|
| READ UNCOMMITTED | Possible | Possible | Possible |
| READ COMMITTED | No | Possible | Possible |
| REPEATABLE READ | No | No | Possible (PG: no, uses snapshots) |
| SERIALIZABLE | No | No | No |

Defaults: PostgreSQL = READ COMMITTED, MySQL InnoDB = REPEATABLE READ, SQL Server = READ COMMITTED (optionally RCSI/Snapshot).

### BASE (many NoSQL systems)
Basically Available, Soft state, Eventually consistent.

### CAP theorem
In a network partition you choose **C**onsistency or **A**vailability. (PACELC extends this: else, latency vs consistency.)
- CP-leaning: MongoDB (default primary reads), HBase, Redis Cluster (best effort), Neo4j cluster
- AP-leaning: Cassandra (tunable), DynamoDB (default)

### Index types (general)

| Index | Best For |
|---|---|
| B-tree | Equality + range, default in most RDBMS |
| Hash | Equality only |
| Bitmap | Low-cardinality columns (warehouses) |
| GIN / inverted | Full text, JSON, arrays |
| GiST / R-tree | Geospatial, ranges |
| LSM-tree (SSTables) | Write-heavy stores (Cassandra) |
| HNSW / IVF / PQ | Vector similarity (ANN) |
| Columnstore | Analytical scans |

### Replication & partitioning vocabulary
- **Replication**: copies for HA/read scaling (sync, async, quorum)
- **Sharding**: horizontal split across nodes by a shard key
- **Partitioning**: splitting a table within one node/cluster (range, list, hash)
- **Consistent hashing**: distributes keys with minimal movement on node changes

---

## 3. Relational Databases (RDBMS)

### 3.1 Universal SQL Cheat Sheet

```sql
-- DDL
CREATE TABLE customers (
  customer_id  BIGINT PRIMARY KEY,
  email        VARCHAR(255) NOT NULL UNIQUE,
  created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE customers ADD COLUMN country CHAR(2);
CREATE INDEX idx_customers_country ON customers(country);
DROP TABLE IF EXISTS tmp_table;

-- DML
INSERT INTO customers (customer_id, email) VALUES (1, 'a@x.com');
UPDATE customers SET country = 'DE' WHERE customer_id = 1;
DELETE FROM customers WHERE customer_id = 1;

-- Query patterns
SELECT c.country, COUNT(*) AS n
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
WHERE c.created_at >= DATE '2025-01-01'
GROUP BY c.country
HAVING COUNT(*) > 10
ORDER BY n DESC
LIMIT 20;

-- CTE + window function
WITH ranked AS (
  SELECT order_id, customer_id, amount,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn
  FROM orders
)
SELECT * FROM ranked WHERE rn = 1;

-- Transactions
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;  -- or ROLLBACK;
```

**Join types**: INNER, LEFT, RIGHT, FULL OUTER, CROSS, SELF, LATERAL (PG) / CROSS APPLY (SQL Server).

**Logical processing order**: FROM -> WHERE -> GROUP BY -> HAVING -> SELECT -> DISTINCT -> ORDER BY -> LIMIT/TOP.

### 3.2 PostgreSQL

**Architecture**
- Process-per-connection model (postmaster forks backends). Use **PgBouncer**/pgcat for pooling.
- **MVCC** with tuple versions in heap; **VACUUM/autovacuum** reclaims dead tuples.
- **WAL** (write-ahead log) for durability, replication, PITR.
- Shared buffers + OS page cache. Extensible via extensions.
- Replication: streaming (physical), logical (publication/subscription).

**Strengths**
- Standards compliant, rich types (JSONB, arrays, ranges, hstore, geometric, UUID)
- Extensions: PostGIS, pgvector, TimescaleDB, Citus, pg_trgm, pg_partman
- Advanced indexing (B-tree, GIN, GiST, BRIN, SP-GiST, hash)
- Strong SQL features: CTEs, window functions, partial/expression indexes, row-level security

**Weaknesses**
- Connection overhead (needs pooler)
- Vacuum tuning and table bloat at scale
- Native horizontal write scaling needs Citus or similar
- Major version upgrades need pg_upgrade/logical replication planning

**Essential commands**

```bash
psql -h host -U user -d db         # connect
pg_dump -Fc db > db.dump           # logical backup (custom format)
pg_restore -d db db.dump           # restore
pg_basebackup -D /backup -Ft -z -P # physical base backup
```

```sql
\l  \c dbname  \dt  \d tablename  \du  \dx  \timing  \x   -- psql meta-commands

EXPLAIN (ANALYZE, BUFFERS) SELECT ...;     -- real execution plan
VACUUM (ANALYZE, VERBOSE) mytable;
REINDEX TABLE mytable;
CREATE INDEX CONCURRENTLY idx ON t(col);    -- no write lock

-- JSONB
SELECT data->>'name' FROM docs WHERE data @> '{"active": true}';
CREATE INDEX idx_docs_gin ON docs USING GIN (data jsonb_path_ops);

-- Partitioning
CREATE TABLE events (id BIGINT, ts TIMESTAMPTZ, payload JSONB)
  PARTITION BY RANGE (ts);
CREATE TABLE events_2026_01 PARTITION OF events
  FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

-- Row level security
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_iso ON orders USING (tenant_id = current_setting('app.tenant')::int);

-- Vector (pgvector)
CREATE EXTENSION vector;
CREATE TABLE items (id BIGSERIAL PRIMARY KEY, embedding vector(1536));
CREATE INDEX ON items USING hnsw (embedding vector_cosine_ops);
SELECT id FROM items ORDER BY embedding <=> '[...]' LIMIT 5;

-- Diagnostics
SELECT * FROM pg_stat_activity WHERE state <> 'idle';
SELECT * FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 10;
```

**Key config knobs**: `shared_buffers` (~25% RAM), `work_mem`, `maintenance_work_mem`, `effective_cache_size` (~50-75% RAM), `max_connections`, `wal_level`, `checkpoint_timeout`, `autovacuum_*`, `random_page_cost` (lower for SSD).

**Best practices**
- Always use a connection pooler (transaction mode) for many clients
- Use `timestamptz`, `bigint`/identity or UUIDv7, `numeric` for money
- Index foreign keys; use partial/covering (`INCLUDE`) indexes
- Monitor bloat, long transactions, replication lag; tune autovacuum per hot table
- Partition large time-based tables; archive/detach old partitions
- Use `EXPLAIN (ANALYZE, BUFFERS)` before and after tuning
- Backups: base backup + WAL archiving (pgBackRest, Barman) and **test restores**

**Use cases**: General OLTP, geospatial (PostGIS), JSON + relational hybrid, analytics at moderate scale, AI/RAG with pgvector, SaaS multi-tenant.

### 3.3 MySQL (and MariaDB)

**Architecture**
- Thread-per-connection, pluggable storage engines. **InnoDB** (default): ACID, row-level locking, MVCC, clustered primary key index.
- Redo log + undo log + binlog. Binlog drives replication (async, semi-sync, Group Replication, InnoDB Cluster).
- Clustered index: rows stored in PK order; secondary indexes store PK values (keep PKs small).

**Strengths**
- Very widely deployed, mature tooling, simple to operate
- Fast reads for simple workloads; strong replication ecosystem
- Managed everywhere (RDS, Aurora MySQL, Cloud SQL, PlanetScale/Vitess for sharding)

**Weaknesses**
- Historically weaker SQL feature set (improved in 8.x: CTEs, window functions)
- Fewer index types and data types than PostgreSQL
- Optimizer can struggle with complex joins/subqueries
- Behavior differences with SQL modes; silent coercion if not strict

**Essential commands**

```bash
mysql -h host -u user -p db
mysqldump --single-transaction --routines --triggers db > db.sql
mysqlpump / mydumper                # parallel logical backups
xtrabackup --backup                 # physical hot backup (Percona)
```

```sql
SHOW DATABASES;  SHOW TABLES;  DESCRIBE t;  SHOW CREATE TABLE t;
SHOW PROCESSLIST;  SHOW ENGINE INNODB STATUS\G
SHOW INDEX FROM t;
EXPLAIN ANALYZE SELECT ...;           -- 8.0.18+
ANALYZE TABLE t;  OPTIMIZE TABLE t;

CREATE TABLE orders (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  customer_id BIGINT UNSIGNED NOT NULL,
  total DECIMAL(12,2) NOT NULL,
  created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_cust_created (customer_id, created_at),
  CONSTRAINT fk_cust FOREIGN KEY (customer_id) REFERENCES customers(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Upsert
INSERT INTO t (id, v) VALUES (1, 'a') ON DUPLICATE KEY UPDATE v = VALUES(v);

-- Replication status
SHOW REPLICA STATUS\G         -- (SHOW SLAVE STATUS in older versions)
```

**Key config knobs**: `innodb_buffer_pool_size` (50-75% RAM), `innodb_flush_log_at_trx_commit` (1 = durable), `sync_binlog`, `max_connections`, `innodb_log_file_size`/`innodb_redo_log_capacity`, `slow_query_log`, `long_query_time`.

**Best practices**
- Use `utf8mb4`, strict `sql_mode` (`STRICT_TRANS_TABLES,ONLY_FULL_GROUP_BY`)
- Short, monotonic PKs (BIGINT auto-increment or ordered UUID) for clustered index efficiency
- Composite indexes follow leftmost-prefix rule; check with `EXPLAIN`
- Enable slow query log + `pt-query-digest`/Performance Schema
- Avoid `SELECT *`, large `OFFSET`, and long transactions
- Use GTID-based replication; monitor lag

**Use cases**: Web apps (LAMP/LEMP), e-commerce, CMS (WordPress), read-heavy OLTP, sharded SaaS via Vitess.

### 3.4 Microsoft SQL Server

**Architecture**
- Single process, SQLOS scheduler (cooperative threads). Pages (8 KB) in extents; buffer pool.
- Transaction log (WAL) with recovery models: SIMPLE, FULL, BULK_LOGGED.
- Locking-based concurrency by default; **RCSI / Snapshot isolation** for row versioning (tempdb or ADR).
- Clustered index (table = B-tree) vs heap; nonclustered indexes; **columnstore** for analytics.
- HA/DR: Always On Availability Groups, Failover Cluster Instances, log shipping.
- Ecosystem: SSIS (ETL), SSRS (reporting), SSAS (OLAP/tabular), Azure SQL, Fabric.

**Strengths**
- Excellent tooling (SSMS, Azure Data Studio), query optimizer, Query Store
- Strong enterprise features: TDE, Always Encrypted, row-level security, dynamic data masking
- Deep Microsoft/.NET/Azure/Power BI integration
- Mixed OLTP + analytics (in-memory OLTP, columnstore)

**Weaknesses**
- Licensing cost (core-based); edition feature gating
- Historically Windows-centric (Linux supported since 2017)
- T-SQL proprietary dialect reduces portability
- Locking/blocking and tempdb contention need tuning

**Essential commands (T-SQL)**

```sql
-- Basics
SELECT TOP (10) * FROM dbo.Orders ORDER BY OrderDate DESC;
SELECT name FROM sys.databases;
EXEC sp_help 'dbo.Orders';
EXEC sp_who2;                      -- sessions (or sys.dm_exec_requests)

-- Table with identity, columnstore
CREATE TABLE dbo.Orders (
  OrderId BIGINT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
  CustomerId INT NOT NULL,
  OrderDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  Total DECIMAL(12,2) NOT NULL
);
CREATE NONCLUSTERED INDEX IX_Orders_Cust ON dbo.Orders(CustomerId) INCLUDE (Total);
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_Orders ON dbo.Orders(OrderDate, Total);

-- MERGE upsert
MERGE dbo.Target AS t USING dbo.Source AS s ON t.Id = s.Id
WHEN MATCHED THEN UPDATE SET t.Val = s.Val
WHEN NOT MATCHED THEN INSERT (Id, Val) VALUES (s.Id, s.Val);

-- Error handling
BEGIN TRY
  BEGIN TRAN; /* work */ COMMIT;
END TRY
BEGIN CATCH
  IF @@TRANCOUNT > 0 ROLLBACK; THROW;
END CATCH;

-- Query Store, plans, waits
ALTER DATABASE MyDb SET QUERY_STORE = ON;
SET STATISTICS IO, TIME ON;
SELECT * FROM sys.dm_os_wait_stats ORDER BY wait_time_ms DESC;
SELECT * FROM sys.dm_db_missing_index_details;

-- Snapshot isolation
ALTER DATABASE MyDb SET READ_COMMITTED_SNAPSHOT ON;

-- Backup / restore
BACKUP DATABASE MyDb TO DISK = 'D:\bk\MyDb.bak' WITH COMPRESSION, CHECKSUM;
BACKUP LOG MyDb TO DISK = 'D:\bk\MyDb.trn';
RESTORE DATABASE MyDb FROM DISK = 'D:\bk\MyDb.bak' WITH NORECOVERY;
```

**Best practices**
- Choose recovery model by RPO; schedule full + diff + log backups; `CHECKSUM` and `RESTORE VERIFYONLY`/test restores
- Enable RCSI to reduce reader/writer blocking
- Use Query Store to find regressions and force plans
- Avoid parameter-sniffing surprises (`OPTION (RECOMPILE)`, `OPTIMIZE FOR`, PSP optimization)
- Multiple equally sized tempdb files; set max server memory
- Maintain statistics and indexes (smart reindex/reorganize thresholds)
- Use least-privilege roles, TDE, auditing

**Use cases**: Enterprise line-of-business apps, ERP/CRM backends, .NET apps, data warehousing with SSIS/SSAS/Power BI, regulated environments.

### 3.5 RDBMS Comparison

| Feature | PostgreSQL | MySQL (InnoDB) | SQL Server |
|---|---|---|---|
| License | Open source (PostgreSQL) | GPL + commercial (Oracle) | Commercial (free Express/Developer) |
| JSON | JSONB (indexed, rich ops) | JSON type (functional indexes) | JSON functions, `OPENJSON`, native JSON type (newer) |
| Concurrency | MVCC | MVCC | Locking (+ RCSI/Snapshot) |
| Extensions | Very strong | Limited | CLR, limited |
| Replication | Streaming/logical | Binlog/Group Replication | Always On AG |
| Analytics | Good (parallel, partitions) | Basic | Columnstore, strong |
| Vector search | pgvector | Limited/HeatWave | Vector support in newer versions/Azure SQL |
| Typical host | Linux, cloud | Linux, cloud | Windows/Linux, Azure |
| Procedural lang | PL/pgSQL, others | SQL/PSM | T-SQL |

**Normal-form/design rules for all RDBMS**: use surrogate or natural keys deliberately, enforce constraints (PK, FK, UNIQUE, CHECK, NOT NULL), avoid EAV for core data, version migrations (Flyway, Liquibase, Alembic), never store passwords unhashed, parametrize queries (prevent SQL injection).

---

## 4. NoSQL Databases

### 4.1 MongoDB (Document)

**Architecture**
- Stores BSON documents in **collections**; flexible schema (optional JSON Schema validation).
- **Replica set**: 1 primary + secondaries (automatic failover via election). Oplog drives replication.
- **Sharded cluster**: `mongos` routers + config servers + shard replica sets. Shard key choice is critical (hashed vs ranged).
- WiredTiger storage engine (document-level concurrency, compression).
- Consistency knobs: `writeConcern` (`w:1`, `w:"majority"`), `readConcern` (`local`, `majority`, `snapshot`), `readPreference`.
- Multi-document ACID transactions supported (replica sets, sharded clusters), but with overhead.

**Strengths**
- Natural mapping to application objects; fast iteration
- Rich query language, aggregation pipeline, change streams, geospatial, text and Atlas Vector Search
- Horizontal scaling and HA built in

**Weaknesses**
- Joins (`$lookup`) are limited vs RDBMS; data duplication from denormalization
- Poor shard key = hotspots/jumbo chunks
- Schema freedom can lead to inconsistent data without validation
- Large documents (16 MB limit) and unbounded arrays are anti-patterns

**Essential commands (mongosh)**

```javascript
show dbs; use shop; show collections;

// CRUD
db.products.insertOne({ sku: "A1", name: "Pen", price: 2.5, tags: ["office"] });
db.products.insertMany([{...}, {...}]);
db.products.find({ price: { $lt: 10 }, tags: "office" }, { name: 1, _id: 0 })
           .sort({ price: -1 }).limit(10);
db.products.updateOne({ sku: "A1" }, { $set: { price: 3 }, $inc: { stock: -1 } });
db.products.updateOne({ sku: "Z9" }, { $setOnInsert: { name: "New" } }, { upsert: true });
db.products.deleteMany({ discontinued: true });

// Indexes
db.products.createIndex({ sku: 1 }, { unique: true });
db.products.createIndex({ category: 1, price: -1 });          // compound (ESR rule)
db.products.createIndex({ createdAt: 1 }, { expireAfterSeconds: 86400 }); // TTL
db.products.explain("executionStats").find({ sku: "A1" });

// Aggregation pipeline
db.orders.aggregate([
  { $match: { status: "paid", date: { $gte: ISODate("2026-01-01") } } },
  { $unwind: "$items" },
  { $group: { _id: "$items.sku", revenue: { $sum: { $multiply: ["$items.qty", "$items.price"] } } } },
  { $sort: { revenue: -1 } },
  { $limit: 10 },
  { $lookup: { from: "products", localField: "_id", foreignField: "sku", as: "product" } }
]);

// Transactions
const s = db.getMongo().startSession();
s.startTransaction({ writeConcern: { w: "majority" } });
// ... operations using s.getDatabase("shop") ...
s.commitTransaction();

// Replica set & sharding
rs.status(); rs.initiate(); sh.enableSharding("shop");
sh.shardCollection("shop.orders", { customerId: "hashed" });
```

```bash
mongodump --uri="mongodb://..." --out=/backup
mongorestore --uri="mongodb://..." /backup
mongoexport / mongoimport    # JSON/CSV
```

**Modeling rules**
- **Embed** when data is read together, 1:few, and bounded (e.g. address in user)
- **Reference** when 1:many unbounded, data shared, or updated independently
- Patterns: Subset, Bucket (time series), Computed, Extended Reference, Outlier, Schema Versioning
- Design from **access patterns**, not entities

**Best practices**
- ESR index rule: Equality, Sort, Range
- Use `w:"majority"` + `readConcern:"majority"` for critical data
- Avoid unbounded arrays; avoid low-cardinality or monotonically increasing shard keys
- Use schema validation (`$jsonSchema`), projection, covered queries
- Monitor with Atlas/Ops Manager, `db.currentOp()`, profiler

**Use cases**: Content management, product catalogs, user profiles, mobile/web back ends, event logging, IoT (time series collections), real-time analytics, semantic search (Atlas Vector Search).

### 4.2 Redis (Key-Value / In-Memory)

**Architecture**
- In-memory data structure store; **single-threaded command execution** (I/O threads in newer versions) so commands are atomic.
- Persistence: **RDB** snapshots, **AOF** append-only log (fsync policy: `always`/`everysec`/`no`), or both.
- HA: **Replication** (async), **Sentinel** (failover), **Redis Cluster** (16384 hash slots, sharding).
- Eviction policies when `maxmemory` hit: `allkeys-lru`, `volatile-lru`, `allkeys-lfu`, `noeviction`, etc.

**Strengths**
- Sub-millisecond latency, extremely high throughput
- Rich structures: strings, hashes, lists, sets, sorted sets, streams, bitmaps, HyperLogLog, geo, JSON/search/vector (modules/Redis Stack)
- Simple, atomic operations; pub/sub; Lua scripting

**Weaknesses**
- Dataset limited by RAM (cost)
- Persistence is secondary; possible data loss window with async replication/AOF everysec
- Limited querying/relationships; multi-key ops in Cluster need same hash slot
- Licensing changes: Redis (7.4+) is source-available; Valkey is the open-source fork

**Essential commands**

```bash
redis-cli -h host -p 6379 -a pass

# Strings & expiry
SET user:1:name "Ada" EX 3600 NX       # set with TTL only if not exists
GET user:1:name ; DEL key ; EXISTS key ; TTL key ; EXPIRE key 60
INCR page:views ; INCRBY cart:1 5

# Hashes
HSET user:1 name Ada age 36 ; HGET user:1 name ; HGETALL user:1

# Lists (queues)
LPUSH q job1 ; RPOP q ; BRPOP q 5 ; LRANGE q 0 -1

# Sets / Sorted sets (leaderboards)
SADD tags:1 a b c ; SMEMBERS tags:1 ; SINTER s1 s2
ZADD leaderboard 100 alice 250 bob ; ZREVRANGE leaderboard 0 9 WITHSCORES ; ZINCRBY leaderboard 10 alice

# Streams (event log / consumer groups)
XADD events * type click user 1
XGROUP CREATE events grp1 $ MKSTREAM
XREADGROUP GROUP grp1 c1 COUNT 10 BLOCK 5000 STREAMS events >
XACK events grp1 <id>

# Pub/Sub, transactions, scripting
PUBLISH chan "hi" ; SUBSCRIBE chan
MULTI ; INCR a ; INCR b ; EXEC
EVAL "return redis.call('GET', KEYS[1])" 1 mykey

# Ops
INFO memory ; INFO replication ; DBSIZE ; SCAN 0 MATCH user:* COUNT 100   # never KEYS * in prod
MEMORY USAGE key ; SLOWLOG GET 10 ; CONFIG SET maxmemory-policy allkeys-lru
BGSAVE ; BGREWRITEAOF ; MONITOR (debug only)
```

**Caching patterns**
- **Cache-aside** (lazy loading): app checks cache, falls back to DB, then populates cache. Most common.
- **Read-through / Write-through / Write-behind**: cache layer handles DB I/O.
- Prevent **cache stampede**: TTL jitter, locking (`SET NX`), request coalescing, probabilistic early refresh.
- **Distributed lock**: `SET lock:x token NX PX 30000` + safe release via Lua (consider Redlock caveats).
- **Rate limiting**: `INCR` + `EXPIRE`, sliding window with sorted sets, token bucket via Lua.

**Best practices**
- Always set TTLs on cache keys; use key naming convention `service:entity:id`
- Set `maxmemory` and an eviction policy; monitor hit ratio and evictions
- Use `SCAN`, pipelining, and avoid big keys/hot keys; use hash tags `{tag}` for multi-key ops in Cluster
- Secure: `requirepass`/ACLs, TLS, never expose to the internet
- Don't treat Redis as the only copy of important data unless persistence + replication are configured and tested

**Use cases**: Caching, session storage, rate limiting, leaderboards, queues/streams, real-time counters, pub/sub, feature flags, semantic caching for LLMs.

### 4.3 Apache Cassandra (Wide-Column)

**Architecture**
- Masterless **ring** of nodes; data distributed by **partition key** hash (consistent hashing, vnodes).
- Write path: commit log + memtable -> flushed to immutable **SSTables** (LSM-tree); background **compaction**; **tombstones** mark deletes.
- Replication via **keyspace replication factor (RF)** and strategy (`NetworkTopologyStrategy`).
- **Tunable consistency** per query: `ONE`, `QUORUM`, `LOCAL_QUORUM`, `ALL`. Rule: `R + W > RF` gives strong consistency.
- Gossip protocol, hinted handoff, read repair, anti-entropy repair (`nodetool repair`).
- **Table = query**: model one table per query pattern; no joins, limited secondary indexes.

**Strengths**
- Linear horizontal scalability, no single point of failure
- Very high write throughput, multi-datacenter replication
- Predictable low latency at scale

**Weaknesses**
- No joins, limited ad hoc queries/aggregations; must know queries upfront
- Denormalization = duplicated data, application-managed consistency
- Operational complexity (repair, compaction, tombstones, GC)
- Lightweight transactions (LWT) are slow; no multi-partition ACID (though Accord/CEP-15 transactions are arriving in 5.x)

**Essential commands (CQL)**

```sql
CREATE KEYSPACE shop WITH replication =
  {'class': 'NetworkTopologyStrategy', 'dc1': 3};
USE shop;

-- Partition key (user_id) + clustering columns (ts) for ordered rows in partition
CREATE TABLE events_by_user (
  user_id uuid,
  ts timestamp,
  event_id timeuuid,
  type text,
  payload text,
  PRIMARY KEY ((user_id), ts, event_id)
) WITH CLUSTERING ORDER BY (ts DESC, event_id DESC)
  AND default_time_to_live = 7776000
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};

INSERT INTO events_by_user (user_id, ts, event_id, type) VALUES (?, ?, now(), 'click') USING TTL 86400;
SELECT * FROM events_by_user WHERE user_id = ? AND ts > '2026-01-01' LIMIT 100;
UPDATE t SET v = 'x' WHERE id = 1 IF EXISTS;        -- LWT (use sparingly)
CONSISTENCY LOCAL_QUORUM;                             -- cqlsh
-- Batches: only for atomicity within ONE partition (logged batch across partitions is an anti-pattern for speed)
```

```bash
cqlsh host 9042 -u user -p pass
nodetool status           # ring health (UN = Up/Normal)
nodetool repair -pr       # primary range repair
nodetool compactionstats ; nodetool tpstats ; nodetool tablestats ks.table
nodetool snapshot -t name ks ; nodetool flush ; nodetool decommission
```

**Best practices**
- Choose partition keys for even distribution; keep partitions < ~100 MB / 100k cells; bucket time-series (e.g. `(user_id, day)`)
- Avoid `ALLOW FILTERING`, large `IN`, secondary indexes on high-cardinality columns, unbounded partitions, and heavy deletes/tombstones
- Prefer TTLs with TWCS compaction for time series
- RF=3 per DC, `LOCAL_QUORUM` read+write for common strong-ish consistency
- Run repairs regularly (or use Reaper); monitor pending compactions, dropped messages, GC pauses

**Use cases**: IoT/telemetry, time series, messaging/activity feeds, product catalogs at scale, fraud event stores, always-on multi-region apps (Netflix, Apple, Discord-style workloads). Alternatives: ScyllaDB (C++ compatible), DynamoDB, HBase.

### 4.4 Neo4j (Graph)

**Architecture**
- **Labeled property graph**: nodes (with labels), relationships (typed, directed), properties on both.
- **Index-free adjacency**: relationships are physical pointers, so traversals cost proportional to the visited subgraph, not total data size.
- ACID transactions; **causal clustering** (Raft-based primaries + secondaries); Infinigraph/Fabric for sharding in newer versions.
- Query language: **Cypher** (declarative, ASCII-art patterns); GQL (ISO standard) alignment.
- Graph Data Science (GDS) library: PageRank, community detection, node similarity, embeddings; vector indexes available.

**Strengths**
- Multi-hop relationship queries that are slow/complex in SQL joins
- Intuitive modeling of connected domains; schema-flexible
- Strong analytics algorithms; GraphRAG for LLMs

**Weaknesses**
- Not ideal for bulk aggregations or simple tabular workloads
- Horizontal write scaling/sharding is harder than in Cassandra/MongoDB
- Supernodes (millions of relationships) hurt performance
- Smaller talent pool; licensing (Community vs Enterprise features)

**Essential commands (Cypher)**

```cypher
// Create
CREATE (a:Person {name:'Ada', born:1815})-[:KNOWS {since:2020}]->(b:Person {name:'Bob'});
MERGE (p:Person {id: $id}) ON CREATE SET p.created = timestamp() ON MATCH SET p.seen = timestamp();

// Read / pattern matching
MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.name = 'Ada' RETURN f.name;
MATCH (p:Person {name:'Ada'})-[:KNOWS*1..3]->(fof) RETURN DISTINCT fof.name;   // variable length
MATCH path = shortestPath((a:Person {name:'Ada'})-[:KNOWS*..6]-(b:Person {name:'Zed'})) RETURN path;

// Recommendation: friends-of-friends not yet friends
MATCH (me:Person {id:$id})-[:KNOWS]->()-[:KNOWS]->(rec)
WHERE NOT (me)-[:KNOWS]->(rec) AND me <> rec
RETURN rec.name, count(*) AS mutual ORDER BY mutual DESC LIMIT 10;

// Update / delete
MATCH (p:Person {name:'Ada'}) SET p.title = 'Countess';
MATCH (p:Person {name:'Bob'}) DETACH DELETE p;

// Schema
CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE;
CREATE INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.name);
CREATE VECTOR INDEX docs_idx IF NOT EXISTS FOR (d:Doc) ON (d.embedding)
  OPTIONS {indexConfig: {`vector.dimensions`: 1536, `vector.similarity_function`: 'cosine'}};

// Diagnostics
EXPLAIN / PROFILE MATCH ... ;     SHOW INDEXES;  SHOW CONSTRAINTS;
// Bulk load
LOAD CSV WITH HEADERS FROM 'file:///people.csv' AS row
MERGE (:Person {id: row.id, name: row.name});
```

```bash
cypher-shell -a neo4j://host:7687 -u neo4j -p pass
neo4j-admin database dump neo4j --to-path=/backup
neo4j-admin database import full ...      # fast offline bulk import
```

**Best practices**
- Model from questions: nodes = nouns, relationships = verbs; use specific relationship types and direction
- Always anchor queries with an indexed/unique starting node; use `PROFILE` to check `db hits`
- Avoid supernodes (introduce intermediate nodes/buckets), unbounded variable-length paths, and Cartesian products
- Use parameters (`$param`) for plan caching; `MERGE` only on unique keys
- Batch writes with `UNWIND` / `CALL {} IN TRANSACTIONS`

**Use cases**: Fraud detection/rings, recommendation engines, social networks, knowledge graphs, identity & access graphs, network/IT topology, supply chain, GraphRAG, data lineage graphs.

### 4.5 NoSQL Comparison

| | MongoDB | Redis | Cassandra | Neo4j |
|---|---|---|---|---|
| Model | Document | Key-value + structures | Wide-column | Property graph |
| Query | MQL + aggregation | Commands | CQL (restricted) | Cypher |
| Scale | Sharding | Cluster slots | Ring, linear | Vertical + clustering |
| Consistency | Tunable, ACID txns | Strong per node, async replicas | Tunable | ACID |
| Storage | Disk (WiredTiger) | RAM (+persist) | Disk (LSM) | Disk (native graph) |
| Latency | ms | sub-ms | low ms | ms (traversals) |
| Best at | Flexible app data | Speed/caching | Write-heavy at scale | Relationships |
| Avoid for | Heavy relational joins | Large cold datasets | Ad hoc queries | Bulk aggregation |

---

## 5. Vector Databases

### 5.1 Fundamentals

- **Embedding**: numeric vector (e.g. 384, 768, 1536, 3072 dims) from a model representing semantic meaning of text, images, audio, code.
- **Similarity metrics**: **cosine** (direction; most text models), **dot product** (when normalized or magnitude matters), **Euclidean/L2** (distance).
- **ANN (Approximate Nearest Neighbor)** trades small recall loss for large speedups.

| Index | How it works | Pros | Cons |
|---|---|---|---|
| **Flat (brute force)** | Compare against all vectors | 100% recall | O(n), slow at scale |
| **HNSW** | Multi-layer navigable small-world graph | High recall, fast, popular default | High RAM, slower builds |
| **IVF / IVF-PQ** | Cluster (centroids), search nearest lists; PQ compresses | Lower memory, scalable | Needs training, tuning `nprobe` |
| **DiskANN / Vamana** | Graph optimized for SSD | Billion-scale on disk | More latency than RAM |
| **Scalar/Product/Binary Quantization** | Compress vectors (int8, PQ, 1-bit) | 4x-32x memory savings | Some recall loss (rescore to recover) |

**Key tuning knobs**: HNSW `M` (links per node), `efConstruction` (build quality), `ef`/`ef_search` (query recall vs speed); IVF `nlist`, `nprobe`.

### 5.2 RAG (Retrieval-Augmented Generation) reference flow

```
INGEST:  Documents -> Clean/parse -> Chunk (300-1000 tokens, 10-20% overlap) -> Embed -> Upsert(vector + metadata + text/ID)
QUERY:   User question -> (rewrite/expand) -> Embed -> Vector search (top-k, metadata filter)
         -> [Hybrid: + BM25/sparse] -> Rerank (cross-encoder) -> Build prompt with context -> LLM -> Answer + citations
```

**RAG best practices**
- Chunk by semantic structure (headings, paragraphs); keep metadata (source, section, date, ACL, tenant)
- Use the **same embedding model** for indexing and querying; store model name/version
- **Hybrid search** (dense + sparse/BM25) + **reranking** typically beats pure vector search
- Use metadata **pre-filtering** for access control/multi-tenancy; never rely on the LLM for permissions
- Evaluate: recall@k, MRR/nDCG, faithfulness, answer relevance (RAGAS, TruLens); build a golden test set
- Re-embed on model change (blue/green collections); handle updates/deletes (stale chunk cleanup)
- Cache frequent queries (semantic cache); consider parent-child / small-to-big retrieval, HyDE, query decomposition, GraphRAG for relational questions

### 5.3 Pinecone (managed, proprietary)

**Architecture**: Fully managed serverless (and pod-based legacy) service; **indexes** contain **namespaces** (logical partitions, great for multi-tenancy). Supports dense and sparse vectors (hybrid), metadata filtering, integrated embedding/reranking inference, and bulk import from object storage. Separation of storage and compute in serverless.

**Pros**
- Zero infrastructure management, fast time to production, auto-scaling
- Namespaces for cheap multi-tenancy; good SDKs and ecosystem integrations (LangChain, LlamaIndex)
- Consistent low latency at large scale; built-in hybrid and reranking options

**Cons**
- Proprietary and cloud-only (BYOC/private options limited); vendor lock-in
- Cost can grow with high volume/QPS; less low-level control over index internals
- Data residency/compliance depends on available regions

**Commands (Python SDK)**

```python
from pinecone import Pinecone, ServerlessSpec
pc = Pinecone(api_key="...")

pc.create_index(name="docs", dimension=1536, metric="cosine",
                spec=ServerlessSpec(cloud="aws", region="us-east-1"))
index = pc.Index("docs")

index.upsert(vectors=[
    {"id": "doc1#0", "values": vec, "metadata": {"source": "faq.pdf", "tenant": "acme", "year": 2026}}
], namespace="acme")

res = index.query(vector=qvec, top_k=5, namespace="acme", include_metadata=True,
                  filter={"year": {"$gte": 2025}, "source": {"$in": ["faq.pdf"]}})

index.fetch(ids=["doc1#0"], namespace="acme")
index.delete(ids=["doc1#0"], namespace="acme")          # or delete(delete_all=True, namespace=...)
index.describe_index_stats()
```

**Use cases**: Production RAG without ops burden, SaaS multi-tenant semantic search, recommendations, startups needing speed to market.

### 5.4 Milvus (open source, distributed; Zilliz Cloud managed)

**Architecture**: Cloud-native, disaggregated: access layer (proxy), coordinators, query/data/index nodes, with **object storage** (S3/MinIO), **message queue/log broker** (Pulsar/Kafka/Woodpecker), and metadata store (etcd). Deployment modes: **Milvus Lite** (embedded/Python), **Standalone**, **Distributed (Kubernetes)**. Rich index support: FLAT, IVF_FLAT, IVF_SQ8, IVF_PQ, HNSW, DiskANN, GPU indexes (CAGRA), sparse indexes; hybrid multi-vector search; partitions and partition keys.

**Pros**
- Designed for billion-scale; many index types incl. GPU acceleration
- Open source (Apache 2.0), flexible deployment, strong hybrid/multi-vector search
- Compute/storage separation and independent scaling

**Cons**
- Operational complexity of the distributed deployment (multiple components)
- Heavier footprint than Qdrant for small workloads
- Steeper learning curve for index/parameter tuning

**Commands (pymilvus)**

```python
from pymilvus import MilvusClient, DataType
client = MilvusClient("http://localhost:19530")      # or "./local.db" for Milvus Lite

schema = client.create_schema(auto_id=False, enable_dynamic_field=True)
schema.add_field("id", DataType.INT64, is_primary=True)
schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=768)
schema.add_field("category", DataType.VARCHAR, max_length=64)

index_params = client.prepare_index_params()
index_params.add_index(field_name="embedding", index_type="HNSW", metric_type="COSINE",
                       params={"M": 16, "efConstruction": 200})

client.create_collection("docs", schema=schema, index_params=index_params)
client.insert("docs", [{"id": 1, "embedding": vec, "category": "faq", "text": "..."}])

hits = client.search("docs", data=[qvec], limit=5, filter='category == "faq"',
                     output_fields=["text"], search_params={"params": {"ef": 64}})
client.query("docs", filter="id in [1,2]", output_fields=["text"])
client.delete("docs", ids=[1])
client.create_partition("docs", "tenant_a")
```

**Use cases**: Very large-scale semantic search, multimodal (image/video) search, recommendation at scale, self-hosted/regulated environments, GPU-accelerated retrieval.

### 5.5 Qdrant (open source, Rust)

**Architecture**: Written in Rust; **collections** of **points** (id + vector(s) + JSON **payload**). HNSW index with filter-aware graph traversal; supports **named/multiple vectors**, sparse vectors, multivectors (late interaction/ColBERT), quantization (scalar, product, binary), on-disk vectors/payloads, **snapshots**, sharding + replication (Raft for cluster metadata), and a Query API for hybrid/fusion (RRF) and reranking. Qdrant Cloud and hybrid/private cloud options.

**Pros**
- Excellent filtered-search performance and rich payload filtering
- Lightweight, fast, easy to self-host (single Docker container) and scale to a cluster
- Strong quantization options, great developer experience; Apache 2.0

**Cons**
- Smaller ecosystem than Pinecone/Milvus (growing quickly)
- Distributed cluster operations still need planning (shard count fixed unless resharding)
- Less GPU-focused tooling than Milvus

**Commands (qdrant-client)**

```python
from qdrant_client import QdrantClient, models
client = QdrantClient(url="http://localhost:6333")

client.create_collection(
    collection_name="docs",
    vectors_config=models.VectorParams(size=1536, distance=models.Distance.COSINE),
    hnsw_config=models.HnswConfigDiff(m=16, ef_construct=200),
    quantization_config=models.ScalarQuantization(
        scalar=models.ScalarQuantizationConfig(type=models.ScalarType.INT8, always_ram=True)),
)
client.create_payload_index("docs", field_name="tenant", field_schema="keyword")   # index filter fields!

client.upsert("docs", points=[
    models.PointStruct(id=1, vector=vec, payload={"tenant": "acme", "source": "faq.pdf", "text": "..."})
])

hits = client.query_points(
    "docs", query=qvec, limit=5, with_payload=True,
    query_filter=models.Filter(must=[models.FieldCondition(key="tenant", match=models.MatchValue(value="acme"))]),
    search_params=models.SearchParams(hnsw_ef=128, exact=False),
).points

client.delete("docs", points_selector=models.PointIdsList(points=[1]))
client.create_snapshot("docs")
```

```bash
docker run -p 6333:6333 -p 6334:6334 -v $(pwd)/qdrant_storage:/qdrant/storage qdrant/qdrant
# REST: http://localhost:6333  | gRPC: 6334 | Dashboard: http://localhost:6333/dashboard
```

**Use cases**: RAG with heavy metadata filtering, multi-tenant search, recommendation APIs (positive/negative examples), edge/on-prem deployments, hybrid dense+sparse search.

### 5.6 Vector Store Comparison

| | Pinecone | Milvus | Qdrant | pgvector (PostgreSQL) |
|---|---|---|---|---|
| Hosting | Managed only | Self-host / Zilliz Cloud | Self-host / Qdrant Cloud | Any PostgreSQL |
| License | Proprietary | Apache 2.0 | Apache 2.0 | PostgreSQL |
| Language | n/a | Go/C++ | Rust | C |
| Scale | Very high | Billions | Hundreds of millions to billions | Millions to tens of millions |
| Indexes | Proprietary | HNSW, IVF*, DiskANN, GPU, sparse | HNSW + quantization | HNSW, IVFFlat |
| Hybrid search | Yes (sparse+dense) | Yes (multi-vector, BM25) | Yes (sparse, RRF) | Via tsvector + manual fusion |
| Filtering | Metadata | Scalar filters, partition key | Payload filters (strong) | Full SQL |
| Ops effort | Lowest | Highest (distributed) | Low-medium | Low (if you have PG) |
| Pick when | Speed to prod, no ops | Massive scale/GPU | Filter-heavy, efficient | Already on PostgreSQL, moderate scale |

**Other options**: Weaviate, Chroma (dev/prototyping), Elasticsearch/OpenSearch (hybrid lexical+vector), MongoDB Atlas Vector Search, Redis vector search, FAISS (library, not a DB), LanceDB.

**Vector DB best practices**
- Start simple (pgvector/Chroma) and move to dedicated systems when scale/latency demands
- Choose dimension/model deliberately; Matryoshka embeddings allow truncating dims for savings
- Index metadata fields used in filters; keep payload small, store large text in object store/DB and reference by ID
- Benchmark on **your** data: measure recall vs latency vs cost; tune HNSW params
- Plan for backups/snapshots, versioned collections, and re-indexing
- Secure: API keys/RBAC, encryption, tenant isolation (namespaces/partition keys/payload filters)

---

## 6. Data Modeling & Architecture

### 6.0 Modeling Levels & Layered Architecture

| Level | Purpose | Audience |
|---|---|---|
| Conceptual | Business entities & relationships | Business |
| Logical | Attributes, keys, normalization, platform-independent | Analysts/architects |
| Physical | Tables, types, indexes, partitions, platform-specific | DBAs/engineers |

**Typical warehouse layers** (medallion / classic EDW)

```
Sources -> [Staging/Raw/Bronze] -> [Integration/Core/Silver] -> [Presentation/Marts/Gold] -> BI / ML / APIs
            as-is, immutable        cleansed, conformed,          star schemas, OBT,
                                    historized (3NF / Vault)      aggregates, semantic layer
```

ETL (transform before load) vs ELT (load then transform in warehouse, e.g. dbt) — ELT dominates in modern cloud stacks.

### 6.1 Third Normal Form (3NF) — OLTP

**Normal forms**

| NF | Rule | Fixes |
|---|---|---|
| 1NF | Atomic values, no repeating groups, unique rows | Multi-valued columns |
| 2NF | 1NF + no partial dependency on part of a composite key | Redundancy in composite-key tables |
| 3NF | 2NF + no transitive dependency (non-key depends only on the key) | "The key, the whole key, and nothing but the key" |
| BCNF | Every determinant is a candidate key | Remaining anomalies |
| 4NF/5NF | Multi-valued / join dependencies | Rare in practice |

**Example (denormalized -> 3NF)**

```sql
-- BAD: customer_city repeats; city depends on zip (transitive)
orders(order_id, customer_id, customer_name, zip, city, product_name, price)

-- 3NF
customers(customer_id PK, name, zip_id FK)
zip_codes(zip_id PK, zip, city)
products(product_id PK, name, price)
orders(order_id PK, customer_id FK, order_date)
order_items(order_id FK, product_id FK, quantity, unit_price, PRIMARY KEY(order_id, product_id))
```

**Pros**: minimal redundancy, easy updates (single place), strong integrity, small storage, ideal for write-heavy OLTP.
**Cons**: many joins -> slower analytics, complex queries for end users, schema changes ripple.
**Use cases**: Transactional systems (ERP, banking, e-commerce, CRM), the **integration layer in Inmon-style EDW**.
**Best practices**: enforce FKs and constraints; index FK columns; denormalize selectively only for proven performance needs; keep `unit_price` snapshot in line items (history) instead of joining to a mutable price.

### 6.2 Dimensional Modeling: Star & Snowflake (Kimball vs Inmon)

**Kimball (bottom-up)**: Build business-process-oriented **data marts** with conformed dimensions; the **bus architecture** ties them together. Fast to deliver value.
**Inmon (top-down)**: Build a normalized (3NF) **Corporate Information Factory / enterprise data warehouse** first, then derive dependent data marts. Strong enterprise consistency, slower start.

| | Kimball | Inmon |
|---|---|---|
| Approach | Bottom-up, dimensional | Top-down, normalized EDW |
| Core store | Star schemas / marts with conformed dimensions | 3NF enterprise warehouse -> marts |
| Time to value | Fast | Slower |
| Integration | Via conformed dimensions & bus matrix | Via central normalized model |
| Complexity | Lower | Higher upfront |
| Best when | Analytics-driven, agile teams | Enterprise-wide single source of truth, many consumers |
| Hybrid | Common: Inmon/Vault core + Kimball marts | |

**Star schema**: one central **fact table** (measures + FKs) surrounded by denormalized **dimension tables**.

```
            dim_date
               |
dim_customer - fact_sales - dim_product
               |
            dim_store
```

```sql
CREATE TABLE dim_product (
  product_key INT PRIMARY KEY,            -- surrogate key
  product_id  VARCHAR(20),                -- natural/business key
  name VARCHAR(100), category VARCHAR(50), brand VARCHAR(50),
  valid_from DATE, valid_to DATE, is_current BOOLEAN   -- SCD2 columns
);
CREATE TABLE fact_sales (
  date_key INT, product_key INT, customer_key INT, store_key INT,
  order_number VARCHAR(20),               -- degenerate dimension
  quantity INT, sales_amount DECIMAL(12,2), discount DECIMAL(12,2)
);
-- Typical BI query
SELECT d.year, p.category, SUM(f.sales_amount) AS revenue
FROM fact_sales f
JOIN dim_date d ON d.date_key = f.date_key
JOIN dim_product p ON p.product_key = f.product_key AND p.is_current
GROUP BY d.year, p.category;
```

**Snowflake schema**: dimensions are further normalized (e.g. `dim_product -> dim_category -> dim_department`). Saves storage and eases hierarchy maintenance but adds joins; rarely worth it in columnar cloud warehouses.

| | Star | Snowflake |
|---|---|---|
| Dimensions | Denormalized | Normalized |
| Joins | Fewer | More |
| Query speed/simplicity | Better | Slightly worse |
| Storage/redundancy | More | Less |
| Maintenance of hierarchies | Harder | Easier |

**Kimball Toolkit essentials**
- **Process**: 1) pick business process, 2) declare **grain**, 3) identify dimensions, 4) identify facts.
- **Fact types**: Transaction, Periodic snapshot, Accumulating snapshot, Factless fact.
- **Measure additivity**: additive (sum anywhere), semi-additive (balances, not across time), non-additive (ratios; store numerator/denominator).
- **Dimension types**: Conformed, Role-playing (date as order/ship date), Junk, Degenerate, Mini-dimension, Outrigger, Bridge (many-to-many).
- **Slowly Changing Dimensions (SCD)**

| Type | Behavior | Use |
|---|---|---|
| 0 | Never change | Fixed attributes (birth date) |
| 1 | Overwrite | Corrections, no history needed |
| 2 | New row + validity dates/flag | Full history (most common) |
| 3 | Previous-value column | Limited history ("before/after") |
| 4 | Separate history table | Rapidly changing attributes |
| 6 | Hybrid (1+2+3) | Current + history together |

- **Unknown members**: add `-1 / Unknown` dimension rows so fact FKs are never NULL.

**Pros (dimensional)**: intuitive for business, fast aggregates, BI tool friendly, stable semantic layer.
**Cons**: ETL complexity for SCDs, less flexible with changing sources, redundancy, needs careful grain discipline.
**Best practices**: always declare grain, use integer surrogate keys, conform dimensions across marts, build a date dimension, keep facts narrow (numbers + keys), partition facts by date, consider incremental loads (dbt incremental, MERGE), document the bus matrix.

### 6.3 Data Vault 2.0

**Concept**: Hybrid, insert-only, auditable modeling methodology (Dan Linstedt) for the **raw/integration layer** of an enterprise warehouse. Includes a modeling pattern, **architecture**, **methodology** (agile, sprints), and **implementation** (hash keys, parallel loading).

**Core entities**

| Entity | Contains | Notes |
|---|---|---|
| **Hub** | Unique **business keys** only (+ hash key, load date, record source) | One per business concept (Customer, Product) |
| **Link** | Relationships between hubs (hash of the combined keys) | Many-to-many by default, flexible to cardinality changes |
| **Satellite** | Descriptive attributes and history (hash diff, load date, record source) | Split by source/rate of change; attached to hub or link |
| **Reference/Code tables** | Static lookups | Avoid FK sprawl |
| **PIT (Point-in-Time) table** | Pre-joined snapshot pointers for satellites at given dates | Query performance |
| **Bridge table** | Pre-joined hub/link paths | Query performance |
| **Effectivity / Status / Multi-active satellites** | Relationship validity, deletes, multiple concurrent rows | Special cases |

```sql
CREATE TABLE hub_customer (
  hub_customer_hk CHAR(32) PRIMARY KEY,    -- MD5/SHA of business key (SHA-256 common in 2.0)
  customer_bk     VARCHAR(50) NOT NULL,
  load_dts        TIMESTAMP NOT NULL,
  record_source   VARCHAR(50) NOT NULL
);
CREATE TABLE link_order_customer (
  link_hk CHAR(32) PRIMARY KEY,
  hub_order_hk CHAR(32), hub_customer_hk CHAR(32),
  load_dts TIMESTAMP, record_source VARCHAR(50)
);
CREATE TABLE sat_customer_details (
  hub_customer_hk CHAR(32), load_dts TIMESTAMP,
  hash_diff CHAR(32),                      -- detect attribute changes
  name VARCHAR(100), email VARCHAR(255), country CHAR(2),
  record_source VARCHAR(50), PRIMARY KEY (hub_customer_hk, load_dts)
);
```

**Key principles**
- Insert-only, never update/delete; full audit trail and **source-system traceability** (load date + record source)
- **Hash keys** enable parallel loads without lookups and cross-platform joins
- **Raw Vault** (unaltered source data) vs **Business Vault** (derived rules, computed satellites) -> **Information Marts** (star/OBT for consumption)
- Business-key driven; schema changes add satellites/links rather than altering structures
- Typical loading: stage -> hubs -> links -> satellites (all parallelizable)

**Pros**: highly scalable & agile for many sources, full history/auditability (GDPR/SOX/Basel friendly), parallel loading, resilient to change, pattern-based (automation: dbt-vault/AutomateDV, VaultSpeed, WhereScape).
**Cons**: many tables and joins, steeper learning curve, **not directly consumable** (needs marts/PIT/bridges), overhead for small/simple projects, can be over-engineering.
**Use cases**: Large enterprises with many changing sources, regulated industries (banking, insurance, healthcare), M&A integration, long-lived warehouses requiring complete audit.
**Best practices**: automate generation; choose hash algorithm once; keep business keys clean/standardized (trim, upper-case, consistent delimiter); split satellites by source and volatility; build PIT/bridge tables for performance; expose marts via views or materialized tables; apply business rules in Business Vault, not Raw Vault.

### 6.4 One Big Table (OBT) / Flat / Wide Tables

**Concept**: A single, fully **denormalized**, wide table combining facts and all relevant dimension attributes. Popular with columnar cloud warehouses (BigQuery, Snowflake, ClickHouse, Redshift) and for ML feature tables, dashboards, and event analytics.

```sql
CREATE TABLE obt_sales AS
SELECT f.order_id, f.quantity, f.sales_amount,
       d.date, d.year, d.month,
       p.product_name, p.category, p.brand,
       c.customer_name, c.segment, c.country,
       s.store_name, s.region
FROM fact_sales f
JOIN dim_date d USING (date_key)
JOIN dim_product p USING (product_key)
JOIN dim_customer c USING (customer_key)
JOIN dim_store s USING (store_key);
-- BigQuery-style nested/repeated fields reduce duplication (ARRAY<STRUCT<...>>)
```

**Pros**: zero joins -> simplest and often fastest queries, excellent compression in columnar stores (repeated values compress well), simple for analysts and BI/ML, cheap to scan only needed columns.
**Cons**: redundant data and larger storage/rebuild cost, harder to maintain history/SCDs and changing attributes (updates require rewriting rows), grain confusion when mixing facts of different grains, **fan-out/double counting** risks, limited reuse across processes, hard to govern many columns.
**Use cases**: Single-subject dashboards, product/event analytics, ML feature stores, embedded analytics, ad-hoc exploration in cloud warehouses.
**Best practices**: keep one grain per OBT and name it; build it **from** a star schema/vault (OBT is a consumption layer, not the source of truth); partition and cluster (date + common filters); document columns in the catalog; use incremental rebuilds; consider nested structures instead of exploding rows.

### 6.5 Modeling Approach Comparison

| Aspect | 3NF | Star/Snowflake | Data Vault 2.0 | OBT |
|---|---|---|---|---|
| Primary workload | OLTP | OLAP/BI | Integration/historization | OLAP/ML/dashboards |
| Redundancy | Minimal | Moderate | Low-moderate | High |
| Query simplicity | Low | High | Low (needs marts) | Highest |
| Write/update friendliness | Highest | Medium | Insert-only | Low |
| History/audit | Manual | SCD-based | Built-in | Weak |
| Change tolerance | Medium | Medium | High | Low |
| Setup effort | Medium | Medium | High | Low |
| Typical layer | Source app / Inmon core | Presentation | Raw/Business vault | Presentation/serving |

**Rule of thumb**: *3NF for apps, Vault (or 3NF) for integration at scale, star schema for analytics, OBT for convenience serving on top of those.*

**Other architectures**: Data Lake (raw files, schema-on-read), **Lakehouse** (Delta Lake/Iceberg/Hudi with ACID on object storage), **Data Mesh** (domain-owned data products, federated governance, self-serve platform), **Data Fabric** (metadata-driven integration), **Lambda/Kappa** (batch+stream vs stream-only), **CDC** (Debezium + Kafka) for real-time sync.

---

## 7. Data Governance & Quality

### 7.1 Governance Foundations

**Data governance** = people, policies, processes, and technology that ensure data is **available, usable, consistent, secure, and compliant**.

| Element | Description |
|---|---|
| Roles | **Data Owner** (accountable), **Data Steward** (day-to-day quality/definitions), **Data Custodian** (technical), **Data Consumer**, **CDO / Governance Council** |
| Frameworks | DAMA-DMBOK, DCAM, COBIT; Data Mesh federated governance |
| Policies | Classification (public/internal/confidential/restricted), retention, access, privacy, quality SLAs |
| Regulations | GDPR, CCPA/CPRA, HIPAA, SOX, BCBS 239, EU AI Act, DORA |
| Controls | RBAC/ABAC, masking, encryption, tokenization, audit logs, consent, DSAR/right-to-erasure |
| Metadata types | Technical, business (glossary), operational (runs/usage), social (ratings, comments) |

**Start small**: pick critical data domains, name owners, define key terms, set a few quality rules, measure, expand.

### 7.2 Data Quality

**Dimensions**: Accuracy, Completeness, Consistency, Timeliness/Freshness, Uniqueness, Validity, Integrity (referential).

```sql
-- Typical checks
SELECT COUNT(*) - COUNT(email) AS missing_emails FROM customers;                       -- completeness
SELECT email, COUNT(*) FROM customers GROUP BY email HAVING COUNT(*) > 1;              -- uniqueness
SELECT * FROM orders o LEFT JOIN customers c USING (customer_id) WHERE c.customer_id IS NULL;  -- integrity
SELECT * FROM customers WHERE email NOT LIKE '%_@_%.__%';                              -- validity
SELECT MAX(loaded_at) FROM fact_sales;                                                 -- freshness
```

**Tooling**: Great Expectations, Soda, dbt tests (`unique`, `not_null`, `relationships`, `accepted_values`), Monte Carlo/Anomalo (observability), Deequ, Informatica DQ, Talend.

**Process**: Profile -> Define rules/SLAs -> Monitor/test in pipelines (shift left) -> Alert -> Remediate (root cause) -> Report quality KPIs. Use **data contracts** between producers and consumers (schema, semantics, SLAs).

### 7.3 Data Catalogs

**Purpose**: Searchable inventory of data assets with business glossary, ownership, classification, lineage, quality, and usage so people can **find, understand, trust, and govern** data.

**Core capabilities**: automated metadata harvesting/connectors, search & discovery, business glossary, classification (PII tagging), lineage, policy workflow, collaboration, API, data product marketplace.

| | Collibra | Apache Atlas |
|---|---|---|
| Type | Commercial enterprise governance platform (Data Intelligence Cloud) | Open source (Apache) metadata & governance framework for Hadoop ecosystem and beyond |
| Strengths | Business glossary, workflows, policy/stewardship management, rich UI, broad connectors, privacy/quality modules | Type system (extensible), classification propagation, lineage, REST API, integrates with Ranger (tag-based policies), Hive/Spark/Kafka hooks |
| Weaknesses | Cost, implementation effort, heavy change-management | Limited UX, more engineering/ops effort, Hadoop-centric heritage, smaller connector set |
| Best for | Large enterprises with formal governance programs | Hadoop/big-data platforms, engineering-led teams needing customization |
| Stack | SaaS / hosted | JanusGraph + Solr/Elasticsearch + Kafka + HBase/Cassandra |

**Other catalogs**: Alation, Atlan, Microsoft Purview, Google Dataplex, AWS Glue Data Catalog, DataHub (LinkedIn), OpenMetadata, Unity Catalog (Databricks), Amundsen.

**Atlas quick reference**

```bash
# REST API (default port 21000)
curl -u admin:admin "http://atlas:21000/api/atlas/v2/search/basic?typeName=hive_table&query=customer"
curl -u admin:admin "http://atlas:21000/api/atlas/v2/lineage/{guid}?direction=BOTH&depth=3"
curl -u admin:admin -X POST "http://atlas:21000/api/atlas/v2/entity/guid/{guid}/classifications" \
  -H 'Content-Type: application/json' -d '[{"typeName":"PII"}]'
```
Concepts: **Types** (entity, classification, relationship), **Entities** (instances with GUID), **Classifications/Tags** (propagate along lineage), **Glossary** (terms linked to entities), **Hooks/bridges** (Hive, HBase, Sqoop, Kafka, Storm).

**Best practices**
- Automate harvesting; assign owners/stewards per asset; certify "golden" datasets
- Link glossary terms to physical columns; tag PII/sensitivity and drive access policies from tags
- Measure adoption (searches, certified assets), not just coverage
- Integrate catalog into workflows (Slack/Jira, BI tools, dbt docs)

### 7.4 Data Lineage

**Definition**: Tracking data's origin, movement, and transformations from source to consumption.

| Type | Description |
|---|---|
| **Business lineage** | High-level flow between systems/domains and concepts |
| **Technical lineage** | Table-level and **column-level** transformations (SQL parsing, ETL jobs) |
| **Operational lineage** | Runtime: which job run touched what, when, rows processed |
| **Forward (impact) analysis** | What breaks downstream if X changes? |
| **Backward (root cause) analysis** | Where did this value come from? |

**How it's captured**: SQL/ETL parsing (static), runtime instrumentation (**OpenLineage** standard, Marquez, Spark/Airflow/dbt integrations), query logs, manual curation (last resort).

**Use cases**: Regulatory reporting proof (BCBS 239, GDPR), impact analysis before schema change, debugging data incidents, PII tracing/erasure, cost optimization (find unused tables), trust in BI metrics.
**Best practices**: prefer automated column-level lineage; adopt OpenLineage across orchestrators; store lineage as a graph (Neo4j/Atlas/DataHub); combine with quality results to show "data health" on lineage nodes; propagate classifications along lineage.

### 7.5 Master Data Management (MDM)

**Definition**: Discipline and technology to create and maintain a single, trusted, consistent view of core business entities (**master data**: customer, product, supplier, employee, location, asset) across systems. Distinct from transactional data (orders, payments) and reference data (country codes).

**MDM styles / implementation architectures**

| Style | How it works | Pros | Cons |
|---|---|---|---|
| **Registry** | Central index of keys/links to source records; data stays in sources | Low intrusion, quick | Golden record built virtually; no write-back governance |
| **Consolidation** | Copies/merges into a hub for analytics/reporting (read-only golden record) | Good for BI | Sources not corrected |
| **Coexistence** | Golden record maintained in hub and synced back to sources | Balanced | Complex sync |
| **Centralized / Transactional** | Hub is the system of record; sources consume | Strongest quality/governance | High disruption, expensive |
| **Hybrid** | Mix per domain | Pragmatic | Complexity |

**Core processes**
1. **Ingest & standardize** (cleanse addresses, case, formats)
2. **Match & merge** (deterministic rules, probabilistic/fuzzy matching, ML; thresholds for auto-merge vs steward review)
3. **Survivorship** (rules to pick best attribute: most recent, most trusted source, most complete)
4. **Golden record creation** + cross-reference (xref) of source IDs
5. **Hierarchy & relationship management** (parent/child companies, product families, household)
6. **Stewardship workflow**, approvals, audit trail
7. **Publish/distribute** (APIs, events, batch)

```
Source A (CRM)  \                              /-> ERP
Source B (ERP)   -> Standardize -> Match/Merge -> Golden Record Hub -> API/Events -> Data Warehouse
Source C (Web)  /       (DQ)       (survivorship)   (xref + hierarchy)             \-> Apps / AI
```

```sql
-- Simplified survivorship / golden record with source priority + recency
WITH ranked AS (
  SELECT m.cluster_id, s.*,
         ROW_NUMBER() OVER (PARTITION BY m.cluster_id
                            ORDER BY s.source_priority ASC, s.updated_at DESC) AS rn
  FROM match_clusters m JOIN source_customers s ON s.src_id = m.src_id
)
SELECT cluster_id, name, email, phone FROM ranked WHERE rn = 1;
```

**Tools**: Informatica MDM, Reltio, Profisee, Stibo STEP (PIM/product MDM), SAP MDG, Semarchy xDM, IBM, Tibco EBX, open source: Apache Unomi (CDP-ish), Zingg/Splink (entity resolution), dedupe.

**Pros**: consistent reporting, better customer 360, fewer duplicates/errors, regulatory compliance, enables analytics/AI.
**Cons**: expensive, organizational politics over ownership, complex matching, long programs, ongoing stewardship effort.
**Use cases**: Customer 360, product information (PIM), supplier management, financial/legal entity hierarchies, healthcare patient indexes (EMPI), M&A consolidation.
**Best practices**: start with one domain and measurable business case; define data ownership and survivorship rules upfront; tune match thresholds with stewards; keep an audit trail and unmerge capability; integrate with catalog/quality; use graph/entity-resolution techniques for relationships; treat MDM as a program, not a project.

### 7.6 Security & Privacy Quick Reference

| Control | Applies To |
|---|---|
| Encryption at rest (TDE, disk) & in transit (TLS) | All databases |
| Least privilege RBAC/ABAC, separate admin/app roles | All |
| Row/column-level security, dynamic masking | PostgreSQL RLS, SQL Server RLS/DDM, warehouse policies |
| Tokenization/pseudonymization/anonymization | PII in analytics and non-prod |
| Audit logging | pgaudit, SQL Server Audit, MongoDB auditing, Redis ACL log |
| Retention & erasure (GDPR Art. 17) | Include backups, vector embeddings and caches |
| Secrets management | Vault, cloud KMS; no credentials in code |

> **Note for AI/RAG**: embeddings can leak source content; apply the same access controls, retention, and deletion processes to vector stores and caches as to the source data.

---

## 8. Cross-Cutting Best Practices

**Design**
- Model from **access patterns** (NoSQL) and **business processes** (analytics); normalize for OLTP
- Prefer simple, boring tech first; add specialized stores when a measured need exists
- Use idempotent, replayable pipelines; separate compute and storage when possible
- Version schemas and migrations; treat schema as code; use data contracts at boundaries

**Performance**
- Measure first (`EXPLAIN`, profilers, slow logs); index for real queries; avoid `SELECT *`
- Keep transactions short; batch writes; use connection pooling
- Cache with clear TTL/invalidation; paginate with keyset instead of large offsets
- Partition/shard on well-distributed keys; avoid hot spots

**Reliability**
- Follow **3-2-1 backups** (3 copies, 2 media, 1 offsite) + **test restores**; define **RPO/RTO**
- Replicate across AZs/regions; practice failover and chaos testing
- Monitor: latency percentiles, saturation (CPU/memory/IO/connections), replication lag, error rates, disk growth
- Capacity plan; set alerts with runbooks

**Security & Compliance**
- Least privilege, encryption, network isolation (private endpoints), regular patching, audit logs
- Classify data; mask in non-prod; document processing activities

**Operations & Process**
- Infrastructure as Code (Terraform, Helm/Operators); CI/CD for schema (Flyway, Liquibase, dbt)
- Observability: Prometheus/Grafana, pg_stat_statements, Query Store, OpenTelemetry
- Document ownership, SLAs/SLOs, and decision records (ADRs)

**Common anti-patterns**
- Using a cache as the only copy; using one giant JSON column for everything in an RDBMS (without need)
- EAV tables for core entities; GUID/random PKs on clustered indexes without thought
- Cassandra/Mongo modeled like relational schemas; unbounded arrays/partitions
- Vector DB without metadata filters, evaluation, or re-embedding strategy
- Building Data Vault for a 3-source, 5-table warehouse; or OBT as the only source of truth
- Governance as paperwork with no owners, tooling, or automation

---

## 9. Decision Cheat Sheet

### 9.1 "Which database should I use?"

| Requirement | Choose |
|---|---|
| Transactions, relationships, SQL reporting, constraints | **PostgreSQL** (default choice) / MySQL / SQL Server |
| Microsoft/.NET/Azure/Power BI enterprise stack | **SQL Server** |
| Simple web app, WordPress, read-heavy, wide hosting | **MySQL** |
| Flexible/evolving JSON documents, rapid development | **MongoDB** (or PostgreSQL JSONB) |
| Sub-ms caching, sessions, rate limits, leaderboards, queues | **Redis** (or Valkey) |
| Massive write volume, multi-DC, time-series/IoT, always-on | **Cassandra** (or ScyllaDB/DynamoDB) |
| Multi-hop relationships, fraud rings, recommendations, knowledge graph | **Neo4j** |
| Semantic search/RAG, managed, minimal ops | **Pinecone** |
| Billion-scale vectors, GPU, self-hosted | **Milvus** |
| Filter-heavy vector search, lightweight self-hosting | **Qdrant** |
| Already on PostgreSQL, < ~10M vectors | **pgvector** |
| BI/analytics at scale | Columnar warehouse (Snowflake, BigQuery, Redshift, ClickHouse) with **star schema/OBT** |

### 9.2 Modeling chooser

| Situation | Model |
|---|---|
| Operational application database | 3NF |
| Business-user analytics, clear processes, fast delivery | Star schema (Kimball) |
| Enterprise-wide integration, normalized single truth first | Inmon (3NF EDW) |
| Many sources, strict auditability, changing structures | Data Vault 2.0 (+ star marts) |
| Single dashboard/ML table in a columnar cloud DW | OBT |

### 9.3 Governance chooser

| Need | Capability |
|---|---|
| "Where is the data and what does it mean?" | Data catalog + business glossary |
| "Where did this number come from / what breaks if I change this?" | Data lineage (OpenLineage, catalog) |
| "Which record is the true customer?" | MDM (match/merge + survivorship) |
| "Can we trust this data?" | Data quality rules, observability, data contracts |
| "Who may see what?" | Classification + RBAC/ABAC + masking policies |

### 9.4 Latency/scale rough guide

| Store | Typical latency | Scale pattern |
|---|---|---|
| Redis | < 1 ms | RAM-bound, cluster |
| PostgreSQL/MySQL/SQL Server (indexed) | 1-10 ms | TB-scale single node, replicas |
| MongoDB | 1-20 ms | Sharded multi-TB+ |
| Cassandra | 1-10 ms | Petabyte, linear |
| Vector DB (ANN, filtered) | 5-100 ms | Millions to billions of vectors |
| Neo4j traversal | ms for local neighborhoods | Single large graph, vertical |
| Warehouse query | 100 ms - minutes | Petabyte scans |

### 9.5 Quick glossary

**ACID** Strong transactional guarantees · **BASE** eventual consistency model · **CDC** change data capture · **CTE** common table expression · **DWH** data warehouse · **ELT** extract-load-transform · **HNSW** hierarchical navigable small world · **LSM** log-structured merge tree · **MVCC** multi-version concurrency control · **OLAP/OLTP** analytical/transactional processing · **PITR** point-in-time recovery · **RAG** retrieval-augmented generation · **RPO/RTO** recovery point/time objective · **SCD** slowly changing dimension · **WAL** write-ahead log.

---

*End of reference card.*
