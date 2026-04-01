Zero Graph Edge Tables Explained

The "zero graph" (or "no-graph") system is a Cassandra-native replacement for JanusGraph. Instead of relying on JanusGraph's TinkerPop abstraction, it stores the graph directly in Cassandra tables. The three edge tables implement a classic adjacency-list      
pattern optimized for Cassandra's partition-key model.

The Three Tables

From CassandraSessionProvider.java:123-165:

┌─────────────┬───────────────┬─────────────────────┬─────────────────────────────┐
│    Table    │ Partition Key │   Clustering Keys   │           Purpose           │
├─────────────┼───────────────┼─────────────────────┼─────────────────────────────┤
│ edges_out   │ out_vertex_id │ edge_label, edge_id │ All edges leaving a vertex  │
├─────────────┼───────────────┼─────────────────────┼─────────────────────────────┤
│ edges_in    │ in_vertex_id  │ edge_label, edge_id │ All edges entering a vertex │
├─────────────┼───────────────┼─────────────────────┼─────────────────────────────┤
│ edges_by_id │ edge_id       │ (none)              │ Direct edge lookup by ID    │
└─────────────┴───────────────┴─────────────────────┴─────────────────────────────┘

Why both edges_in and edges_out?

This is a fundamental Cassandra modeling constraint. Cassandra can only efficiently query by partition key. A single edge table can't serve both directions:

- "Give me all edges going out of vertex A" — needs partition key = out_vertex_id → use edges_out
- "Give me all edges coming into vertex B" — needs partition key = in_vertex_id → use edges_in

In Atlas, these two directions serve very different operations:

- Forward traversal (edges_out): "What columns does this table have?" (table → columns), "What downstream assets does this process produce?" (lineage forward)
- Reverse traversal (edges_in): "Which table does this column belong to?" (column → table), "What upstream sources feed this asset?" (lineage backward)

You can see this clearly in CassandraLineageService.java which prepares separate statements for each direction, and in EdgeRepository.java:186-194 where the direction determines which table to query.

The tradeoff is write amplification — every edge insert/update/delete writes to all 3 tables atomically via a LOGGED BATCH (EdgeRepository.java:128-145). This is the standard Cassandra denormalization pattern: duplicate data at write time to enable fast reads
in multiple access patterns.

Why edges_by_id?

The adjacency-list tables (edges_in/edges_out) are partitioned by vertex, so you cannot look up a specific edge by its ID without knowing which vertex it belongs to. edges_by_id solves this — it's a simple key-value lookup:

- Use case: When you have an edge GUID (e.g., from a relationship entity or an index) and need to fetch/update/delete that specific edge directly.
- The edge index table (edge_index) maps property values like relationship GUIDs → edge_id, and then edges_by_id resolves the edge_id to the full edge record.

Summary

edges_out:    "vertex A → what edges leave it?"     (forward graph traversal)
edges_in:     "vertex B → what edges arrive at it?"  (reverse graph traversal)
edges_by_id:  "edge X → what are its details?"       (direct edge lookup)

All three are kept in sync atomically via Cassandra logged batches. This is the standard query-driven denormalization pattern — you model one table per query access pattern, trading storage and write cost for read performance.


 ---
1. vertices — The Entity Store

PRIMARY KEY: vertex_id
Columns: properties (JSON), vertex_label, type_name, state, created_at, modified_at

This is the single source of truth for all graph vertices. Each Atlas entity (table, column, glossary term, typedef, etc.) is one row. All entity attributes are stored as a single JSON blob in the properties column — no per-attribute columns.

Key design choices:
- Update = full overwrite (VertexRepository.java:62-65): updateVertex() just calls insertVertex() again, overwriting the whole row. This is simple and works because Cassandra UPSERTs are idempotent.
- Property normalization (VertexRepository.java:193-211): When reading, JanusGraph-style type-qualified property names like Referenceable.qualifiedName or Asset.name are stripped to just qualifiedName and name. Properties starting with __ (like __guid,
  __typeName) are never normalized.
- Async bulk fetch (VertexRepository.java:83-110): Multiple vertices are fetched in parallel using executeAsync, turning N sequential round-trips into ~1 wall-clock round-trip.

  ---
2. Index Tables — Replacing JanusGraph's Built-in Indexes

In JanusGraph, you define composite and mixed indexes declaratively. In Cassandra, you have to build them yourself as separate tables. There are three index tables:

vertex_index — 1:1 Unique Lookups

PRIMARY KEY: (index_name, index_value) → vertex_id

This is for unique property lookups — given a property value, find exactly one vertex. The specific indexes maintained (CassandraGraph.java:676-718):

┌───────────────────┬────────────────────────┬──────────────────────────────────────────────────────────────────────────────────────┐
│    Index Name     │      Index Value       │                                       Use Case                                       │
├───────────────────┼────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ __guid_idx        │ entity GUID            │ "Find entity by GUID" — the most common lookup in all of Atlas                       │
├───────────────────┼────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ qn_type_idx       │ qualifiedName:typeName │ "Find entity by qualifiedName + type" — e.g., find the Table with QN db.schema.table │
├───────────────────┼────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ type_typename_idx │ vertexType:typeDefName │ "Find typedef vertex by name" — e.g., find the typedef vertex for type Table         │
└───────────────────┴────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────┘

Why a composite key (index_name, index_value)? This lets one table serve multiple different indexes. index_name acts as a namespace — __guid_idx, qn_type_idx, type_typename_idx are all stored in the same table but partitioned separately.

vertex_property_index — 1:N Non-Unique Lookups

PRIMARY KEY: (index_name, index_value), vertex_id  ← vertex_id is a clustering key

This is for non-unique property lookups — one value maps to multiple vertices. Currently used for:

┌───────────────────┬─────────────────────────┬─────────────────────────────────────────────────────────────────────────────────┐
│    Index Name     │       Index Value       │                                    Use Case                                     │
├───────────────────┼─────────────────────────┼─────────────────────────────────────────────────────────────────────────────────┤
│ type_category_idx │ vertexType:typeCategory │ "Find all typedefs of category ENTITY" — e.g., list all entity type definitions │
└───────────────────┴─────────────────────────┴─────────────────────────────────────────────────────────────────────────────────┘

The vertex_id is a clustering column, so multiple vertices can share the same (index_name, index_value) partition. This is the difference from vertex_index where the result is always exactly one vertex.

edge_index — 1:1 Edge Lookups by Property

PRIMARY KEY: (index_name, index_value) → edge_id

Same pattern as vertex_index but for edges. Currently used for:

┌──────────────┬───────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────┐
│  Index Name  │    Index Value    │                                           Use Case                                            │
├──────────────┼───────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
│ _r__guid_idx │ relationship GUID │ "Find the edge for this relationship GUID" — Atlas exposes relationships with their own GUIDs │
└──────────────┴───────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────┘

The lookup flow is: relationship GUID → edge_index → edge_id → edges_by_id → full edge data.

  ---
3. schema_registry — Property Key Metadata

PRIMARY KEY: property_name
Columns: property_class, cardinality, created_at

This replaces JanusGraph's PropertyKey management. When Atlas defines property keys (e.g., qualifiedName is a String with SINGLE cardinality), this table records that metadata. It's the schema catalog — what properties exist and their types.

  ---
4. TypeDef Tables — Accelerated Type Lookups

type_definitions:             PRIMARY KEY (type_name) → type_category, vertex_id
type_definitions_by_category: PRIMARY KEY (type_category), type_name → vertex_id

These are dedicated denormalized tables for fast TypeDef access, separate from the generic vertex/index tables:

- type_definitions: "Given type name Table, get its vertex ID" — O(1) lookup
- type_definitions_by_category: "Give me all ENTITY typedefs" — partitioned by category (ENTITY, ENUM, STRUCT, etc.), clustered by name

These are synced during commit (CassandraGraph.java:729-764) and backed by a Caffeine in-memory cache for even faster reads, since TypeDef lookups are extremely hot in Atlas.

  ---
5. entity_claims — LWT Deduplication

PRIMARY KEY: identity_key
Columns: vertex_id, claimed_at, source

This table enforces exactly-one-vertex-per-entity using Cassandra Lightweight Transactions (LWT). During commit, each new entity vertex races to claim its identity key (typeName|qualifiedName) via INSERT IF NOT EXISTS. If another pod already claimed it, the loser reuses the existing vertex_id instead of creating a duplicate.

Key design choices:
- identity_key = "typeName|qualifiedName" — the natural unique identity of an Atlas entity
- LWT guarantees: Even under concurrent writes from multiple pods, exactly one vertex_id wins
- Used by ClaimRepository.java — if [applied]=true, caller won the claim; if [applied]=false, caller uses the existing vertex_id and rewrites edges to point to the winner

  ---
6. es_outbox — Elasticsearch Sync Reliability

PRIMARY KEY: (status), vertex_id   ← partitioned by status
Columns: es_action, properties_json, attempt_count, created_at, last_attempted_at
TTL: 7 days (auto-cleanup of stuck entries)
gc_grace_seconds: 3600 (1 hour — fast tombstone compaction for this transient queue)

This is a durable outbox for Cassandra→ES sync. Before attempting ES writes during commit, entries are written to the PENDING partition. On success, they're deleted from PENDING. On failure, they remain for the ESOutboxProcessor background job to retry.

Key design choices:
- Partitioned by status: The query "give me all PENDING entries" is a direct partition scan — no ALLOW FILTERING needed. This scales to millions of entries under sustained ES outages without scanning the entire table.
- Status transitions require cross-partition operations:
  - Mark done: DELETE from PENDING partition (UNLOGGED batch for multiple entries — single partition)
  - Mark failed: LOGGED batch DELETE from PENDING + INSERT into FAILED (cross-partition atomic move)
  - Increment attempt: UPDATE within PENDING partition
- ESOutboxProcessor uses adaptive polling: idle mode (30s) when PENDING is empty, drain mode (2s, batch 500) when entries are found. Throughput: ~15K entries/min in drain mode.
- Lease-guarded: Only one pod runs the processor at a time (via job_leases). Crashed pods auto-release within 60s.
- Max 10 retry attempts before marking FAILED (preserved for investigation).

  ---
7. job_leases — Distributed Job Coordination

PRIMARY KEY: job_name
Columns: owner (pod hostname), acquired_at
Written with TTL (auto-release on crash)

This table provides distributed mutex coordination for background jobs using Cassandra LWT. Only one pod runs each job at a time — no ZooKeeper or Redis needed.

Key design choices:
- Acquire: INSERT INTO job_leases ... IF NOT EXISTS USING TTL ? — atomic claim with auto-expiry
- Release: DELETE FROM job_leases WHERE job_name = ? IF owner = ? — conditional, only the holder can release
- Crash safety: If a pod dies mid-job, the TTL expires and another pod claims the lease automatically
- No dependency on pod count or ordinals — pods compete naturally for leases on scale-up/down
- Used by: ESOutboxProcessor (TTL=60s), ESReconciliationJob (TTL=2h), OrphanVertexCleanup (TTL=30min), OrphanEdgeCleanup (TTL=30min)

  ---
8. repair_progress — Progressive Scan Cursors

PRIMARY KEY: job_name
Columns: last_token (bigint), last_run_at, rows_processed, cycle_complete (boolean)

This table tracks the progress of progressive background scans (OrphanVertexCleanup, OrphanEdgeCleanup). Instead of scanning the entire vertices or edges_by_id table in one shot, these jobs process a bounded batch per cycle (e.g., 10K rows) and persist the Cassandra token of the last processed row.

Key design choices:
- last_token: The Cassandra token (hash) of the last processed row. Next cycle resumes from token(partition_key) > last_token.
- cycle_complete: Set to true when the scan wraps around to the end of the token ring. Reset to false on the next cycle start.
- Crash-safe: If a pod crashes mid-scan, the next pod reads last_token and resumes from the checkpoint — no work is lost or repeated.
- Progressive coverage: With 10K rows/cycle at 24h intervals, the full table is scanned over multiple days. This avoids the I/O spike of a single massive full-table scan.

  ---
Summary: How It All Fits Together

vertices                      → "what data does this entity have?"
edges_out / edges_in          → "what's connected to this entity?"  (forward/reverse)
edges_by_id                   → "get this specific edge"

vertex_index                  → GUID → vertex,  QN+type → vertex    (1:1 unique)
vertex_property_index         → category → [vertices]                (1:N)
edge_index                    → relationship GUID → edge             (1:1 unique)

schema_registry               → property key metadata
type_definitions              → fast typedef by name
type_definitions_by_category  → fast typedef listing by category

entity_claims                 → LWT dedup: one vertex per entity     (concurrent-safe)
es_outbox                     → durable Cassandra→ES sync queue      (partitioned by status)
job_leases                    → distributed job mutex                 (LWT + TTL)
repair_progress               → progressive scan cursors              (crash-resumable)

The whole design follows the Cassandra principle: one table per query pattern. JanusGraph gives you a graph abstraction with built-in indexing but adds overhead. The zero graph approach trades that convenience for direct, predictable Cassandra performance —
every query is a single-partition lookup with no hidden graph traversal overhead.