# Cassandra Graph: Super Vertex & Hot Partition Analysis

## Problem Statement

A "super vertex" is a graph vertex with a disproportionately large number of edges. The entity hierarchy in Atlas uses **level-by-level edges only** — each entity links only to its direct children, not to all descendants:

```
Connection                        (edges to → Databases only)
  └── Database                    (edges to → Schemas only)
        └── Schema                (edges to → Tables only)
              └── Table           (edges to → Columns only)
                    └── Column    (leaf — few edges)
```

**A Connection does NOT hold direct edges to all its assets.** It only links to Databases. The real super vertex candidates are:

| Super Vertex Candidate | Edge Label | Max Realistic Edges | Why |
|---|---|---|---|
| **Table with 100K+ Columns** | `__Table.columns` (OUT) | 100,000+ | Some database engines have no column limit |
| **Schema with 10K+ Tables** | `__Schema.tables` (OUT) | 10,000+ | Data lakes, denormalized schemas |
| **Process with 1K+ outputs** | `__Process.outputs` (OUT) | 1,000+ | Bulk ETL writing many tables |
| **Glossary with 50K+ terms** | `__AtlasGlossaryTerm.anchor` (IN) | 50,000+ | Enterprise-wide glossary |
| **Table with many lineage edges** | `__Process.inputs` (IN) | 100+ | Popular table consumed by many processes |

**Key insight**: The largest tenant has **92M assets** spread across many Connections. Tables with 100K+ columns are a real scenario — some database engines impose no column limit.

In JanusGraph, super vertices caused GC pressure and slow traversals but were mitigated by JanusGraph's vertex-centric index (sorted edge adjacency list in the same partition). In CassandraGraph, the concern shifts to **Cassandra partition sizing** — all edges of a vertex live in a single partition. If that partition exceeds Cassandra's recommended limits (~100MB), queries timeout and nodes go into compaction storms.

## Current Edge Storage Schema

### Table Definitions

From `CassandraSessionProvider.java` lines 124-165:

```sql
-- Outgoing edges: "vertex A has edge to vertex B"
CREATE TABLE edges_out (
    out_vertex_id text,       -- PARTITION KEY
    edge_label    text,       -- CLUSTERING KEY 1
    edge_id       text,       -- CLUSTERING KEY 2
    in_vertex_id  text,
    properties    text,       -- JSON blob
    state         text,
    created_at    timestamp,
    modified_at   timestamp,
    PRIMARY KEY ((out_vertex_id), edge_label, edge_id)
) WITH CLUSTERING ORDER BY (edge_label ASC, edge_id ASC);

-- Incoming edges: "vertex B is pointed to by vertex A"
CREATE TABLE edges_in (
    in_vertex_id  text,       -- PARTITION KEY
    edge_label    text,       -- CLUSTERING KEY 1
    edge_id       text,       -- CLUSTERING KEY 2
    out_vertex_id text,
    properties    text,
    state         text,
    created_at    timestamp,
    modified_at   timestamp,
    PRIMARY KEY ((in_vertex_id), edge_label, edge_id)
) WITH CLUSTERING ORDER BY (edge_label ASC, edge_id ASC);

-- Edge lookup by ID
CREATE TABLE edges_by_id (
    edge_id       text PRIMARY KEY,
    out_vertex_id text,
    in_vertex_id  text,
    edge_label    text,
    properties    text,
    state         text,
    created_at    timestamp,
    modified_at   timestamp
);
```

### The Critical Design Choice

**The partition key is `(vertex_id)` ONLY — NOT `(vertex_id, edge_label)`.**

This means:

```
┌───────────────────────────────────────────────────────────┐
│  Cassandra Partition for Table vertex "T1" in edges_out   │
│                                                           │
│  Partition Key: out_vertex_id = "T1"                      │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ edge_label = "__Table.columns"                      │  │
│  │   edge_id=e1 → Column C1                           │  │
│  │   edge_id=e2 → Column C2                           │  │
│  │   edge_id=e3 → Column C3                           │  │
│  │   ... (5,000 rows)                                  │  │
│  ├─────────────────────────────────────────────────────┤  │
│  │ edge_label = "__Table.meanings"                     │  │
│  │   edge_id=e5001 → Term T1                          │  │
│  │   edge_id=e5002 → Term T2                          │  │
│  │   ... (20 rows)                                     │  │
│  ├─────────────────────────────────────────────────────┤  │
│  │ edge_label = "__Process.inputs"                     │  │
│  │   edge_id=e5021 → Process P1                        │  │
│  │   ... (50 rows)                                     │  │
│  ├─────────────────────────────────────────────────────┤  │
│  │ edge_label = "__Asset.policies"                     │  │
│  │   ... (8 rows)                                      │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                           │
│  TOTAL: 5,078 rows in one partition                       │
│  Estimated size: ~1.8 MB (at ~350 bytes/row)              │
└───────────────────────────────────────────────────────────┘
```

### Partition Size Estimates

| Edge Count | Partition Size (min) | Partition Size (max) | Status |
|-----------|---------------------|---------------------|--------|
| 100 | ~35 KB | ~210 KB | Safe |
| 1,000 | ~350 KB | ~2.1 MB | Safe |
| 5,000 | ~1.75 MB | ~10.5 MB | Safe |
| 10,000 | ~3.5 MB | ~21 MB | Caution |
| 50,000 | ~17.5 MB | ~105 MB | **At Cassandra recommended limit** |
| 100,000 | ~35 MB | ~210 MB | **Exceeds recommended limit** |
| 500,000 | ~175 MB | ~1.05 GB | **Dangerous** |

**Row size estimate**: 350-2100 bytes depending on properties JSON size. Edges in Atlas typically have minimal properties (just `__state`, `__createdBy`, `__modifiedBy`), so ~350 bytes is realistic for most edges.

**Cassandra limits**:
- **Recommended max partition size**: ~100 MB
- **Hard limit**: ~2 GB
- **Practical limit**: Partitions >10 MB cause noticeable read latency increases

## Query Patterns and Risk Assessment

### Queries Defined in EdgeRepository

```
┌──────────────────────────────────────────────────────────────────────┐
│  QUERY                                    │ PARTITION SCAN? │ RISK  │
├──────────────────────────────────────────────────────────────────────┤
│ WHERE vertex_id = ?                       │ FULL SCAN       │ HIGH  │
│ WHERE vertex_id = ? AND edge_label = ?    │ Prefix slice    │ LOW   │
│ WHERE vertex_id = ? AND edge_label = ?    │ Prefix slice    │       │
│   LIMIT ?                                 │ + capped        │ SAFE  │
└──────────────────────────────────────────────────────────────────────┘
```

The clustering order `(edge_label ASC, edge_id ASC)` means:
- **By-label queries** (`WHERE vertex_id = ? AND edge_label = ?`) do an efficient range scan within the partition — they read ONLY the rows for that label. This is fast even in large partitions.
- **All-edges queries** (`WHERE vertex_id = ?`) read the ENTIRE partition — every edge of every label. This is the danger zone.

### Code Locations: Unbounded Full-Partition Scans

These are the **most dangerous** code paths — they read ALL edges of a vertex across ALL labels:

| Location | Method | When Called | Impact |
|----------|--------|-------------|--------|
| `DeleteHandlerV1.java:1254` | `vertex.getEdges(IN)` + `getEdges(OUT)` | Deleting ANY entity | Reads all edges in both directions |
| `EntityGraphMapper.java:5971` | `vertex.getEdges(BOTH)` | Entity import/activation | Reads all edges in both directions |
| `GraphHelper.java:2308` | `vertex.getEdges(BOTH)` | `retrieveEdgeLabelsAndTypeName()` — Cassandra-specific path | Reads all edges just to get unique labels |
| `EdgeRepository.java:507` | `getEdgesForVertex(id, BOTH, null)` | `deleteEdgesForVertex()` | Cascade-delete reads everything first |
| `CassandraVertex.java:60` | `getEdges(IN)` then filters | `getInEdges()` | Reads all incoming then filters |
| `CassandraVertexQuery.java:67` | `getMatchingEdges(-1)` | `count()` | Materializes all edges just to count |

### Code Locations: By-Label but Unbounded

These are **lower risk** (only read one label's edges) but can still be large for high-cardinality labels:

| Location | Method | Typical Label | Risk |
|----------|--------|---------------|------|
| `GraphHelper.java:311` | `getAdjacentEdgesByLabel()` | `__Table.columns` | HIGH if 5K+ columns |
| `GraphHelper.java:759` | `getEdgeForLabel()` | Various | LOW (finds first active) |
| `EntityLineageService.java:482` | `getActiveEdges()` | `__Process.inputs/outputs` | MEDIUM |
| `EntityGraphMapper.java:3701` | `getRestoredInputOutputEdges()` | `__Process.inputs/outputs` | MEDIUM |

### The `CassandraVertexQuery` Problem

`CassandraVertexQuery.getMatchingEdges(limit)` (line 71) does **NOT push LIMIT down to Cassandra**:

```java
// CURRENT BEHAVIOR:
List<AtlasEdge> edges = vertex.getEdges(direction, label);  // reads ALL from Cassandra
// Then applies has() predicates in Java
// Then applies limit in Java
return edges.subList(0, Math.min(limit, edges.size()));
```

Even with `query().label("X").edges(100)`, ALL edges for label X are read from Cassandra, then truncated to 100 in Java. For a label with 50,000 edges, this reads 50,000 rows over the network and discards 49,900.

**The LIMIT prepared statement exists** (`selectEdgesOutByLabelLimitStmt`) but is only used by `getEdgesForVerticesByLabelsAsync(..., limitPerLabel)` — a method that callers must explicitly opt into.

## Real-World Super Vertex Scenarios

### Entity Hierarchy (Corrected Model)

**Edges only connect adjacent levels.** Connection does NOT link directly to Tables or Columns.

```
Connection ──[__Connection.databases]──→ Database     (few edges, ~5-50)
Database   ──[__Database.schemas]──────→ Schema       (few edges, ~5-100)
Schema     ──[__Schema.tables]─────────→ Table        (moderate, ~100-10K)
Table      ──[__Table.columns]─────────→ Column       (THIS IS THE BIG ONE, up to 100K+)
Process    ──[__Process.inputs]────────→ Catalog      (moderate, ~2-100)
Process    ──[__Process.outputs]───────→ Catalog      (moderate, ~2-1000)
```

### Scenario 1: Table with 100,000 Columns (PRIMARY CONCERN)

```
Table vertex "T1" — edges_out partition:
  - __Table.columns edges:      100,000  (one per column)
  - __Asset.meanings edges:          20  (glossary term assignments)
  - __Process.inputs edges:           5  (lineage — processes reading this table)
  - __Asset.policies edges:           8  (bootstrap policies)
  - __Table.schema edges:             1  (schema reference)
  Total edges in partition:     100,034
  Estimated partition size:       ~35 MB
```

**Risk**: **HIGH**. This is the most likely super vertex in production.
- Some database engines (BigQuery, Snowflake wide tables, Spark schemas) have no practical column limit
- `deleteVertex()` reads all 100K edges in one query → ~35 MB read
- `getAdjacentEdgesByLabel("__Table.columns")` reads 100K rows
- Any `getEdges(BOTH)` reads the entire 100K+ partition

**Likely timeout?** At 100K rows:
- Pure Cassandra read: 500ms-2s (35 MB from disk, faster if cached)
- Java deserialization of 100K edge objects: 1-5s (GC pressure)
- If properties JSON is large: multiply accordingly
- **Total realistic time: 2-10 seconds** — risky for API request timeouts

### Scenario 2: Schema with 10,000 Tables

```
Schema vertex "S1" — edges_out partition:
  - __Schema.tables edges:       10,000  (one per table)
  - __Asset.policies edges:           8
  Total edges in partition:      10,008
  Estimated partition size:       ~3.5 MB
```

**Risk**: MEDIUM. 3.5 MB partition is well within Cassandra limits.
- Read time: <100ms from Cassandra
- Java materialization of 10K edges: ~200ms
- **Safe for Cassandra, but watch out for full-partition scans in delete/import paths**

### Scenario 3: ETL Process with 1,000 Outputs

```
Process vertex "P1" — edges_out partition:
  - __Process.outputs edges:      1,000
  - __Process.inputs edges:          50
  Total edges in partition:       1,050
  Estimated partition size:       ~370 KB
```

**Risk**: LOW. This is well within all limits.
- Lineage traversal reads 1,000 edges — fast
- Only becomes a concern at 10K+ outputs

### Scenario 4: Glossary with 50,000 Terms

```
Glossary vertex "G1" — edges_in partition:
  - __AtlasGlossaryTerm.anchor edges:  50,000  (IN direction)
  - __AtlasGlossaryCategory.anchor:     1,000
  Total incoming edges:                 51,000
  Estimated partition size:              ~18 MB
```

**Risk**: HIGH for operations that read all term edges.
- Glossary deletion: reads all 51K incoming edges
- `GlossaryService.getGlossary()` with relationship attributes
- 18 MB partition is within limits but full-scan is slow

### Summary: Where the Pain Actually Is

| Scenario | Partition Size | Cassandra Risk | Application Risk |
|---|---|---|---|
| **Table with 100K columns** | ~35 MB | MEDIUM (within limits) | **HIGH (full-scan in delete/import)** |
| Schema with 10K tables | ~3.5 MB | LOW | MEDIUM (full-scan in delete) |
| Process with 1K outputs | ~370 KB | SAFE | LOW |
| Glossary with 50K terms | ~18 MB | MEDIUM | HIGH (deletion, listing) |
| Connection | ~2 KB (only DB edges) | SAFE | SAFE |

**Bottom line: The #1 super vertex risk is Table → Column.** If we solve for a Table with 100K columns, we've solved the problem for practically everything else.

## Mitigation Strategies

### Strategy 1: Split Partition Key to `(vertex_id, edge_label)`

**The most impactful change.** Move `edge_label` from clustering key to partition key:

```sql
-- CURRENT
PRIMARY KEY ((out_vertex_id), edge_label, edge_id)

-- PROPOSED
PRIMARY KEY ((out_vertex_id, edge_label), edge_id)
```

**Before (all labels in one partition)**:
```
Partition: vertex_id=T1
  ├── __Table.columns → 5000 rows
  ├── __Asset.meanings → 20 rows
  ├── __Process.inputs → 50 rows
  └── __Asset.policies → 8 rows
```

**After (each label is its own partition)**:
```
Partition: (T1, __Table.columns)   → 5000 rows (~1.75 MB)
Partition: (T1, __Asset.meanings)  → 20 rows (~7 KB)
Partition: (T1, __Process.inputs)  → 50 rows (~17 KB)
Partition: (T1, __Asset.policies)  → 8 rows (~3 KB)
```

**Benefits**:
- Each partition is bounded by label cardinality, not total edge count
- "All edges" query now requires multiple partition reads (one per label), which can be fired in parallel
- The 5,000-column label is still ~1.75 MB, well within limits
- Even 100,000 assets per connection = ~35 MB partition (still within limits, and this is the ONLY label that gets this big)

**Tradeoffs**:
- "Get all edges for vertex" now requires knowing all possible labels, or doing a `SELECT DISTINCT edge_label` (not efficient) or maintaining a label registry
- Edge queries without a label filter become multi-partition reads

**Implementation impact on existing code**:

| Query Type | Current | After Split | Impact |
|-----------|---------|-------------|--------|
| `WHERE vertex_id = ? AND edge_label = ?` | Works (clustering prefix) | Works (full partition key) | None |
| `WHERE vertex_id = ?` (all labels) | Works (full partition scan) | **BROKEN** — needs all labels | Must refactor to multi-label query |
| `LIMIT` queries | Works on clustering | Works on partition | None |

**Critical refactoring needed**: Every call to `getEdges(direction)` or `getEdges(direction, null)` (without a label) must be changed to either:
1. Query a known set of labels: `getEdges(direction, [label1, label2, ...])`
2. Or query a separate label-registry table first

### Strategy 2: Push LIMIT Down to Cassandra

**Fix `CassandraVertexQuery` to use LIMIT-based prepared statements**:

```java
// CURRENT (all in Java):
List<AtlasEdge> getMatchingEdges(int limit) {
    List<AtlasEdge> edges = vertex.getEdges(direction, label);  // ALL from Cassandra
    // filter by predicates in Java
    // truncate to limit in Java
}

// PROPOSED (push to Cassandra):
List<AtlasEdge> getMatchingEdges(int limit) {
    List<AtlasEdge> edges;
    if (limit > 0 && labels != null) {
        edges = edgeRepo.getEdgesWithLimit(vertexId, direction, label, limit);  // LIMIT in CQL
    } else {
        edges = vertex.getEdges(direction, label);
    }
    // filter by predicates in Java (still needed for has() clauses)
}
```

**Important caveat**: If there are `has()` predicates (e.g., `state = ACTIVE`), the LIMIT must be applied AFTER filtering. Since `state` is not in the clustering key, Cassandra can't filter it server-side. Options:
1. Fetch `limit * 2` from Cassandra, filter in Java, refetch if needed
2. Add `state` to clustering key (Strategy 4)

### Strategy 3: Add Pagination Support

For operations that genuinely need to process all edges of a super vertex (e.g., delete), add pagination:

```java
// Instead of:
List<CassandraEdge> allEdges = getEdgesForVertex(vertexId, BOTH, null);

// Use paged iteration:
Iterator<CassandraEdge> edgeIterator = getEdgesForVertexPaged(vertexId, label, pageSize=1000);
while (edgeIterator.hasNext()) {
    List<CassandraEdge> page = edgeIterator.nextPage();
    processBatch(page);
}
```

The Cassandra driver already supports automatic paging via `setPageSize()` on the statement. The current code materializes the entire `ResultSet` into a `List<CassandraEdge>`. Switching to streaming iteration would bound memory usage.

### Strategy 4: Add `state` to Clustering Key

Most edge queries filter by `state = ACTIVE` in Java. If `state` is part of the clustering key, Cassandra can skip deleted edges at the storage level:

```sql
-- CURRENT
PRIMARY KEY ((out_vertex_id), edge_label, edge_id)

-- PROPOSED (with state in clustering key)
PRIMARY KEY ((out_vertex_id, edge_label), state, edge_id)
```

**Benefits**:
- `WHERE vertex_id = ? AND edge_label = ? AND state = 'ACTIVE'` is a pure clustering prefix scan
- Deleted edges are never read from disk
- Combined with Strategy 1 (partition by label), this is very efficient

**Tradeoff**: Changing `state` requires delete + re-insert (since clustering keys are immutable in Cassandra). Soft-delete currently just updates the `state` column — with this schema, soft-delete would need to delete the ACTIVE row and insert a DELETED row.

### Strategy 5: Edge Count Metadata Table

Add a lightweight table that tracks edge counts per (vertex, label):

```sql
CREATE TABLE edge_counts (
    vertex_id  text,
    direction  text,     -- 'OUT' or 'IN'
    edge_label text,
    count      counter,
    PRIMARY KEY ((vertex_id), direction, edge_label)
);
```

**Benefits**:
- `getEdgesCount()` becomes a single-row read instead of materializing all edges
- Can detect super vertices before querying edges (e.g., if count > 10,000, use pagination)
- Enables proactive warnings in the UI ("This table has 50,000 columns — operations may be slow")

**Tradeoff**: Counter columns have limitations (no batch, no conditional update). Must be updated on every edge add/remove. If counts drift, need a reconciliation job.

### Strategy 6: Label Registry for "All Edges" Queries

If Strategy 1 (split partition key) is adopted, "get all edges" requires knowing all labels. Add a registry:

```sql
CREATE TABLE vertex_edge_labels (
    vertex_id  text,
    direction  text,
    edge_label text,
    PRIMARY KEY ((vertex_id), direction, edge_label)
);
```

**Benefits**:
- `SELECT edge_label FROM vertex_edge_labels WHERE vertex_id = ?` returns all labels (~5-10 rows)
- Then fire parallel queries per label
- Much more efficient than a full partition scan

**Tradeoff**: Must be maintained on every edge add/remove. Small extra write cost.

## Recommended Strategy: Prioritized Rollout

```
┌─────────────────────────────────────────────────────────────┐
│  Phase 0 (Quick Wins — No Schema Change)                    │
│                                                             │
│  1. Push LIMIT down in CassandraVertexQuery                 │
│  2. Add pagination to deleteVertex() edge fetch             │
│  3. Replace getEdges(BOTH) in GraphHelper with              │
│     per-label queries where labels are known                │
│  4. Enable OPTIMISE_SUPER_VERTEX flag                       │
│                                                             │
│  Effort: LOW | Risk: LOW | Impact: MEDIUM                   │
├─────────────────────────────────────────────────────────────┤
│  Phase 1 (Schema Change — Split Partition Key)              │
│                                                             │
│  1. Change edge tables to PRIMARY KEY ((vertex_id,          │
│     edge_label), edge_id)                                   │
│  2. Add vertex_edge_labels registry table                   │
│  3. Refactor all getEdges(direction, null) callers          │
│     to use label registry + parallel per-label fetch        │
│  4. Migration: re-migrate edge tables with new schema       │
│                                                             │
│  Effort: HIGH | Risk: MEDIUM | Impact: HIGH                 │
├─────────────────────────────────────────────────────────────┤
│  Phase 2 (Optimization — State in Clustering Key)           │
│                                                             │
│  1. Add state to clustering key                             │
│  2. Change soft-delete to delete+reinsert                   │
│  3. Benefit: Cassandra-side filtering of deleted edges      │
│                                                             │
│  Effort: MEDIUM | Risk: MEDIUM | Impact: MEDIUM             │
├─────────────────────────────────────────────────────────────┤
│  Phase 3 (Observability — Edge Count Metadata)              │
│                                                             │
│  1. Add edge_counts counter table                           │
│  2. Instrument edge add/remove to update counts             │
│  3. Use counts for super-vertex detection                   │
│                                                             │
│  Effort: MEDIUM | Risk: LOW | Impact: LOW-MEDIUM            │
└─────────────────────────────────────────────────────────────┘
```

## Critical Assessment: Do We Actually Have a Problem Today?

**Yes, for Table → Column.** With 92M total assets in the largest tenant and no column limits on some database engines, Tables with 100K+ columns are a real scenario — not theoretical.

**Corrected edge count estimates** (edges only to next hierarchy level):

| Entity Type | Edge Label (direction) | Typical | Realistic Max | Partition Size (max) |
|---|---|---|---|---|
| **Table (columns)** | `__Table.columns` (OUT) | 10-500 | **100,000+** | **~35 MB+** |
| Schema (tables) | `__Schema.tables` (OUT) | 10-200 | 10,000 | ~3.5 MB |
| Database (schemas) | `__Database.schemas` (OUT) | 5-50 | 500 | ~175 KB |
| Connection (databases) | (to databases only) | 2-20 | 100 | ~35 KB |
| Process (outputs) | `__Process.outputs` (OUT) | 2-50 | 1,000 | ~350 KB |
| Glossary (terms, IN) | `__GlossaryTerm.anchor` (IN) | 10-1,000 | 50,000 | ~18 MB |
| Column (leaf) | (1 edge back to table) | 2-5 | 20 | ~7 KB |

**The partition key concern is focused**: Table → Column is the one relationship that can produce partitions exceeding 10 MB. Everything else stays comfortably small because edges only go to the next level.

**Phase 0 (quick wins) is essential and should be done now.** The `CassandraVertexQuery` LIMIT push-down and pagination for delete/import are critical for Tables with many columns. Phase 1 (partition key split) should be evaluated after we see real edge count distributions from the largest tenant.

**Bulk request size is 20 entities.** This is small — for the partial failure strategy, the blast radius per request is very limited. A failed commit of 20 entities leaves at most 20 orphaned vertices, which retry will naturally reconcile via `findByUniqueAttributes()`.

## Appendix: Dangerous Code Paths (Full Inventory)

### Priority 1: Full Partition Scans (MUST FIX)

```java
// DeleteHandlerV1.java:1254 — entity deletion
instanceVertex.getEdges(AtlasEdgeDirection.IN)   // ALL incoming edges
instanceVertex.getEdges(AtlasEdgeDirection.OUT)  // ALL outgoing edges

// GraphHelper.java:2308 — label discovery (Cassandra-specific)
vertex.getEdges(AtlasEdgeDirection.BOTH)         // ALL edges both directions

// EdgeRepository.java:507 — cascade delete
getEdgesForVertex(vertexId, BOTH, null, graph)   // ALL edges both directions

// CassandraGraph.java:149 — bulk edge fetch
getEdgesForVerticesAsync(vertexIds, BOTH, this)  // ALL edges for multiple vertices
```

### Priority 2: By-Label but Unbounded

```java
// GraphHelper.java:311 — most common edge fetch
instanceVertex.getEdges(direction, edgeLabel).iterator()  // all edges for one label

// EntityLineageService.java:482 — lineage traversal
GraphHelper.getActiveEdges(vertex, label, IN)    // uses MAX_EDGES_SUPER_VERTEX=10000

// EntityGraphMapper.java:3701 — lineage repair
vertex.getEdges(BOTH, [PROCESS_INPUTS, PROCESS_OUTPUTS])
```

### Priority 3: Count Queries (Materializes Everything)

```java
// CassandraVertex.java:72 — edge count
getEdges(direction, label).stream().count()      // reads all, counts in Java

// CassandraVertexQuery.java:67 — query count
getMatchingEdges(-1).size()                      // reads all, counts in Java
```
