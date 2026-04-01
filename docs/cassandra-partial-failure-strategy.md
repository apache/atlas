# Cassandra Graph: Partial Failure & Transaction Strategy

## Problem Statement

JanusGraph gave us ACID transactions. A bulk request with 20 entities either fully committed or fully rolled back. With CassandraGraph, `commit()` executes **14 sequential write operations** across different Cassandra tables. If operation N+1 fails, operations 1..N are already persisted with no undo.

**The user-visible contract**: The bulk entity API (`POST /entity/bulk`) is all-or-nothing. Clients (SDK, workflow engine) expect either a full `EntityMutationResponse` or a clean error. Partial success is invisible — the client gets 500 and retries.

## Current Architecture

### Commit Sequence (CassandraGraph.commit())

```
┌─────────────────────────────────────────────────────────────┐
│                   @GraphTransaction boundary                 │
│                                                              │
│  1. preCreateOrUpdate()     ← entity discovery, vertex alloc │
│  2. Authorization checks                                     │
│  3. executePreProcessor()   ← QN generation, validation      │
│  4. mapAttributes()         ← set properties, create edges   │
│  5. entityChangeNotifier    ← Kafka notifications            │
│                                                              │
│  ── All above is IN-MEMORY (TransactionBuffer) ──            │
│  ── Any failure here → clean rollback, no side effects ──    │
│                                                              │
│  graph.commit() called by GraphTransactionInterceptor:       │
│                                                              │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  Step 1:  batchInsertVertices()    [LOGGED BATCH]       │ │
│  │  Step 2:  updateVertex() × N      [individual stmts]   │ │
│  │  Step 3:  batchInsertEdges()       [LOGGED BATCH]       │ │
│  │  Step 4:  updateEdge() × N        [individual stmts]   │ │
│  │  Step 5:  batchAddEdgeIndexes()    [LOGGED BATCH]       │ │
│  │  Step 6:  batchDeleteEdges()       [LOGGED BATCH, ≤150] │ │
│  │  Step 7:  batchRemoveEdgeIndexes() [LOGGED BATCH]       │ │
│  │  Step 8:  deleteVertex() × N      [individual stmts]   │ │
│  │  Step 9:  batchRemoveIndexes()     [LOGGED BATCH]       │ │
│  │  Step 10: batchRemovePropertyIdx() [LOGGED BATCH]       │ │
│  │  Step 11: batchAddIndexes()        [LOGGED BATCH]       │ │
│  │  Step 12: batchAddPropertyIdx()    [LOGGED BATCH]       │ │
│  │  Step 13: syncTypeDefsToCache()    [cache puts]         │ │
│  │  Step 14: syncToElasticsearch()    [ES bulk, 3 retries] │ │
│  │                                                         │ │
│  │  finally { buffer.clear(); vertexCache.clear(); }       │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                              │
│  Each step is an INDEPENDENT Cassandra call.                 │
│  No cross-step atomicity. No rollback of persisted writes.   │
└─────────────────────────────────────────────────────────────┘
```

### Failure Windows

| Window | What Succeeded | What Failed | Impact | Severity |
|--------|---------------|-------------|--------|----------|
| **W1** | Vertices written | Edges failed | Orphaned vertices, no relationships | HIGH |
| **W2** | Vertices + edges | Vertex indexes failed | Entities exist but unreachable by GUID/QN lookup | **CRITICAL** |
| **W3** | All Cassandra | ES sync failed | Data correct but invisible to search | MEDIUM |
| **W4** | Some dirty vertex updates | Later update failed | Some entities updated, others stale | MEDIUM |
| **W5** | Edges too large for batch | `InvalidQueryException` | Vertices persisted, edges lost | HIGH |

### The Good News: Entity Resolution Is Already Mostly Idempotent

**This is a critical finding.** The entity resolution in `AtlasEntityStoreV2.preCreateOrUpdate()` uses `findByUniqueAttributes(qualifiedName + typeName)` — NOT the GUID — to decide CREATE vs UPDATE:

```
Client sends:  { guid: "-1234", typeName: "Table", qualifiedName: "db/schema/table1" }
                                          │
                                          ▼
                    findByUniqueAttributes("Table", "db/schema/table1")
                                          │
                           ┌──────────────┴──────────────┐
                           │                             │
                     Vertex found                   Vertex NOT found
                           │                             │
                     → UPDATE path               → CREATE path
                     (merge attributes)          (new vertex, new GUID)
```

**On retry after partial failure**, entities that were already persisted to Cassandra will be found by `findByUniqueAttributes()` and routed to UPDATE — **not** duplicate CREATE. The retry naturally converges.

### Where It Breaks: Glossary/Domain Preprocessors

The generic `createOrUpdate` path is safe. But **specific preprocessors** throw "already exists" errors that break retry:

| Preprocessor | Error | When Thrown | Retry Safe? |
|---|---|---|---|
| `GlossaryPreProcessor` | `GLOSSARY_ALREADY_EXISTS` (409) | `processCreateGlossary()` checks name uniqueness | **NO** — retry of CREATE throws 409 |
| `TermPreProcessor` | `GLOSSARY_TERM_ALREADY_EXISTS` (409) | `processCreateTerm()` checks name | **NO** |
| `CategoryPreProcessor` | `GLOSSARY_CATEGORY_ALREADY_EXISTS` (409) | Checks name uniqueness | **NO** |
| `PersonaPreProcessor` | `ACCESS_CONTROL_ALREADY_EXISTS` (409) | Checks name | **NO** |
| `PurposePreProcessor` | `ACCESS_CONTROL_ALREADY_EXISTS` (409) | Checks name | **NO** |
| `DataDomainPreProcessor` | `BAD_REQUEST` (400) | Domain name conflict | **NO** |
| `DataProductPreProcessor` | `BAD_REQUEST` (400) | Product name conflict | **NO** |

**The preprocessor "already exists" checks run only on CREATE, not UPDATE.** So if the first attempt created the entity (persisted to Cassandra) but failed later, the retry finds the entity by QN and routes to UPDATE — which skips the preprocessor "already exists" check. **This is actually safe for most cases.**

**The real danger is when the first attempt created the vertex but NOT the vertex indexes (Window W2).** Without the `qn_type_idx` index entry, `findByUniqueAttributes()` returns null → CREATE path → preprocessor checks name → finds existing entity via ES search → throws "already exists" (409).

### Where It Breaks: GUID Assignment Mapping

The `EntityMutationResponse.guidAssignments` maps negative GUIDs to assigned GUIDs:
```json
{ "-1234": "a1b2c3d4-..." }
```

On retry, a NEW UUID is generated for entities that go through the CREATE path again. The client's negative GUID now maps to a different assigned GUID than the first attempt. This can break:
- Inter-entity references within the same request (e.g., entity B references entity A by negative GUID)
- Client-side caching of GUIDs from previous attempt

---

## Proposed Approaches

### Approach 1: Pre-Validate, Then Commit (Fail-Fast)

**Principle**: Move ALL validation before any Cassandra writes. If validation passes, the commit is very likely to succeed. If it fails, nothing was written.

```
┌──────────────────────────────────────────────────────┐
│  PHASE 1: Validate Everything (no Cassandra writes)  │
│                                                      │
│  1. Parse and resolve all entities                   │
│  2. Run ALL preprocessors (QN generation, etc.)      │
│  3. Run ALL authorization checks                     │
│  4. Validate ALL attribute types and constraints     │
│  5. Check ALL uniqueness constraints (QN+typeName)   │
│  6. Verify ALL relationship references resolve       │
│                                                      │
│  If ANY validation fails → return 400/409            │
│  Zero Cassandra writes. Clean rejection.             │
├──────────────────────────────────────────────────────┤
│  PHASE 2: Commit (Cassandra writes)                  │
│                                                      │
│  All entities are validated. Commit is "mechanical"  │
│  — only infrastructure failures can cause errors.    │
│                                                      │
│  If Cassandra fails → partial state, but:            │
│  - No business logic errors (those were caught)      │
│  - Retry is safe (createOrUpdate resolves by QN)     │
│  - Only infrastructure retries needed                │
└──────────────────────────────────────────────────────┘
```

**What this solves**: Eliminates the most common failure path — business logic errors during commit. The only failures during commit would be infrastructure-level (Cassandra timeouts, batch-too-large), which are safe to retry because of the createOrUpdate idempotency.

**What this doesn't solve**: Infrastructure failures during commit still produce partial state. But these are rare and retryable.

**Implementation effort**: LOW. Most validation already happens before commit. The main change is ensuring preprocessors don't have side effects during validation and that authorization is fully evaluated upfront.

**Risk**: LOW. This is a reordering of existing logic, not new logic.

### Approach 2: Reorder Commit Steps for Safety

**Principle**: Write vertex indexes BEFORE or TOGETHER WITH vertices. This eliminates the most critical failure window (W2: vertices exist but indexes don't).

**Current order** (dangerous):
```
Step 1:  vertices        ← persisted
Step 3:  edges           ← persisted
Step 11: vertex indexes  ← FAILS HERE → vertices unreachable
```

**Safe order**:
```
Step 1:  vertex indexes (qn_type_idx, __guid_idx)  ← write first
Step 2:  vertices
Step 3:  edges
Step 4:  edge indexes
...
```

If indexes are written first:
- Failure after indexes but before vertices → index points to non-existent vertex → `findByUniqueAttributes()` returns null → safe retry creates the vertex
- Failure after vertices but before edges → vertex exists AND is reachable by QN → retry finds it as UPDATE → safe

**Better yet: combine vertex + index in a single LOGGED batch**:
```java
// Single LOGGED batch: vertex + its indexes are atomic
BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
for (CassandraVertex v : newVertices) {
    batch.addStatement(insertVertexStmt.bind(...));
    batch.addStatement(insertIndexStmt.bind("__guid_idx", guid, vertexId));
    batch.addStatement(insertIndexStmt.bind("qn_type_idx", qn + "#" + typeName, vertexId));
}
session.execute(batch.build());
```

**Constraint**: All statements in a LOGGED batch must be in the same keyspace. The `vertices` and `vertex_index` tables are in the same keyspace (`atlas_graph`), so this works.

**Implementation effort**: MEDIUM. Requires refactoring `commit()` step ordering and potentially merging vertex + index into a single batch.

**Risk**: LOW. LOGGED batches across tables in the same keyspace are well-supported by Cassandra.

### Approach 3: Chunk Commit by Entity (Entity-Level Atomicity)

**Principle**: Instead of one massive commit for all entities, commit each entity (or small group) independently. Each entity's vertex + edges + indexes go in a single LOGGED batch.

```
For each entity in the bulk request:
  ┌───────────────────────────────────────────────────┐
  │  Single LOGGED batch for entity:                  │
  │    - INSERT vertex                                │
  │    - INSERT __guid_idx                            │
  │    - INSERT qn_type_idx                           │
  │    - INSERT edges_out (for this entity's edges)   │
  │    - INSERT edges_in (for this entity's edges)    │
  │    - INSERT edge_index entries                    │
  └───────────────────────────────────────────────────┘
```

**Benefits**:
- Each entity is atomic (LOGGED batch)
- Partial failure = some entities committed, others not
- Can return a partial success response (see Approach 5)

**Constraints**:
- LOGGED batches work best when touching few partitions. An entity's vertex, indexes, and edges may span many partitions (vertex table, index table, edges_out for source vertex, edges_in for target vertex).
- Cassandra LOGGED batches across many partitions have high coordination cost.
- Edge inserts touch 3 tables × 2 directions = up to 6 partitions per edge.

**Implementation effort**: HIGH. Requires restructuring the TransactionBuffer to group by entity and the commit path to iterate per-entity.

**Risk**: MEDIUM. Large per-entity batches (entity with 50 edges = 300+ statements) could exceed batch size limits. Multi-partition LOGGED batches are slower.

### Approach 4: Deterministic Retry (Make Retry Convergent)

**Principle**: Ensure that retrying the exact same request always converges to the correct final state, regardless of what was partially written. This builds on the existing `findByUniqueAttributes` behavior.

**Key changes**:

1. **Deterministic GUID generation** — Cache GUID per (typeName + qualifiedName) in `RequestContext` so retries reuse the same GUID:
```java
// In EntityGraphMapper.createVertex():
String cacheKey = typeName + ":" + qualifiedName;
String guid = RequestContext.get().getCachedGuid(cacheKey);
if (guid == null) {
    guid = UUID.randomUUID().toString();
    RequestContext.get().cacheGeneratedGuid(cacheKey, guid);
}
```

2. **Idempotent edge creation** — Before creating an edge, check if one already exists for the same (outVertex, inVertex, label):
```java
// In CassandraGraph.addEdge():
CassandraEdge existing = edgeRepository.findEdge(outId, inId, label);
if (existing != null) return existing;  // reuse on retry
```

3. **Idempotent index writes** — Use `INSERT ... IF NOT EXISTS` (LWT) for unique indexes:
```java
// Returns true if inserted, false if already exists with same vertex_id
boolean inserted = indexRepository.insertIfNotExists("qn_type_idx", key, vertexId);
```

4. **Make preprocessor "already exists" checks retry-aware** — If the existing entity has the same qualifiedName pattern, treat it as a retry rather than a conflict.

**Implementation effort**: MEDIUM. Each piece is small, but there are many touchpoints.

**Risk**: LOW individually. The GUID caching and edge dedup are purely additive safety nets.

### Approach 5: Partial Success Response

**Principle**: Accept that partial failures happen and make them visible to the client. Return a response that says "entities A, B, C succeeded; entities D, E failed."

**Changes**:

1. **Extend `EntityMutationResponse`**:
```java
public class EntityMutationResponse {
    Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities;  // existing
    Map<String, String> guidAssignments;                            // existing
    List<EntityFailure> failedEntities;                             // NEW
    boolean partialSuccess;                                         // NEW
}

public class EntityFailure {
    String entityGuid;          // negative or assigned GUID
    String qualifiedName;
    String errorCode;
    String errorMessage;
}
```

2. **Return HTTP 207 (Multi-Status)** for partial success instead of 200 or 500.

3. **Client SDK handles 207** by separating succeeded and failed entities.

**Why I'm cautious about this approach**: It fundamentally changes the API contract. Every SDK client, every workflow, every integration must be updated to handle partial success. This is a massive compatibility break. The "all-or-nothing" contract is simpler and most clients are built around it.

**Implementation effort**: HIGH (including SDK changes, client changes, testing).

**Risk**: HIGH (compatibility break).

---

## Recommended Strategy: Layered Defense

I recommend combining Approaches 1 + 2 + 4, which together provide strong guarantees without changing the API contract:

```
┌─────────────────────────────────────────────────────────────┐
│ Layer 1: Pre-Validate Everything (Approach 1)               │
│                                                             │
│ Catch 95%+ of errors before any Cassandra writes.           │
│ Business logic errors → clean 400/409 response.             │
│ Zero Cassandra side effects on validation failure.          │
├─────────────────────────────────────────────────────────────┤
│ Layer 2: Safe Commit Order (Approach 2)                     │
│                                                             │
│ Write vertex + indexes in single LOGGED batch.              │
│ Eliminates the most critical failure window (W2).           │
│ Even if edges fail, entities are reachable for retry.       │
├─────────────────────────────────────────────────────────────┤
│ Layer 3: Deterministic Retry (Approach 4)                   │
│                                                             │
│ Cache GUIDs, dedup edges, idempotent indexes.               │
│ If Layer 1+2 still produce a partial failure,               │
│ client retry converges to correct state.                    │
├─────────────────────────────────────────────────────────────┤
│ Layer 4: ES Sync Resilience (existing + improved)           │
│                                                             │
│ ES sync already has 3-retry with backoff.                   │
│ ES failures don't affect Cassandra correctness.             │
│ RepairIndex endpoint handles ES catch-up.                   │
└─────────────────────────────────────────────────────────────┘
```

### Why NOT Partial Success Response (Approach 5)?

- Changes the API contract for ALL clients
- Requires SDK changes across multiple languages
- "All-or-nothing" is a simpler, more predictable contract
- With Layers 1-3, the probability of a commit failure is very low (only infrastructure failures)
- Infrastructure failures are transient and retryable

### Implementation Priority

| Phase | What | Effort | Impact |
|-------|------|--------|--------|
| **P1** | Pre-validate: ensure all preprocessors and auth checks run before commit | LOW | Eliminates most 500s during commit |
| **P2** | Reorder commit: vertex + index in single LOGGED batch | MEDIUM | Eliminates the critical W2 failure window |
| **P3** | Deterministic GUID caching in RequestContext | LOW | Makes retry produce same GUIDs |
| **P4** | Idempotent edge creation (check-before-create) | LOW | Prevents duplicate edges on retry |
| **P5** | LWT for unique indexes (IF NOT EXISTS) | MEDIUM | Detects rather than silently overwrites uniqueness conflicts |

---

## Detailed Failure Probability Analysis

How likely is each failure window in practice?

| Failure Window | Cause | Likelihood | Notes |
|---|---|---|---|
| W1: Vertices ok, edges fail | Batch too large, timeout | **LOW** for normal ops. **MEDIUM** for bulk imports with many edges. | Mitigated by chunking edge batches (already done for deletes at 150/batch, NOT for inserts). |
| W2: Data ok, indexes fail | Timeout, batch too large | **LOW** | The index batch is small (2 entries per entity). |
| W3: Cassandra ok, ES fails | ES cluster issues | **MEDIUM** | Already has 3-retry. ES is eventually consistent anyway. |
| W4: Partial dirty updates | Timeout mid-loop | **VERY LOW** | Individual updates are fast. |
| W5: Batch too large on edges | >50KB batch payload | **MEDIUM** for bulk requests with many edges | Need to add chunking to `batchInsertEdges()`. |

**Bottom line**: With pre-validation (Layer 1) catching business logic errors, the remaining failure modes are infrastructure-only. Adding edge batch chunking and vertex+index atomic writes makes infrastructure failures recoverable via retry.

---

## Comparison: JanusGraph vs CassandraGraph Transaction Safety

| Aspect | JanusGraph | CassandraGraph (current) | CassandraGraph (with Layers 1-3) |
|--------|-----------|-------------------------|--------------------------------|
| Pre-commit validation | Same | Same | Enhanced (all validation before writes) |
| Commit atomicity | Full ACID | 14 independent operations | Vertex+index atomic; rest sequential |
| Rollback effectiveness | Full undo | Buffer clear only (no Cassandra undo) | Buffer clear only (but retry is safe) |
| Retry safety | Clean slate (full rollback) | createOrUpdate resolves by QN (mostly safe) | Deterministic GUIDs + edge dedup (fully safe) |
| "Already exists" on retry | No (clean rollback) | Possible for glossary/domain types | No (preprocessors handle retry case) |
| GUID stability across retries | N/A (rollback means no prior GUID) | Different GUID per attempt | Same GUID per attempt (cached) |
| Partial success visibility | N/A | 500 error, no detail | 500 error, no detail (by design) |

---

## Key Parameters (Confirmed)

- **Bulk request size**: 20 entities (confirmed by user)
- **Largest tenant**: 92M assets across many connections
- **Entity hierarchy**: Connection → Database → Schema → Table → Column (edges only to next level)

With 20 entities per bulk request, the blast radius of a partial failure is small:
- At most 20 orphaned vertices on commit failure
- Retry via `findByUniqueAttributes()` naturally reconciles all 20
- Approach 3 (per-entity commit) is NOT needed — the cost of retrying 20 entities is negligible

## Open Questions

1. **Should `batchInsertEdges()` be chunked like `batchDeleteEdges()`?** Currently, edge inserts are a single LOGGED batch regardless of size. Edge deletes are chunked at 150. With 20 entities × ~3 edges each = ~180 CQL statements — this is right at the boundary. We should add chunking to inserts to prevent "batch too large" errors for requests with many relationship edges.

2. **Should the pre-validation phase be a separate API call?** A "dry-run" endpoint that validates without persisting could give clients confidence before committing. This would be a new endpoint, not a change to the existing one.

3. **ES sync failure frequency**: How often does ES sync actually fail in production? If it's rare, the existing 3-retry may be sufficient. If it's common, we need a background reconciliation job.
