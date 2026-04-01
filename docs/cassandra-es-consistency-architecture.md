# CassandraGraph: Consistency, Convergence & ES Reliability

## Overall Architecture

```
+---------------------------------------------------------------+
|                        Atlas Metastore                         |
+---------------------------------------------------------------+
|                                                                |
|  +-----------+    +-------------------+    +----------------+  |
|  | EntityREST|    | EntityMutationSvc |    | AtlasEntity    |  |
|  | (webapp)  |--->| (transaction      |--->| StoreV2        |  |
|  | POST /v2/ |    |  boundary)        |    | (@GraphTxn)    |  |
|  | entity    |    |                   |    |                |  |
|  +-----------+    +--------+----------+    +-------+--------+  |
|                            |                       |           |
|                   finally: ES denorm       Preprocessors       |
|                   (ESConnector)            EntityGraphMapper    |
|                            |               (TransactionBuffer)  |
|                            v                       |           |
|                   +-----------------+              v           |
|                   | ESConnector     |    +------------------+  |
|                   | writeTagProps() |    | GraphTransaction |  |
|                   | (bulk update)   |    | Interceptor      |  |
|                   +--------+--------+    +--------+---------+  |
|                            |                      |            |
|                            v                      v            |
|                   +----------------+    +------------------+   |
|                   | Elasticsearch  |    | CassandraGraph   |   |
|                   | (tag denorm    |    | .commit()        |   |
|                   |  attributes)   |    | (14-step flow)   |   |
|                   +----------------+    +--------+---------+   |
|                                                  |             |
|                   +------------------------------+             |
|                   |              |                |             |
|                   v              v                v             |
|           +------------+  +----------+  +--------------+       |
|           | Cassandra  |  | Elastic- |  | Kafka        |       |
|           | (graph     |  | search   |  | (post-commit |       |
|           |  tables)   |  | (entity  |  |  hooks)      |       |
|           |            |  |  index)  |  |              |       |
|           +------------+  +----------+  +--------------+       |
|                   ^              ^                              |
|                   |              |                              |
|           +-------+--------------+------+                      |
|           |  Background Processors      |                      |
|           |                             |                      |
|           |  +---------------------+    |                      |
|           |  | JobLeaseManager     |    |  LWT lease coord     |
|           |  | (per-job exclusion) |    |  (Cassandra LWT)     |
|           |  +---------------------+    |                      |
|           |  +---------------------+    |                      |
|           |  | ESOutboxProcessor   |    |  Adaptive polling:   |
|           |  | (idle=30s,drain=2s) |    |  lease-guarded       |
|           |  +---------------------+    |                      |
|           |  +---------------------+    |                      |
|           |  | ESReconciliationJob |    |  Sample-based audit  |
|           |  | (every 6h)          |    |  ~1000 random GUIDs  |
|           |  +---------------------+    |                      |
|           |  +---------------------+    |                      |
|           |  | OrphanVertexCleanup |    |  Progressive cursor  |
|           |  | (every 24h)         |    |  lease-guarded       |
|           |  +---------------------+    |                      |
|           |  +---------------------+    |                      |
|           |  | OrphanEdgeCleanup   |    |  Progressive cursor  |
|           |  | (every 24h)         |    |  lease-guarded       |
|           |  +---------------------+    |                      |
|           +-----------------------------+                      |
+----------------------------------------------------------------+
```

## Cassandra Tables (atlas_graph keyspace)

```
+--------------------+     +--------------------+     +--------------------+
| vertices           |     | edges_out          |     | edges_in           |
|--------------------|     |--------------------|     |--------------------|
| PK: vertex_id      |     | PK: (out_vertex_id)|     | PK: (in_vertex_id) |
| properties (JSON)  |     | CK: edge_label,    |     | CK: edge_label,    |
| vertex_label       |     |     edge_id         |     |     edge_id         |
| type_name          |     | in_vertex_id       |     | out_vertex_id      |
| state              |     | properties (JSON)  |     | properties (JSON)  |
| created_at         |     | state, timestamps  |     | state, timestamps  |
| modified_at        |     +--------------------+     +--------------------+
+--------------------+

+--------------------+     +--------------------+     +--------------------+
| edges_by_id        |     | vertex_index       |     | vertex_property_   |
|--------------------|     |--------------------|     | index              |
| PK: edge_id        |     | PK: (index_name,   |     |--------------------|
| out_vertex_id      |     |     index_value)    |     | PK: (index_name,   |
| in_vertex_id       |     | vertex_id          |     |     index_value)    |
| edge_label         |     +--------------------+     | CK: vertex_id      |
| properties (JSON)  |     Indexes:                   +--------------------+
| state, timestamps  |     - __guid_idx               Indexes:
+--------------------+     - qn_type_idx              - type_category_idx
                           - type_typename_idx

+--------------------+     +--------------------+     +--------------------+
| edge_index         |     | entity_claims      |     | es_outbox          |
|--------------------|     |--------------------|     |--------------------|
| PK: (index_name,   |     | PK: identity_key   |     | PK: (status)       |
|     index_value)    |     | vertex_id          |     | CK: vertex_id      |
| edge_id            |     | claimed_at         |     | es_action          |
+--------------------+     | source             |     | properties_json    |
Index:                     +--------------------+     | attempt_count      |
- _r__guid_idx             LWT: INSERT IF NOT EXISTS  | created_at         |
                                                      | last_attempted_at  |
                                                      | TTL: 7 days        |
                                                      | gc_grace: 3600s    |
                                                      +--------------------+
                                                      Partitioned by status:
                                                      PENDING/FAILED scans
                                                      are direct partition
                                                      reads (no ALLOW
                                                      FILTERING)

+--------------------+     +--------------------+     +--------------------+
| type_definitions   |     | type_definitions_  |     | schema_registry    |
|--------------------|     | by_category        |     |--------------------|
| PK: type_name      |     |--------------------|     | PK: property_name  |
| type_category      |     | PK: (type_category)|     | property_class     |
| vertex_id          |     | CK: type_name      |     | cardinality        |
| timestamps         |     | vertex_id          |     | created_at         |
+--------------------+     +--------------------+     +--------------------+

+--------------------+     +--------------------+
| job_leases         |     | repair_progress    |
|--------------------|     |--------------------|
| PK: job_name       |     | PK: job_name       |
| owner (pod ID)     |     | last_token (bigint)|
| acquired_at        |     | last_run_at        |
| Written with TTL   |     | rows_processed     |
| (auto-release on   |     | cycle_complete     |
|  crash)            |     +--------------------+
| LWT: INSERT IF     |     Progressive cursor
|  NOT EXISTS        |     tracking for orphan
+--------------------+     scans (crash-resume)
```

## Entity Ingestion Data Flow

```
HTTP POST /api/atlas/v2/entity
         |
         v
+-------------------+
| EntityREST        |
| .createOrUpdate() |
+--------+----------+
         |
         v
+-------------------+    +----------------------------------------------+
| EntityMutation    |    | On failure: rollbackNativeCassandraOperations |
| Service           |    | (undo tag inserts via TagDAO.deleteTags)     |
| .createOrUpdate() |--->+----------------------------------------------+
|                   |    | On finally: executeESPostProcessing           |
+--------+----------+    | -> ESConnector.writeTagProperties()           |
         |               |    (denorm tag attrs to ES, bulk update)      |
         |               +----------------------------------------------+
         v
+--------------------+  @GraphTransaction
| AtlasEntityStoreV2 |
| .createOrUpdate()  |
+--------+-----------+
         |
         +----> 1. preCreateOrUpdate()
         |         - Entity discovery (resolve GUIDs, unique attrs)
         |         - New: EntityGraphMapper.createVertex() -> UUID GUID
         |         - Existing: route to UPDATE path
         |
         +----> 2. Authorization checks (create/update permissions)
         |
         +----> 3. Diff calculation (AtlasEntityComparator)
         |         - Skip unchanged entities
         |
         +----> 4. executePreProcessor()
         |         - Type-specific: GlossaryPreProcessor, TermPreProcessor, etc.
         |         - QualifiedName generation (NanoIdUtils.randomNanoId)
         |         - Validation, attribute enrichment
         |
         +----> 5. entityGraphMapper.mapAttributesAndClassifications()
         |         - mapAttributes() -> vertex properties in TransactionBuffer
         |         - mapRelationshipAttributes() -> edges in TransactionBuffer
         |         - handleAddClassifications() -> classification vertices/edges
         |         NOTE: All mutations are IN-MEMORY only at this point
         |
         +----> 6. entityChangeNotifier.onEntitiesMutated()
         |         - Full-text mapping
         |         - Queue Kafka notifications to PostCommitNotificationHook
         |         - EntityAuditListenerV2 writes audit records
         |
         v
+------------------------+
| GraphTransaction       |  The interceptor calls graph.commit()
| Interceptor            |  after business logic completes
| .invoke()              |
+--------+---------------+
         |
         v
+------------------------+
| CassandraGraph         |
| .commit()              |          See detailed flow below
+--------+---------------+
         |
         v (on success)
+------------------------+
| PostTransactionHooks   |
| .onComplete(true)      |
|   |                    |
|   +-> KafkaNotification|
|       .send(ENTITIES)  |
|       .send(RELATIONS) |
+------------------------+
```

## CassandraGraph.commit() — Detailed 14-Step Flow

```
commit()
  |
  |  STEP 1: Claim Resolution (LWT Dedup)
  |  +------------------------------------------------------------+
  |  | For each new vertex with QN + typeName:                     |
  |  |   identityKey = "typeName|qualifiedName"                    |
  |  |   ClaimRepository.claimOrGet(identityKey, vertexId)         |
  |  |     -> INSERT INTO entity_claims ... IF NOT EXISTS          |
  |  |     -> If [applied]=true: we won, use candidateVertexId     |
  |  |     -> If [applied]=false: duplicate, use existing vertexId |
  |  |   rewriteEdgesForClaimedVertices() if duplicates found      |
  |  +------------------------------------------------------------+
  |
  |  STEP 2: Atomic Vertex + Index Batch          <<< NEW (Change 1)
  |  +------------------------------------------------------------+
  |  | Single LOGGED batch containing:                             |
  |  |   FOR each new vertex:                                      |
  |  |     INSERT INTO vertices (vertex_id, properties, ...)       |
  |  |     INSERT INTO vertex_index (__guid_idx, guid, vertexId)   |
  |  |     INSERT INTO vertex_index (qn_type_idx, qn:type, vId)   |
  |  |     INSERT INTO vertex_index (type_typename_idx, ...)       |
  |  |     INSERT INTO vertex_property_index (type_category_idx)   |
  |  |                                                             |
  |  | Eliminates W2 window: vertex + indexes are all-or-nothing   |
  |  +------------------------------------------------------------+
  |
  |  STEP 3: Update Dirty Vertices
  |  +------------------------------------------------------------+
  |  | For each modified vertex: full overwrite via INSERT         |
  |  +------------------------------------------------------------+
  |
  |  STEP 4: Batch Insert New Edges
  |  +------------------------------------------------------------+
  |  | LOGGED batch across edges_out + edges_in + edges_by_id     |
  |  +------------------------------------------------------------+
  |
  |  STEP 5: Update Dirty Edges
  |  +------------------------------------------------------------+
  |  | Per-edge LOGGED batch across 3 edge tables                 |
  |  +------------------------------------------------------------+
  |
  |  STEP 6: Edge Index Entries
  |  +------------------------------------------------------------+
  |  | LOGGED batch: _r__guid_idx -> edge_id in edge_index        |
  |  +------------------------------------------------------------+
  |
  |  STEP 7: Process Removals
  |  +------------------------------------------------------------+
  |  | Removed edges: batch delete from edge tables + edge_index  |
  |  | Removed vertices: cascade-delete edges (paginated),        |
  |  |   delete vertex, remove all vertex index entries            |
  |  +------------------------------------------------------------+
  |
  |  STEP 8: Dirty Vertex Index Updates
  |  +------------------------------------------------------------+
  |  | Idempotent overwrite of index entries for dirty vertices   |
  |  +------------------------------------------------------------+
  |
  |  STEP 9: TypeDef Cache Sync
  |  +------------------------------------------------------------+
  |  | TypeDef vertices -> type_definitions table + Caffeine cache|
  |  +------------------------------------------------------------+
  |
  |  STEP 10: ES Outbox Write                      <<< NEW (Change 2)
  |  +------------------------------------------------------------+
  |  | BEFORE attempting ES sync, write to es_outbox table:       |
  |  |   LOGGED batch:                                             |
  |  |     INSERT INTO es_outbox (status='PENDING', vertex_id,    |
  |  |       es_action='index', properties_json, 0, now, now)     |
  |  |       USING TTL 604800                                      |
  |  |     INSERT INTO es_outbox (status='PENDING', deleteId,     |
  |  |       es_action='delete', null, ...) USING TTL 604800      |
  |  |                                                             |
  |  | Schema: PK=(status), CK=vertex_id                          |
  |  |   -> PENDING partition scan needs no ALLOW FILTERING       |
  |  |   -> gc_grace_seconds=3600 (1h tombstone compaction)       |
  |  |                                                             |
  |  | Guarantees: even if ES sync fails AND process crashes,     |
  |  | the outbox entry persists for background retry              |
  |  +------------------------------------------------------------+
  |
  |  STEP 11: Synchronous ES Sync (3 retries)
  |  +------------------------------------------------------------+
  |  | Build ES _bulk request body:                                |
  |  |   {"index":{"_index":"janusgraph_vertex_index","_id":"vId"}}|
  |  |   {filtered properties JSON}                                |
  |  |   {"delete":{"_index":"...","_id":"deleteId"}}              |
  |  |                                                             |
  |  | POST /_bulk with retry:                                     |
  |  |   attempt 0: immediate                                      |
  |  |   attempt 1: +100ms backoff                                 |
  |  |   attempt 2: +300ms backoff                                 |
  |  |   attempt 3: +1000ms backoff                                |
  |  |                                                             |
  |  | Per-item response parsing:                                  |
  |  |   2xx without errors -> all succeeded                       |
  |  |   "errors":true -> parse items:                             |
  |  |     5xx/429 -> retryable (retry only these items)           |
  |  |     4xx -> permanent failure (log, skip)                    |
  |  |   5xx bulk status -> retry everything                       |
  |  |   4xx bulk status -> abort                                  |
  |  +------------------------------------------------------------+
  |
  |  STEP 12: Outbox Cleanup                        <<< NEW (Change 2)
  |  +------------------------------------------------------------+
  |  | For successfully synced vertices:                           |
  |  |   DELETE FROM es_outbox WHERE status='PENDING'              |
  |  |     AND vertex_id = ?                                       |
  |  |   (UNLOGGED batch — single partition)                       |
  |  |                                                             |
  |  | For failed vertices:                                        |
  |  |   Leave PENDING -> ESOutboxProcessor will retry             |
  |  +------------------------------------------------------------+
  |
  |  STEP 13: Mark Persisted (in-memory)
  |  STEP 14: Clear TransactionBuffer + VertexCache
```

## ES Outbox: Failure Recovery Flow

```
                       HAPPY PATH                    FAILURE PATH
                       ----------                    ------------

commit() Step 10       commit() Step 10
  |                      |
  v                      v
Write PENDING to       Write PENDING to
es_outbox              es_outbox
  |                      |
  v                      v
Step 11: ES sync       Step 11: ES sync
  |                      |
  v                      v
SUCCESS                FAILURE (ES down / partial / crash)
  |                      |
  v                      |
Step 12: DELETE          |  Outbox entries remain PENDING
from es_outbox           |
  |                      v
  v               +-----------------------+
DONE              | ESOutboxProcessor     |  Adaptive polling:
                  | polls es_outbox       |  - Idle: 30s (no PENDING)
                  | WHERE status='PENDING'|  - Drain: 2s, batch 500
                  | (direct partition     |  - Lease-guarded: only 1
                  |  scan, no ALLOW       |    pod processes at a time
                  |  FILTERING)           |  - 15K entries/min drain
                  +-----------+-----------+    throughput
                              |
                    +---------+---------+
                    |                   |
                    v                   v
              attempt < 10         attempt >= 10
                    |                   |
                    v                   v
              Retry ES _bulk      LOGGED batch:
              POST /_bulk           DELETE from PENDING
                    |               INSERT into FAILED
                    |               (cross-partition move,
              +-----+-----+         preserved for investigation)
              |           |
              v           v
          SUCCESS      FAILURE
              |           |
              v           v
          DELETE      Increment
          from        attempt_count
          PENDING     (retry next cycle,
          partition   2s in drain mode)

              +-------> TTL auto-cleanup after 7 days
```

## Repair Loops: Detection & Convergence

All background jobs are lease-guarded via `JobLeaseManager` (Cassandra LWT).
Only one pod runs each job at a time. Crashed pods auto-release via TTL expiry.

```
+-----------------------------------------------------------------------+
|                    RepairJobScheduler                                   |
|                    (started at CassandraGraph construction)             |
|                    All jobs lease-guarded via JobLeaseManager           |
+-----------------------------------------------------------------------+
|                                                                        |
|  +----------------------------+                                        |
|  | JobLeaseManager            |  Reusable LWT lease coordinator        |
|  |                            |                                        |
|  | tryAcquire(jobName, ttl):  |  INSERT INTO job_leases ...            |
|  |   INSERT IF NOT EXISTS     |  IF NOT EXISTS USING TTL ?             |
|  |   USING TTL ?              |  -> true if acquired                   |
|  |                            |  -> false if another pod holds it      |
|  | release(jobName):          |                                        |
|  |   DELETE IF owner = podId  |  Conditional: only owner can release   |
|  |                            |                                        |
|  | Crash recovery:            |  TTL auto-expires the lease            |
|  |   Pod dies -> lease TTL    |  Another pod acquires within TTL       |
|  |   expires -> next pod      |                                        |
|  |   acquires naturally       |                                        |
|  +----------------------------+                                        |
|                                                                        |
|  +----------------------------+                                        |
|  | ESReconciliationJob        |  Every 6h (lease: "es-reconciliation") |
|  | SAMPLE-BASED (not full     |  Lease TTL: 2h                         |
|  | scan)                      |                                        |
|  |                            |                                        |
|  | 1. Random token probes:    |  Pick random Cassandra token,          |
|  |    SELECT FROM vertex_index|  scan forward 200 rows.                |
|  |    WHERE index_name =      |  Repeat 5x with different tokens       |
|  |    '__guid_idx'            |  -> ~1000 GUIDs spread across ring     |
|  |    AND token(...) >= ?     |                                        |
|  |    LIMIT 200               |                                        |
|  |         |                  |                                        |
|  |         v                  |                                        |
|  | 2. Batch _mget against ES  |  100 GUIDs/batch                       |
|  |    (check doc existence)   |                                        |
|  |         |                  |                                        |
|  |         v                  |                                        |
|  | 3. Missing? -> reindex     |  CassandraGraph                        |
|  |    via reindexVertices()   |  .reindexVertices(guids)               |
|  |    (read from Cassandra,   |  50 GUIDs/batch                        |
|  |     bulk-write to ES)      |                                        |
|  |         |                  |                                        |
|  |         v                  |                                        |
|  | 4. Miss rate > 5%?         |  LOG.error critical alert              |
|  |    -> signals systemic     |  (signals ES outage or bug)            |
|  |    problem                 |                                        |
|  +----------------------------+                                        |
|                                                                        |
|  +----------------------------+                                        |
|  | OrphanVertexCleanup        |  Every 24h (lease: "orphan-vertex")    |
|  | PROGRESSIVE CURSOR         |  Lease TTL: 30min                      |
|  |                            |                                        |
|  | 1. Read repair_progress    |  Resume from last_token                |
|  |    for last_token          |  (or Long.MIN_VALUE if first run)      |
|  |         |                  |                                        |
|  |         v                  |                                        |
|  | 2. Scan vertices table:    |  Bounded work per cycle:               |
|  |    WHERE token(vertex_id)  |  LIMIT = 10,000 rows/cycle             |
|  |    > ? LIMIT 10000         |  Full table scanned over multiple      |
|  |         |                  |  days progressively                    |
|  |         v                  |                                        |
|  | 3. For each vertex with    |  vertex_index                          |
|  |    __guid, check if        |  lookup                                |
|  |    __guid_idx exists       |                                        |
|  |         |                  |                                        |
|  |         v                  |                                        |
|  | 4. Missing? -> repair      |  INSERT INTO vertex_index              |
|  |    - __guid_idx            |  (__guid_idx, guid, vertexId)          |
|  |    - qn_type_idx           |  (qn_type_idx, qn:type, vertexId)     |
|  |         |                  |                                        |
|  |         v                  |                                        |
|  | 5. Update repair_progress  |  Persist cursor for crash resume       |
|  |    (last_token, rows_      |  When scan wraps around: reset         |
|  |     processed, cycle_      |  cursor, set cycle_complete=true       |
|  |     complete)              |                                        |
|  +----------------------------+                                        |
|                                                                        |
|  +----------------------------+                                        |
|  | OrphanEdgeCleanup          |  Every 24h (lease: "orphan-edge")      |
|  | PROGRESSIVE CURSOR         |  Lease TTL: 30min                      |
|  |                            |                                        |
|  | 1. Read repair_progress    |  Resume from last_token                |
|  |         |                  |                                        |
|  |         v                  |                                        |
|  | 2. Scan edges_by_id:       |  Bounded: LIMIT 10,000 rows/cycle     |
|  |    WHERE token(edge_id)    |                                        |
|  |    > ? LIMIT 10000         |                                        |
|  |         |                  |                                        |
|  |         v                  |                                        |
|  | 3. For each edge, check:   |  vertices table lookup                 |
|  |    - out_vertex exists?    |                                        |
|  |    - in_vertex exists?     |                                        |
|  |         |                  |                                        |
|  |         v                  |                                        |
|  | 4. Missing endpoint? ->    |  DELETE from edges_out +               |
|  |    delete orphan edge      |  edges_in + edges_by_id                |
|  |    from all 3 edge tables  |                                        |
|  |         |                  |                                        |
|  |         v                  |                                        |
|  | 5. Update repair_progress  |  Persist cursor for crash resume       |
|  +----------------------------+                                        |
+------------------------------------------------------------------------+
```

## ESConnector: Tag/Classification Denorm Write Flow

```
                    (runs AFTER graph.commit() succeeds)

EntityMutationService.createOrUpdate()
  |
  | finally:
  v
executeESPostProcessing()
  |
  v
EntityCreateOrUpdateMutationPostProcessor
.executeESOperations(RequestContext.getESDeferredOperations())
  |
  | Group operations by entity ID
  | Priority: ADD > UPDATE > DELETE
  |
  +----> ADD operations (upsert=true)
  |        |
  |        v
  |      ESConnector.writeTagProperties(batch, upsert=true)  <<< FIXED (Change 3)
  |        |
  |        v
  |      Build ES _bulk request:
  |        {"update":{"_index":"janusgraph_vertex_index","_id":"<docId>"}}
  |        {"doc":{...denorm attrs...},"upsert":{...denorm attrs...}}
  |        |
  |        v
  |      POST /_bulk with retry (exponential backoff)
  |        |
  |        +---> 2xx: parse per-item results     <<< NEW (Change 3)
  |        |       5xx/429 items -> retry only those
  |        |       4xx items -> log permanent failure
  |        |       all succeeded -> return
  |        |
  |        +---> 5xx: retry entire batch
  |        +---> 4xx: throw RuntimeException
  |        +---> IOException: retry
  |
  +----> Other operations (upsert=false)
           |
           v
         ESConnector.writeTagProperties(batch, upsert=false)
           (same flow as above, without upsert clause)

  DENORM_ATTRS synced to ES:
    - __propagatedTraitNames
    - __propagatedClassificationNames
    - __classificationText
    - __traitNames
    - __classificationNames
```

## Consistency Guarantees Summary

```
+-------------------+------------------+---------------------------------+
| Failure Scenario  | Before Changes   | After Changes                   |
+-------------------+------------------+---------------------------------+
| Crash between     | Vertex exists    | IMPOSSIBLE: vertex + indexes    |
| vertex write and  | but unreachable  | written in single LOGGED batch  |
| index write       | (W2 window)      | (Change 1)                      |
+-------------------+------------------+---------------------------------+
| ES sync fails     | Entity missing   | Outbox entry persists in        |
| during commit()   | from search      | Cassandra (PENDING partition).   |
|                   | forever (until   | ESOutboxProcessor retries in    |
|                   | manual reindex)  | drain mode (2s), up to 10       |
|                   |                  | attempts. Lease-guarded: only   |
|                   |                  | 1 pod processes (Change 2)      |
+-------------------+------------------+---------------------------------+
| Process crashes   | ES never gets    | Outbox entry survives crash     |
| during ES sync    | the write        | (written BEFORE ES attempt).    |
|                   |                  | Lease TTL (60s) expires,        |
|                   |                  | another pod acquires lease and  |
|                   |                  | picks up PENDING entries         |
|                   |                  | (Change 2)                      |
+-------------------+------------------+---------------------------------+
| ES bulk 2xx with  | Partial failure  | Per-item response parsing       |
| per-item errors   | silently ignored | detects failures. 5xx retried,  |
| (ESConnector)     | (tag denorm)     | 4xx logged (Change 3)           |
+-------------------+------------------+---------------------------------+
| Gradual ES drift  | Undetected       | ESReconciliationJob samples     |
| (docs missing     |                  | ~1000 random GUIDs every 6h,    |
| from ES)          |                  | reindexes missing. Alerts if    |
|                   |                  | miss rate > 5% (Change 4)       |
+-------------------+------------------+---------------------------------+
| Orphan vertices   | Unreachable      | OrphanVertexCleanup: progressive|
| (missing indexes) | entities pile up | cursor scan (10K rows/cycle),   |
|                   |                  | repairs missing index entries.   |
|                   |                  | Crash-safe: resumes from cursor |
|                   |                  | (Change 4)                      |
+-------------------+------------------+---------------------------------+
| Orphan edges      | Dangling refs    | OrphanEdgeCleanup: progressive  |
| (missing endpoint | accumulate       | cursor scan, deletes edges with |
| vertices)         |                  | missing endpoints. Crash-safe   |
|                   |                  | (Change 4)                      |
+-------------------+------------------+---------------------------------+
| Pod crashes       | Background jobs  | Lease TTL auto-expires (60s for |
| mid-processing    | orphaned, never  | outbox, 30min for orphan scans).|
|                   | resume           | Another pod acquires lease and  |
|                   |                  | resumes from cursor checkpoint  |
+-------------------+------------------+---------------------------------+
| Multi-pod (8+)    | N/A (single-pod  | Lease-guarded: exactly 1 pod    |
| redundant work    | deployments      | runs each job. No duplicate ES  |
|                   | assumed)         | writes. Scale-up/down is auto-  |
|                   |                  | matic (lease competition)       |
+-------------------+------------------+---------------------------------+

Convergence guarantee:
  Even if ALL real-time mechanisms fail, the system self-heals within:
  - 2 seconds (outbox drain mode — adaptive, 30s in idle)
  - 60 seconds (lease failover on pod crash)
  - 6 hours (ES reconciliation sample audit)
  - Multiple days (progressive orphan repair — 10K rows/cycle)
```
