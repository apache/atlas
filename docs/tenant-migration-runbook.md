# Tenant Migration Runbook: JanusGraph → Cassandra Backend

**For Atlas Metastore operators — step-by-step guide to migrate a tenant**

---

## Overview

This runbook walks you through migrating a single tenant from the JanusGraph graph backend to the new Cassandra+ES backend. The process has four stages:

```
1. Deploy  →  2. Migrate Data  →  3. Switch Backend  →  4. Validate
```

**Key safety guarantees:**
- JanusGraph data is **never modified** — migration is copy-not-move
- Instant rollback: flip one config property back to `janus`, restart the pod
- Each tenant has its own pods, so there is zero blast radius to other tenants

**Time estimates by tenant size:**

| Tenant size | Asset count | Migration time |
|-------------|------------|----------------|
| Empty | 0 | Instant |
| Tiny | < 100K | < 1 minute |
| Small | 100K – 1M | 1–5 minutes |
| Medium | 1M – 10M | 10–30 minutes |
| Large | 10M – 50M | 1–2 hours |
| Very Large | 50M+ | 4–8 hours |

---

## Prerequisites

- `kubectl` access to the tenant's namespace
- The tenant is running a build that includes the `switchable-graph-provider` code
- You know the tenant's namespace and Atlas pod name (e.g., `atlas-0` in namespace `duair15p01`)

---

## Stage 1: Deploy the Switchable Graph Provider Build

### 1.1 Verify the build includes the Cassandra backend

The build must include:
- `graphdb/cassandra/` module (the new backend)
- `graphdb/migrator/` module (migration tool)
- `tools/atlas_migrate.sh` (migration script)

These ship automatically in any build from the `switchable-graph-provider` branch.

### 1.2 Deploy to the tenant

Deploy the new build using your standard deployment process (Helm, ArgoCD, etc.). **No config changes yet** — the default backend is `janus`, so the tenant continues running on JanusGraph exactly as before.

### 1.3 Verify the deployment is healthy

```bash
# Check pod is running
kubectl get pods -n atlas -l app=atlas

# Check Atlas is healthy
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  curl -s http://localhost:21000/api/atlas/admin/status

# Expected: {"Status":"ACTIVE"}
```

### 1.4 Verify the migrator is available

```bash
# Check the migration script exists
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  ls -la /opt/apache-atlas/bin/atlas_migrate.sh

# Check the migrator JAR exists
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  ls -la /opt/apache-atlas/tools/atlas-graphdb-migrator.jar
```

If either file is missing, the build does not include the migration modules. Check that the build was from the correct branch.

---

## Stage 2: Migrate Data (JanusGraph → Cassandra)

This stage copies all graph data from JanusGraph's binary `edgestore` table into the new human-readable Cassandra schema, and re-indexes everything into Elasticsearch.

### 2.1 Dry run — verify config

Run a dry run first. This reads the pod's `atlas-application.properties`, generates the migrator config, checks Cassandra/ES connectivity, and exits without migrating anything.

```bash
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  /opt/apache-atlas/bin/atlas_migrate.sh --dry-run
```

**Check the output for:**
- Cassandra host/port/datacenter are correct
- ES host/port are correct
- Source keyspace matches the tenant's JanusGraph keyspace (usually `atlas`)
- Target keyspace is `atlas_graph` (or whatever the tenant will use)
- Migrator JAR was found
- Preflight shows "Cassandra reachable" and "ES reachable"

**Example output:**
```
[migrator] Reading connection config from /opt/apache-atlas/conf/atlas-application.properties
[migrator]   Cassandra: atlas-cassandra:9042 (dc=datacenter1)
[migrator]   ES:        http://atlas-elasticsearch-master:9200
[migrator]   Source keyspace: atlas (table: edgestore)
[migrator]   Target keyspace: atlas_graph
[migrator]   Source ES index: janusgraph_vertex_index (copy mappings from)
[migrator]   Target ES index: atlas_graph_vertex_index (write docs to)
[migrator] Running preflight checks...
[migrator]   Cassandra reachable at atlas-cassandra:9042
[migrator]   ES reachable at http://atlas-elasticsearch-master:9200
[migrator] Dry run complete. Properties at /tmp/migrator.properties
```

### 2.2 Run the full migration

```bash
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  /opt/apache-atlas/bin/atlas_migrate.sh
```

The migration runs in three phases:

| Phase | What it does | Progress indicator |
|-------|-------------|-------------------|
| **Phase 1: Scan + Write** | Reads JanusGraph edgestore via CQL token-range scan, decodes binary data, writes to new Cassandra tables | `Vertices: X, Edges: Y ... Token ranges: A/B (ETA: ...)` |
| **Phase 2: ES Reindex** | Bulk-indexes all vertices into Elasticsearch | `ES docs: X` |
| **Phase 3: Validate** | Counts vertices/edges, samples GUID index, checks TypeDefs | Pass/fail per check |

**Progress is logged every 10 seconds** with throughput rates, ETA, and error counts.

**The migration is resumable.** If the pod restarts or the process is interrupted, re-run the same command — it will pick up from where it left off (completed token ranges are tracked in a `migration_state` table).

### 2.3 Monitor the migration

In a separate terminal, you can tail the log:

```bash
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  tail -f /opt/apache-atlas/logs/migrator-*.log
```

### 2.4 What a successful migration looks like

```
Migration complete in 12m 30s — Vertices: 2,345,678, Edges: 5,678,901,
Indexes: 3,456,789, TypeDefs: 953, ES docs: 2,345,000 | Avg rate: 3,127 vertices/s |
Decode errors: 0, Write errors: 0 | CQL rows: 8,900,000

=== Validation Results ===
  Vertex count:     PASS  (target: 2,345,678, expected: 2,345,678)
  Edge count:       PASS  (edges_out: 5,678,901, edges_by_id: 5,678,901)
  GUID index:       PASS  (1000/1000 sampled vertices found in index)
  TypeDef table:    PASS  (953 entries in type_definitions)
  Sample properties:PASS  (100/100 sampled vertices have non-empty properties)
```

**Key things to verify:**
- `Decode errors: 0` — all JanusGraph data was readable
- `Write errors: 0` — all data was written successfully
- All validation checks show `PASS`

### 2.5 If migration fails or has errors

| Symptom | Action |
|---------|--------|
| Pod restarted mid-migration | Re-run the same command — it resumes automatically |
| "Cannot reach Cassandra" | Check Cassandra is up: `kubectl get pods -n atlas -l app=cassandra` |
| Write errors > 0 | Check the log for specific CQL errors. Often a Cassandra resource issue. |
| Decode errors > 0 | Some JanusGraph rows couldn't be decoded. Check log for vertex IDs. Usually safe to proceed if < 0.01% of total. |
| Validation failed | See Stage 4 for detailed validation steps |
| Want to start over | Run with `--fresh` flag to clear resume state: `atlas_migrate.sh --fresh` |

### 2.6 ES-only reindex (optional)

If you need to rebuild the Elasticsearch index without re-scanning Cassandra (e.g., after an ES cluster issue):

```bash
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  /opt/apache-atlas/bin/atlas_migrate.sh --es-only
```

---

## Stage 3: Switch the Backend

### 3.1 Add the Cassandra backend config

Add these properties to the tenant's `atlas-application.properties`:

```properties
# ---- Switch to Cassandra graph backend ----
atlas.graphdb.backend=cassandra

# Cassandra connection (new graph schema)
atlas.cassandra.graph.hostname=<cassandra-host>
atlas.cassandra.graph.port=9042
atlas.cassandra.graph.keyspace=atlas_graph
atlas.cassandra.graph.datacenter=<datacenter-name>

# ES index prefix auto-derives from backend:
#   cassandra → atlas_graph_
#   janus     → janusgraph_
# Only set this if you need a non-standard prefix:
# atlas.graph.index.search.es.prefix=atlas_graph_
```

**Replace:**
- `<cassandra-host>` with the Cassandra service hostname (e.g., `atlas-cassandra`)
- `<datacenter-name>` with the Cassandra datacenter (usually `datacenter1`)

> **ES index prefix:** The ES index prefix now **auto-derives from `atlas.graphdb.backend`**: `cassandra` → `atlas_graph_`, `janus` → `janusgraph_`. You no longer need to set `atlas.graph.index.search.es.prefix` explicitly — it just works. You can still override it if needed for non-standard setups.

### 3.2 How to apply the config change

This depends on your deployment setup:

**Option A: ConfigMap/Secret update + pod restart**
```bash
# Edit the config (via Helm values, ConfigMap, etc.)
# Then restart the Atlas pod to pick up the new config
kubectl rollout restart statefulset/atlas -n atlas
```

**Option B: Direct edit (for testing only)**
```bash
# Edit the config file directly in the pod
kubectl edit configmap atlas-config -n atlas

# Add: atlas.graphdb.backend=cassandra
# Add: atlas.cassandra.graph.hostname=atlas-cassandra
# Add: atlas.cassandra.graph.port=9042
# Add: atlas.cassandra.graph.keyspace=atlas_graph
# Add: atlas.cassandra.graph.datacenter=datacenter1
# ES prefix auto-derives from backend — no need to set explicitly

# Restart the pod (if auto rollout restart doesn't happen)
kubectl delete pod atlas-0 -n atlas
```

### 3.3 Verify the pod starts with the Cassandra backend

```bash
# Wait for pod to be ready
kubectl wait --for=condition=ready pod/atlas-0 -n atlas --timeout=120s

# Check Atlas status
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  curl -s http://localhost:21000/api/atlas/admin/status

# Expected: {"Status":"ACTIVE"}
```

**Check the startup logs** to confirm Cassandra backend was loaded:

```bash
kubectl logs atlas-0 -n atlas -c atlas-main | grep -i "graphdb\|cassandra\|backend"
```

You should see log lines indicating:
- `atlas.graphdb.backend=cassandra`
- `CassandraGraphDatabase` being initialized
- No JanusGraph/TinkerPop initialization (RepairIndex, ManagementSystem, etc.)

**Startup should be ~50% faster** (~22s vs ~45s) because JanusGraph initialization is skipped entirely.

---

## Stage 4: Validate

### 4.1 API smoke tests

Run these basic API calls to verify the tenant is working:

```bash
ATLAS_URL="http://localhost:21000"
NS="atlas"

# 1. Search — verify index search works
kubectl exec -it atlas-0 -n $NS -c atlas-main -- \
  curl -s -u admin:admin "$ATLAS_URL/api/atlas/v2/search/basic?typeName=Table&limit=5" | python3 -m json.tool

# 2. Entity by GUID — verify entity retrieval works
# (use a GUID from the search result above)
kubectl exec -it atlas-0 -n $NS -c atlas-main -- \
  curl -s -u admin:admin "$ATLAS_URL/api/atlas/v2/entity/guid/<some-guid>" | python3 -m json.tool

# 3. Lineage — verify lineage traversal works
kubectl exec -it atlas-0 -n $NS -c atlas-main -- \
  curl -s -u admin:admin "$ATLAS_URL/api/atlas/v2/lineage/<process-guid>?direction=BOTH&depth=3" | python3 -m json.tool

# 4. TypeDefs — verify type system works
kubectl exec -it atlas-0 -n $NS -c atlas-main -- \
  curl -s -u admin:admin "$ATLAS_URL/api/atlas/v2/types/typedefs" | python3 -m json.tool | head -50

# 5. Create a test entity — verify writes work
kubectl exec -it atlas-0 -n $NS -c atlas-main -- \
  curl -s -u admin:admin -X POST "$ATLAS_URL/api/atlas/v2/entity" \
  -H "Content-Type: application/json" \
  -d '{
    "entity": {
      "typeName": "Table",
      "attributes": {
        "qualifiedName": "migration-test-table-DELETE-ME",
        "name": "migration-test-table"
      }
    }
  }' | python3 -m json.tool

# 6. Delete the test entity
# (use the GUID from the create response above)
kubectl exec -it atlas-0 -n $NS -c atlas-main -- \
  curl -s -u admin:admin -X DELETE "$ATLAS_URL/api/atlas/v2/entity/guid/<test-guid>"
```

### 4.2 Run the migrator validation separately

If you want to re-run validation after migration without migrating again:

```bash
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  /opt/apache-atlas/bin/atlas_migrate.sh --validate-only
```

### 4.3 Spot-check with cqlsh

Connect to Cassandra and verify the new tables have data:

```bash
# Get into a Cassandra pod
kubectl exec -it atlas-cassandra-0 -n atlas -- cqlsh

# Check the new keyspace exists
DESCRIBE KEYSPACES;

# Check tables exist
USE atlas_graph;
DESCRIBE TABLES;
-- Expected: vertices, edges_out, edges_in, edges_by_id, vertex_index,
--           vertex_property_index, edge_index, type_definitions,
--           type_definitions_by_category, migration_state

-- Count vertices
SELECT COUNT(*) FROM vertices;

-- Spot-check a vertex (properties are human-readable JSON)
SELECT * FROM vertices LIMIT 1;

-- Check GUID index
SELECT * FROM vertex_index WHERE index_name = '__guid_idx' LIMIT 5;
```

### 4.4 Validation checklist

| Check | How | Expected |
|-------|-----|----------|
| Pod starts | `kubectl get pods` | Running, Ready |
| Startup time | Check logs for total init time | ~20-25s (was ~45s) |
| Admin status | `GET /api/atlas/admin/status` | `{"Status":"ACTIVE"}` |
| Search works | `GET /api/atlas/v2/search/basic?typeName=Table&limit=5` | Returns entities |
| Entity by GUID | `GET /api/atlas/v2/entity/guid/<guid>` | Returns entity with attributes |
| Lineage works | `GET /api/atlas/v2/lineage/<guid>?direction=BOTH&depth=3` | Returns lineage graph |
| TypeDefs load | `GET /api/atlas/v2/types/typedefs` | Returns all type definitions |
| Entity create | `POST /api/atlas/v2/entity` | Returns GUID |
| Entity delete | `DELETE /api/atlas/v2/entity/guid/<guid>` | 200 OK |
| No errors in logs | `kubectl logs atlas-0 -n <ns> -c atlas-main` | No stack traces |

---

## Stage 5: Rollback (if needed)

If anything goes wrong, rolling back is instant:

### 5.1 Switch back to JanusGraph

Change the config properties back:

```properties
atlas.graphdb.backend=janus
# ES prefix auto-derives: janus → janusgraph_ (no need to set explicitly)
```

Or simply remove the `atlas.graphdb.backend` line — the default backend is `janus` and the ES prefix auto-derives to `janusgraph_`.

### 5.2 Restart the pod

```bash
kubectl delete pod atlas-0 -n atlas
# or
kubectl rollout restart statefulset/atlas -n atlas
```

### 5.3 Verify JanusGraph is back

```bash
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  curl -s http://localhost:21000/api/atlas/admin/status

# Should return {"Status":"ACTIVE"}
```

The JanusGraph data was never modified, so the tenant returns to its exact previous state.

### 5.4 What about the migrated data?

The new Cassandra keyspace (`atlas_graph`) and its data remain in place — they don't affect JanusGraph operation. You can clean them up later or leave them for a future migration retry.

To clean up (optional):
```sql
-- In cqlsh
DROP KEYSPACE atlas_graph;
```

---

## Advanced: Environment Overrides

The migration script accepts environment variable overrides for non-standard setups:

```bash
# Custom keyspace names
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  env SOURCE_KEYSPACE=custom_atlas TARGET_KEYSPACE=custom_atlas_graph \
  /opt/apache-atlas/bin/atlas_migrate.sh

# More scanner threads for large tenants
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  env SCANNER_THREADS=32 WRITER_THREADS=16 \
  /opt/apache-atlas/bin/atlas_migrate.sh

# Custom ES index names (source = copy mappings from, target = write docs to)
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  env SOURCE_ES_INDEX=custom_source_index TARGET_ES_INDEX=custom_target_index \
  /opt/apache-atlas/bin/atlas_migrate.sh

# Deterministic IDs + LWT dedup claims
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  env ID_STRATEGY=deterministic CLAIM_ENABLED=true \
  /opt/apache-atlas/bin/atlas_migrate.sh --fresh
```

| Variable | Default | Description |
|----------|---------|-------------|
| `SOURCE_KEYSPACE` | `atlas` | JanusGraph keyspace (where `edgestore` lives) |
| `SOURCE_EDGESTORE_TABLE` | `edgestore` | JanusGraph edge store table name |
| `TARGET_KEYSPACE` | `atlas_graph` | New Cassandra schema keyspace |
| `SOURCE_ES_INDEX` | `janusgraph_vertex_index` | Source ES index (copy mappings from) |
| `TARGET_ES_INDEX` | `atlas_graph_vertex_index` | Target ES index (write migrated docs to) |
| `SCANNER_THREADS` | `16` | Parallel CQL scan threads |
| `WRITER_THREADS` | `8` | Parallel write threads |
| `ES_BULK_SIZE` | `1000` | Documents per ES bulk request |
| `ID_STRATEGY` | `legacy` | ID generation: `legacy` (UUID) or `deterministic` (SHA-256) |
| `CLAIM_ENABLED` | `false` | LWT dedup claims during migration: `true`/`false` |

---

## Troubleshooting

### Migration script not found

```
bash: /opt/apache-atlas/bin/atlas_migrate.sh: No such file or directory
```

The build does not include the migration tools. Ensure you're using a build from the `switchable-graph-provider` branch with the `graphdb/cassandra` and `graphdb/migrator` modules.

### Migrator JAR not found

```
[migrator] ERROR: Migrator JAR not found ...
```

The migrator fat JAR is not in the expected location. Check:
```bash
find /opt/apache-atlas -name "atlas-graphdb-migrator*" 2>/dev/null
```

### Pod won't start after backend switch

Check the Atlas logs for the specific error:
```bash
kubectl logs atlas-0 -n atlas -c atlas-main --previous
```

Common causes:
- Missing `atlas.cassandra.graph.*` properties — add them to `atlas-application.properties`
- Cassandra `atlas_graph` keyspace doesn't exist — run the migrator first (it creates the keyspace)
- Wrong datacenter name — verify with `nodetool status` on a Cassandra pod

**Immediate fix:** Roll back to `atlas.graphdb.backend=janus` and restart.

### Search returns empty results after switch

The migrator's Phase 2 (ES reindex) may not have completed. Run:

```bash
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  /opt/apache-atlas/bin/atlas_migrate.sh --es-only
```

Then restart the Atlas pod.

### Lineage not working

If lineage returns empty results, check the `edges_out` and `edges_in` tables in Cassandra:

```sql
USE atlas_graph;
SELECT COUNT(*) FROM edges_out;
SELECT COUNT(*) FROM edges_in;
```

If counts are zero, the migration may have failed in Phase 1. Check the migrator log and re-run.

### OOM during migration (large tenants)

For very large tenants (50M+ assets), the migrator might need more heap:

```bash
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  bash -c 'JAVA_OPTS="-Xmx8g -Xms4g" /opt/apache-atlas/bin/atlas_migrate.sh'
```

Or set conservative tuning:

```bash
kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
  env SCANNER_THREADS=8 WRITER_THREADS=4 ES_BULK_SIZE=500 \
  /opt/apache-atlas/bin/atlas_migrate.sh
```

---

## Quick Reference: Complete Migration in 5 Commands

```bash
NS="atlas"

# 1. Dry run — check config
kubectl exec -it atlas-0 -n $NS -c atlas-main -- /opt/apache-atlas/bin/atlas_migrate.sh --dry-run

# 2. Migrate data
kubectl exec -it atlas-0 -n $NS -c atlas-main -- /opt/apache-atlas/bin/atlas_migrate.sh

# 3. Switch backend config (add atlas.graphdb.backend=cassandra + cassandra.graph.* — ES prefix auto-derives)
# ... via your config management tool ...

# 4. Restart pod
kubectl rollout restart statefulset/atlas -n $NS

# 5. Validate
kubectl exec -it atlas-0 -n $NS -c atlas-main -- curl -s http://localhost:21000/api/atlas/admin/status
kubectl exec -it atlas-0 -n $NS -c atlas-main -- curl -s -u admin:admin http://localhost:21000/api/atlas/v2/search/basic?typeName=Table\&limit=5
```

---

## Appendix: Legacy vs Deterministic ID Strategy

When migrating with `ID_STRATEGY=deterministic`, the ID format changes from random UUIDs to content-addressed SHA-256 hashes. This makes migrations idempotent and enables dedup claims.

| Aspect | Legacy (UUID) | Deterministic (SHA-256) |
|--------|--------------|------------------------|
| Vertex ID | `UUID.randomUUID()` (36 chars) | `SHA-256("v\|" + typeName + "\|" + qualifiedName)` (32-char hex) |
| Edge ID | `UUID.randomUUID()` | `SHA-256("e\|" + outId + "\|" + label + "\|" + inId)` |
| ES doc ID | UUID string | 32-char hex string |
| entity_claims | Not populated | Populated (one claim per entity) |
| Dedup safety | None at migration time | LWT `INSERT IF NOT EXISTS` prevents duplicate vertices |
| Idempotent re-run | No (generates new UUIDs each time) | Yes (same input = same IDs) |
| Runtime new entities | UUID (always) | UUID (addVertex always uses UUID) |
| Runtime edge creation | UUID | Deterministic SHA-256 |
| Runtime claim check | Disabled | Enabled — concurrent pods race on claims |
