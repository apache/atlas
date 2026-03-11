# Gremlin Atlas — In-Pod JanusGraph Debugging

Run read-only Gremlin/Groovy scripts directly on the `atlas-0` pod via `kubectl exec`. No port-forwarding needed — the pod already has network access to Cassandra and Elasticsearch.

> **All built-in scripts are read-only.** Management transactions use `mgmt.rollback()`, graph traversals are read-only. No data is modified. Custom scripts you write are your responsibility.

## When to Use

- Check if a vertex property exists in JanusGraph
- Verify if a property is in the mixed index (`vertex_index`)
- Debug why a field is missing from ES but present in the entity store
- Inspect JanusGraph schema (property keys, index registration)
- Run Gremlin traversal queries against a tenant's graph
- Debug repairindex failures (property not in mixed index)
- Check index status (INSTALLED vs REGISTERED vs ENABLED)

## How It Works

```
local machine                          atlas-0 pod
─────────────                          ────────────
gremlin-run.sh ──kubectl exec──►  java -cp gremlin-100/lib groovy.ui.GroovyMain script.groovy
                                       │
                                       ├── atlas-cassandra:9042
                                       └── atlas-elasticsearch-master:9200
```

Scripts run non-interactively via Groovy on the pod. The [gremlin-100](https://github.com/nikhilbonte/gremlin-100) distribution provides the JanusGraph + TinkerPop classpath. Config points to in-cluster service DNS names.

## Prerequisites

1. **kubectl** configured with vcluster access to the target tenant
2. **VPN** connected to reach the tenant's cluster

## Quick Start

```bash
# 1. Connect to the tenant's vcluster
vcluster platform connect vcluster <tenant-name>

# 2. One-time setup: download gremlin-100 onto the atlas pod (~379MB, takes ~10s)
tools/gremlin-atlas/scripts/gremlin-setup.sh [kubectl-context]

# 3. Run a built-in script
tools/gremlin-atlas/scripts/gremlin-run.sh index-status [kubectl-context]
tools/gremlin-atlas/scripts/gremlin-run.sh sanity-check [kubectl-context]
tools/gremlin-atlas/scripts/gremlin-run.sh schema-info [kubectl-context]
```

## Built-in Scripts

All scripts in `scripts/groovy/` are **read-only** (no graph writes, all management transactions are rolled back).

| Script | Description |
|--------|-------------|
| `sanity-check` | Verify graph connectivity, show sample vertices |
| `check-vertex` | Look up a vertex by GUID and show all properties |
| `index-status` | Show vertex_index field status summary (INSTALLED/REGISTERED/ENABLED) |
| `index-fields` | List all fields in vertex_index grouped by status |
| `check-property-in-index` | Check if a specific property is registered in the mixed index |
| `schema-info` | Show all property keys, vertex labels, and graph indexes |

## Running Custom Scripts

Write any Groovy script locally and run it on the pod. The runner copies it via `kubectl cp` and executes it.

```groovy
// my-query.groovy
import org.janusgraph.core.JanusGraphFactory

graph = JanusGraphFactory.open('/tmp/gremlin-100-gremlin-1/conf/janusgraph-cql-es.properties')
g = graph.traversal()

result = g.V().has('__guid', 'YOUR-GUID-HERE').valueMap().next()
println result

graph.close()
```

```bash
tools/gremlin-atlas/scripts/gremlin-run.sh my-query.groovy [kubectl-context]
```

## Common Investigation Queries

### Check vertex by GUID
```groovy
g.V().has('__guid','<GUID>').valueMap('qualifiedName','connectionQualifiedName','__typeName')
```

### Check if ANY vertex has a property
```groovy
g.V().has('connectionQualifiedName').limit(5).valueMap('qualifiedName','connectionQualifiedName','__typeName')
```

### Check property key exists in schema
```groovy
mgmt = graph.openManagement()
println mgmt.getPropertyKey('connectionQualifiedName')
mgmt.rollback()
```

### Check if property is in mixed index
```groovy
mgmt = graph.openManagement()
idx = mgmt.getGraphIndex('vertex_index')
pk = mgmt.getPropertyKey('connectionQualifiedName')
println idx.getIndexStatus(pk)  // ENABLED = good, throws exception = not in index
mgmt.rollback()
```

### Index status summary
```groovy
mgmt = graph.openManagement()
idx = mgmt.getGraphIndex('vertex_index')
def counts = [:]
idx.getFieldKeys().each { pk ->
    def s = idx.getIndexStatus(pk).toString()
    counts[s] = (counts[s] ?: 0) + 1
}
counts.each { k, v -> println "${k}: ${v}" }
println "Total: ${idx.getFieldKeys().size()}"
mgmt.rollback()
```

## Known Issues

### Cassandra Connection Warnings
The Cassandra driver discovers all pod IPs from cluster metadata but can only reach the service DNS. Warnings like `NotYetConnectedException` for non-service IPs are harmless.

### Groovy Variable Scoping
Use bare assignments (`x = value`) instead of `def x = value` for variables that need to persist across statements in the same script.

### Competing Management Transactions
If Atlas is running and holds a management lock, `awaitGraphIndexStatus` calls may block indefinitely. For index status transitions (INSTALLED -> REGISTERED -> ENABLED), prefer triggering through Atlas's own typedef seeder or API rather than external scripts.

### gremlin-100 lost after pod restart
The gremlin-100 files live in `/tmp` which is ephemeral. Re-run `gremlin-setup.sh` after pod restarts.

## Architecture Notes

- **gremlin-100** is a pre-packaged JanusGraph 1.0.0 console from [github.com/nikhilbonte/gremlin-100](https://github.com/nikhilbonte/gremlin-100)
- JanusGraph 1.0.0 uses Groovy 4.0.9 and TinkerPop 3.7.0 — matching Atlas's own versions
- Config at `/tmp/gremlin-100-gremlin-1/conf/janusgraph-cql-es.properties`
- The `vertex_index` is the ES-backed mixed index — only properties registered here appear in ES
- `repairindex` only reindexes properties that are part of the mixed index
- Vertex properties exist in Cassandra regardless of mixed index registration

## Troubleshooting

| Symptom | Script | Root Cause if... |
|---------|--------|-----------------|
| Field missing from ES `_source` | `check-property-in-index` | Property not in `vertex_index` |
| repairindex returns 204 but no change | `index-status` | Property not registered or not ENABLED |
| Term query returns 0 results | `check-vertex` + ES mapping | Field not indexed or missing |
| Asset visible in UI but not searchable | Compare JanusGraph vs ES | Mixed index gap |
| Index fields stuck in INSTALLED | `index-status` | Competing mgmt transaction or typedef seeder issue |
