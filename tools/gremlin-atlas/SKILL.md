---
name: gremlin-atlas
description: >
  Run read-only Gremlin/Groovy scripts on a tenant's atlas-0 pod to debug
  JanusGraph: check vertex properties, mixed index registration, schema
  inspection, and JanusGraph vs ES sync issues. Use when users ask about
  vertex properties, mixed index status, repairindex failures, or graph schema.
disable-model-invocation: false
user-invocable: true
allowed-tools: Bash, Read, Grep, Glob
argument-hint: "[script-name] [kubectl-context]"
---

# Gremlin Atlas — In-Pod JanusGraph Debugging

Run read-only Gremlin/Groovy scripts directly on the `atlas-0` pod via `kubectl exec`. No port-forwarding needed.

> **All built-in scripts are read-only.** Management transactions use `mgmt.rollback()`, graph traversals are read-only. No data is modified.

## Prerequisites

1. **kubectl** configured with vcluster access to the target tenant
2. **VPN** connected to reach the tenant's cluster

## Usage

```bash
# 1. One-time setup per pod restart: download gremlin-100 onto the atlas pod
tools/gremlin-atlas/scripts/gremlin-setup.sh [kubectl-context]

# 2. Run a built-in script
tools/gremlin-atlas/scripts/gremlin-run.sh <script-name> [kubectl-context]

# 3. Run a custom local groovy script
tools/gremlin-atlas/scripts/gremlin-run.sh /path/to/my-script.groovy [kubectl-context]
```

If `$ARGUMENTS` is provided, run:
```bash
tools/gremlin-atlas/scripts/gremlin-run.sh $ARGUMENTS
```

## Built-in Scripts

All scripts in `scripts/groovy/` are **read-only**.

| Script | Description |
|--------|-------------|
| `sanity-check` | Verify graph connectivity, show sample vertices |
| `check-vertex` | Look up a vertex by GUID and show all properties |
| `index-status` | Show vertex_index field status summary (INSTALLED/REGISTERED/ENABLED) |
| `index-fields` | List all fields in vertex_index grouped by status |
| `check-property-in-index` | Check if a specific property is registered in the mixed index |
| `schema-info` | Show all property keys, vertex labels, and graph indexes |

## How It Works

```
local machine                          atlas-0 pod
─────────────                          ────────────
gremlin-run.sh ──kubectl exec──►  java -cp gremlin-100/lib groovy.ui.GroovyMain script.groovy
                                       │
                                       ├── atlas-cassandra:9042
                                       └── atlas-elasticsearch-master:9200
```

The [gremlin-100](https://github.com/nikhilbonte/gremlin-100) distribution provides JanusGraph 1.0.0 + TinkerPop 3.7.0 + Groovy 4.0.9 (matching Atlas's versions). Config points to in-cluster service DNS names.

## Custom Groovy Scripts

```groovy
// my-query.groovy
import org.janusgraph.core.JanusGraphFactory

graph = JanusGraphFactory.open('/tmp/gremlin-100-gremlin-1/conf/janusgraph-cql-es.properties')
g = graph.traversal()

result = g.V().has('__guid', 'YOUR-GUID-HERE').valueMap().next()
println result

graph.close()
```

## Common Queries

**Check vertex by GUID:**
```groovy
g.V().has('__guid','<GUID>').valueMap('qualifiedName','connectionQualifiedName','__typeName')
```

**Check if property is in mixed index:**
```groovy
mgmt = graph.openManagement()
idx = mgmt.getGraphIndex('vertex_index')
pk = mgmt.getPropertyKey('connectionQualifiedName')
println idx.getIndexStatus(pk)  // ENABLED = good, throws exception = not in index
mgmt.rollback()
```

**Index status summary:**
```groovy
mgmt = graph.openManagement()
idx = mgmt.getGraphIndex('vertex_index')
def counts = [:]
idx.getFieldKeys().each { pk ->
    def s = idx.getIndexStatus(pk).toString()
    counts[s] = (counts[s] ?: 0) + 1
}
counts.each { k, v -> println "${k}: ${v}" }
mgmt.rollback()
```

## Known Issues

- **Cassandra warnings**: `NotYetConnectedException` for non-service IPs are harmless
- **Groovy scoping**: Use bare `x = value` instead of `def x = value`
- **Competing mgmt transactions**: If Atlas holds a lock, `awaitGraphIndexStatus` blocks. Use Atlas's own API for index transitions.
- **Pod restarts**: gremlin-100 lives in `/tmp` — re-run `gremlin-setup.sh` after restart

## Architecture

- `vertex_index` is the ES-backed mixed index — only properties registered here appear in ES
- `repairindex` only reindexes properties that are part of the mixed index
- Vertex properties exist in Cassandra regardless of mixed index registration

See [README.md](README.md) for full documentation.
