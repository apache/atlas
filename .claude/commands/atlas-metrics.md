Query Atlas metastore PromQL/VictoriaMetrics metrics using the Grafana ClickHouse MCP.

## Usage
`/atlas-metrics <query_description_or_promql> [from=<time>] [to=<time>]`

Examples:
- `/atlas-metrics JVM heap usage last 1 hour`
- `/atlas-metrics API request rate last 30 minutes`
- `/atlas-metrics garbage collection pause time`
- `/atlas-metrics rate(atlas_requests_total[5m]) from=now-1h`

## Instructions

Use the `mcp__grafana-clickhouse__run_promql_query` tool to query Atlas metrics from VictoriaMetrics/Prometheus.

If the user provided a raw PromQL expression, use it directly. Otherwise, translate their natural language request into PromQL based on these Atlas metric patterns:

### Common Atlas PromQL Queries

**JVM Memory:**
```
# Heap usage by Atlas pod
jvm_memory_bytes_used{area="heap", job=~"atlas.*"}
# Non-heap usage
jvm_memory_bytes_used{area="nonheap", job=~"atlas.*"}
# Heap usage percentage
jvm_memory_bytes_used{area="heap"} / jvm_memory_bytes_max{area="heap"}
```

**JVM GC (from Telegraf/Jolokia):**
```
rate(jvm_gc_collection_seconds_sum{gc=~".*", job=~"atlas.*"}[5m])
jvm_gc_collection_seconds_count{job=~"atlas.*"}
```

**Thread Count:**
```
jvm_threads_current{job=~"atlas.*"}
jvm_threads_daemon{job=~"atlas.*"}
```

**Atlas API Metrics (from Telegraf Prometheus scrape):**
```
# API request count
atlas_requests_total
# API latency
atlas_request_duration_seconds
# Active searches
atlas_active_searches
# Entity count
atlas_entity_count
```

**JanuGraph Metrics:**
```
# Transaction rate
rate(janusgraph_transactions_total[5m])
# Cache hit rate
janusgraph_cache_hit_ratio
```

**Default time range:** `now-1h` to `now` unless specified.
**Default timezone:** `Asia/Kolkata`

After running the query, present the results in a clear table or summary format. If the query returns no data, suggest alternative metric names or check connectivity with `mcp__grafana-clickhouse__health_check`.

$ARGUMENTS
