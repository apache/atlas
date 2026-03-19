Run a comprehensive Atlas metastore health check using Grafana + ClickHouse MCP.

## Usage
`/atlas-health [tenant=<tenantName>] [from=<time>]`

Examples:
- `/atlas-health`
- `/atlas-health tenant=acme`
- `/atlas-health tenant=acme from=now-30m`

## Instructions

Run ALL of the following checks in sequence using the Grafana ClickHouse MCP tools. Summarize findings at the end with a RED/YELLOW/GREEN status per area.

**Default time window:** last 15 minutes (`now-15m` to `now`)
**Timezone:** `Asia/Kolkata`

---

### Step 1: MCP Connectivity
Call `mcp__grafana-clickhouse__health_check` — verify ClickHouse and VictoriaMetrics are reachable.
Flag if either is unhealthy before proceeding.

---

### Step 2: JVM Health (PromQL via VictoriaMetrics)
Call `mcp__grafana-clickhouse__run_promql_query` with:

```
# Heap utilization % (alert if >85%)
100 * jvm_memory_bytes_used{area="heap", job=~"atlas.*"} / jvm_memory_bytes_max{area="heap", job=~"atlas.*"}
```

```
# GC pause rate (alert if >5 pauses/min)
rate(jvm_gc_collection_seconds_count{job=~"atlas.*"}[5m]) * 60
```

```
# Thread count (alert if >500)
jvm_threads_current{job=~"atlas.*"}
```

---

### Step 3: Recent Errors (ClickHouse SQL)
Call `mcp__grafana-clickhouse__run_sql_query` — if tenant provided, filter by TenantName:

```sql
SELECT
  ServiceName,
  SeverityText,
  count() as cnt,
  max(Timestamp) as last_seen
FROM otel_logs.service_logs
WHERE ServiceName IN ('atlas', 'atlas-read')
  AND SeverityText IN ('ERROR', 'FATAL')
  AND Timestamp >= now() - INTERVAL 15 MINUTE
  /* AND TenantName = '{tenant}' -- add if tenant provided */
GROUP BY ServiceName, SeverityText
ORDER BY cnt DESC
```

Alert if error count > 10 in last 15 min.

---

### Step 4: Slow Operations (ClickHouse SQL)
Call `mcp__grafana-clickhouse__run_sql_query`:

```sql
SELECT
  LogAttributes['method'] as method,
  count() as calls,
  avg(toInt64OrNull(LogAttributes['duration_ms'])) as avg_ms,
  max(toInt64OrNull(LogAttributes['duration_ms'])) as max_ms
FROM otel_logs.service_logs
WHERE ServiceName IN ('atlas', 'atlas-read')
  AND LogAttributes['logtype'] = 'perf'
  AND Timestamp >= now() - INTERVAL 15 MINUTE
  /* AND TenantName = '{tenant}' -- add if tenant provided */
GROUP BY method
HAVING max_ms > 2000
ORDER BY max_ms DESC
LIMIT 10
```

Alert if any method max_ms > 5000.

---

### Step 5: Audit Log Activity (ClickHouse SQL)
Call `mcp__grafana-clickhouse__run_sql_query`:

```sql
SELECT
  Operation,
  count() as cnt,
  countIf(StatusCode >= 400) as errors,
  avg(DurationMs) as avg_ms
FROM atlas_audit.audit_logs
WHERE Timestamp >= now() - INTERVAL 15 MINUTE
  /* AND TenantName = '{tenant}' -- add if tenant provided */
GROUP BY Operation
ORDER BY cnt DESC
LIMIT 10
```

---

### Final Summary Format

Present a summary table:

| Area | Status | Details |
|------|--------|---------|
| MCP Connectivity | 🟢/🔴 | ... |
| JVM Heap | 🟢/🟡/🔴 | X% used |
| GC Pressure | 🟢/🟡/🔴 | X pauses/min |
| Error Rate | 🟢/🟡/🔴 | X errors in 15m |
| Slow Ops | 🟢/🟡/🔴 | Max Xms (method) |
| Audit Activity | 🟢/🟡/🔴 | X ops, Y errors |

**Thresholds:**
- 🔴 RED: Heap >85%, Errors >50/15m, Max op >10s, GC >10/min
- 🟡 YELLOW: Heap >70%, Errors >10/15m, Max op >5s, GC >5/min
- 🟢 GREEN: All within thresholds

$ARGUMENTS
