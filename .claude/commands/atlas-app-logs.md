Query Atlas metastore application logs from ClickHouse (otel_logs) using the Grafana ClickHouse MCP.

## Usage
`/atlas-app-logs <query_description> [tenant=<tenantName>] [from=<time>] [to=<time>]`

Examples:
- `/atlas-app-logs errors in the last 30 minutes`
- `/atlas-app-logs OOM or OutOfMemory errors today`
- `/atlas-app-logs slow elastic search queries last 2 hours`
- `/atlas-app-logs startup failures tenant=acme`
- `/atlas-app-logs graphCommit latency spikes`

## Instructions

First call `mcp__grafana-clickhouse__get_schema_context` with `use_case="atlas_application_logs"` to get the current schema and example queries, then run the appropriate SQL via `mcp__grafana-clickhouse__run_sql_query`.

### Key Schema (otel_logs.service_logs)

```sql
-- Core columns:
-- Timestamp          DateTime64(9)
-- TenantName         LowCardinality(String)   -- REQUIRED filter
-- ServiceName        LowCardinality(String)   -- filter: 'atlas' or 'atlas-read'
-- SeverityText       String                   -- INFO, WARN, ERROR, FATAL
-- Body               String                   -- log message text
-- LogAttributes      Map(LowCardinality(String), String)
--   LogAttributes['logtype']     -- 'metric', 'perf', 'application'
--   LogAttributes['class']       -- Java class name
--   LogAttributes['thread']      -- thread name
--   LogAttributes['duration_ms'] -- method duration (for perf logs)
--   LogAttributes['method']      -- method name (for perf logs)
```

### Common SQL Patterns

**Recent errors from Atlas:**
```sql
SELECT Timestamp, TenantName, SeverityText, Body
FROM otel_logs.service_logs
WHERE TenantName = '{tenant}'
  AND ServiceName IN ('atlas', 'atlas-read')
  AND SeverityText IN ('ERROR', 'FATAL')
  AND Timestamp >= now() - INTERVAL 1 HOUR
ORDER BY Timestamp DESC
LIMIT 100
```

**Atlas performance method logs (slow operations):**
```sql
SELECT
  Timestamp,
  LogAttributes['method'] as method,
  toInt64(LogAttributes['duration_ms']) as duration_ms,
  TenantName
FROM otel_logs.service_logs
WHERE TenantName = '{tenant}'
  AND ServiceName IN ('atlas', 'atlas-read')
  AND LogAttributes['logtype'] = 'perf'
  AND Timestamp >= now() - INTERVAL 1 HOUR
  AND toInt64OrNull(LogAttributes['duration_ms']) > 1000
ORDER BY duration_ms DESC
LIMIT 50
```

**Error frequency by log class:**
```sql
SELECT
  LogAttributes['class'] as java_class,
  count() as error_count,
  max(Timestamp) as last_seen
FROM otel_logs.service_logs
WHERE TenantName = '{tenant}'
  AND ServiceName IN ('atlas', 'atlas-read')
  AND SeverityText = 'ERROR'
  AND Timestamp >= now() - INTERVAL 6 HOUR
GROUP BY java_class
ORDER BY error_count DESC
LIMIT 20
```

**Log volume by severity (timeline):**
```sql
SELECT
  toStartOfMinute(Timestamp) as minute,
  SeverityText,
  count() as cnt
FROM otel_logs.service_logs
WHERE TenantName = '{tenant}'
  AND ServiceName IN ('atlas', 'atlas-read')
  AND Timestamp >= now() - INTERVAL 2 HOUR
GROUP BY minute, SeverityText
ORDER BY minute DESC
```

**Search for specific error pattern:**
```sql
SELECT Timestamp, TenantName, SeverityText, Body
FROM otel_logs.service_logs
WHERE TenantName = '{tenant}'
  AND ServiceName IN ('atlas', 'atlas-read')
  AND Timestamp >= now() - INTERVAL 2 HOUR
  AND Body LIKE '%{search_term}%'
ORDER BY Timestamp DESC
LIMIT 100
```

**Time range defaults:** `now-1h` to `now` unless specified.
**Timezone default:** `Asia/Kolkata`

If TenantName is not provided, ask for it — it's required for query performance.

Present results in a readable table with counts in the summary. For error logs, highlight the most frequent error patterns.

$ARGUMENTS
