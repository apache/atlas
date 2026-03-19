Query Atlas metastore audit logs from ClickHouse using the Grafana ClickHouse MCP.

## Usage
`/atlas-audit-logs <query_description> [tenant=<tenantName>] [from=<time>] [to=<time>]`

Examples:
- `/atlas-audit-logs failed login attempts last 1 hour`
- `/atlas-audit-logs entity deletions by user tenant=acme from=now-24h`
- `/atlas-audit-logs top API users last 6 hours`
- `/atlas-audit-logs search queries for tenant acme`

## Instructions

First call `mcp__grafana-clickhouse__get_schema_context` with `use_case="atlas_audit_logs"` to get the current schema and example queries, then run the appropriate SQL via `mcp__grafana-clickhouse__run_sql_query`.

### Key Schema (atlas_audit database)

```sql
-- Core audit log columns:
-- Timestamp        DateTime64(9)
-- TenantName       LowCardinality(String)   -- REQUIRED filter
-- UserName         String
-- Operation        String   (CREATE, UPDATE, DELETE, READ, etc.)
-- EntityType       String
-- EntityId         String
-- EntityName       String
-- ClientIP         String
-- HttpMethod       String
-- ApiEndpoint      String
-- StatusCode       Int32
-- DurationMs       Int64
-- RequestBody      String
-- ResponseBody     String
```

### Common SQL Patterns

**Top operations by user (last 1 hour):**
```sql
SELECT UserName, Operation, count() as cnt
FROM atlas_audit.audit_logs
WHERE TenantName = '{tenant}'
  AND Timestamp >= now() - INTERVAL 1 HOUR
GROUP BY UserName, Operation
ORDER BY cnt DESC
LIMIT 20
```

**Failed API requests:**
```sql
SELECT Timestamp, UserName, ApiEndpoint, StatusCode, DurationMs
FROM atlas_audit.audit_logs
WHERE TenantName = '{tenant}'
  AND Timestamp >= now() - INTERVAL 1 HOUR
  AND StatusCode >= 400
ORDER BY Timestamp DESC
LIMIT 100
```

**Entity operations timeline:**
```sql
SELECT toStartOfMinute(Timestamp) as minute, Operation, count() as cnt
FROM atlas_audit.audit_logs
WHERE TenantName = '{tenant}'
  AND Timestamp >= now() - INTERVAL 6 HOUR
GROUP BY minute, Operation
ORDER BY minute DESC
```

**Slow API calls (>5s):**
```sql
SELECT Timestamp, UserName, ApiEndpoint, DurationMs, StatusCode
FROM atlas_audit.audit_logs
WHERE TenantName = '{tenant}'
  AND Timestamp >= now() - INTERVAL 1 HOUR
  AND DurationMs > 5000
ORDER BY DurationMs DESC
LIMIT 50
```

**Time range defaults:** `now-1h` to `now` unless specified.
**Timezone default:** `Asia/Kolkata`

If TenantName is not provided by the user, ask for it as it is a required filter for performance.

Present results in a readable table. Include row counts in the summary.

$ARGUMENTS
