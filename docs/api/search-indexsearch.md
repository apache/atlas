# POST /api/atlas/v2/search/indexsearch

Execute an Elasticsearch DSL query against the Atlas entity index. This is the primary search API for discovering and filtering metadata entities. It accepts a raw Elasticsearch query DSL, applies authorization filters automatically, and returns entity headers with requested attributes.

**Content-Type:** `application/json`

---

## Request

### Query Parameters

None. All parameters are passed in the request body.

### Request Body

```json
{
  "dsl": { <Elasticsearch DSL query> },
  "attributes": ["attr1", "attr2"],
  "relationAttributes": ["relAttr1"],
  "suppressLogs": false,
  "showSearchScore": false,
  "showHighlights": false,
  "excludeMeanings": false,
  "excludeClassifications": false,
  "includeClassificationNames": true,
  "purpose": "<purpose-guid>",
  "persona": "<persona-guid>",
  "allowDeletedRelations": false,
  "requestMetadata": {
    "searchInput": "users",
    "utmTags": ["ui_main_list"],
    "saveSearchLog": true
  }
}
```

#### Top-level fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dsl` | object | **Required** | Elasticsearch DSL query object. Supports the full ES query DSL including `bool`, `term`, `match`, `range`, `nested`, aggregations, sorting, pagination, etc. See [DSL structure](#dsl-structure) below. |
| `attributes` | array of string | `[]` | Entity attributes to include in each result's `attributes` map. Only the listed attributes are returned — this controls the "columns" in your result set. |
| `relationAttributes` | array of string | `[]` | Attributes of related entities to include. For example, if an entity has a relationship attribute `table`, listing `"name"` here returns the related table's `name`. **Warning:** This is the most expensive parameter — it triggers graph lookups for every result entity. See [Performance Guidelines](#performance-guidelines). |
| `suppressLogs` | boolean | `false` | Suppress authorization audit logging for this search. Use for high-frequency polling queries. |
| `showSearchScore` | boolean | `false` | Include Elasticsearch relevance scores in the response (`searchScore` map). |
| `showHighlights` | boolean | `false` | Include Elasticsearch hit highlights in the response (`searchMetadata` map). Requires highlight config in the DSL. |
| `showSearchMetadata` | boolean | `false` | Include full search metadata (highlights + sort values) in the response. |
| `excludeMeanings` | boolean | `false` | Exclude glossary term assignments (`meanings`) from results. Reduces response size. |
| `excludeClassifications` | boolean | `false` | Exclude classification (tag) details from results. Reduces response size. |
| `includeClassificationNames` | boolean | `false` | Include classification type names as a flat list, even when full classification details are excluded. |
| `purpose` | string | `null` | Purpose GUID for access-control scoping. Filters results to entities visible under this Purpose policy. |
| `persona` | string | `null` | Persona GUID for access-control scoping. Filters results to entities visible under this Persona policy. |
| `allowDeletedRelations` | boolean | `false` | Include relationships with `DELETED` status in the results. By default only `ACTIVE` relationships are returned. |
| `accessControlExclusive` | boolean | `false` | When `true`, only apply access control from `purpose`/`persona` without the user's default permissions. |
| `includeRelationshipAttributes` | boolean | `false` | Include relationship attributes in the entity response. |
| `enableFullRestriction` | boolean | config | Apply full authorization restriction (row-level security). Defaults to the server-side configuration value. |
| `requestMetadata` | object | `null` | Metadata about the search request. See [Request Metadata](#request-metadata). |
| `async` | object | `null` | Async search configuration. See [Async Search](#async-search). |

#### Request Metadata

| Field | Type | Description |
|-------|------|-------------|
| `searchInput` | string | The original user-typed search string (used for search logging/analytics). |
| `utmTags` | array of string | UTM tracking tags identifying the search origin (e.g. `"ui_main_list"`, `"ui_popup_searchbar"`, `"project_webapp"`). Certain tags may bypass query size limits (configured via `atlas.indexsearch.limit.utm.tags`). |
| `saveSearchLog` | boolean | Whether to persist this search in the search log for analytics. |

#### Async Search

| Field | Type | Description |
|-------|------|-------------|
| `isCallAsync` | boolean | Execute the search asynchronously. |
| `searchContextId` | string | Context ID for tracking async search results. |
| `searchContextSequenceNo` | integer | Sequence number within an async search context. |
| `requestTimeoutInSecs` | long | Timeout for async search in seconds. |

#### DSL Structure

The `dsl` field accepts standard Elasticsearch query DSL. Key fields:

| Field | Type | Description |
|-------|------|-------------|
| `query` | object | The ES query clause (`bool`, `term`, `match`, `range`, `wildcard`, `nested`, etc.) |
| `from` | integer | Pagination offset (default: `0`) |
| `size` | integer | Page size / max results (default: `10`). Subject to server-side max limit. |
| `sort` | array | Sort criteria. Each entry is `{ "field": { "order": "asc" } }` |
| `_source` | object/array | Source filtering — controls which fields ES returns. Atlas merges system fields automatically. |
| `aggs` | object | Elasticsearch aggregations (terms, histogram, date_histogram, etc.) |
| `post_filter` | object | Post-query filter (applied after aggregations) |
| `highlight` | object | Highlight configuration (requires `showHighlights: true`) |
| `collapse` | object | Field collapsing configuration |

**Atlas index field naming:** Entity attributes are indexed with their Atlas attribute name directly (e.g. `qualifiedName`, `name`, `description`). System fields use double-underscore prefix:

| Index Field | Description |
|-------------|-------------|
| `__guid` | Entity GUID |
| `__typeName.keyword` | Entity type name (use `.keyword` for exact match) |
| `__state` | Entity status: `ACTIVE` or `DELETED` |
| `__qualifiedName` | Qualified name |
| `__name` | Display name |
| `__createdBy` | Creator username |
| `__modifiedBy` | Last modifier username |
| `__timestamp` | Creation timestamp (epoch ms) |
| `__modificationTimestamp` | Last modification timestamp (epoch ms) |
| `__classificationNames` | Space-separated classification names |
| `__labels` | Entity labels |
| `__meanings` | Linked glossary term references |
| `__superTypeNames.keyword` | Super type names for type hierarchy queries |

---

## Performance Guidelines

These guidelines are critical for production use. Poorly constructed queries can cause cluster-wide performance degradation.

### Size Limits

| Context | Max `size` | Notes |
|---------|-----------|-------|
| Default | **20** | Standard API queries should use `size: 20` or less |
| UI list views | **50** | Maximum for paginated list views |
| Aggregation-only | **0** | Set `size: 0` when you only need aggregation results |
| Background/analytics | **100** | Only for batch jobs with appropriate `utmTags` |

> **Rule:** Never use `size` > 100 in user-facing queries. For bulk data export, paginate with `search_after`.

### Query Construction Rules

1. **Always filter by `__state`:**
   ```json
   { "term": { "__state": "ACTIVE" } }
   ```
   Every query MUST include a state filter. Without it, you'll get deleted entities in results and cause confusion.

2. **Use `term`/`terms` for exact matches, not `match`:**
   ```json
   // GOOD — exact match, no analysis
   { "term": { "__typeName.keyword": "Table" } }

   // BAD — uses text analysis, slower, may return unexpected results
   { "match": { "__typeName": "Table" } }
   ```

3. **Use `bool.filter` for non-scoring conditions:**
   ```json
   {
     "bool": {
       "must": [
         { "match": { "__name": "revenue" } }
       ],
       "filter": [
         { "term": { "__state": "ACTIVE" } },
         { "term": { "__typeName.keyword": "Table" } }
       ]
     }
   }
   ```
   Conditions in `filter` skip relevance scoring and are cached by ES — significantly faster than `must` for filtering.

4. **Avoid leading wildcards:**
   ```json
   // GOOD — prefix wildcard (uses index efficiently)
   { "wildcard": { "__name.keyword": { "value": "customer*" } } }

   // BAD — leading wildcard (full index scan, extremely slow on large indices)
   { "wildcard": { "__name.keyword": { "value": "*revenue*" } } }
   ```
   Leading wildcards (`*term`) force Elasticsearch to scan every term in the index. Use `match` or `multi_match` for full-text search instead.

5. **Prefer `.keyword` suffix for exact match and sorting:**
   ```json
   // For exact match
   { "term": { "__typeName.keyword": "Table" } }

   // For sorting
   { "sort": [{ "__name.keyword": { "order": "asc" } }] }
   ```
   The `.keyword` sub-field is not analyzed — it matches the exact stored value. Without `.keyword`, text fields are tokenized and may not sort or match as expected.

6. **Use `terms` for multi-value filtering:**
   ```json
   // GOOD — single query for multiple values
   { "terms": { "__typeName.keyword": ["Table", "View", "MaterializedView"] } }

   // BAD — multiple should clauses
   {
     "bool": {
       "should": [
         { "term": { "__typeName.keyword": "Table" } },
         { "term": { "__typeName.keyword": "View" } },
         { "term": { "__typeName.keyword": "MaterializedView" } }
       ]
     }
   }
   ```

### Pagination

| Method | Use When | Limit |
|--------|----------|-------|
| `from` / `size` | Simple offset pagination | Max 10,000 total results (`from + size <= 10000`) |
| `sort` + `search_after` | Deep pagination, infinite scroll | No limit |
| `nextMarker` | Opaque cursor from Atlas response | Use the marker from previous response |

```json
// search_after pagination — requires a sort field
{
  "size": 20,
  "query": { "term": { "__state": "ACTIVE" } },
  "sort": [{ "__timestamp": { "order": "desc" } }, { "__guid": { "order": "asc" } }],
  "search_after": [1710288000000, "abc-123-guid"]
}
```

> **Rule:** Always include a tiebreaker field in sort (like `__guid`) to ensure stable pagination.

### `relationAttributes` Cost

`relationAttributes` is the **single most expensive parameter** in the search API. For each result entity, Atlas must:
1. Resolve all relationship edges from the graph database
2. Load each related entity vertex
3. Extract the requested attributes

**Guidelines:**
- Only request `relationAttributes` when absolutely necessary
- Keep `size` small (max 20) when using `relationAttributes`
- Never combine `relationAttributes` with `size` > 50
- If you need relationship data for many entities, make a separate bulk lookup instead

### Aggregation Guidelines

1. **Always set `size: 0` for aggregation-only queries.** Don't fetch entities you won't use:
   ```json
   { "size": 0, "query": { ... }, "aggs": { ... } }
   ```

2. **Limit aggregation bucket sizes.** Default ES returns 10 buckets. Be explicit:
   ```json
   { "terms": { "field": "__typeName.keyword", "size": 50 } }
   ```

3. **Avoid high-cardinality aggregations** on fields like `__guid`, `qualifiedName`, or `description`. These create millions of buckets and can OOM the ES node.

4. **Don't nest aggregations more than 2 levels deep** unless you have a specific need. Each nesting level multiplies the bucket count.

### Response Size Optimization

Always set these flags when you don't need tag/term data:

```json
{
  "excludeMeanings": true,
  "excludeClassifications": true,
  "includeClassificationNames": true
}
```

This combination excludes the heavy classification and meanings objects but still returns classification names as a flat string list — typically reducing response size by 50-80%.

---

## Response

**Status:** `200 OK`

```json
{
  "queryType": "INDEX",
  "searchParameters": { ... },
  "entities": [ <AtlasEntityHeader>, ... ],
  "referredEntities": { "<guid>": <AtlasEntityHeader>, ... },
  "approximateCount": 1500,
  "searchScore": { "<guid>": 12.5, ... },
  "searchMetadata": { "<guid>": { "highlights": {...}, "sort": [...] }, ... },
  "aggregations": { ... },
  "nextMarker": "eyJ..."
}
```

#### Top-level fields

| Field | Type | Description |
|-------|------|-------------|
| `queryType` | string | Always `"INDEX"` for this endpoint. |
| `searchParameters` | object | Echo of the search parameters used. |
| `entities` | array of AtlasEntityHeader | Matching entities. Each header contains the `attributes` you requested plus system fields. Entities the user lacks permission to view are scrubbed (guid set to `"-1"`, attributes cleared). |
| `referredEntities` | map | Related entities referenced by the main results (populated when `relationAttributes` is specified). |
| `approximateCount` | long | Approximate total count of matching entities (ES `total.value`). Use for pagination UI. Not exact for large result sets. |
| `searchScore` | map | GUID → relevance score. Only present when `showSearchScore: true`. |
| `searchMetadata` | map | GUID → `{ highlights, sort }`. Only present when `showSearchMetadata: true`. |
| `aggregations` | map | ES aggregation results. Only present when `aggs` is specified in the DSL. |
| `nextMarker` | string | Opaque pagination marker for the next page. Use in the next request's DSL for search-after pagination. |

#### AtlasEntityHeader

| Field | Type | Description |
|-------|------|-------------|
| `typeName` | string | Entity type |
| `guid` | string | Entity GUID (`"-1"` if scrubbed due to auth) |
| `attributes` | map | Requested attributes |
| `status` | string | `"ACTIVE"` or `"DELETED"` |
| `displayText` | string | Human-readable name |
| `classificationNames` | array | Tag names (when `includeClassificationNames: true`) |
| `classifications` | array | Full classification objects (unless `excludeClassifications: true`) |
| `meanings` | array | Glossary term assignments (unless `excludeMeanings: true`) |
| `scrubbed` | boolean | `true` if entity was scrubbed due to authorization |

---

## Examples

### 1. Search for all Tables (paginated, optimized)

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/search/indexsearch' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "dsl": {
    "from": 0,
    "size": 20,
    "query": {
      "bool": {
        "filter": [
          { "term": { "__typeName.keyword": "Table" } },
          { "term": { "__state": "ACTIVE" } }
        ]
      }
    },
    "sort": [{ "__name.keyword": { "order": "asc" } }]
  },
  "attributes": ["qualifiedName", "name", "description", "ownerUsers"],
  "excludeMeanings": true,
  "excludeClassifications": true,
  "includeClassificationNames": true
}'
```

Note: Both conditions are in `filter` (not `must`) since we don't need relevance scoring for exact-match filters. This enables ES query caching.

### 2. Full-text search with relevance scoring

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/search/indexsearch' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "dsl": {
    "size": 20,
    "query": {
      "bool": {
        "must": [
          {
            "multi_match": {
              "query": "customer revenue",
              "fields": ["__name^3", "description^2", "__qualifiedName"],
              "type": "best_fields"
            }
          }
        ],
        "filter": [
          { "term": { "__state": "ACTIVE" } }
        ]
      }
    }
  },
  "attributes": ["qualifiedName", "name", "description", "ownerUsers"],
  "showSearchScore": true,
  "excludeMeanings": true,
  "excludeClassifications": true,
  "requestMetadata": {
    "searchInput": "customer revenue",
    "utmTags": ["ui_popup_searchbar"],
    "saveSearchLog": true
  }
}'
```

The `multi_match` is in `must` (needs scoring), while `__state` filter is in `filter` (no scoring needed).

### 3. Filter by type hierarchy (e.g. all SQL assets)

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/search/indexsearch' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "dsl": {
    "size": 20,
    "query": {
      "bool": {
        "filter": [
          { "term": { "__superTypeNames.keyword": "SQL" } },
          { "term": { "__state": "ACTIVE" } },
          { "range": { "__modificationTimestamp": { "gte": 1709251200000 } } }
        ]
      }
    },
    "sort": [{ "__modificationTimestamp": { "order": "desc" } }]
  },
  "attributes": ["qualifiedName", "name", "__modifiedBy"],
  "excludeMeanings": true,
  "excludeClassifications": true
}'
```

### 4. Filter by multiple types

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/search/indexsearch' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "dsl": {
    "size": 20,
    "query": {
      "bool": {
        "filter": [
          { "terms": { "__typeName.keyword": ["Table", "View", "MaterializedView"] } },
          { "term": { "__state": "ACTIVE" } }
        ]
      }
    },
    "sort": [{ "__name.keyword": { "order": "asc" } }]
  },
  "attributes": ["qualifiedName", "name", "description"],
  "excludeMeanings": true,
  "excludeClassifications": true
}'
```

### 5. Aggregation: count entities by type

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/search/indexsearch' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "dsl": {
    "size": 0,
    "query": {
      "bool": {
        "filter": [
          { "term": { "__state": "ACTIVE" } }
        ]
      }
    },
    "aggs": {
      "by_type": {
        "terms": {
          "field": "__typeName.keyword",
          "size": 50,
          "order": { "_count": "desc" }
        }
      }
    }
  },
  "suppressLogs": true
}'
```

Response:

```json
{
  "entities": [],
  "approximateCount": 125000,
  "aggregations": {
    "by_type": {
      "buckets": [
        { "key": "Column", "doc_count": 92000 },
        { "key": "Table", "doc_count": 15000 },
        { "key": "Process", "doc_count": 8000 }
      ]
    }
  }
}
```

### 6. Search with highlights

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/search/indexsearch' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "dsl": {
    "size": 10,
    "query": {
      "bool": {
        "must": [
          {
            "multi_match": {
              "query": "revenue",
              "fields": ["__name", "description"]
            }
          }
        ],
        "filter": [
          { "term": { "__state": "ACTIVE" } }
        ]
      }
    },
    "highlight": {
      "fields": {
        "__name": {},
        "description": { "fragment_size": 150 }
      },
      "pre_tags": ["<em>"],
      "post_tags": ["</em>"]
    }
  },
  "attributes": ["name", "description"],
  "showHighlights": true,
  "showSearchMetadata": true,
  "excludeMeanings": true,
  "excludeClassifications": true
}'
```

### 7. Search with persona/purpose access control

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/search/indexsearch' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "dsl": {
    "size": 20,
    "query": {
      "bool": {
        "filter": [
          { "term": { "__typeName.keyword": "Table" } },
          { "term": { "__state": "ACTIVE" } }
        ]
      }
    }
  },
  "attributes": ["qualifiedName", "name"],
  "persona": "default/persona-abc123",
  "purpose": "default/purpose-xyz789",
  "excludeMeanings": true,
  "excludeClassifications": true
}'
```

Results are automatically filtered to entities visible under the specified Persona and Purpose policies.

### 8. Deep pagination with search_after

First request:

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/search/indexsearch' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "dsl": {
    "size": 20,
    "query": {
      "bool": {
        "filter": [
          { "term": { "__typeName.keyword": "Column" } },
          { "term": { "__state": "ACTIVE" } }
        ]
      }
    },
    "sort": [
      { "__timestamp": { "order": "desc" } },
      { "__guid": { "order": "asc" } }
    ]
  },
  "attributes": ["qualifiedName", "name"],
  "excludeMeanings": true,
  "excludeClassifications": true
}'
```

Subsequent request using `search_after` with the last result's sort values:

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/search/indexsearch' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "dsl": {
    "size": 20,
    "query": {
      "bool": {
        "filter": [
          { "term": { "__typeName.keyword": "Column" } },
          { "term": { "__state": "ACTIVE" } }
        ]
      }
    },
    "sort": [
      { "__timestamp": { "order": "desc" } },
      { "__guid": { "order": "asc" } }
    ],
    "search_after": [1710288000000, "abc-def-123-guid"]
  },
  "attributes": ["qualifiedName", "name"],
  "excludeMeanings": true,
  "excludeClassifications": true
}'
```

### 9. Find entities by classification (tag)

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/search/indexsearch' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "dsl": {
    "size": 20,
    "query": {
      "bool": {
        "filter": [
          { "term": { "__state": "ACTIVE" } },
          { "match": { "__classificationNames": "PII" } }
        ]
      }
    }
  },
  "attributes": ["qualifiedName", "name", "ownerUsers"],
  "includeClassificationNames": true,
  "excludeMeanings": true,
  "excludeClassifications": true
}'
```

### 10. Find recently modified entities by a user

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/search/indexsearch' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "dsl": {
    "size": 20,
    "query": {
      "bool": {
        "filter": [
          { "term": { "__state": "ACTIVE" } },
          { "term": { "__modifiedBy": "john.doe" } },
          { "range": { "__modificationTimestamp": { "gte": "now-7d" } } }
        ]
      }
    },
    "sort": [{ "__modificationTimestamp": { "order": "desc" } }]
  },
  "attributes": ["qualifiedName", "name", "__typeName", "__modifiedBy"],
  "excludeMeanings": true,
  "excludeClassifications": true
}'
```

---

## Anti-Patterns

### Don't: Fetch everything with large size

```json
// BAD — fetches 1000 entities with full relationship data
{
  "dsl": { "size": 1000, "query": { "match_all": {} } },
  "relationAttributes": ["name", "description"]
}
```

### Don't: Use leading wildcards for search

```json
// BAD — causes full index scan
{
  "dsl": {
    "query": { "wildcard": { "__name.keyword": "*revenue*" } }
  }
}

// GOOD — use multi_match for full-text search
{
  "dsl": {
    "query": { "multi_match": { "query": "revenue", "fields": ["__name", "description"] } }
  }
}
```

### Don't: Skip state filter

```json
// BAD — returns deleted entities
{
  "dsl": { "query": { "term": { "__typeName.keyword": "Table" } } }
}

// GOOD — always filter by state
{
  "dsl": {
    "query": {
      "bool": {
        "filter": [
          { "term": { "__typeName.keyword": "Table" } },
          { "term": { "__state": "ACTIVE" } }
        ]
      }
    }
  }
}
```

### Don't: High-cardinality aggregations

```json
// BAD — creates millions of buckets, can OOM the ES node
{
  "dsl": {
    "size": 0,
    "aggs": { "by_qn": { "terms": { "field": "qualifiedName.keyword", "size": 100000 } } }
  }
}
```

---

## Error Responses

| Status | Error Code | Condition |
|--------|-----------|-----------|
| `400` | `INVALID_PARAMETERS` | Empty or missing `dsl` query |
| `400` | `INVALID_DSL_QUERY_SIZE` | `dsl.size` exceeds the server-configured maximum (unless UTM tag is in the allowlist) |
| `403` | `UNAUTHORIZED_ACCESS` | User lacks search permission |

---

## Notes

- **Authorization filtering:** Atlas automatically injects authorization filters into the Elasticsearch query before execution. Results only include entities the user has permission to read. Entities that fail row-level authorization are returned with `scrubbed: true` and have their attributes cleared.
- **Query size limits:** The `size` field in the DSL is validated against a server-configured maximum. Certain `utmTags` values can bypass this limit (configured via `atlas.indexsearch.limit.utm.tags`).
- **Pagination:** Use `from`/`size` for simple offset pagination (limited to 10,000 results by ES). For deep pagination, use `sort` with `search_after` in the DSL. Always include a tiebreaker sort field like `__guid`.
- **System fields:** You don't need to request system fields like `__guid`, `__typeName`, `__state` in `attributes` — they are always included in the `AtlasEntityHeader`.
- **Suppressing logs:** Set `suppressLogs: true` for automated/polling queries to avoid polluting audit logs and search analytics.
- **Approximate counts:** The `approximateCount` value uses ES `track_total_hits` and may not be exact for result sets over 10,000. Use it for pagination UI indicators, not for exact counting.
