# Atlas Metastore — Repair Endpoints

All endpoints are under the base path `/api/meta`. Requires an active admin session.

---

## Table of Contents

- [Classification / Tag Repair](#classification-tag-repair)
- [Entity Index Repair](#entity-index-repair)
- [Lineage Repair](#lineage-repair)
- [Access Control Repair](#access-control-repair)
- [Migration Repair](#migration-repair)

---

## Classification / Tag Repair

These endpoints fix ES/Cassandra desyncs where `__traitNames` / `__classificationNames` in Elasticsearch are out of sync with the ground truth in Cassandra. Symptoms: tag shows on asset overview page but asset is invisible in tag filter search or the tag's linked assets list.

---

### Repair classifications for a single entity

```
POST /api/meta/entity/repairClassificationsMappings/{guid}
```

Reads classification state from Cassandra for the given entity and writes the correct `__traitNames`, `__classificationNames`, and `__classificationsText` values back to ES.

**Path params**

| Param | Type   | Description          |
|-------|--------|----------------------|
| guid  | String | GUID of the entity   |

**Auth:** Any authenticated user (no privilege check)

**Example**
```bash
curl -X POST "https://<tenant>/api/meta/entity/repairClassificationsMappings/f731c318-1f73-419a-b16d-069506608c52" \
  -H "Authorization: Bearer <token>"
```

---

### Repair classifications for multiple entities (bulk)

```
POST /api/meta/entity/bulk/repairClassificationsMappings
Content-Type: application/json
```

Same as the single-GUID endpoint but accepts a set of GUIDs. Returns a map of GUID → error message for any failures.

**Request body**

```json
["guid-1", "guid-2", "guid-3"]
```

**Response**

```json
{
  "guid-2": "vertex not found"
}
```
An empty object `{}` means all succeeded.

**Auth:** Any authenticated user (no privilege check)

**Example**
```bash
curl -X POST "https://<tenant>/api/meta/entity/bulk/repairClassificationsMappings" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '["f731c318-1f73-419a-b16d-069506608c52", "a2b3c4d5-..."]'
```

---

### Repair classifications for all entities (tenant-wide)

```
POST /api/meta/entity/repairAllClassifications
```

Scans the entire `effective_tags` Cassandra table page by page, and for each batch of vertex IDs calls the classification resync. This is the recommended operation when the full scope of desynced assets is unknown.

**Query params**

| Param     | Type | Default | Description                                                    |
|-----------|------|---------|----------------------------------------------------------------|
| fetchSize | int  | 5000    | Number of vertex IDs read from Cassandra per page              |
| batchSize | int  | 1000    | Number of vertices repaired per batch                          |
| delay     | int  | 0       | Milliseconds to sleep between batches (throttle on large tenants) |

**Auth:** `ADMIN_REPAIR_INDEX` privilege required

**Example**
```bash
# Conservative — small batches with 200ms delay between each
curl -X POST "https://<tenant>/api/meta/entity/repairAllClassifications?batchSize=500&delay=200" \
  -H "Authorization: Bearer <token>"
```

> **Note:** This is a long-running operation on large tenants. Use `delay` to throttle load on Cassandra and ES.

---

## Entity Index Repair

These endpoints fix corrupted or missing entries in the JanusGraph composite and mixed (ES) indices, without modifying the primary Cassandra/JanusGraph store.

---

### Repair index for a single entity

```
POST /api/meta/entity/guid/{guid}/repairindex
```

Re-indexes a single entity and its referred entities.

**Path params**

| Param | Type   | Description        |
|-------|--------|--------------------|
| guid  | String | GUID of the entity |

**Auth:** `ADMIN_REPAIR_INDEX` privilege required

**Example**
```bash
curl -X POST "https://<tenant>/api/meta/entity/guid/f731c318-1f73-419a-b16d-069506608c52/repairindex" \
  -H "Authorization: Bearer <token>"
```

---

### Repair index for multiple entities (bulk)

```
POST /api/meta/entity/guid/bulk/repairindex
Content-Type: application/json
```

Re-indexes a set of entities.

**Request body**

```json
["guid-1", "guid-2", "guid-3"]
```

**Auth:** `ADMIN_REPAIR_INDEX` privilege required

---

### Repair index for all entities of a type

```
POST /api/meta/entity/repairindex/{typename}
```

Queries all entities of the given type name from the graph (with pagination) and re-indexes them in batches.

**Path params**

| Param    | Type   | Description                               |
|----------|--------|-------------------------------------------|
| typename | String | Atlas type name, e.g. `Table`, `Column`   |

**Query params**

| Param     | Type | Default | Description                                            |
|-----------|------|---------|--------------------------------------------------------|
| limit     | int  | 1000    | Total number of entities to process                    |
| offset    | int  | 0       | Starting offset in the result set                      |
| batchSize | int  | 1000    | Number of entities re-indexed per batch                |
| delay     | int  | 0       | Milliseconds to sleep between batches                  |

**Auth:** `ADMIN_REPAIR_INDEX` privilege required

**Example**
```bash
# Re-index all Table entities, 500 at a time, 100ms pause between batches
curl -X POST "https://<tenant>/api/meta/entity/repairindex/Table?limit=5000&batchSize=500&delay=100" \
  -H "Authorization: Bearer <token>"
```

---

### Repair all mixed (ES) indices

```
POST /api/meta/entity/repairindex
```

Triggers a full reindex of all JanusGraph mixed indices. This is a heavy operation — prefer the type-scoped endpoint when possible.

**Auth:** Any authenticated user (no privilege check)

---

### Repair entity attributes (bulk)

```
POST /api/meta/entity/guid/bulk/repairattributes
Content-Type: application/json
```

Repairs a specific attribute for a set of entities. Currently supports one repair type.

**Query params**

| Param               | Type   | Required | Description                                              |
|---------------------|--------|----------|----------------------------------------------------------|
| repairType          | String | Yes      | Strategy to apply. Currently: `REMOVE_INVALID_OUTPUT_PORT_GUIDS` |
| repairAttributeName | String | Yes      | Attribute to repair, e.g. `outputPorts`                  |

**Request body**

```json
["guid-1", "guid-2"]
```

**Auth:** `ADMIN_REPAIR_INDEX` privilege required

**Example**
```bash
curl -X POST "https://<tenant>/api/meta/entity/guid/bulk/repairattributes?repairType=REMOVE_INVALID_OUTPUT_PORT_GUIDS&repairAttributeName=outputPorts" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '["guid-1", "guid-2"]'
```

---

### Repair single JanusGraph index (by qualifiedName)

```
POST /api/meta/repair/single-index?qualifiedName=<qn>
```

Finds and repairs a corrupted vertex in the single (non-composite) JanusGraph index for the given `qualifiedName`.

**Query params**

| Param         | Type   | Required | Description                  |
|---------------|--------|----------|------------------------------|
| qualifiedName | String | Yes      | qualifiedName of the asset   |

**Auth:** `ADMIN_REPAIR_INDEX` privilege required

**Response**
```json
{
  "success": true,
  "message": "...",
  "repairedVertexId": 12345,
  "qualifiedName": "default/snowflake/...",
  "indexType": "SINGLE"
}
```

---

### Repair composite JanusGraph index (by qualifiedName + typeName)

```
POST /api/meta/repair/composite-index
Content-Type: application/json
```

Finds and repairs a corrupted vertex in the composite JanusGraph index. Requires both `qualifiedName` and `typeName` because composite indices are keyed on both.

**Request body**
```json
{
  "qualifiedName": "default/snowflake/...",
  "typeName": "Table"
}
```

**Auth:** `ADMIN_REPAIR_INDEX` privilege required

**Response**
```json
{
  "success": true,
  "message": "...",
  "repairedVertexId": 12345,
  "qualifiedName": "default/snowflake/...",
  "typeName": "Table",
  "indexType": "COMPOSITE"
}
```

---

### Batch repair JanusGraph indices

```
POST /api/meta/repair/batch?indexType=COMPOSITE
Content-Type: application/json
```

Repairs multiple assets in a single call. `indexType` controls which index is targeted.

**Query params**

| Param     | Type   | Default   | Values                          |
|-----------|--------|-----------|---------------------------------|
| indexType | String | COMPOSITE | `SINGLE`, `COMPOSITE`, `AUTO`   |

`AUTO` detects per-entity which index type needs repair.

**Request body**
```json
{
  "entities": [
    { "qualifiedName": "default/snowflake/...", "typeName": "Table" },
    { "qualifiedName": "default/bigquery/...",  "typeName": "Column" }
  ]
}
```

**Auth:** `ADMIN_REPAIR_INDEX` privilege required

---

## Lineage Repair

---

### Repair `hasLineage` for assets and processes

```
POST /api/meta/entity/repairhaslineage
Content-Type: application/json
```

Recalculates and corrects the `hasLineage` boolean attribute for a list of assets or process entities. Use this when lineage tabs show incorrectly or the `hasLineage` flag is stale after lineage deletion.

**Request body**
```json
{
  "requests": [
    { "guid": "asset-guid-1", "typeName": "Table" },
    { "guid": "process-guid-1", "typeName": "Process" }
  ]
}
```

**Auth:** Any authenticated user (no privilege check)

---

### Repair `hasLineage` by vertex IDs

```
POST /api/meta/entity/repairhaslineagebyids
Content-Type: application/json
```

Same as above but accepts internal JanusGraph vertex IDs instead of GUIDs. Used when GUID lookup is not available (e.g., low-level repair scripts).

**Request body**
```json
{
  "12345": "Table",
  "67890": "Process"
}
```

Key = vertex ID (as string), value = type name.

**Auth:** Any authenticated user (no privilege check)

---

## Access Control Repair

---

### Repair Elasticsearch alias for a Persona

```
POST /api/meta/entity/repair/accesscontrolAlias/{guid}
```

Rebuilds the Elasticsearch alias for a Persona entity. Use when a Persona's alias is missing or out of sync after an ES index migration.

**Path params**

| Param | Type   | Description       |
|-------|--------|-------------------|
| guid  | String | GUID of the Persona |

**Auth:** Any authenticated user (no privilege check)

**Example**
```bash
curl -X POST "https://<tenant>/api/meta/entity/repair/accesscontrolAlias/persona-guid-here" \
  -H "Authorization: Bearer <token>"
```

---

## Migration Repair

These are one-off data migration endpoints, not routine repair operations.

---

### Repair unique qualifiedName for assets

```
POST /api/meta/migration/repair-unique-qualified-name
Content-Type: application/json
```

Re-derives and writes the `uniqueAttributes.qualifiedName` value for the given asset GUIDs. Used after QN format changes.

**Request body**
```json
["guid-1", "guid-2"]
```

**Response:** `true` on success.

---

### Repair qualifiedName for Stakeholder entities

```
POST /api/meta/migration/repair-stakeholder-qualified-name
Content-Type: application/json
```

Same as above but scoped to `Stakeholder` entity type.

**Request body**
```json
["stakeholder-guid-1", "stakeholder-guid-2"]
```

**Response:** `true` on success.

---

## Quick Reference

| What's broken | Endpoint |
|---|---|
| Tag filter / linked assets missing assets (ES desync) — known GUIDs | `POST /entity/bulk/repairClassificationsMappings` |
| Tag filter / linked assets missing assets — unknown scope | `POST /entity/repairAllClassifications` |
| Asset not found in search, counts wrong | `POST /entity/guid/bulk/repairindex` |
| All assets of a type are missing from search | `POST /entity/repairindex/{typename}` |
| Single asset not found by qualifiedName | `POST /repair/single-index?qualifiedName=...` |
| Asset found but wrong typeName in index | `POST /repair/composite-index` |
| `hasLineage` flag wrong on assets/processes | `POST /entity/repairhaslineage` |
| Persona alias missing after ES migration | `POST /entity/repair/accesscontrolAlias/{guid}` |
| `outputPorts` contains invalid GUIDs | `POST /entity/guid/bulk/repairattributes?repairType=REMOVE_INVALID_OUTPUT_PORT_GUIDS` |
