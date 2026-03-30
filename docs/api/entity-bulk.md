# POST /api/atlas/v2/entity/bulk

Create or update multiple entities in a single request. This is the primary write API for ingesting metadata into Atlas. Entities are matched by `typeName` + `qualifiedName` — if a match exists the entity is updated, otherwise it is created (upsert semantics).

**Content-Type:** `application/json`

---

## Request

### Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `replaceClassifications` | boolean | `false` | **Replace mode:** Remove all existing classifications (tags) on matched entities and replace with the ones in this request. If the request has no `classifications` array, all existing tags are removed. |
| `replaceTags` | boolean | `false` | Alias for `replaceClassifications`. Identical behavior. |
| `appendTags` | boolean | `false` | **Append mode:** Add classifications from this request to the entity's existing tags. Without this flag, classifications on an existing entity are **ignored** during update (only new entities get tags applied). |
| `replaceBusinessAttributes` | boolean | `false` | **Gates business attribute handling.** When `true`, the `overwriteBusinessAttributes` flag determines the mode. When `false`, business attributes in the request are merged (only keys present are upserted, others untouched). |
| `overwriteBusinessAttributes` | boolean | `false` | **Controls replacement scope.** Only takes effect when `replaceBusinessAttributes` is also `true`. When both are `true`, ALL business metadata on the entity is wiped and replaced with what's in the request. When `replaceBusinessAttributes=true` but `overwriteBusinessAttributes=false`, only the business metadata types present in the request are fully replaced (other BM types are untouched). |
| `skipProcessEdgeRestoration` | boolean | `false` | Skip restoration of Process input/output edges during bulk ingest. Set to `true` when ingesting lineage-only Process entities where edge repair is not needed. Improves performance for bulk lineage writes. |

> **Constraint:** At most one of `replaceClassifications`, `replaceTags`, `appendTags` can be `true`. Sending more than one returns `400 Bad Request`.

#### Classification Behavior Matrix

| Flag | On CREATE | On UPDATE (entity exists) |
|------|-----------|--------------------------|
| None (all false) | Tags in request are applied | Tags in request are **ignored** |
| `appendTags=true` | Tags applied | Tags in request are **added** to existing tags |
| `replaceClassifications=true` | Tags applied | All existing tags **removed**, replaced with request tags |

#### Business Attribute Behavior Matrix

| `replaceBusinessAttributes` | `overwriteBusinessAttributes` | Behavior |
|-----------------------------|-------------------------------|----------|
| `false` | (ignored) | **Merge:** Only keys present in request are upserted. Other keys/BM types untouched. |
| `true` | `false` | **Replace by type:** For each BM type in the request, replace all its attributes. Other BM types untouched. |
| `true` | `true` | **Full replace:** Remove ALL business metadata on entity, then set only what's in the request. |

### Request Body

```json
{
  "entities": [ <AtlasEntity>, ... ],
  "referredEntities": { "<temp-guid>": <AtlasEntity>, ... }
}
```

#### Top-level fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `entities` | array of [AtlasEntity](#atlasentity) | Yes | The entities to create or update. Must not be empty. Max count is configurable (default: **10,000**, via `atlas.bulk.api.max.entities.allowed`). |
| `referredEntities` | map of guid → [AtlasEntity](#atlasentity) | No | Entities referenced by the main entities (e.g. Columns referenced by a Table via `relationshipAttributes`). Keyed by temporary negative GUIDs (`"-1"`, `"-2"`, ...) to allow cross-referencing within the same request. These entities are created/updated alongside the main entities. |

#### AtlasEntity

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `typeName` | string | Yes | Entity type (e.g. `"Table"`, `"Column"`, `"Process"`). Must match a registered TypeDef. |
| `guid` | string | No | For **new** entities, use a temporary negative GUID (`"-1"`, `"-2"`, ...) so other entities in the same request can reference it. For **existing** entities, use their real GUID, or omit and rely on `qualifiedName` matching. |
| `attributes` | map | Yes | Key-value map of entity attributes. **`qualifiedName`** is always required. Other required attributes depend on the TypeDef. String values limited to **100,000** characters (or **2,000,000** for whitelisted attributes like `rawQueryText`, `dataContractJson`). |
| `relationshipAttributes` | map | No | Set relationships. Values are a single `AtlasObjectId` or a list of them. On **CREATE**, sets the initial relationships. On **UPDATE**, fully replaces the specified relationship attributes (unspecified relationship attrs are untouched). |
| `appendRelationshipAttributes` | map | No | **Add** to existing relationships without removing current ones. Only works on UPDATE/PARTIAL_UPDATE. Use this to add new columns to a table or new inputs to a process without overwriting existing ones. |
| `removeRelationshipAttributes` | map | No | **Remove** specific relationships. Only works on UPDATE/PARTIAL_UPDATE. Use this to detach specific columns from a table or remove specific process inputs. |
| `classifications` | array | No | Classifications (tags) to attach. See [Classification Object](#classification-object). Behavior depends on query parameters. |
| `businessAttributes` | map | No | Business metadata. Outer key = business metadata type name, inner map = attribute name → value. See [Business Attributes](#business-attributes). |
| `customAttributes` | map | No | Arbitrary string key-value pairs stored on the entity. Not typed or validated — use for ad-hoc metadata. |
| `labels` | set of string | No | Labels attached to the entity. Simple string tags (different from classifications — labels have no attributes or propagation). |
| `meanings` | array | No | Glossary term assignments. Each entry is an `AtlasTermAssignmentHeader`. See [Terms/Meanings](#termsmeanings). |
| `status` | string | No | Entity status: `"ACTIVE"` (default) or `"DELETED"`. |
| `isIncomplete` | boolean | No | Marks the entity as having incomplete data. Used by connectors to indicate partial metadata. Default: `false`. |

#### AtlasObjectId (for relationship references)

Reference by temporary GUID (for new entities in the same request):

```json
{ "typeName": "Column", "guid": "-2" }
```

Reference by real GUID (for existing entities):

```json
{ "typeName": "Column", "guid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890" }
```

Reference by unique attributes (most common for existing entities):

```json
{
  "typeName": "Column",
  "uniqueAttributes": {
    "qualifiedName": "mydb.public.users.id@snowflake"
  }
}
```

#### Classification Object

```json
{
  "typeName": "PII",
  "attributes": { "level": "high" },
  "propagate": true,
  "removePropagationsOnEntityDelete": true,
  "validityPeriods": [
    {
      "startTime": "2025-01-01T00:00:00Z",
      "endTime": "2025-12-31T23:59:59Z",
      "timeZone": "UTC"
    }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `typeName` | string | Classification type name. Must match a registered classification TypeDef. |
| `attributes` | map | Optional attributes defined on the classification TypeDef. |
| `propagate` | boolean | Whether to propagate this tag through lineage relationships to downstream entities. Default: `false`. |
| `removePropagationsOnEntityDelete` | boolean | If `true`, remove propagated tags when the source entity is deleted. Default: `true`. |
| `validityPeriods` | array | Optional time-bounded validity periods for this tag. |

#### Business Attributes

```json
{
  "businessAttributes": {
    "DataGovernance": {
      "owner": "data-team",
      "certification": "certified",
      "expiryDate": 1735689600000
    },
    "DataQuality": {
      "score": 95,
      "lastChecked": 1710288000000
    }
  }
}
```

The outer key is the **business metadata type name** (must match a registered BM TypeDef). The inner map contains attribute name → value pairs defined in that BM type. Values must match the declared attribute types (string, int, long, float, boolean, date as epoch ms, enum).

#### Terms/Meanings

To attach glossary terms to an entity, include them in the `meanings` array:

```json
{
  "meanings": [
    {
      "termGuid": "term-guid-here",
      "displayText": "Revenue",
      "relationGuid": null
    }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `termGuid` | string | GUID of the glossary term to link. |
| `displayText` | string | Display name of the term (informational). |
| `relationGuid` | string | GUID of the relationship (set by server on response, null on request). |

---

## Response

**Status:** `200 OK`

```json
{
  "mutatedEntities": {
    "CREATE": [ <AtlasEntityHeader>, ... ],
    "UPDATE": [ <AtlasEntityHeader>, ... ],
    "DELETE": [ <AtlasEntityHeader>, ... ]
  },
  "guidAssignments": {
    "-1": "a1b2c3d4-...",
    "-2": "e5f6g7h8-..."
  }
}
```

#### Top-level fields

| Field | Type | Description |
|-------|------|-------------|
| `mutatedEntities` | map | Keyed by operation: `CREATE`, `UPDATE`, `PARTIAL_UPDATE`, `DELETE`, `PURGE`. Each value is a list of `AtlasEntityHeader` objects describing what was affected. |
| `guidAssignments` | map | Maps temporary negative GUIDs from the request to the real UUIDs assigned by Atlas. Only present when new entities were created. |

#### AtlasEntityHeader

| Field | Type | Description |
|-------|------|-------------|
| `typeName` | string | Entity type |
| `guid` | string | Real GUID |
| `attributes` | map | Subset of entity attributes (typically `qualifiedName`, `name`) |
| `status` | string | `ACTIVE` or `DELETED` |
| `displayText` | string | Human-readable display name |
| `classificationNames` | array | Tag names attached to this entity |
| `meaningNames` | array | Glossary term names linked to this entity |

---

## Examples

### 1. Create entities with simple attributes

Create a Table with basic attributes. No relationships, tags, or business metadata.

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Table",
      "attributes": {
        "qualifiedName": "mydb.public.users@snowflake",
        "name": "users",
        "description": "User accounts table",
        "comment": "Contains all registered user accounts",
        "ownerUsers": ["data-team@company.com"],
        "ownerGroups": ["analytics"],
        "displayName": "Users Table"
      }
    }
  ]
}'
```

### 2. Create a Table with Columns (cross-referencing with temporary GUIDs)

Use negative GUIDs to create parent-child relationships in a single request.

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Table",
      "guid": "-1",
      "attributes": {
        "qualifiedName": "mydb.public.orders@snowflake",
        "name": "orders"
      },
      "relationshipAttributes": {
        "columns": [
          { "typeName": "Column", "guid": "-2" },
          { "typeName": "Column", "guid": "-3" },
          { "typeName": "Column", "guid": "-4" }
        ]
      }
    },
    {
      "typeName": "Column",
      "guid": "-2",
      "attributes": {
        "qualifiedName": "mydb.public.orders.order_id@snowflake",
        "name": "order_id",
        "data_type": "INTEGER"
      },
      "relationshipAttributes": {
        "table": { "typeName": "Table", "guid": "-1" }
      }
    },
    {
      "typeName": "Column",
      "guid": "-3",
      "attributes": {
        "qualifiedName": "mydb.public.orders.customer_id@snowflake",
        "name": "customer_id",
        "data_type": "INTEGER"
      },
      "relationshipAttributes": {
        "table": { "typeName": "Table", "guid": "-1" }
      }
    },
    {
      "typeName": "Column",
      "guid": "-4",
      "attributes": {
        "qualifiedName": "mydb.public.orders.total_amount@snowflake",
        "name": "total_amount",
        "data_type": "DECIMAL"
      },
      "relationshipAttributes": {
        "table": { "typeName": "Table", "guid": "-1" }
      }
    }
  ]
}'
```

**Response:**

```json
{
  "mutatedEntities": {
    "CREATE": [
      { "typeName": "Table", "guid": "a1b2c3d4-...", "status": "ACTIVE",
        "attributes": { "qualifiedName": "mydb.public.orders@snowflake" } },
      { "typeName": "Column", "guid": "b2c3d4e5-...", "status": "ACTIVE",
        "attributes": { "qualifiedName": "mydb.public.orders.order_id@snowflake" } },
      { "typeName": "Column", "guid": "c3d4e5f6-...", "status": "ACTIVE",
        "attributes": { "qualifiedName": "mydb.public.orders.customer_id@snowflake" } },
      { "typeName": "Column", "guid": "d4e5f6a7-...", "status": "ACTIVE",
        "attributes": { "qualifiedName": "mydb.public.orders.total_amount@snowflake" } }
    ]
  },
  "guidAssignments": {
    "-1": "a1b2c3d4-...",
    "-2": "b2c3d4e5-...",
    "-3": "c3d4e5f6-...",
    "-4": "d4e5f6a7-..."
  }
}
```

### 3. Update an existing entity (partial update)

Only attributes present in the request are modified. Others are untouched.

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Table",
      "attributes": {
        "qualifiedName": "mydb.public.orders@snowflake",
        "description": "Updated: Customer orders with PII data"
      }
    }
  ]
}'
```

The entity is matched by `typeName` + `qualifiedName`. Only `description` is updated; `name`, `ownerUsers`, and all other attributes remain unchanged.

### 4. Classifications (tags)

#### 4a. Attach tags to a new entity

Tags are applied automatically when creating a new entity:

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Table",
      "attributes": {
        "qualifiedName": "mydb.public.customers@snowflake",
        "name": "customers"
      },
      "classifications": [
        { "typeName": "PII" },
        {
          "typeName": "Confidential",
          "attributes": { "level": "high" },
          "propagate": true
        }
      ]
    }
  ]
}'
```

#### 4b. Append tags to an existing entity

Without `appendTags=true`, classifications on existing entities are **ignored**:

```bash
# This ADDS the "GDPR" tag to existing tags on the entity
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk?appendTags=true' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Table",
      "attributes": {
        "qualifiedName": "mydb.public.customers@snowflake"
      },
      "classifications": [
        { "typeName": "GDPR" }
      ]
    }
  ]
}'
```

#### 4c. Replace all tags on an existing entity

Removes all existing tags and applies only the ones in the request:

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk?replaceClassifications=true' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Table",
      "attributes": {
        "qualifiedName": "mydb.public.customers@snowflake"
      },
      "classifications": [
        { "typeName": "PII" },
        { "typeName": "Confidential", "attributes": { "level": "high" } }
      ]
    }
  ]
}'
```

To remove **all** tags from an entity, use `replaceClassifications=true` with an empty or omitted `classifications` array.

### 5. Business attributes

#### 5a. Merge business attributes (default)

Only the keys present in the request are upserted. Other BM types and keys are untouched:

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Table",
      "attributes": {
        "qualifiedName": "mydb.public.orders@snowflake"
      },
      "businessAttributes": {
        "DataGovernance": {
          "owner": "data-team",
          "certification": "certified"
        }
      }
    }
  ]
}'
```

If the entity already has `DataGovernance.retentionPolicy = "90d"`, it remains untouched. Only `owner` and `certification` are set.

#### 5b. Replace business attributes for specific types

Replace all attributes within the specified BM type, but leave other BM types untouched:

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk?replaceBusinessAttributes=true' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Table",
      "attributes": {
        "qualifiedName": "mydb.public.orders@snowflake"
      },
      "businessAttributes": {
        "DataGovernance": {
          "owner": "data-team",
          "certification": "certified"
        }
      }
    }
  ]
}'
```

This replaces all `DataGovernance` attributes (removing any not in the request like `retentionPolicy`), but leaves other BM types like `DataQuality` untouched.

#### 5c. Full overwrite of ALL business metadata

Remove all business metadata and set only what's in the request:

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk?replaceBusinessAttributes=true&overwriteBusinessAttributes=true' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Table",
      "attributes": {
        "qualifiedName": "mydb.public.orders@snowflake"
      },
      "businessAttributes": {
        "DataGovernance": {
          "owner": "new-owner"
        }
      }
    }
  ]
}'
```

This removes ALL business metadata (including `DataQuality` and any other BM types), then sets only `DataGovernance.owner`.

### 6. Lineage — Create a Process with inputs and outputs

A `Process` entity represents a transformation. Its `inputs` and `outputs` relationship attributes define the lineage graph.

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Process",
      "attributes": {
        "qualifiedName": "mydb.etl.customer_transform@snowflake",
        "name": "Customer Transform ETL"
      },
      "relationshipAttributes": {
        "inputs": [
          {
            "typeName": "Table",
            "uniqueAttributes": {
              "qualifiedName": "mydb.raw.customers@snowflake"
            }
          },
          {
            "typeName": "Table",
            "uniqueAttributes": {
              "qualifiedName": "mydb.raw.addresses@snowflake"
            }
          }
        ],
        "outputs": [
          {
            "typeName": "Table",
            "uniqueAttributes": {
              "qualifiedName": "mydb.analytics.dim_customer@snowflake"
            }
          }
        ]
      }
    }
  ]
}'
```

This creates lineage: `raw.customers` + `raw.addresses` → **Customer Transform ETL** → `analytics.dim_customer`.

The referenced Tables (`raw.customers`, `raw.addresses`, `analytics.dim_customer`) must already exist.

### 7. Lineage — Add inputs with `appendRelationshipAttributes`

Add a new input to an existing Process **without removing existing inputs**. Using `relationshipAttributes` would replace all inputs — `appendRelationshipAttributes` only adds.

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Process",
      "attributes": {
        "qualifiedName": "mydb.etl.customer_transform@snowflake"
      },
      "appendRelationshipAttributes": {
        "inputs": [
          {
            "typeName": "Table",
            "uniqueAttributes": {
              "qualifiedName": "mydb.raw.phone_numbers@snowflake"
            }
          }
        ]
      }
    }
  ]
}'
```

After this call, the process has **three** inputs: `raw.customers`, `raw.addresses`, and `raw.phone_numbers`.

### 8. Lineage — Remove inputs with `removeRelationshipAttributes`

Remove a specific input from an existing Process without affecting other inputs.

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Process",
      "attributes": {
        "qualifiedName": "mydb.etl.customer_transform@snowflake"
      },
      "removeRelationshipAttributes": {
        "inputs": [
          {
            "typeName": "Table",
            "uniqueAttributes": {
              "qualifiedName": "mydb.raw.addresses@snowflake"
            }
          }
        ]
      }
    }
  ]
}'
```

After this call, the process has **two** inputs: `raw.customers` and `raw.phone_numbers`. The `raw.addresses` relationship edge is set to `DELETED`.

### 9. Attach glossary terms (meanings)

Link glossary terms to entities using the `meanings` field:

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Table",
      "attributes": {
        "qualifiedName": "mydb.public.orders@snowflake"
      },
      "meanings": [
        {
          "termGuid": "abc12345-def6-7890-ghij-klmnopqrstuv",
          "displayText": "Revenue"
        },
        {
          "termGuid": "xyz98765-uvw4-3210-qrst-abcdefghijkl",
          "displayText": "Customer Data"
        }
      ]
    }
  ]
}'
```

> **Note:** The `termGuid` must reference an existing `AtlasGlossaryTerm`. Use the search API to find term GUIDs.

### 10. Create a Readme for an entity

A `Readme` entity contains rich-text documentation attached to an asset.

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Readme",
      "attributes": {
        "qualifiedName": "mydb.public.orders@snowflake/readme",
        "name": "orders readme",
        "description": "## Orders Table\n\nThis table contains all customer orders.\n\n### Schema\n- **order_id**: Primary key\n- **customer_id**: FK to customers table\n- **total_amount**: Order total in USD"
      },
      "relationshipAttributes": {
        "asset": {
          "typeName": "Table",
          "uniqueAttributes": {
            "qualifiedName": "mydb.public.orders@snowflake"
          }
        }
      }
    }
  ]
}'
```

The `description` attribute holds the markdown content. The `asset` relationship links it to the parent entity. One Readme per asset.

### 11. Create a Link for an entity

A `Link` entity attaches an external URL to an asset.

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Link",
      "attributes": {
        "qualifiedName": "mydb.public.orders@snowflake/link/runbook",
        "name": "Orders Runbook",
        "link": "https://wiki.company.com/data-team/orders-runbook"
      },
      "relationshipAttributes": {
        "asset": {
          "typeName": "Table",
          "uniqueAttributes": {
            "qualifiedName": "mydb.public.orders@snowflake"
          }
        }
      }
    }
  ]
}'
```

The `link` attribute must be a valid URL (http/https/mailto or a relative path). Multiple Links can attach to the same asset.

### 12. Using `referredEntities` for cross-references

`referredEntities` is useful when you want the main `entities` array to be the "primary" objects, with supporting entities defined separately. Functionally identical to putting everything in `entities`.

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Table",
      "guid": "-1",
      "attributes": {
        "qualifiedName": "mydb.public.products@snowflake",
        "name": "products"
      },
      "relationshipAttributes": {
        "columns": [
          { "typeName": "Column", "guid": "-100" },
          { "typeName": "Column", "guid": "-101" }
        ]
      }
    }
  ],
  "referredEntities": {
    "-100": {
      "typeName": "Column",
      "guid": "-100",
      "attributes": {
        "qualifiedName": "mydb.public.products.id@snowflake",
        "name": "id",
        "data_type": "INTEGER"
      },
      "relationshipAttributes": {
        "table": { "typeName": "Table", "guid": "-1" }
      }
    },
    "-101": {
      "typeName": "Column",
      "guid": "-101",
      "attributes": {
        "qualifiedName": "mydb.public.products.name@snowflake",
        "name": "name",
        "data_type": "VARCHAR"
      },
      "relationshipAttributes": {
        "table": { "typeName": "Table", "guid": "-1" }
      }
    }
  }
}'
```

### 13. Custom attributes and labels

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Table",
      "attributes": {
        "qualifiedName": "mydb.public.orders@snowflake",
        "name": "orders"
      },
      "customAttributes": {
        "source_system": "salesforce",
        "refresh_schedule": "daily",
        "team_slack_channel": "#data-orders"
      },
      "labels": ["production", "tier-1", "pii-reviewed"]
    }
  ]
}'
```

`customAttributes` are arbitrary string key-value pairs — no TypeDef required. `labels` are simple string tags without attributes or propagation (unlike classifications).

---

## Error Responses

| Status | Error Code | Condition |
|--------|-----------|-----------|
| `400` | `INVALID_PARAMETERS` | Empty entities list, or entities count exceeds bulk limit |
| `400` | `BAD_REQUEST` | More than one of `replaceClassifications`/`replaceTags`/`appendTags` is `true` |
| `400` | `INVALID_PARAMETERS` | String attribute exceeds 100K character limit |
| `400` | `INVALID_PARAMETERS` | Invalid URL in Link entity `link` attribute |
| `403` | `UNAUTHORIZED_ACCESS` | User lacks create/update permission for the entity type or connection |
| `404` | `TYPE_NAME_NOT_FOUND` | `typeName` does not match any registered TypeDef |
| `409` | `ENTITY_ALREADY_EXISTS` | Conflict on unique attribute (rare, usually upsert handles this) |

---

## Notes

- **Upsert semantics:** The API is idempotent for the same `qualifiedName` + `typeName` combination. Sending the same request twice will create on first call and update on second.
- **Temporary GUIDs:** Use negative integers (`"-1"`, `"-2"`, ...) to cross-reference new entities within the same request. The response `guidAssignments` maps these to real UUIDs.
- **Attribute merging:** On update, only attributes present in the request are modified. Attributes not included are left unchanged (partial update semantics).
- **Relationship attributes:** Can reference entities by GUID or by `uniqueAttributes` (typically `qualifiedName`). Both inline references and `referredEntities` are supported.
- **`relationshipAttributes` vs `appendRelationshipAttributes`:** On UPDATE, `relationshipAttributes` **replaces** the entire relationship list for that attribute. Use `appendRelationshipAttributes` to add without removing existing ones. Use `removeRelationshipAttributes` to remove specific relationships.
- **Authorization:** Each entity is authorized individually. A bulk request may partially succeed — some entities created/updated while others are denied. Check the response for which entities were mutated.
- **Max bulk size:** Default 10,000 entities per request (configurable via `atlas.bulk.api.max.entities.allowed`). Recommended batch size for SDK clients: **20 entities per request**.
- **Process edge restoration:** When creating Process entities with lineage, Atlas automatically restores input/output edges. Set `skipProcessEdgeRestoration=true` if the referenced datasets already have correct edges and you want to skip this (performance optimization).
- **Classification propagation:** Tags with `propagate: true` are automatically pushed to downstream entities via lineage. Propagation is async — there may be a delay before downstream entities show the tag.
