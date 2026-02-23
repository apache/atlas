# Atlas Write (CUD) API Endpoints

All write endpoints exposed by the Atlas metastore, as consumed by its clients:
`marketplace-packages`, `marketplace-scripts`, `marketplace-kotlin`, and `atlan-java`.

**Base URL:** `http://atlas-ratelimited.atlas.svc.cluster.local/api/atlas/v2`
(proxied as `/api/meta` in atlan-java)

---

## 1. Entity Operations

| # | Method | Endpoint | Description | Clients |
|---|--------|----------|-------------|---------|
| 1 | POST | `/entity/bulk` | Bulk create/update entities | All 4 repos |
| 2 | POST | `/entity/bulk?replaceClassifications=...&replaceBusinessAttributes=...&overwriteBusinessAttributes=...` | Bulk upsert with classification and business-attribute control | marketplace-packages, marketplace-scripts, atlan-java |
| 3 | POST | `/entity/bulk?skipProcessEdgeRestoration=true` | Bulk upsert skipping edge restoration (lineage use-case) | marketplace-kotlin (spark-openlineage, alteryx-openlineage) |
| 4 | DELETE | `/entity/bulk?deleteType=SOFT&guid=...` | Soft-delete entities by GUID | marketplace-scripts, atlan-java |
| 5 | DELETE | `/entity/bulk?deleteType=HARD&guid=...` | Hard-delete entities by GUID | atlan-java |
| 6 | DELETE | `/entity/bulk/uniqueAttribute` | Delete entities by unique attributes (stale lineage cleanup) | marketplace-kotlin (commons, airflow-openlineage) |
| 7 | PUT | `/entity/uniqueAttribute/type/{typeName}?attr:qualifiedName=...` | Update simple attributes on an entity by qualifiedName | atlan-java |
| 8 | POST | `/entity/bulk/setClassifications` | Set/transfer classifications on entities in bulk | marketplace-scripts |
| 9 | DELETE | `/entity/uniqueAttribute/type/{typeName}/classification/{atlanTagId}?attr:qualifiedName=...` | Remove an Atlan tag from an entity | atlan-java |

## 2. Custom Metadata (Business Metadata)

| # | Method | Endpoint | Description | Clients |
|---|--------|----------|-------------|---------|
| 10 | POST | `/entity/guid/{guid}/businessmetadata?isOverwrite=false` | Append/update custom metadata attributes on an entity | atlan-java, marketplace-kotlin |
| 11 | POST | `/entity/guid/{guid}/businessmetadata?isOverwrite=true` | Replace all custom metadata on an entity | atlan-java |
| 12 | POST | `/entity/guid/{guid}/businessmetadata/displayName?isOverwrite=false` | Update custom metadata by display name | marketplace-kotlin (spark-openlineage) |

## 3. Type Definitions

| # | Method | Endpoint | Description | Clients |
|---|--------|----------|-------------|---------|
| 13 | POST | `/types/typedefs` | Create new type definitions (entity, classification, struct, relationship, BM, enum) | marketplace-packages, atlan-java |
| 14 | PUT | `/types/typedefs` | Update existing type definitions | marketplace-packages, atlan-java |
| 15 | PUT | `/types/typedefs?patch=true` | Patch-update type definitions | marketplace-packages |
| 16 | DELETE | `/types/typedef/name/{internalName}` | Purge a type definition by internal name | atlan-java |
| 17 | POST | `/types/typedefs?type=BUSINESS_METADATA` | Create business metadata typedef | marketplace-packages |
| 18 | PUT | `/types/typedefs?type=BUSINESS_METADATA` | Update business metadata typedef | marketplace-packages |
| 19 | DELETE | `/types/typedefs?type=BUSINESS_METADATA` | Delete business metadata typedef | marketplace-packages |
| 20 | POST | `/types/typedefs?type=ENUM` | Create enum typedef | marketplace-packages |
| 21 | PUT | `/types/typedefs?type=ENUM` | Update enum typedef | marketplace-packages |
| 22 | DELETE | `/types/typedefs?type=ENUM` | Delete enum typedef | marketplace-packages |

## 4. Relationships

| # | Method | Endpoint | Description | Clients |
|---|--------|----------|-------------|---------|
| 23 | POST | `/relationship` | Create a relationship between entities | marketplace-scripts |
| 24 | DELETE | `/relationship/guid/{guid}?deleteType=hard` | Hard-delete a relationship by GUID | marketplace-scripts |

## 5. Search (POST)

| # | Method | Endpoint | Description | Clients |
|---|--------|----------|-------------|---------|
| 25 | POST | `/search/indexsearch` | Index search with DSL query body | All 4 repos |
| 26 | POST | `/task/search` | Search async tasks | marketplace-scripts |

## 6. Admin / Repair

| # | Method | Endpoint | Description | Clients |
|---|--------|----------|-------------|---------|
| 27 | POST | `/admin/featureFlag?key=...&value=...` | Set a feature flag | marketplace-scripts |
| 28 | DELETE | `/admin/featureFlag/{feature_flag_name}` | Delete a feature flag | marketplace-scripts |
| 29 | POST | `/entity/repair/accesscontrolAlias/{persona_guid}` | Repair/rebuild persona access control alias | marketplace-scripts |
| 30 | POST | `/entity/accessors` | Check/set entity access permissions | marketplace-scripts |

## 7. Direct Elasticsearch (backing store)

| # | Method | Endpoint | Description | Clients |
|---|--------|----------|-------------|---------|
| 31 | POST | `(ES):9200/janusgraph_vertex_index/_update_by_query` | Directly update ES documents (qualified-name hierarchy migration) | marketplace-scripts |

---

## Summary

| Category | Count |
|----------|-------|
| Entity CRUD | 9 |
| Custom Metadata | 3 |
| Type Definitions | 10 |
| Relationships | 2 |
| Search (POST) | 2 |
| Admin / Repair | 4 |
| Direct Elasticsearch | 1 |
| **Total** | **31** |

The core write surface is concentrated around three endpoint families:
- **`/entity/bulk`** — the primary path for all entity creates, updates, and deletes.
- **`/types/typedefs`** — schema and type management.
- **`/entity/guid/{guid}/businessmetadata`** — custom metadata mutations.
