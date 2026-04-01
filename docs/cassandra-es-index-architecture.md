# CassandraGraph ES Index Architecture

This document covers the index building, ES property filtering, and ES mapping registration
for the CassandraGraph backend. Use this as reference for writing unit tests.

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          ATLAS STARTUP SEQUENCE                                 │
│                                                                                 │
│                     ┌──────────────────────┐                                    │
│                     │    Cassandra          │                                    │
│                     │  type_definitions     │                                    │
│                     │  table                │                                    │
│                     └──────────┬────────────┘                                   │
│                                │ reads on startup                               │
│                                ▼                                                │
│                     ┌──────────────────────┐                                    │
│                     │  AtlasTypeRegistry   │                                    │
│                     │  (in-memory)         │                                    │
│                     │                      │                                    │
│                     │  getAllEntityDefs()   │                                    │
│                     │  → Table, Column,    │                                    │
│                     │    Connection, ...   │                                    │
│                     └──────────┬───────────┘                                    │
│                                │ typedef attributes                             │
│                                │ drive index building                           │
│                                ▼                                                │
│  ┌─────────────────────┐    ┌──────────────────────┐    ┌───────────────────┐  │
│  │ GraphBackedSearch-  │    │ CassandraGraph-      │    │ CassandraGraph    │  │
│  │ Indexer             │───▶│ Management           │───▶│ (in-memory maps)  │  │
│  │ (repository module) │    │ (graphdb/cassandra)   │    │                   │  │
│  └─────────┬───────────┘    └──────────┬───────────┘    │ propertyKeys: {}  │  │
│            │                           │                 │ graphIndexes: {}  │  │
│            │                           │                 │ ← ALL EMPTY on   │  │
│            │                           │                 │   restart!        │  │
│            │                           │                 └───────────────────┘  │
│            │                           │                                        │
│   Phase 1: initialize()      makePropertyKey()                                 │
│   ├─ createVertexMixedIndex  addMixedIndex()                                   │
│   │  ("vertex_index")        ├─ index.addFieldKey()  ◀── in-memory only!      │
│   └─ initializeSystem-       └─ addESFieldMapping()  ◀── PUT ES _mapping      │
│      Properties (~50 keys)                                                     │
│                                                                                 │
│   Phase 2: onLoadCompletion()                                                  │
│   │                                                                            │
│   │  typeRegistry.getAllEntityDefs() ◀── reads typedefs loaded from Cassandra  │
│   │  typeRegistry.getAllBusinessMetadataDefs()                                  │
│   │                                                                            │
│   ├─ updateIndexForTypeDef() ◀── FIX: was missing, caused the bug            │
│   │  for ALL existing                                                          │
│   │  entity typedefs          iterates typedef.attributeDefs                   │
│   │                           (qualifiedName, name, connectorName, ...)        │
│   │                           createVertexIndex()                              │
│   │                           ├─ makePropertyKey()                             │
│   │                           └─ addMixedIndex()                               │
│   │                              ├─ fieldKeys += propertyKey                   │
│   │                              └─ PUT ES _mapping                            │
│   │                                                                            │
│   └─ resolveIndexFieldNames()                                                  │
│      for ALL entity attrs     getPropertyKey()                                 │
│                               getIndexFieldName()                              │
│                               ├─ attribute.setIndexFieldName()                 │
│                               └─ typeRegistry.addIndexFieldName()              │
│                                                                                 │
│   Phase 3: onChange() (runtime, when new typedefs are created/updated)         │
│   ├─ updateIndexForTypeDef()  (same as Phase 2)                               │
│   └─ resolveIndexFieldNames() (same as Phase 2)                               │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Key point**: CassandraGraph has NO persistent schema store. The mixed index (`graphIndexes`
map, `propertyKeys` map) starts empty on every restart and is rebuilt entirely from the
typedefs that `AtlasTypeDefStoreInitializer` loads from the `type_definitions` Cassandra
table into `AtlasTypeRegistry`. The typedef attributes drive which property keys are created
and which fields are registered in the mixed index.

---

## Index Building — Detailed Flow

### Phase 1: `initialize()` — System Properties

Called once during startup by `instanceIsActive()`.

```
GraphBackedSearchIndexer.initialize()
│
├── management = graph.getManagementSystem()
│   └── returns new CassandraGraphManagement(this)
│
├── createVertexMixedIndex("vertex_index", "search", [])
│   └── CassandraGraphManagement:
│       ├── new CassandraGraphIndex("vertex_index", mixed=true, vertex=true)
│       ├── graphIndexes.put("vertex_index", index)
│       └── ensureESIndexExists("atlas_graph_vertex_index")
│           ├── HEAD /atlas_graph_vertex_index → 200? preloadESMappingCache()
│           └── 404? PUT /atlas_graph_vertex_index (create index)
│
├── createEdgeMixedIndex("edge_index", "search", [])
│   └── (same pattern)
│
├── createFullTextMixedIndex("fulltext_index", "search", [])
│   └── (same pattern)
│
└── createCommonVertexIndex() × ~50 system properties
    │
    │   Examples:
    │   ├── __guid          (String, SINGLE, unique=GLOBAL_UNIQUE)
    │   ├── __typeName      (String, SINGLE, unique=GLOBAL_UNIQUE)
    │   ├── __state         (String, SINGLE)
    │   ├── __timestamp     (Long, SINGLE, multifields={date: epoch_millis})
    │   ├── __superTypeNames (String, SET, multifields={keyword: normalizer})
    │   ├── __traitNames    (String, SET)
    │   └── ... ~45 more
    │
    └── createVertexIndex(management, propName, uniqueKind, propClass, ...)
        │
        ├── management.makePropertyKey(propName, propClass, cardinality)
        │   └── CassandraPropertyKey stored in graph.propertyKeys map
        │
        ├── isIndexApplicable(propClass, cardinality)?
        │   └── true unless BigDecimal/BigInteger
        │
        ├── !management.getGraphIndex("vertex_index").getFieldKeys().contains(key)?
        │   └── true (first time → not in fieldKeys yet)
        │
        └── management.addMixedIndex("vertex_index", propertyKey, isStringField, esConfig, esFields)
            ├── index.addFieldKey(propertyKey)  ← REGISTERS IN fieldKeys
            ├── addESFieldMapping() or addESFieldMappingWithSubFields()
            │   ├── Check esMappingCache → skip if already exists
            │   ├── Map Java class → ES type (String→keyword/text, Long→long, etc.)
            │   └── PUT /atlas_graph_vertex_index/_mapping {"properties":{"field":{...}}}
            └── return propertyKey.getName()  (no JanusGraph field encoding)
```

### Phase 2: `onLoadCompletion()` — Entity Attribute Properties

Called after all typedefs are loaded from the store.

```
GraphBackedSearchIndexer.onLoadCompletion()
│
├── typeDefs = typeRegistry.getAllEntityDefs()
│              + typeRegistry.getAllBusinessMetadataDefs()
│
├── management = graph.getManagementSystem()
│
├── ┌─────────────────────────────────────────────────────┐
│   │  NEW (the fix):                                     │
│   │  for each typeDef:                                  │
│   │    updateIndexForTypeDef(management, typeDef)        │
│   │    └── addIndexForType() → createIndexForAttribute() │
│   │        └── createVertexIndex()                       │
│   │            ├── makePropertyKey(attrName, ...)        │
│   │            └── addMixedIndex("vertex_index", key)    │
│   │                └── fieldKeys += key                  │
│   └─────────────────────────────────────────────────────┘
│
│   Without this fix, CassandraGraph's in-memory fieldKeys
│   only contained ~50 system properties, not the ~200+
│   entity attribute properties.
│
├── resolveIndexFieldNames(management, changedTypeDefs)
│   │
│   └── for each entity/businessMetadata typedef:
│       for each attribute:
│         ├── management.getPropertyKey(attr.vertexPropertyName)
│         │   └── NOW returns non-null (created by updateIndexForTypeDef above)
│         │       Previously returned null → attribute skipped with warning
│         │
│         ├── management.getIndexFieldName("vertex_index", key, isString)
│         │   └── returns key.getName() directly
│         │
│         └── attribute.setIndexFieldName(fieldName)
│             typeRegistry.addIndexFieldName(vertexPropName, fieldName)
│
└── commit(management)
```

### JanusGraph vs CassandraGraph Schema Persistence

```
┌──────────────────────────────────────┐  ┌──────────────────────────────────────┐
│         JANUSGRAPH                    │  │         CASSANDRAGRAPH               │
│                                       │  │                                      │
│  Schema stored in:                    │  │  Schema stored in:                   │
│  - Titan keyspace on disk             │  │  - ConcurrentHashMap<> in memory     │
│  - Survives restart                   │  │  - LOST on restart                   │
│                                       │  │                                      │
│  On restart:                          │  │  On restart:                         │
│  ┌──────────────────────────┐         │  │  ┌──────────────────────────┐        │
│  │ getPropertyKey("name")   │         │  │  │ getPropertyKey("name")   │        │
│  │ → returns from disk ✓    │         │  │  │ → returns NULL ✗         │        │
│  └──────────────────────────┘         │  │  └──────────────────────────┘        │
│                                       │  │                                      │
│  ┌──────────────────────────┐         │  │  ┌──────────────────────────┐        │
│  │ getGraphIndex("vertex_   │         │  │  │ getGraphIndex("vertex_   │        │
│  │   index").getFieldKeys() │         │  │  │   index").getFieldKeys() │        │
│  │ → all keys from disk ✓   │         │  │  │ → only system keys ✗    │        │
│  └──────────────────────────┘         │  │  └──────────────────────────┘        │
│                                       │  │                                      │
│  onLoadCompletion():                  │  │  onLoadCompletion():                 │
│  resolveIndexFieldNames()             │  │  updateIndexForTypeDef() ← FIX      │
│  └── getPropertyKey() non-null        │  │  └── creates keys + adds to index   │
│      → resolves correctly ✓           │  │  resolveIndexFieldNames()            │
│                                       │  │  └── getPropertyKey() now non-null ✓ │
└──────────────────────────────────────┘  └──────────────────────────────────────┘

How CassandraGraph rebuilds the mixed index on restart:

┌──────────────────┐     ┌─────────────────────┐     ┌──────────────────────────┐
│ Cassandra        │     │ AtlasTypeRegistry   │     │ CassandraGraph           │
│ type_definitions │     │ (in-memory)         │     │ (in-memory maps)         │
│ table            │     │                     │     │                          │
│                  │     │ Entity typedefs:    │     │ propertyKeys: {}         │
│ Row: Table       │────▶│  Table              │     │ graphIndexes:            │
│ Row: Column      │     │  Column             │     │  "vertex_index" →        │
│ Row: Connection  │     │  Connection         │     │    fieldKeys: {}         │
│ Row: Database    │     │  Database           │     │                          │
│ ...              │     │  ...                │     │                          │
└──────────────────┘     └─────────┬───────────┘     └────────────▲─────────────┘
                                   │                              │
                     onLoadCompletion()                           │
                                   │                              │
                                   ▼                              │
                    ┌──────────────────────────────┐              │
                    │ for each typedef:            │              │
                    │   for each attribute:        │              │
                    │     e.g. Table.qualifiedName │              │
                    │     e.g. Table.name          │              │
                    │     e.g. Column.dataType     │──────────────┘
                    │                              │  makePropertyKey()
                    │   createVertexIndex()        │  addMixedIndex()
                    │   └─ addMixedIndex()         │  → fieldKeys += key
                    │      └─ addESFieldMapping()  │
                    └──────────────────────────────┘
```

---

## ES Property Filtering — `filterPropertiesForES()`

Called by both commit-time sync and repair reindex paths.

```
CassandraGraph.commit()                          CassandraGraph.reindexVertices()
│                                                 │
├── syncVerticesToElasticsearch()                  ├── lookup GUIDs → vertices
│   ├── for each vertex:                          │   ├── for each vertex:
│   │   └── appendESIndexAction(bulk, idx, v)     │   │   └── appendESIndexAction(bulk, idx, v)
│   │       └── filterPropertiesForES(v.props)    │   │       └── filterPropertiesForES(v.props)
│   └── POST /_bulk (with retry)                  │   └── POST /_bulk (with retry)
│                                                 │
▼                                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    filterPropertiesForES(props)                      │
│                                                                     │
│  Input: raw vertex properties from Cassandra                        │
│  Output: filtered properties safe for ES                            │
│                                                                     │
│  For each (key, value) in props:                                    │
│                                                                     │
│  ┌─── LAYER 1: Property Name Blacklist ────────────────────────┐   │
│  │                                                              │   │
│  │  if key.startsWith("__type_") → SKIP                        │   │
│  │     reason: typedef metadata (~2500+ unique fields like      │   │
│  │     __type_atlas_operation, __type_atlas_operation.CREATE)    │   │
│  │                                                              │   │
│  │  if key.startsWith("__type.") → SKIP                        │   │
│  │     reason: typedef attribute metadata (dot notation)        │   │
│  │                                                              │   │
│  │  if key in {endDef1, endDef2, relationshipCategory,          │   │
│  │             relationshipLabel, tagPropagation} → SKIP        │   │
│  │     reason: relationship typedef properties, not entity data │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌─── LAYER 2: Value Type Exclusion ───────────────────────────┐   │
│  │                                                              │   │
│  │  if value instanceof Map → SKIP                              │   │
│  │     reason: JanusGraph doesn't register map-type attributes  │   │
│  │     in the mixed index. Stored via EntityGraphMapper.        │   │
│  │     mapMapValue() as setProperty(key, new HashMap<>(...))    │   │
│  │                                                              │   │
│  │  if value instanceof BigDecimal/BigInteger → SKIP            │   │
│  │     reason: JanusGraph's INDEX_EXCLUSION_CLASSES             │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌─── JSON Array Fix ──────────────────────────────────────────┐   │
│  │                                                              │   │
│  │  if value is String AND starts with "[" and ends with "]":   │   │
│  │     try parse as JSON array → send parsed value to ES        │   │
│  │     (these come from setJsonProperty() / multi-valued attrs) │   │
│  │                                                              │   │
│  │  if value is String AND starts with "{" and ends with "}":   │   │
│  │     DO NOT parse → keep as plain string                      │   │
│  │     reason: __customAttributes, __businessAttributes are     │   │
│  │     mapped as "text" in ES. Parsing to Map causes            │   │
│  │     mapper_parsing_exception (ES expects text, gets object)  │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  filtered.put(key, value) → include in ES doc                      │
└─────────────────────────────────────────────────────────────────────┘
```

### `isEntityVertex()` — Pre-filter Before ES Sync

```
isEntityVertex(CassandraVertex v)
│
├── v.getProperty("__typeName") != null?  → true (entity vertex)
│   examples: "Table", "Column", "Connection", "AtlasGlossaryTerm"
│
├── v.getProperty("__type") != null?      → true (typedef vertex)
│   value is always "typeSystem"
│
└── both null?                            → false (system vertex, skip)
    examples: patch vertices, index recovery vertices
```

---

## ES Mapping Registration

```
CassandraGraphManagement.addMixedIndex("vertex_index", propertyKey, isStringField, esConfig, esFields)
│
├── index.addFieldKey(propertyKey)         ← in-memory fieldKeys
│
├── esIndexName = "atlas_graph_" + "vertex_index"
│
└── Which ES mapping method?
    │
    ├── indexTypeESFields != null && !empty?
    │   └── addESFieldMappingWithSubFields()
    │       │
    │       │  Example: "name" field with sub-fields
    │       │  PUT /atlas_graph_vertex_index/_mapping
    │       │  {
    │       │    "properties": {
    │       │      "name": {
    │       │        "type": "keyword",
    │       │        "ignore_above": 5120,
    │       │        "fields": {
    │       │          "keyword": { "type": "keyword", "normalizer": "atlan_normalizer" },
    │       │          "stemmed": { "type": "text", "analyzer": "atlan_text_analyzer" }
    │       │        }
    │       │      }
    │       │    }
    │       │  }
    │       │
    │       ├── Check esRichMappingCache → skip if already done
    │       ├── Check esMappingCache (field exists?) → addSubFieldsToExistingField()
    │       └── Otherwise: buildRichFieldMappingJson() → PUT _mapping
    │
    ├── indexTypeESConfig != null && !empty?
    │   └── addESFieldMappingWithConfig()
    │       │
    │       │  Example: "meaningNames" with analyzer
    │       │  PUT /atlas_graph_vertex_index/_mapping
    │       │  {
    │       │    "properties": {
    │       │      "meaningNames": {
    │       │        "type": "keyword",
    │       │        "analyzer": "atlan_text_comma_analyzer"
    │       │      }
    │       │    }
    │       │  }
    │       │
    │       └── Check esMappingCache → skip if exists
    │
    └── else (simple field)
        └── addESFieldMapping()
            │
            │  Example: "__guid"
            │  PUT /atlas_graph_vertex_index/_mapping
            │  {
            │    "properties": {
            │      "__guid": { "type": "keyword", "ignore_above": 5120 }
            │    }
            │  }
            │
            └── Check esMappingCache → skip if exists

Java Class → ES Type Mapping:
┌──────────────────┬──────────────────────────────────────┐
│ Java Class        │ ES Type                              │
├──────────────────┼──────────────────────────────────────┤
│ String (STRING)   │ "keyword" (exact match, normalizer)  │
│ String (default)  │ "text" (analyzed, full-text)          │
│ Integer / int     │ "integer"                            │
│ Long / long       │ "long"                               │
│ Float / float     │ "float"                              │
│ Double / double   │ "double"                             │
│ Boolean / boolean │ "boolean"                            │
│ other             │ "keyword"                            │
└──────────────────┴──────────────────────────────────────┘
```

### ES Mapping Caches

```
┌─────────────────────────────────────────────────────────────────┐
│                      esMappingCache                             │
│  ConcurrentHashMap<String indexName, Set<String> fieldNames>    │
│                                                                 │
│  Populated by:                                                  │
│  1. preloadESMappingCache() on startup (GET _mapping, parse)    │
│  2. addESFieldMapping() after successful PUT _mapping           │
│  3. addESFieldMapping() after "already exists" error            │
│                                                                 │
│  Purpose: avoid redundant PUT _mapping calls on restart         │
│  (~200+ fields × every restart = unnecessary ES traffic)        │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    esRichMappingCache                            │
│  ConcurrentHashMap<String indexName, Set<String> fieldNames>    │
│                                                                 │
│  Populated by:                                                  │
│  addESFieldMappingWithSubFields() after success                 │
│                                                                 │
│  Purpose: don't retry adding sub-fields (they're additive and  │
│  idempotent, but the PUT call is expensive)                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## The Bug and Fix

### Root Cause

On restart with CassandraGraph backend, `onLoadCompletion()` only called
`resolveIndexFieldNames()`, which tried `getPropertyKey()` for each entity
attribute — returned **null** because entity attribute property keys were never
created (`makePropertyKey()` was never called for them). As a result:

1. `CassandraGraphIndex.fieldKeys` only had ~50 system properties
2. `getESEligiblePropertyNames()` returned a set missing `qualifiedName`, `name`, etc.
3. `filterPropertiesForES()` Layer 1 whitelist stripped ALL entity attributes
4. ES docs only had `__typeName`, `__state`, `__guid` — no business attributes
5. Search/UI couldn't find entities (no `qualifiedName` to query)

### Fix (Two Parts)

**Part 1 — Root Cause** (`GraphBackedSearchIndexer.onLoadCompletion()`):
```java
// BEFORE (broken for CassandraGraph):
resolveIndexFieldNames(management, changedTypeDefs);

// AFTER (fixed):
for (AtlasBaseTypeDef typeDef : typeDefs) {
    updateIndexForTypeDef(management, typeDef);  // ← creates keys + registers in index
}
resolveIndexFieldNames(management, changedTypeDefs);
```

For JanusGraph this is a no-op (schema already loaded from disk, all checks
return "already exists"). For CassandraGraph this rebuilds the in-memory schema.

**Part 2 — Defense-in-depth** (`CassandraGraph.filterPropertiesForES()`):

Removed the whitelist (Layer 1) entirely. The blacklist + value-type filter is
sufficient. If the in-memory registry is ever incomplete for any reason, entity
attributes still pass through to ES instead of being silently dropped.

---

## Unit Test Coverage Matrix

### 1. Index Building Tests

| Test | What to Assert | Key Setup |
|------|---------------|-----------|
| System properties registered at startup | `fieldKeys` contains `__guid`, `__typeName`, `__state`, `__timestamp` | Call `initialize()` with mock management |
| Entity attributes registered on load completion | `fieldKeys` contains `qualifiedName`, `name`, `connectorName` | Call `onLoadCompletion()` with entity typedefs |
| `updateIndexForTypeDef()` calls `addMixedIndex()` | Verify `addMixedIndex()` called for each indexable attribute | Mock management, capture calls |
| New typedef via `onChange()` adds to fieldKeys | After `onChange()`, new attribute is in `fieldKeys` | Call `onChange()` with new typedef |
| `isIndexApplicable()` excludes BigDecimal/BigInteger | Returns false for BigDecimal.class | Direct method call |
| `isIndexApplicable()` includes String, Integer, Long | Returns true for these classes | Direct method call |
| `makePropertyKey()` stores in propertyKeys map | `getPropertyKey(name)` returns non-null after `makePropertyKey()` | Call makePropertyKey, then getPropertyKey |
| CassandraPropertyKey has no `equals()`/`hashCode()` | Two keys with same name are different objects in HashSet | Create two, add to HashSet, check size=2 |

### 2. ES Property Filtering Tests

| Test | Input | Expected Output |
|------|-------|----------------|
| System properties pass through | `{__typeName: "Table", __guid: "abc"}` | Same |
| Entity attributes pass through | `{qualifiedName: "x", name: "y", connectorName: "z"}` | Same |
| `__type_*` blacklisted | `{__type_atlas_operation: "CREATE"}` | Empty |
| `__type.*` blacklisted | `{__type.Asset.name: "x"}` | Empty |
| Relationship props blacklisted | `{endDef1: "x", endDef2: "y", tagPropagation: "z"}` | Empty |
| Map values excluded | `{businessAttributes: {key: "val"}}` | Empty |
| BigDecimal excluded | `{someNum: BigDecimal("1.23")}` | Empty |
| BigInteger excluded | `{someNum: BigInteger("123")}` | Empty |
| JSON array string parsed | `{__superTypeNames: "[\"Asset\",\"Catalog\"]"}` | `{__superTypeNames: ["Asset","Catalog"]}` |
| JSON object string NOT parsed | `{__customAttributes: "{\"key\":\"val\"}"}` | `{__customAttributes: "{\"key\":\"val\"}"}` (unchanged) |
| Invalid JSON array kept as string | `{field: "[not valid json"}` | `{field: "[not valid json"}` |
| Mixed props filtered correctly | Full vertex props map | Only entity attrs + system props |

### 3. ES Mapping Registration Tests

| Test | What to Assert |
|------|---------------|
| Simple field mapping PUT | Correct JSON body with type from `mapPropertyClassToESType()` |
| Keyword field has `ignore_above: 5120` | JSON body includes `ignore_above` |
| Rich field with sub-fields | JSON includes both base type and `fields` section |
| Existing field → add sub-fields only | Calls `addSubFieldsToExistingField()`, not full mapping |
| `esMappingCache` prevents duplicate PUT | Second call for same field skips PUT |
| `esRichMappingCache` prevents duplicate sub-field PUT | Second rich mapping call skips PUT |
| Mapping cache preloaded on startup | After `preloadESMappingCache()`, known fields in cache |
| Mapping conflict cached | After `mapper_parsing_exception`, field added to cache |
| Analyzer forces text type | `keyword` + analyzer config → `text` type in mapping |

### 4. Entity Vertex Detection Tests

| Test | Input | Expected |
|------|-------|----------|
| Entity vertex (has `__typeName`) | `{__typeName: "Table"}` | `true` |
| Typedef vertex (has `__type`) | `{__type: "typeSystem"}` | `true` |
| System vertex (neither) | `{__patch.id: "001"}` | `false` |
| Empty vertex | `{}` | `false` |

### 5. Reindex / Repair Tests

| Test | What to Assert |
|------|---------------|
| `reindexVertices()` uses same `filterPropertiesForES()` | ES bulk body has filtered properties |
| GUID not in index → logged, skipped | Warn logged, count incremented |
| Vertex not in Cassandra → logged, skipped | Warn logged, count incremented |
| Non-entity vertex → skipped | `notEntity` count incremented |
| ES bulk 5xx → retried | Retry with backoff, eventually succeeds |
| ES bulk 4xx → permanently failed | Logged, not retried |
| `reindexByQualifiedNamePrefix()` progress callback | Callback invoked with running count |

### 6. Integration / End-to-End Scenarios

| Scenario | Steps | Assertion |
|----------|-------|-----------|
| Fresh startup → entity creation → ES has all attrs | Start, create entity, query ES | ES doc has `qualifiedName`, `name`, etc. |
| Restart → repair → ES has all attrs | Restart server, call `reindexVertices()`, query ES | ES doc has full attributes |
| New typedef → entity creation → ES mapping exists | Create typedef, create entity | ES has field mapping for new attribute |
| `__customAttributes` with JSON object → no crash | Create entity with `__customAttributes: "{...}"` | ES doc has `__customAttributes` as text, no `mapper_parsing_exception` |

---

## Key Files

| File | Module | Role |
|------|--------|------|
| `GraphBackedSearchIndexer.java` | repository | Orchestrates index building during startup and typedef changes |
| `CassandraGraph.java` | graphdb/cassandra | ES sync, property filtering, reindex operations |
| `CassandraGraphManagement.java` | graphdb/cassandra | In-memory schema management, ES mapping registration |
| `CassandraGraphIndex.java` | graphdb/cassandra | Stores index metadata and `fieldKeys` set |
| `CassandraPropertyKey.java` | graphdb/cassandra | Property key metadata (no `equals`/`hashCode` override) |
| `VertexRepository.java` | graphdb/cassandra | Cassandra vertex CRUD, full table scan for prefix reindex |
