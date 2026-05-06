---

## name: Rename Propagation
route: /RenamePropagation
menu: Documentation
submenu: Features

# Rename propagation

**Rename propagation** is an Atlas feature for **partial entity updates**: when a renamed instance’s `**name`** (or a mapped source attribute) changes, the server can find **related entities** whose **unique attribute** (for most types, `**qualifiedName`**) still embeds the old segment, recompute that unique attribute from the type’s `**autoComputeFormat**`, and **persist those updates in the same transaction** as the triggering entity. Discovery is driven by **typedefs** (relationship ends and templates), not by hard-coded type names in application code.

---

## Why it exists

The unique attribute (qualified name) is often a **composite string** built from ancestor names (database, table, cluster, and so on). If only the root entity is updated in a hook payload, **downstream** instances can keep an outdated unique attribute until something rewrites them. Rename propagation closes that gap by following **marked relationship edges** from the updated vertex and applying the same **parse → replace one template segment → rebuild** logic per dependent type.

---

## Example: Hive database rename

Assume a Hive model where:

- `**hive_db`** has unique attribute (qualified name) shaped like `{name}@{clusterName}` (for example `sales@cm`).
- `**hive_table**` uses `{db.name}.{name}@{clusterName}` (for example `sales.orders@cm`).
- `**hive_column**` uses `{table.db.name}.{table.name}.{name}@{clusterName}`.

The `**hive_table_db**` relationship links each table to its database; `**hive_table_columns**` links columns to a table. Relationship ends that opt in with `**propagateRename**` tell Atlas: when the **trigger** side is renamed, walk to neighbors and refresh their unique attribute (qualified name) where the template depends on that rename.

1. An integration renames the database `**sales`** → `**sales_archive**` (partial update; `**name**` and thus the unique attribute on the `hive_db` vertex change).
2. Atlas detects that the entity’s **unique attribute (qualified name)** changed and that `**hive_db`** has **rename propagation targets** from typedef resolution.
3. `**EntityRenameHandler`** follows the configured relationship from the database vertex to related `**hive_table**` vertices, recomputes each table’s string (for example `sales.orders@cm` → `sales_archive.orders@cm`), and registers those rows on the mutation context.
4. If `**hive_table**` also declares propagation targets, the same process continues to `**hive_column**` rows (for example `sales.orders.col1@cm` → `sales_archive.orders.col1@cm`).

All of this happens **without** requiring the hook to send every table and column in one batch.

---

## How it works (two stages)

### 1. Typedef resolution (startup / type updates)

While the type system resolves references, Atlas builds **per-entity-type** metadata used later at runtime:

- `**propagateRename`** on a `**AtlasRelationshipEndDef**` marks which end acts as the **rename source** for that relationship so instances reached through that edge can be updated when the source side’s name changes.
- `**attributeDefOverrides`** on `**AtlasEntityDef**` can set `**autoComputeFormat**` for the **unique attribute** (qualified name), describing how the string is composed from named segments.
- For paths that do not use explicit `**propagateAttributes`** maps on the relationship end, `**AtlasEntityType**` also maintains `**autoComputeFormatPathByRefTypeNameMap**`: referenced type name → **dotted path** in the template for the segment to replace when that referenced type was renamed.
- Optional `**propagateAttributes`** on a relationship end lists `**source**` / `**target**` attribute pairs so values such as `**name**` can be written to the right stub fields on dependents when models need more than template substitution alone.

Together, these produce a list of `**RenamePropagationTarget**` entries on each `**AtlasEntityType**`.

### 2. Entity update (runtime)

On **partial update**, `**AtlasEntityStoreV2`** compares the stored **unique attribute (qualified name)** on the graph vertex to the incoming value. If it changed and the type has **rename propagation targets**, `**EntityRenameHandler`** walks the graph along those relationships, recomputes each dependent’s unique attribute from its effective `**autoComputeFormat**`, and adds minimal **dependent stubs** (guid, new unique attribute, optional mapped attributes) to the **same** mutation context so they are saved with the root change.

---

## Key typedef concepts


| Concept                                     | Where it lives            | Role                                                                                                                                                                                                 |
| ------------------------------------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `**attributeDefOverrides`**                 | `AtlasEntityDef`          | Supplies per-type overrides such as `**autoComputeFormat**` for the unique attribute (qualified name); merged during hierarchy resolution and stored on the entity type vertex when supported.       |
| `**propagateRename**`                       | `AtlasRelationshipEndDef` | Marks the end whose entity type is the **trigger** for propagation across that relationship.                                                                                                         |
| `**propagateAttributes`**                   | `AtlasRelationshipEndDef` | Optional maps from a **source** attribute on the trigger side to **target** attribute names on the dependent entity.                                                                                 |
| `**RenamePropagationTarget`**               | `AtlasEntityType`         | Precomputed link: relationship attribute, category, and propagate-attribute list.                                                                                                                    |
| `**autoComputeFormatPathByRefTypeNameMap**` | `AtlasEntityType`         | Referenced entity type name → dotted path in the dependent’s **autoComputeFormat** for the segment to rewrite when that type was renamed (when `**propagateAttributes`** is not used for that path). |


---

## Implementation reference


| Component                                                | Role                                                                                                                                                               |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `**AtlasEntityType**`                                    | Registers rename propagation targets and builds `**autoComputeFormatPathByRefTypeNameMap**` during reference resolution.                                           |
| `**EntityRenameHandler**`                                | Traverses edges, parses/rebuilds the unique attribute string, registers `**DependentUpdate**` entries on the mutation context.                                     |
| `**AtlasEntityStoreV2**`                                 | Detects a changed unique attribute (qualified name) on partial update and invokes the handler.                                                                     |
| `**AtlasEntityDefStoreV2` / `AtlasTypeDefGraphStoreV2**` | Persist and read `**attributeDefOverrides**` via the type vertex property keyed by `**TYPE_ATTR_DEF_OVERRIDES_PROPERTY_KEY**`.                                     |
| `**AtlasTypeDefStoreInitializer**`                       | Applies typedef patches such as `**SET_ATTRIBUTE_DEF_OVERRIDES**` and `**SET_PROPAGATE_RENAME**` (patch params use `**endDefToken**`: `"endDef1"` or `"endDef2"`). |
| `**GraphBackedSearchIndexer**`                           | Indexes the overrides property for search where applicable.                                                                                                        |


---

## Models and patches

- **Bootstrap model JSON** — Declare `**attributeDefOverrides`** for the unique attribute (qualified name) and `**propagateRename**` / `**propagateAttributes**` on relationship ends as required by your connectors (for example Hive, Trino).
- **Patch files** — Ship one-off typedef updates under `**addons/.../patches/`** so each patch runs once and is tracked (for example `**AtlasPatchRegistry**`), avoiding reliance on version-only bumps on every restart.

---

## Tests

- `**AtlasTypeDefStoreInitializerTest**` — Patch handlers for overrides and `**SET_PROPAGATE_RENAME**`.
- `**EntityRenameHandlerTest**` — End-to-end behavior: unique attribute recompute, mapped attributes, and multi-hop propagation through typedefs.

