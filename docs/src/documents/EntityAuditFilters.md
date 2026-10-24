---
name: Entity Audit Filters
route: /EntityAuditFilters
menu: Documentation
submenu: Features
---

import themen from 'theme/styles/styled-colors';
import * as theme from 'react-syntax-highlighter/dist/esm/styles/hljs';
import SyntaxHighlighter from 'react-syntax-highlighter';

# Entity Audit Filters

#### Background

Apache Atlas records an entity audit event for many entity lifecycle operations (create, update, delete, classification changes, and others). In large deployments, persisting every audit event can increase storage cost and noise in audit search.

Entity audit filters let administrators define **rules** that match entity audit events and choose whether each matching event is **accepted** (persisted) or **discarded** (not written to the entity audit store). Rules are stored in the Atlas graph as internal entities of type `__AtlasRule` and are managed through admin REST APIs (or `AtlasClientV2`).

Filtering is applied in `EntityAuditListenerV2` **before** events are written to the audit repository, so all configured entity audit backends behave consistently.

#### Enabling the feature

Set the following properties in `atlas-application.properties` and **restart Atlas** (the enabled flag is read when `AtlasEntityAuditFilterService` starts):

<SyntaxHighlighter wrapLines={true} language="bash" style={theme.dark}>
{`# Master switch for entity audit filters (default: false)
atlas.entity.audit.filter.enabled=true

# Action when no rule matches, or when rule evaluation falls back (default: ACCEPT)
# Valid values: ACCEPT | DISCARD
atlas.entity.audit.filter.default.action=ACCEPT`}
</SyntaxHighlighter>

When `atlas.entity.audit.filter.enabled` is `false`, all entity audit events are persisted and the rule admin APIs return HTTP 400 with an error indicating that `atlas.entity.audit.filter.enabled` is not enabled.

See also [Configuration](#/Configuration).

#### Authorization

Rule management APIs require the **`admin-audits`** privilege (`AtlasPrivilege.ADMIN_AUDITS`), the same privilege used for other admin audit operations.

#### Rule evaluation

Rules are evaluated when an entity audit event is created (only if the feature is enabled):

1. Rules are loaded from the graph and ordered by **`createdTime`** (oldest first).
2. Each rule’s `ruleExpr` is converted to [JsonLogic](https://jsonlogic.com/) and evaluated against a map built from the entity under audit plus a synthetic field **`operationType`** (the `EntityAuditActionV2` name, for example `ENTITY_DELETE`). Entity attributes are exposed under `attributes.<name>` (for example `attributes.name`).
3. When a rule **matches**:
   - **`DISCARD`**: evaluation stops; the event is marked discarded and is **not** persisted.
   - **`ACCEPT`**: evaluation **continues** with the next rule.
4. If no rule matches, the configured **`atlas.entity.audit.filter.default.action`** applies.
5. If rules cannot be loaded, Atlas logs the error and uses the default action for the whole batch of evaluation; **entity mutations are not blocked**. If an individual rule fails to evaluate, Atlas logs the error and **continues** with the remaining rules.

Multiple objects in `ruleExpr.ruleExprObjList` are combined with **OR** when there is more than one entry (any expression may match the rule).

#### Rule model

| Field | Description |
|-------|-------------|
| `ruleName` | Unique name for the rule (required). |
| `desc` | Optional description. |
| `action` | `ACCEPT` or `DISCARD` (required). |
| `ruleExpr.ruleExprObjList` | One or more expressions (see below); combined with OR when multiple. |
| `guid` | Assigned by Atlas on create; required for update. |
| `createdTime` / `updatedTime` | Set by Atlas. |

Each element of `ruleExprObjList` describes criteria for one or more entity types:

| Field | Description |
|-------|-------------|
| `typeName` | Entity type name, comma-separated list (for example `hive_table,hive_db`), wildcard pattern (for example `hive*`), or `_ALL_ENTITY_TYPES`. |
| `includeSubTypes` | When `true`, subtypes of the given type(s) are included in validation and matching. Defaults to `true` if omitted in stored JSON used by the rules engine. |
| `attributeName` / `operator` / `attributeValue` | Simple predicate on a single attribute (see operators below). |
| `condition` / `criterion` | Nested predicates: `condition` is `AND` or `OR`, and `criterion` is a list of `{ "attributeName", "operator", "attributeValue" }` objects. |

**Operators in rule JSON** use the operator **symbol** (not the Java enum name), for example:

| Symbol | Meaning |
|--------|---------|
| `<`, `>`, `<=`, `>=`, `==`, `!=` | Comparison |
| `startsWith`, `endsWith` | String prefix / suffix |
| `contains`, `notContains` | Substring / wildcard match |
| `containsIgnoreCase`, `notContainsIgnoreCase` | Case-insensitive substring / wildcard match |
| `isNull`, `notNull` | Unary null checks |

String-matching operators treat the pattern in `attributeValue` (and type-name patterns) as supporting **wildcard `*`** semantics.

**`operationType`:** Rules may reference `operationType` (not under `attributes.`) with `==`, `startsWith`, `endsWith`, `contains`, `containsIgnoreCase`, `notContains`, or `notContainsIgnoreCase` against valid `EntityAuditActionV2` names.

**Type-only rules:** An expression object may specify only `typeName` (and optional `includeSubTypes`) with no attribute criteria to match all audits for that type.

#### Example rule (discard hive tables whose name contains "temp")

<SyntaxHighlighter wrapLines={true} language="json" style={theme.dark}>
{`{
  "ruleName": "discard_temp_hive",
  "desc": "Do not persist audits for selected hive types when name contains temp",
  "action": "DISCARD",
  "ruleExpr": {
    "ruleExprObjList": [
      {
        "typeName": "hive_table,hive_db",
        "attributeName": "name",
        "operator": "containsIgnoreCase",
        "attributeValue": "temp",
        "includeSubTypes": false
      }
    ]
  }
}`}
</SyntaxHighlighter>

#### REST APIs

Base path: `/api/atlas/admin/audits/rules` (requires `atlas.entity.audit.filter.enabled=true` and `admin-audits` access).

| Operation | Method | Path | Notes |
|-----------|--------|------|-------|
| Create rule | POST | `/api/atlas/admin/audits/rules` | Body: `AtlasRule` JSON. |
| List rules | GET | `/api/atlas/admin/audits/rules` | Returns list of `AtlasRule`. |
| Update rule | PUT | `/api/atlas/admin/audits/rules/{ruleGuid}` | Body: updated `AtlasRule`; `ruleGuid` in path. |
| Delete by guid | DELETE | `/api/atlas/admin/audits/rules/guid/{ruleGuid}` | |
| Delete by guids | DELETE | `/api/atlas/admin/audits/rules` | Body: JSON array of rule GUID strings. |
| Delete all rules | DELETE | `/api/atlas/admin/audits/rules/all` | |

##### Create rule (curl)

<SyntaxHighlighter wrapLines={true} language="bash" style={theme.dark}>
{`curl -u admin:admin -X POST \\
  -H "Content-Type: application/json" \\
  -d @rule.json \\
  http://localhost:21000/api/atlas/admin/audits/rules`}
</SyntaxHighlighter>

##### List rules (curl)

<SyntaxHighlighter wrapLines={true} language="bash" style={theme.dark}>
{`curl -u admin:admin -X GET \\
  -H "Content-Type: application/json" \\
  http://localhost:21000/api/atlas/admin/audits/rules`}
</SyntaxHighlighter>

Common error conditions include duplicate `ruleName`, duplicate rule expression, unknown or invalid type or attribute names, invalid operators on `operationType`, and calls made while `atlas.entity.audit.filter.enabled` is `false`.

#### AtlasClientV2

When entity audit filters are enabled on the server, the Java client exposes:

- `createRule(AtlasRule)`
- `getAllRules()`
- `updateRule(String ruleGuid, AtlasRule updatedRule)`
- `deleteRuleByGuid(String guid)`
- `deleteRulesByGuid(List<String> guidList)`
- `deleteAllRules()`
