---
description: Understand, trace, debug, and review Atlas authorization — PolicyRefresher, Persona/Purpose/Connection/Collection lifecycles, Ranger + ABAC engines, policy anatomy, and access denied issues
allowed-tools: [Bash, Read, Grep, Glob, Task, Agent, mcp__grafana-clickhouse__get_schema_context, mcp__grafana-clickhouse__run_sql_query]
argument-hint: "[explain | trace <user> <entity-qn> <action> | debug <scenario> | logs <query> [tenant=X] | review [branch|PR]]"
---

# Atlas Authorization Guide

This skill covers four modes depending on `$ARGUMENTS`:

| Mode | Trigger | Purpose |
|------|---------|---------|
| `explain` | `explain` or no argument | Full architecture walkthrough |
| `trace` | `trace <user> <qn> <action>` | Trace one auth check end-to-end |
| `debug` | `debug <scenario> [tenant=X]` | Diagnose a 403 — queries live authz logs + code analysis |
| `logs` | `logs <query> [tenant=X]` | Query authz logs directly (DENY patterns, user activity, etc.) |
| `review` | `review [branch/PR]` | Check code changes for authz gaps |

Parse `$ARGUMENTS` to decide which mode to run. If no argument or just "explain", run **Explain mode**. If it starts with "trace", run **Trace mode**. If it starts with "debug", run **Debug mode**. If it starts with "logs", run **Logs mode**. If it starts with "review", run **Review mode**.

---

## REFERENCE: Complete System Map

Internalize this before any mode. Every answer should trace back to one of these layers.

```
HTTP Request
     │
     ▼
AtlasKeycloakAuthenticationProvider        ← Spring Security filter
  • Validates JWT / API key (token introspection, optional caching)
  • Extracts user + groups from UGI or configurable JWT claim
  • Populates Spring SecurityContext
     │
     ▼
REST Layer (webapp/)
  • verifyAccess(...)   → throws AtlasBaseException(403) on deny
  • isAccessAllowed(...) → returns boolean for conditional logic
     │
     ▼
AtlasAuthorizationUtils (dual-engine orchestrator)
  ┌───────────────────────────┐    ┌──────────────────────────────┐
  │   Ranger/ACL Engine        │    │   ABAC Engine                 │
  │   policyServiceName=atlas  │    │   policyServiceName=atlas_abac│
  │   policyServiceName=atlas_ │    │   enabled: atlas.authorizer   │
  │   tag (for Purpose)        │    │   .enable.abac=true           │
  │   TRIE structure on        │    │   Iterates all matching       │
  │   policyResources          │    │   policies, evaluates         │
  └──────────────┬─────────────┘    │   policyFilterCriteria JSON   │
                 │                  └──────────────┬────────────────┘
                 └──────────────┬──────────────────┘
                                ▼
                     Priority Merge → AtlasAccessResult
                     { isAllowed, policyId, policyPriority,
                       explicitDeny, enforcer }

     ┌──────────────────────────────────────────────────────┐
     │  In-Memory Policy Cache (per pod, per 30s refresh)    │
     │  Populated by: PolicyRefresher thread                 │
     │  Source of truth: AuthPolicy assets in JanusGraph     │
     │  Transformed to: Ranger-compatible policy format      │
     │  Written to: pod file storage (for investigation)     │
     │                                                       │
     │  Cache tables:                                        │
     │    resourcePolicies  (policyServiceName=atlas)        │
     │    tagPolicies       (policyServiceName=atlas_tag)    │
     │    abacPolicies      (policyServiceName=atlas_abac)   │
     │    userStore         (user→group mappings)            │
     │    rolesStore        (role→sub-roles/users/groups)    │
     └──────────────────────────────────────────────────────┘
```

### Priority Merge Logic (Atlas + ABAC Truth Table)

ABAC is only evaluated for entity permission checks (`isAccessAllowed(AtlasEntityAccessRequest, boolean)`). For type, relationship, and admin checks, only the Atlas/Ranger engine runs.

**Short-circuit:** If Atlas returns DENY at OVERRIDE priority, ABAC is never called — final result is immediately DENY.

For all other cases, both engines run and results are merged per this truth table (derived from `AtlasAuthorizationUtils.isAccessAllowed`):

| # | Atlas Result | Atlas Priority | ABAC Result | ABAC Priority | Final | Winner |
|---|---|---|---|---|---|---|
| 1 | DENY | **OVERRIDE** | *(not evaluated)* | — | **DENY** | Atlas |
| 2 | DENY (explicit) | NORMAL | DENY or ALLOW | **OVERRIDE** | **ABAC result** | ABAC |
| 3 | DENY (explicit) | NORMAL | DENY (explicit) | NORMAL | **DENY** | Atlas |
| 4 | DENY (explicit) | NORMAL | DENY (implicit) | NORMAL | **DENY** | Atlas |
| 5 | DENY (explicit) | NORMAL | ALLOW | NORMAL | **DENY** | Atlas |
| 6 | DENY (implicit) | NORMAL | ALLOW | any | **ALLOW** | ABAC |
| 7 | DENY (implicit) | NORMAL | DENY | any | **DENY** | ABAC |
| 8 | ALLOW | **OVERRIDE** | DENY | **OVERRIDE** | **DENY** | ABAC |
| 9 | ALLOW | **OVERRIDE** | ALLOW | OVERRIDE | **ALLOW** | Atlas |
| 10 | ALLOW | **OVERRIDE** | DENY | NORMAL | **ALLOW** | Atlas |
| 11 | ALLOW | **OVERRIDE** | ALLOW | NORMAL | **ALLOW** | Atlas |
| 12 | ALLOW | NORMAL | DENY (explicit) | any | **DENY** | ABAC |
| 13 | ALLOW | NORMAL | ALLOW | **OVERRIDE** | **ALLOW** | ABAC |
| 14 | ALLOW | NORMAL | ALLOW | NORMAL | **ALLOW** | Atlas |
| 15 | ALLOW | NORMAL | DENY (implicit) | any | **ALLOW** | Atlas |

Key rules to internalize:
- Atlas OVERRIDE DENY is the highest possible priority — nothing overrides it (row 1)
- ABAC OVERRIDE (either direction) can rescue from Atlas explicit DENY (row 2)
- Atlas explicit DENY beats ABAC normal ALLOW (rows 3-5)
- Atlas implicit DENY yields completely to ABAC (rows 6-7)
- ABAC OVERRIDE DENY is the only thing that beats Atlas OVERRIDE ALLOW (row 8 vs 9-11)
- ABAC explicit DENY beats Atlas normal ALLOW (row 12)
- ABAC OVERRIDE ALLOW beats Atlas normal ALLOW (row 13)

---

## REFERENCE: PolicyRefresher

`PolicyRefresher` is a background thread running in every Atlas pod. It is the single source of truth for what policies are currently active in memory. Understanding it is essential for any cache/timing 403 bug.

### Refresh cycle

- Runs every **30 seconds** (fixed interval, not configurable at runtime)
- On each tick it decides whether to refresh. The decision logic:
  1. **First-ever load**: Skip the event check, load everything via `CachePolicyTransformerImpl.getPoliciesAll()`
  2. **Subsequent loads**: Call GET `download/policies/{serviceName}` passing `lastSuccessfulRefreshTimestamp` — the server checks if any `AuthPolicy` audit event exists since that timestamp; if none, skip refresh

### First-load behavior & startup window

- Very first attempts during server startup usually fail because the indexsearch beans are not yet initialized
- Once the server is fully up, the next 30-second tick performs a full load
- **There is a brief window** after the server becomes HTTP-ready but before the first successful cache load where all requests get implicit DENY — this is by design; it closes within one refresh interval

### Delta-based refresh (policy cache)

- Controlled by `atlas.authorizer.enable.delta_based_refresh` (default `false`, but enabled for all tenants in production)
- When enabled: only policies that changed since last refresh are replaced in the cache — not a full reload
- When disabled: any single AuthPolicy audit event triggers a full reload of all policies

### Roles & groups cache refresh (different behavior)

- PolicyRefresher also refreshes `userStore` (user→group) and `rolesStore` (role hierarchy)
- Checks for Keycloak admin events via direct API calls to the Keycloak service (not audit events)
- **If any event found → full reload** (no delta support here)
- The full reload reads from **Heracles service**, which exposes a Postgres view over Keycloak's database for faster reads
- This means a user added to a group in Keycloak takes up to 30 seconds + Heracles sync time to take effect in Atlas

### Policy transformation & file output

- Policies are transformed using Jinja-like templates per entity type:
  - Persona: `addons/static/templates/policy_cache_transformer_persona.json`
  - Purpose: `addons/static/templates/policy_cache_transformer_purpose.json`
- Template keys map custom Persona/Purpose `policyActions` to Ranger resource/tag action names
- Transformed policies are written to pod file storage (useful for investigating 403s without live debugging)

### Heka service

- Heka also uses Atlas as source of truth for data access and masking policies
- It calls the same GET APIs (`download/policies/heka`, `download/users/heka`, `download/roles/heka`)
- Heka has its own in-memory cache, its own refresh thread, and its own authorization engine for `select` + data masking
- Policies destined for Heka have `policyServiceName = "heka"`
- All data authorization and column masking logic lives in Heka, not Atlas

### Atlas service definition

The file `addons/policies/atlas_service.json` defines all supported AuthService assets. Each entry specifies:
- The tag service name and ABAC service name for the service
- This is what ties `policyServiceName` values to the correct filtering path in PolicyRefresher
- Supported service names are enumerated here — adding a new service requires a new entry in this file

---

## REFERENCE: AuthPolicy Anatomy

Every authorization decision ultimately traces to an `AuthPolicy` asset. These are the attributes that matter:

### policyType
Controls the effect of the policy.
- `allow` — grants access
- `deny` — explicitly denies (can override allow at same priority)
- `dataMask` — column masking (Purpose data policies only, used by Heka)
- `allowExceptions`, `denyExceptions`, `rowFilter` — exist but not actively supported

### policyServiceName
Routes the policy to the correct cache and engine.
- `atlas` — Ranger engine, resource-based (entity QN / type)
- `atlas_tag` — Ranger engine, tag-based (Purpose metadata/data)
- `atlas_abac` — ABAC engine
- `heka` — Heka's own authorization engine (data select / masking)

### policyCategory + policySubCategory
Describes who created the policy and what kind of access it governs.

| policyCategory | policySubCategory | Description |
|---------------|------------------|-------------|
| `persona` | `metadata` | QN/typeName access to assets under a specific Connection |
| `persona` | `data` | Data select access to assets under a specific Connection |
| `persona` | `glossary` | QN access to Glossary, Categories, Terms |
| `persona` | `ai` | Access to AI application/model assets |
| `purpose` | `metadata` | Tag-based metadata access (direct + propagated tags) |
| `purpose` | `data` | Tag-based data select + masking |
| `datamesh` | `dataProduct` | QN/typeName access to assets under a DataProduct (ENTITY_READ only) |
| `bootstrap` | *(various)* | System-seeded or Connection/Collection auto-created policies |

For exact valid subcategory values per category, read `AuthPolicyValidator.java`.

### policyActions
The allowed or denied actions. Must match exactly (string, not enum name).

**Persona metadata actions:**
`persona-asset-read`, `persona-asset-update`, `persona-api-create`, `persona-api-delete`, `persona-business-update-metadata`, `persona-entity-add-classification`, `persona-entity-update-classification`, `persona-entity-remove-classification`, `persona-add-terms`, `persona-remove-terms`, `persona-dq-update`, `persona-dq-read`, `persona-dq-create`, `persona-dq-delete`

**Persona glossary actions:**
`persona-glossary-read`, `persona-glossary-update`, `persona-glossary-create`, `persona-glossary-delete`, `persona-glossary-update-custom-metadata`, `persona-glossary-add-classifications`, `persona-glossary-update-classifications`, `persona-glossary-delete-classifications`

**Persona AI actions:**
`persona-ai-application-read`, `persona-ai-application-create`, `persona-ai-application-update`, `persona-ai-application-delete`, `persona-ai-application-business-update-metadata`, `persona-ai-application-add-terms`, `persona-ai-application-remove-terms`, `persona-ai-application-add-classification`, `persona-ai-application-remove-classification`, `persona-ai-model-read`, `persona-ai-model-create`, `persona-ai-model-update`, `persona-ai-model-delete`, `persona-ai-model-business-update-metadata`, `persona-ai-model-add-terms`, `persona-ai-model-remove-terms`, `persona-ai-model-add-classification`, `persona-ai-model-remove-classification`

**Data policy actions (Persona data + Purpose data):**
`select`

**Purpose metadata actions:** Same as persona metadata actions but evaluated against tag-bearing assets.

For authoritative valid values per category+subcategory combination, read `AuthPolicyValidator.java`.

### policyResources
Specifies which assets the policy applies to. Format follows Ranger resource conventions (check `auth-common/src/main/resources/service-defs` for the full Ranger resource definitions).

```
entity:<qualifiedName>              exact asset match
entity:<qualifiedName>/*            all children (wildcard)
entity-type:<typeName>              all assets of a type
entity-classification:<tagName>     assets with this tag (Purpose)
tag:<tagName>                       Purpose tag resource
```

Supports `*` (any chars) and `?` (single char) wildcards. Full regex is NOT supported.

### policyUsers / policyGroups / policyRoles
- `policyUsers`: Keycloak usernames directly
- `policyGroups`: Keycloak group names (all members implicitly included)
- `policyRoles`: Keycloak role IDs (not names) — all users/groups/sub-roles under the role are implicitly included
- For Persona policies: `policyRoles` always contains `"persona_<persona_guid>"` — users/groups are never listed directly in policies, they're managed via Keycloak role membership

### policyPriority
- `0` = NORMAL priority
- `1` = OVERRIDE priority
- OVERRIDE ALLOW can unblock a NORMAL explicit DENY
- OVERRIDE DENY beats everything including OVERRIDE ALLOW from the other engine

### policyResourceCategory
- `ENTITY` — for Persona (resource = entity QN)
- `TAG` — for Purpose (resource = tag)
- `CUSTOM` — for custom resource definitions

### isPolicyEnabled
Boolean. When `false`, PolicyRefresher skips this policy entirely during cache load. This is also how disabling a Persona or Purpose is implemented — all their policies get `isPolicyEnabled = false`.

### policyFilterCriteria
ABAC policies only. A JSON tree defining attribute-based filtering. The `entity` key contains the filter expression:

```json
{
  "entity": {
    "condition": "AND",
    "criterion": [
      {
        "attributeName": "connectionQualifiedName",
        "attributeValue": "default/snowflake/1732534495",
        "operator": "EQUALS"
      },
      {
        "condition": "OR",
        "criterion": [
          {
            "attributeName": "schemaQualifiedName",
            "attributeValue": ["prefix123"],
            "operator": "STARTS_WITH"
          }
        ]
      }
    ]
  },
  "filterSchema": { ... },   ← UI representation only, not used by engine
  "config": { "query": { "dsl": { ... } } }  ← ES DSL for search filtering
}
```

Supported operators: `EQUALS`, `NOT_EQUALS`, `IN`, `NOT_IN`, `STARTS_WITH`, `ENDS_WITH`

Special `attributeName` values:
- `__typeName` — matches entity type and all its supertypes
- `__traitNames` — direct classifications on the entity
- `__propagatedTraitNames` — classifications propagated from related entities

Only attributes that are primitive and have mixed index enabled (i.e., synced to ES) can be used. The `config.query.dsl` portion is the precomputed ES query used for search result filtering.

### policyMaskType
Purpose data policies only. Controls how column data is masked in Heka:
- `MASK_REDACT` — replace with fixed string
- `MASK_SHOW_LAST_4` — show only last 4 characters
- `MASK_SHOW_FIRST_4` — show only first 4 characters
- `MASK_HASH` — hash the value
- `MASK_NULL` — return null
- `MASK_NONE` — no masking (pass-through)

### policyConditions
Additional business logic conditions beyond `policyResources`, used to further filter assets. Not commonly used today.

---

## REFERENCE: Entity Lifecycles

### Persona

**On creation:**
1. Atlas creates the Persona entity in JanusGraph
2. A Keycloak role named `persona_<persona_guid>` is created
3. Users from `personaUsers` and groups from `personaGroups` are assigned to the Keycloak role
4. An ES alias is created with name = the unique string portion of the Persona's QN
   - Persona QN pattern: `default/<unique_generated_string>`
   - ES alias name = `<unique_generated_string>`
5. The alias is initialized with an empty filter (no policies yet)

**On policy create/update/delete under the Persona:**
- The ES alias is recalculated from scratch using all active policies' `policyResources`
- `policyResources` values contribute to the alias filter so searches are scoped to permitted assets

**On user/group update (`personaUsers` / `personaGroups` change):**
- Only Keycloak role membership is updated (add/remove users/groups to `persona_<guid>` role)
- **No policy update needed** — policies already reference `policyRoles: ["persona_<guid>"]`

**On deletion:**
1. Keycloak role `persona_<guid>` is deleted
2. ES alias is deleted
3. All child AuthPolicy entities are deleted

**Policy structure under Persona:**
- `policyCategory = "persona"`
- `policyRoles = ["persona_<guid>"]` always — never lists users/groups directly in the policy
- `policyResources` uses `entity:` prefix
- Policy QN pattern: `default/<persona_unique_str>/<policy_unique_str>`
- Policy connected to Persona via `relationshipAttributes.accessControl`

**Cache transformation template:** `addons/static/templates/policy_cache_transformer_persona.json`

### Purpose

**On creation:**
1. Atlas creates the Purpose entity in JanusGraph
2. **No Keycloak role is created** (unlike Persona)
3. An ES alias is created with name = the unique string portion of the Purpose's QN
   - Purpose QN pattern: `default/<unique_generated_string>`
   - ES alias name = `<unique_generated_string>`

**On policy create/update/delete under the Purpose:**
- ES alias recalculated from all active policies' `policyResources` (tag-based)

**On user/group update:**
- Updated directly on each AuthPolicy (`policyUsers`, `policyGroups` attributes)
- No Keycloak role intermediary

**On deletion:**
1. ES alias is deleted
2. All child AuthPolicy entities are deleted
3. No Keycloak role to clean up

**Policy structure under Purpose:**
- `policyCategory = "purpose"`
- `policyUsers` and `policyGroups` set directly on each policy (no role indirection)
- `policyResources` uses `tag:` prefix
- `policyResourceCategory = "TAG"`
- Policy QN pattern: `default/<purpose_unique_str>/<policy_unique_str>`

**Cache transformation template:** `addons/static/templates/policy_cache_transformer_purpose.json`

### Connection

**On creation:**
1. A Keycloak role named `connection_admins_<connection_guid>` is created
2. **8 bootstrap policies** are auto-created from template: `addons/static/templates/connection_bootstrap_policies.json`
   - Template placeholders like `{guid}` are replaced with the actual Connection GUID and QN

**"All Admins" feature:**

A Connection can be made accessible to all `$admin` role users without listing them individually in a policy. This works through Keycloak role nesting + role inversion:

1. The client sends the `$admin` role's ID in the Connection asset's `adminRoles` attribute
2. Atlas creates a role mapping making `connection_admins_<guid>` a **child role** of `$admin` in Keycloak
   - In Keycloak: `$admin` → composite roles: `[..., connection_admins_<guid>]`
3. At cache refresh time, `KeycloakUserStore.loadRolesIfUpdated()` reads all role mappings from Heracles and calls `invertRoles()`
4. **`invertRoles()` flips the direction**: instead of "parent has child", the cache stores "child has parent as a member role"
   - After inversion: `connection_admins_<guid>` has `$admin` in its `roles[]` list
5. The bootstrap connection policies have `policyRoles = ["connection_admins_<guid>"]`
6. When the authorization engine resolves roles for a `$admin` user, it traverses the inverted graph: user → `$admin` role → `connection_admins_<guid>` → policy match → ALLOW

**Why inversion?** Ranger's authorization model evaluates access bottom-up: "does this policy's role transitively contain this user?" The inversion pre-computes the role containment relationship so the TRIE lookup can find it efficiently. Without inversion, you'd need to walk the entire role hierarchy top-down for every request.

### Collection

**On creation:**
1. Two Keycloak roles are created:
   - `collection_admins_<collection_guid>`
   - `collection_viewer_<collection_guid>`
2. Bootstrap policies are auto-created from: `addons/static/templates/collection_bootstrap_policies.json`

### Bootstrap / System Policies

Seeded when Atlas starts with `atlas.authorizer.impl=atlas`. These are common across all tenants:

| File | Contents |
|------|----------|
| `addons/policies/bootstrap_entity_policies.json` | Default entity access policies |
| `addons/policies/bootstrap_relationship_policies.json` | Relationship permission policies |
| `addons/policies/bootstrap_typedef_policies.json` | TypeDef CRUD policies |
| `addons/policies/bootstrap_heka_policies.json` | Heka data access baseline policies |

---

## REFERENCE: User Types

Every user in the system falls into one of two categories based on their Keycloak role membership. This determines their baseline access before any Persona or Purpose policies are evaluated.

### Member Users

- Users who are members of the **`$admin`** role in Keycloak
- Granted a meaningful set of default permissions via the bootstrap entity, relationship, and typedef policies (`addons/policies/bootstrap_entity_policies.json`, etc.)
- Can read and write metadata, create assets, manage relationships — the full metadata platform experience
- Additional permissions can be granted through:
  1. **Persona** — scoped access to specific connections/glossaries/domains via `personaUsers` / `personaGroups` assignments
  2. **Purpose** — tag-based access to assets bearing specific classifications
  3. **Connection All Admins route** — `$admin` role mapped as parent of `connection_admins_<guid>` (see Connection lifecycle section)

### Guest Users

- Users who are members of the **`$guest`** role in Keycloak
- Only have **READ** permissions via bootstrap policies — write, create, delete, and classification operations are explicitly denied
- The guest deny policies are typically higher-priority than any persona allow, meaning a guest user cannot be elevated to write access via a Persona alone
- Intended for read-only consumers of the metadata catalog who should never mutate assets

### Why this matters for debugging

When investigating a 403 or unexpected deny:
- **First check**: Is the user a `$admin` member or `$guest` member?
  - Guest + write operation = expected deny, not a bug
  - Member + write operation = investigate Persona/ABAC/policy cache
- The `$guest` explicit deny policies at bootstrap level will appear in authz logs as `isExplicitDeny=true, engine=atlas` — don't confuse this with a misconfigured Persona policy
- Adding a guest user to a Persona granting write permissions will NOT work — the bootstrap `$guest` deny wins over a normal-priority Persona allow (truth table row 3/4: Atlas explicit DENY at normal priority beats ABAC/Persona normal ALLOW)

### Connection between user type and invertRoles

The same `invertRoles()` mechanism that handles Connection All Admins applies here:
- `$admin` → users under `$admin` role are treated as members
- `$guest` → users under `$guest` role get the guest bootstrap deny policies applied
- After inversion, the authorization engine can resolve "is this user under `$admin`?" or "is this user under `$guest`?" by traversing the inverted role graph

---

## REFERENCE: API Keys

API keys are Keycloak **Clients** (not users) with a service account. Naming convention: `apikey-<generated_guid>`.

### How API key authorization works

1. Every API key client has a service account
2. On creation, `$api-token-default-access` role is added to the client's "Service Account Roles"
3. `$api-token-default-access` is referenced in all bootstrap policies (`addons/policies/bootstrap_entity_policies.json`, `bootstrap_relationship_policies.json`, etc.)
4. This means every API key automatically gets the same baseline access as bootstrap policies grant

### Linking a Persona to an API key

To give an API key elevated access via a Persona:
1. The `persona_<persona_guid>` Keycloak role is added to the API key client's "Service Account Roles" in Keycloak
2. This can be done from the Persona management UI
3. The same role-inversion mechanism (via `invertRoles`) means the API key's service account user is treated as a member of `persona_<persona_guid>`
4. All persona policies (which reference `policyRoles: ["persona_<guid>"]`) then apply to that API key

### Key difference from regular users

- Regular users: grouped via Keycloak groups → roles via group memberships
- API keys: have no groups; roles are assigned directly as "Service Account Roles" on the client
- The same `UsersStore` role resolution path handles both

---

## REFERENCE: The Two Authorization Engines

### 1. Ranger / ACL Engine

- Active when `atlas.authorizer.impl=atlas`
- Handles: entity access, relationship access, typedef access
- Builds a **TRIE** structure from `policyResources` for fast prefix/wildcard matching
- Supports relationship resources with `end1` and `end2` matchers (QN, type, or tag)
- `policyFilterCriteria` is NOT evaluated here — that's ABAC only

### 2. ABAC Engine

- Active when `atlas.authorizer.enable.abac=true` (on top of Ranger being enabled)
- Policies always have `policyServiceName = "atlas_abac"`
- Instead of a TRIE, evaluates each matching policy's `policyFilterCriteria` against the entity
- Evaluation order: DENY policies first (fail-fast), then ALLOW policies
- **Performance note**: ABAC iterates all matching policies and evaluates criteria for each — can be slower at scale than Ranger's TRIE but offers attribute-level granularity
- Managed by `ABACAuthorizerUtils`

---

## REFERENCE: Key Source Files

| Concern | File / URL |
|---------|------|
| **Authz evaluation logs (Grafana)** | `https://observability.atlan.com/d/decuqe5bel0jkd/atlas-authz-logs?orgId=1` |
| Dual-engine orchestrator | `repository/.../authorizer/AtlasAuthorizationUtils.java` |
| ABAC entity evaluation | `repository/.../authorizer/authorizers/EntityAuthorizer.java` |
| ABAC list/search filtering | `repository/.../authorizer/authorizers/ListAuthorizer.java` |
| Relationship auth | `repository/.../authorizer/authorizers/RelationshipAuthorizer.java` |
| Common matchers & user lookup | `repository/.../authorizer/authorizers/AuthorizerCommonUtil.java` |
| ABAC orchestration | `repository/.../authorizer/ABACAuthorizerUtils.java` |
| In-memory policy cache | `repository/.../authorizer/store/PoliciesStore.java` |
| User/group/role cache | `repository/.../authorizer/store/UsersStore.java` |
| Policy lifecycle (preprocessor) | `repository/.../preprocessor/AuthPolicyPreProcessor.java` |
| Policy validation | `repository/.../preprocessor/AuthPolicyValidator.java` |
| Access control constants | `repository/.../repository/util/AccessControlUtils.java` |
| ES filter conversion (ABAC→DSL) | `repository/.../authorizer/JsonToElasticsearchQuery.java` |
| Ranger policy model | `auth-common/.../plugin/model/RangerPolicy.java` |
| Authorizer interface | `authorization/.../authorize/AtlasAuthorizer.java` |
| Authorizer factory + impl selection | `authorization/.../authorize/AtlasAuthorizerFactory.java` |
| Privileges enum | `authorization/.../authorize/AtlasPrivilege.java` |
| Keycloak auth provider | `webapp/.../web/security/AtlasKeycloakAuthenticationProvider.java` |
| Keycloak user store | `repository/.../repository/store/users/KeycloakStore.java` |
| Atlas service definitions | `addons/policies/atlas_service.json` |
| Persona cache template | `addons/static/templates/policy_cache_transformer_persona.json` |
| Purpose cache template | `addons/static/templates/policy_cache_transformer_purpose.json` |
| Connection bootstrap policies | `addons/static/templates/connection_bootstrap_policies.json` |
| Collection bootstrap policies | `addons/static/templates/collection_bootstrap_policies.json` |

---

## MODE: EXPLAIN

Show the full REFERENCE sections above in a readable narrative form, then walk through each subsystem in order:

1. **Authentication flow** — JWT/API key → Keycloak → SecurityContext → RequestContext
2. **PolicyRefresher lifecycle** — startup window, 30s tick, delta vs full reload, roles/groups via Heracles
3. **AuthPolicy anatomy** — walk through each attribute with examples
4. **Persona vs Purpose** — creation, policy attachment, ES alias, deletion, why Persona uses a role indirection and Purpose doesn't
5. **Connection & Collection** — auto-created Keycloak roles and bootstrap policies
6. **Ranger engine** — TRIE structure, bootstrap policies, relationship resources
7. **ABAC engine** — policyFilterCriteria evaluation, operator semantics, performance trade-offs
8. **Priority merge** — step through the precedence table with concrete examples
9. **Heka** — what it takes from Atlas, what it owns independently

---

## MODE: TRACE

**Arguments parsed:** `trace <username> <entity-qualified-name> <action>`

Example: `trace alice default/snowflake/1732534495/schema1/table1 persona-asset-read`

### Phase 1: Gather context (parallel)

Launch two agents simultaneously:

**Agent 1 — User context:**
Read `UsersStore.java`. Explain how `getRolesForUser()` resolves roles — direct, group-based, and nested (transitive) role memberships. For the given username, identify:
- What groups would they likely be in (based on Keycloak naming conventions)
- What Persona roles (`persona_<guid>`) they might be assigned to
- The nested role resolution path via `getNestedRolesForUser()`

**Agent 2 — Policy routing:**
Read `PoliciesStore.java`. For the given entity QN and action, determine:
- Which `policyServiceName` values would be queried (atlas, atlas_tag, atlas_abac)?
- What prefix/wildcard matching would happen against `policyResources`?
- What action string must appear in `policyActions`?
- Would this be a Persona (entity: prefix) or Purpose (tag: prefix) policy match?

### Phase 2: Trace the call stack

Read `AtlasAuthorizationUtils.java`. Narrate the exact decision path step by step:

1. Which `verifyAccess()` / `isAccessAllowed()` overload is called for this action?
2. What `AtlasPrivilege` corresponds to the action string?
3. Ranger engine check: resource match → action match → subject (user/group/role) match
4. Is ABAC enabled? If so: policy fetch → `policyFilterCriteria` evaluation per matching policy
5. Apply the priority merge. State the final `AtlasAccessResult`.

### Phase 3: Output

```
## Authorization Trace: <user> → <entity-qn> → <action>

### User Context
- Groups: [...]
- Roles: [...] (including transitively resolved)

### Atlas/Ranger Engine
- Service queried: atlas | atlas_tag
- Resource pattern matched: <pattern> or "none"
- Action matched: yes/no
- Subject matched: yes/no
- Result: ALLOW/DENY (NORMAL/OVERRIDE, explicit/implicit)
- Winning policy ID: <id> or "none"

### ABAC Engine (if enabled)
- Policies evaluated: N
- Filter criteria result: <summary of criterion evaluation>
- Result: ALLOW/DENY

### PolicyRefresher State (potential timing issues)
- Is this within the startup window? Could cache be empty?
- Has the policy been created long enough ago for the next 30s tick?

### Final Decision
- Enforcer: atlas | abac_auth | merged_auth
- Allowed: true/false
- Winning policy: <policyId>
- Reason: <plain English explanation>

### Recommendation
If denied: what policy change would grant access, and how long until the next cache refresh picks it up
If allowed: which policy is responsible and why
```

---

## MODE: DEBUG

**Argument:** `debug <description of access denied scenario>`

Parse the description to extract: user, entity type/QN, operation, and error observed (403, empty results, etc.).

### Phase 0: Query the authz logs (do this first — often solves the case immediately)

Extract `tenant` from the argument (e.g., `debug alice gets 403 on table X tenant=acme` → tenant=`acme`). If not provided, ask before proceeding — tenant is required.

Also extract any identifiers mentioned: username, entity QN fragment, time window.

**Step 0a: Discover the authz log schema**

Call `mcp__grafana-clickhouse__get_schema_context` with `use_case="atlas_authz_logs"` to get the current table name, columns, and example queries. If that use_case isn't recognized, try `use_case="atlas_application_logs"` — authz logs may be in `otel_logs.service_logs` filtered by class or logtype.

**Step 0b: Run these queries in sequence — stop as soon as you have enough signal**

**Query 1 — Recent DENY events for the user (last 2 hours):**
Adapt column names from the schema discovered above. Target columns to extract:
- timestamp, user/subject, entity qualified name, action, result (allow/deny), engine used (atlas/abac/merged), policy ID matched, whether it was explicit deny

```sql
-- Pseudocode — adapt to real schema from Step 0a
SELECT timestamp, user, entityQN, action, result, engine, policyId, isExplicitDeny
FROM <authz_logs_table>
WHERE TenantName = '{tenant}'
  AND user = '{username}'        -- if known
  AND result = 'deny'
  AND timestamp >= now() - INTERVAL 2 HOUR
ORDER BY timestamp DESC
LIMIT 50
```

**Query 2 — DENY events for the specific entity (if QN known):**
```sql
SELECT timestamp, user, action, result, engine, policyId, isExplicitDeny
FROM <authz_logs_table>
WHERE TenantName = '{tenant}'
  AND entityQN LIKE '%{qn_fragment}%'
  AND timestamp >= now() - INTERVAL 2 HOUR
ORDER BY timestamp DESC
LIMIT 50
```

**Query 3 — Was this user ever ALLOWED? (confirms whether it's a regression):**
```sql
SELECT result, count() as cnt, min(timestamp) as first_seen, max(timestamp) as last_seen
FROM <authz_logs_table>
WHERE TenantName = '{tenant}'
  AND user = '{username}'
  AND entityQN LIKE '%{qn_fragment}%'
  AND timestamp >= now() - INTERVAL 24 HOUR
GROUP BY result
ORDER BY last_seen DESC
```

**Query 4 — If policy ID is in the logs, check for OVERRIDE deny from other policies:**
```sql
SELECT policyId, policyPriority, isExplicitDeny, count() as cnt
FROM <authz_logs_table>
WHERE TenantName = '{tenant}'
  AND user = '{username}'
  AND timestamp >= now() - INTERVAL 2 HOUR
GROUP BY policyId, policyPriority, isExplicitDeny
ORDER BY cnt DESC
LIMIT 20
```

**Step 0c: Interpret the log results against the truth table**

Once you have log data, map the observed `engine`, `result`, `policyPriority`, and `isExplicitDeny` values to the ABAC truth table rows in the REFERENCE section to explain exactly why the decision was made.

Key interpretations:
- `result=deny, engine=atlas, isExplicitDeny=false` → implicit deny → no matching Atlas policy at all → check policyResources pattern, isPolicyEnabled, policyServiceName
- `result=deny, engine=atlas, isExplicitDeny=true, policyPriority=0` → explicit DENY policy matched → find and check that policy
- `result=deny, engine=merged_auth` → conflict between Atlas and ABAC — look at both policyId values to find which row of the truth table applies
- `result=deny, engine=abac_auth` → ABAC DENY override — look for `policyPriority=1` ABAC deny policies
- Timestamps show the 403 started at a specific time → check for policy changes or role changes around that time

**If no logs appear for the time window:** Either the tenant name is wrong, the issue is older than the retention window, or the issue is in Heka (data/select access) — Heka has no authz logs in this system.

### Phase 1: Parallel investigation

Launch three agents in parallel:

**Agent 1 — Call site:**
Find the REST endpoint or service method handling this operation. Identify the `verifyAccess()` call and the `AtlasPrivilege` used:
```bash
grep -r "verifyAccess\|isAccessAllowed" webapp/src/ repository/src/ --include="*.java" -l
```

**Agent 2 — Policy cache pipeline:**
Read `PoliciesStore.java` fully. Understand:
- How policies are loaded and filtered on each refresh tick
- How `policyServiceName` is used to bucket policies into `resourcePolicies`, `tagPolicies`, `abacPolicies`
- What conditions would silently drop a policy from the cache (disabled, wrong service, malformed resource)

**Agent 3 — Roles/groups resolution:**
Read `UsersStore.java`. Trace `getRolesForUser()` → `getNestedRolesForUser()`. Identify:
- Is the user in the expected Keycloak group?
- Is the Keycloak role (`persona_<guid>`) properly nested/assigned?
- Could a Heracles sync delay mean the role assignment hasn't propagated yet?

### Phase 2: Root causes checklist

Systematically evaluate each category:

#### Category A: Cache not loaded / stale

**A1: Server startup window**
If the request came in shortly after server restart, the PolicyRefresher may not have completed its first successful load. All requests during this window get implicit DENY.
- Evidence: Errors disappear after ~30s, or coincide with a deploy event
- Fix: Wait for first successful cache load; consider adding readiness probe that checks cache state

**A2: Delta refresh missing a policy change**
With `atlas.authorizer.enable.delta_based_refresh=true`, if the audit event for an AuthPolicy mutation was missed or the timestamp check has a bug, a policy change may not be reflected in cache.
- Evidence: Policy exists in JanusGraph but behavior hasn't changed after >30s
- Fix: Check pod file storage for the last written cache file; verify the audit event was recorded

**A3: Roles/groups cache lag**
Keycloak admin event check → Heracles read → UsersStore update. If Heracles is slow or the admin event is delayed, a newly added user→group→role chain won't be in cache.
- Evidence: User was recently added to a group or Persona; issue resolves after 30-60s
- Fix: Verify Heracles service health; check Keycloak admin events API response

#### Category B: User type mismatch (check this before any policy investigation)

**B0: Guest user attempting a write operation**
If the user is a `$guest` role member, write/create/delete/classification operations are blocked by bootstrap deny policies — this is correct behavior, not a bug. The authz logs will show `isExplicitDeny=true, engine=atlas` from a bootstrap policy.

Symptoms: 403 on any mutation. User is read-only by design.

Resolution: If write access is intentional, the user must be moved from `$guest` to `$admin` in Keycloak — adding them to a Persona will not help because the `$guest` explicit deny at normal priority beats a normal Persona allow (truth table rows 3-5).

**B0b: Guest user added to a Persona expecting elevated access**
This is a common misconception. A Persona policy is a normal-priority allow. A `$guest` bootstrap deny is a normal-priority explicit deny. Per truth table row 3-5: Atlas explicit DENY at normal priority beats Atlas normal ALLOW. The Persona has no effect for guest users on write operations.

#### Category C: Policy configuration issues

**B1: isPolicyEnabled = false**
The Persona or Purpose is disabled, or the policy was explicitly disabled. PolicyRefresher skips all policies where `isPolicyEnabled = false`.

**B2: Wrong policyServiceName**
If the policy has `policyServiceName = "atlas"` but the check queries for `"atlas_tag"` (or vice versa), the policy is invisible to that engine. Purpose metadata policies should use `"atlas_tag"` or `"atlas_abac"`, not `"atlas"`.

**B3: policyResources pattern mismatch**
Common mistakes:
- Missing `/*` for subtree: `entity:default/snowflake/123` won't match `entity:default/snowflake/123/schema/table`
- Using GUID directly instead of qualifiedName
- Case sensitivity mismatch
- Extra or missing slashes
- Missing `entity:` or `tag:` prefix entirely

**B4: Action not in policyActions**
The action string in the policy must exactly match what the engine checks. Confusion between:
- Custom action names (`persona-asset-read`) and AtlasPrivilege names (`ENTITY_READ`) — they're mapped via the transformer template
- Missing a related action (e.g., read granted but `entity-read-classification` not included)

**B5: User/group/role not matched**
- User not in `policyUsers` (Purpose policies)
- User's group not in `policyGroups`
- User not assigned to Keycloak role `persona_<guid>` (Persona policies) — check Keycloak, not the policy itself
- Role assignment exists in Keycloak but UsersStore hasn't synced yet

**B6: policyCategory / policySubCategory mismatch**
A persona metadata policy won't authorize glossary actions — the subcategory must match the type of operation. Check `AuthPolicyValidator.java` for which actions are valid per category+subcategory.

#### Category C: ABAC interference

**C1: ABAC DENY overriding Ranger ALLOW**
An ABAC deny policy with a filter criterion that matches the entity is blocking an Atlas allow. Check for `policyType = "deny"` ABAC policies in the user's accessible policy set.

**C2: policyFilterCriteria evaluated to false**
The ABAC allow policy exists and is in cache, but its filter criteria don't match the entity's current attribute values. Common cause: the entity attribute used in the criterion isn't indexed (not a mixed index field), so `EntityAuthorizer` gets null for that attribute and the criterion fails.

**C3: OVERRIDE priority DENY elsewhere**
A policy with `policyPriority = 1` (OVERRIDE) and `policyType = "deny"` in any service is blocking the request. Check pod cache file for override deny policies touching the user's roles.

#### Category D: API key specific issues

If the requester is an API key (not a human user):

**D1: $api-token-default-access role missing from service account**
All API keys should have `$api-token-default-access` in their Keycloak client's "Service Account Roles". If this role was not added at creation time, the API key won't match any bootstrap policies.

**D2: Persona not linked to API key**
For elevated access, the API key's client needs `persona_<guid>` in its "Service Account Roles". Unlike users (who get this via group membership), API keys have no groups — the role must be assigned directly. Check the API key client in Keycloak for its service account roles.

**D3: invertRoles not picking up API key service account**
After linking a Persona to an API key, the roles/groups cache needs to refresh. The `invertRoles()` processing reads user-role mappings from Heracles. If the service account user isn't being returned in the Heracles user list, the role assignment won't appear in cache.

**D4: Heka access for API keys**
If the API key is trying to run `select` queries, that goes through Heka's authorization engine — not Atlas. Heka has no authz logs. Check that the API key's Persona has a data policy (`policySubCategory = "data"`) and `policyServiceName = "heka"`.

#### Category E: ES alias / search filtering issues

If the problem is "entity not appearing in search results" rather than a 403:
- The ES alias for the user's Persona or Purpose may not include the entity
- Check if the alias was recalculated after the last policy change (alias update is synchronous with policy write, but ES indexing lag can delay visibility)
- Check if the entity's QN matches the alias filter pattern

### Phase 3: Output

```
## Authorization Debug Report

### Scenario
<restate the access denied/missing entity situation>

### Call Site
- REST endpoint: <method + file:line>
- AtlasPrivilege: <ENTITY_READ | ...>
- verifyAccess() location: <file:line>

### Root Cause Analysis

#### Most Likely Cause: <title from checklist>
<explanation with file references and specific attribute values to check>

#### Other Candidates (ranked by likelihood)
- [ ] Checked authz logs at https://observability.atlan.com/d/decuqe5bel0jkd/atlas-authz-logs
- [ ] **User type**: Is the user $guest attempting a write? (expected deny — not a bug)
- [ ] **User type**: Guest user in a Persona expecting write access? (won't work — $guest deny beats Persona allow)
- [ ] Cache not loaded / stale (startup window, delta refresh miss, Heracles lag)
- [ ] isPolicyEnabled = false on policy or parent Persona/Purpose
- [ ] policyServiceName mismatch (atlas vs atlas_tag vs atlas_abac vs heka)
- [ ] policyResources pattern doesn't match entity QN (missing /*, prefix off, wrong format)
- [ ] User not in policyUsers/policyGroups or not assigned to Keycloak role persona_<guid>
- [ ] Action missing from policyActions or using wrong action string
- [ ] policySubCategory doesn't cover this type of operation
- [ ] ABAC DENY policy overriding Ranger ALLOW (see truth table rows 2, 12)
- [ ] policyFilterCriteria evaluates to false (attribute not indexed, wrong value)
- [ ] OVERRIDE priority DENY from another policy (truth table row 1 or 8)
- [ ] ES alias not updated after last policy change
- [ ] API key: $api-token-default-access role missing from service account
- [ ] API key: Persona not linked (persona_<guid> not in service account roles)
- [ ] API key: data access → goes through Heka, no authz logs available there
- [ ] Connection "All Admins": adminRoles set but role inversion not yet refreshed (Heracles lag)

### Fix Recommendation
<concrete steps: which attribute to change on which entity, which Keycloak assignment to make, or which service to check>

### How Long Until Fix Takes Effect
<time estimate based on PolicyRefresher 30s cycle + Heracles sync if roles involved>

### Verification
<specific thing to check to confirm the fix worked — pod cache file path, audit log, test request>
```

---

## MODE: LOGS

**Argument:** `logs <free-text query> [tenant=<name>] [from=<time>] [to=<time>] [user=<username>]`

Examples:
- `/authz logs all denies for user alice last 2 hours tenant=acme`
- `/authz logs which policy is blocking entity default/snowflake/123/table1 tenant=prod`
- `/authz logs ABAC override denies in the last hour tenant=acme`
- `/authz logs did user bob ever have access to this entity tenant=acme from=now-7d`

Tenant is required — ask if not provided.

### Step 1: Discover schema

Call `mcp__grafana-clickhouse__get_schema_context` with `use_case="atlas_authz_logs"`. If not recognized, try `use_case="atlas_application_logs"` and filter by authz-related class names (`AtlasAuthorizationUtils`, `EntityAuthorizer`, `ABACAuthorizerUtils`).

Note the exact table name, column names, and any enum values for result/engine/priority fields before writing queries.

### Step 2: Build and run the query

Translate the user's free-text request into one or more SQL queries. Common patterns:

**All DENY events for a user:**
```sql
SELECT timestamp, entityQN, action, engine, policyId, policyPriority, isExplicitDeny
FROM <authz_table>
WHERE TenantName = '{tenant}'
  AND userName = '{user}'
  AND result = 'deny'
  AND timestamp BETWEEN '{from}' AND '{to}'
ORDER BY timestamp DESC
LIMIT 100
```

**Events for a specific entity (by QN fragment):**
```sql
SELECT timestamp, userName, action, result, engine, policyId
FROM <authz_table>
WHERE TenantName = '{tenant}'
  AND entityQN LIKE '%{qn_fragment}%'
  AND timestamp >= now() - INTERVAL 2 HOUR
ORDER BY timestamp DESC
LIMIT 100
```

**ABAC OVERRIDE denies (policyPriority=1, result=deny, engine includes abac):**
```sql
SELECT timestamp, userName, entityQN, action, policyId
FROM <authz_table>
WHERE TenantName = '{tenant}'
  AND result = 'deny'
  AND engine LIKE '%abac%'
  AND policyPriority = 1
  AND timestamp >= now() - INTERVAL 2 HOUR
ORDER BY timestamp DESC
LIMIT 50
```

**Access history for a user+entity pair (regression check):**
```sql
SELECT
  result,
  engine,
  policyId,
  count() as cnt,
  min(timestamp) as first_seen,
  max(timestamp) as last_seen
FROM <authz_table>
WHERE TenantName = '{tenant}'
  AND userName = '{user}'
  AND entityQN LIKE '%{qn_fragment}%'
  AND timestamp >= now() - INTERVAL 7 DAY
GROUP BY result, engine, policyId
ORDER BY last_seen DESC
```

**Which policies are causing the most denies (top offenders):**
```sql
SELECT policyId, count() as deny_count, uniqExact(userName) as affected_users
FROM <authz_table>
WHERE TenantName = '{tenant}'
  AND result = 'deny'
  AND timestamp >= now() - INTERVAL 1 HOUR
GROUP BY policyId
ORDER BY deny_count DESC
LIMIT 20
```

**Deny rate over time (detect when a regression started):**
```sql
SELECT
  toStartOfMinute(timestamp) as minute,
  countIf(result = 'deny') as denies,
  countIf(result = 'allow') as allows
FROM <authz_table>
WHERE TenantName = '{tenant}'
  AND userName = '{user}'   -- optional
  AND timestamp >= now() - INTERVAL 6 HOUR
GROUP BY minute
ORDER BY minute DESC
```

### Step 3: Interpret and explain

After running queries, map findings to the truth table and root cause checklist:

- `engine=atlas, isExplicitDeny=false` → implicit deny → no matching policy (check resources pattern, isPolicyEnabled, action string)
- `engine=atlas, isExplicitDeny=true` → explicit deny policy matched → find that policy by `policyId`
- `engine=abac_auth` → ABAC-only result → check `policyFilterCriteria` for the matched ABAC policy
- `engine=merged_auth` → both engines ran → use truth table to explain which row applied
- `policyPriority=1 + result=deny` → OVERRIDE deny — hardest to fix, needs an OVERRIDE allow or policy removal
- Regression visible in time series → check what changed around that timestamp (policy edit, role change, Keycloak sync)

Present results as a clean table followed by a plain-English interpretation. If the query returns no results, note the possible reasons: wrong tenant, time window too narrow, or the issue is in Heka (no authz logs there).

---

## MODE: REVIEW

Review code changes for authorization correctness.

**Input resolution** — same as other review skills:
1. **PR number** (`#123` or `123`): `gh pr diff <number> --repo atlanhq/atlas-metastore`
2. **Branch name**: `git diff $(git merge-base master origin/<branch>)..origin/<branch>`
3. **No argument**: `git diff $(git merge-base HEAD master)..HEAD`

Get changed files first. Then launch agents in parallel.

### Phase 1: Parallel analysis

**Agent 1 — REST endpoint authorization:**
Find all new or modified REST endpoint methods (JAX-RS: `@GET`, `@POST`, `@PUT`, `@DELETE`). For each:
- Is `AtlasAuthorizationUtils.verifyAccess()` called before any entity data access?
- Is the correct `AtlasPrivilege` used for the operation type?
- For list/search endpoints: is `scrubSearchResults()` called or is the ES alias mechanism used?

**Agent 2 — Policy creation and mutation code:**
Find changes to `AuthPolicyPreProcessor.java`, `AuthPolicyValidator.java`, or any code that constructs/mutates AuthPolicy attributes. Check:
- `policyResources` format: `entity:` or `tag:` prefix present?
- `policyServiceName` set to a value defined in `atlas_service.json`?
- `isPolicyEnabled` explicitly set?
- `policyCategory` + `policySubCategory` combination consistent?
- ES alias update triggered after policy changes?

**Agent 3 — Cache store changes:**
Find changes to `PoliciesStore.java`, `UsersStore.java`, `PolicyRefresher`, or `CachePolicyTransformerImpl`. Check:
- New policy attributes included in the fetch query?
- New service names handled in the filtering pipeline?
- Delta refresh logic correctly identifies changed policies?
- Transformer templates updated if new action types were added?

**Agent 4 — Keycloak / role lifecycle:**
Find changes related to Persona/Purpose/Connection/Collection creation or deletion. Check:
- Keycloak role created with correct naming convention (`persona_<guid>`, `connection_admins_<guid>`, etc.)?
- Keycloak role deleted on entity deletion?
- ES alias created, updated, and deleted at the right lifecycle points?
- Bootstrap policies created from the correct template files?

### Phase 2: Authorization pattern checks

#### 2a: verifyAccess before data access
```java
// REQUIRED before any entity fetch or mutation driven by user input
AtlasAuthorizationUtils.verifyAccess(
    new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, entityHeader),
    "reading entity " + guid
);
```
**Flag:** Any method that fetches entity data using a user-supplied GUID/QN without a prior `verifyAccess`.

#### 2b: Search result scrubbing
List-returning endpoints must filter results:
```java
// Either:
AtlasAuthorizationUtils.scrubSearchResults(searchResult, AtlasPrivilege.ENTITY_READ);
// Or: route search through ES alias (for Persona/Purpose-aware queries)
```
**Flag:** New list-returning endpoints returning raw graph results without filtering.

#### 2c: Correct privilege mapping

| Operation | Required AtlasPrivilege |
|-----------|------------------------|
| Read entity | `ENTITY_READ` |
| Create entity | `ENTITY_CREATE` |
| Update entity | `ENTITY_UPDATE` |
| Delete entity | `ENTITY_DELETE` |
| Add classification | `ENTITY_ADD_CLASSIFICATION` |
| Remove classification | `ENTITY_REMOVE_CLASSIFICATION` |
| Update classification | `ENTITY_UPDATE_CLASSIFICATION` |
| Read type | `TYPE_READ` |
| Create relationship | `RELATIONSHIP_ADD` |
| Delete relationship | `RELATIONSHIP_REMOVE` |

**Flag:** Weaker privilege used, or operation performed without any privilege check.

#### 2d: policyResources format
```java
// Persona — entity resources
"entity:" + qualifiedName            // exact
"entity:" + qualifiedName + "/*"     // subtree

// Purpose — tag resources
"tag:" + classificationName
"tag:" + classificationName + "." + key + "=" + value  // with KV attachment

// Type scoping
"entity-type:" + typeName
```
**Flag:** Raw GUID instead of QN, missing prefix, wrong separator, unsupported regex instead of `*`/`?`.

#### 2e: policyFilterCriteria validity (ABAC)
If ABAC criteria JSON is constructed or modified:
- `condition` must be `"AND"` or `"OR"`
- `criterion[].operator` must be one of: `EQUALS`, `NOT_EQUALS`, `IN`, `NOT_IN`, `STARTS_WITH`, `ENDS_WITH`
- `criterion[].attributeName` for special attrs: `__typeName`, `__traitNames`, `__propagatedTraitNames`
- Attributes used must be primitive types with mixed index enabled (ES-indexed)
- The `config.query.dsl` must be updated consistently with the `entity` filter criteria

#### 2f: policyServiceName against atlas_service.json
Any new `policyServiceName` value must be registered in `addons/policies/atlas_service.json`. An unregistered service name causes the policy to be silently ignored.

#### 2g: isPolicyEnabled
Any code creating a new AuthPolicy must explicitly set `isPolicyEnabled = true`.
**Flag:** New policy creation code that omits this attribute.

#### 2h: policyPriority = OVERRIDE
OVERRIDE priority is for exceptional escalation. Any new code setting `policyPriority = 1` must have a comment explaining why and what it's unblocking.
**Flag:** OVERRIDE priority in a new policy without clear justification.

#### 2i: Persona/Purpose deletion completeness
Code that deletes a Persona or Purpose must:
1. Delete the Keycloak role (Persona only)
2. Delete the ES alias (both)
3. Delete all child AuthPolicy entities
**Flag:** Deletion code missing any of these three steps.

#### 2j: Bootstrap template placeholders
If `connection_bootstrap_policies.json` or `collection_bootstrap_policies.json` are modified:
- All `{placeholder}` values must be replaced before persisting
- New placeholders must be documented and filled in the policy creation code

### Phase 3: Output

```
## Authorization Review

### Summary
- Changed files: N
- Auth-related files changed: N
- Total findings: N (X critical, Y warnings, Z info)
- Verdict: PASS / PASS WITH NOTES / FAIL

### Findings

#### [CRITICAL] <title>
- **File:** `path/to/file.java:line`
- **Issue:** <description>
- **Risk:** <what can go wrong — 403, data leak, privilege escalation, cache miss>
- **Fix:**
  ```java
  // concrete fix
  ```

#### [WARNING] <title>
...

### Authorization Checklist
- [ ] All new REST endpoints call verifyAccess() with correct AtlasPrivilege
- [ ] List/search endpoints scrub results or use ES alias
- [ ] policyResources use entity:/tag: prefix, QN not GUID, wildcard not regex
- [ ] policyFilterCriteria JSON valid, supported operators, ES-indexed attributes only
- [ ] policyServiceName registered in atlas_service.json
- [ ] isPolicyEnabled set to true on all new policies
- [ ] No OVERRIDE priority without explicit justification
- [ ] Persona/Purpose deletion cleans up Keycloak role + ES alias + child policies
- [ ] Bootstrap template placeholders fully resolved
- [ ] PolicyRefresher transformer templates updated if new action types added
```
