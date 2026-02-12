# MS-546: Bedrock Purpose Fetching API - Backend Optimization for whoami

> **Linear Issue**: [MS-546](https://linear.app/atlan-epd/issue/MS-546/bedrock-purpose-fetching-api-backend-optimization-for-whoami)
> **Project**: Project Bedrock
> **Milestone**: Enterprise User Scale (100k)
> **Priority**: High
> **Assignee**: Arnab Saha
> **Status**: Backlog
> **Created**: 2026-01-29

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Requirements & Goals](#2-requirements--goals)
3. [Acceptance Criteria](#3-acceptance-criteria)
4. [Technical Context](#4-technical-context)
5. [Constraints & Dependencies](#5-constraints--dependencies)
6. [Related Issues](#6-related-issues)
7. [Architecture Overview](#7-architecture-overview)
8. [Data Flow](#8-data-flow)
9. [Current Performance Metrics](#9-current-performance-metrics)
10. [API Contracts](#10-api-contracts)
11. [Proposed Solutions](#11-proposed-solutions)
12. [Code References](#12-code-references)
13. [Additional Context](#13-additional-context)

---

## 1. Problem Statement

### The Challenge

Frontend needs to fetch personalized **Purposes** for users, but the current approach creates significant performance issues at scale.

### Current Flow (Problematic)

```
1. Frontend fetches Auth policies for the user
2. Frontend aggregates unique Purposes from all Auth policies
3. Frontend fetches Purpose details for each unique Purpose
```

### Why This Doesn't Scale

The core issue is that **tenants with thousands of Auth policies create an aggregation bottleneck on the frontend**.

The relationship between Purpose and Auth policy is **one-to-many**:
- One Purpose can have multiple Auth policies attached
- Auth policies contain user/group permissions

When a tenant has thousands of Auth policies, the frontend must:
1. Make a large API call to fetch all relevant Auth policies
2. Process and deduplicate Purpose references from potentially thousands of Auth policies
3. Make additional calls to fetch Purpose details

**This aggregation work belongs on the backend, not the frontend.**

### Why a Single API Call Isn't Currently Possible

The current architecture stores **Purpose** and **Auth policy** as separate entities in Elasticsearch. There is no direct API to query:

> "Give me all Purposes that this user has access to"

The join relationship exists through `policy_resources` tags matching Purpose classification, but this resolution currently happens through multiple Index Search calls and frontend aggregation.

---

## 2. Requirements & Goals

### Primary Goal

Enable the frontend to fetch Purpose entities efficiently **without** needing to:
- Orchestrate multiple API calls
- Handle aggregation logic that currently creates performance bottlenecks at scale

### Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| **P99 Latency** | < 1 second | For `/whoami` API (Tier-1 Critical API) |
| **User Scale** | 100,000 users | Enterprise scale target |
| **Concurrent Users** | 10+ VU | Stress test requirement |

### Optimization Objectives

1. **Decouple persona and purpose** from `/whoami` API
2. Move aggregation logic from frontend to backend
3. Reduce API calls needed to fetch user's accessible Purposes
4. Eliminate N+1 query patterns

---

## 3. Acceptance Criteria

Based on the Linear issue and related Jira tickets:

1. **New dedicated API endpoint** that returns all Purposes accessible to a user
2. **Backend handles all aggregation logic** - no frontend orchestration required
3. **Single API call** to fetch all user-accessible Purposes
4. **P99 latency < 1 second** for the new endpoint at scale (100K users)
5. **Frontend can fetch persona and purpose independently** - removing dependency on `/whoami` to return these fields

---

## 4. Technical Context

### Identified Bottlenecks

#### 4.1 N+1 Query Pattern in `/whoami`

The `/whoami` API currently:
1. Fetches roles of the user in bulk
2. Then separately fetches:
   - Each realm role to get role attributes
   - Role attached to each group assigned to user
   - Composite roles of the realm roles

**Impact**: During load tests with 20K users, Heracles generates **~350 RPS** to Keycloak due to this pattern.

#### 4.2 Frontend Aggregation Bottleneck

- Tenants with thousands of Auth policies overwhelm the frontend
- Multiple Index Search calls required to resolve Purpose-AuthPolicy relationships
- Deduplication of Purpose references happens client-side

#### 4.3 High RPS to Atlas

- API calls from Heracles to Atlas for listing persona/purpose: **200+ RPS**
- This creates resource spikes and throttles CNPG CPU

### Current vs Target Performance Numbers

| Scenario | Current P99 | Target P99 |
|----------|-------------|------------|
| `/whoami` (20K users, 50 groups) | > 1s | < 1s |
| `/whoami` (20K users, 50 groups, 100 personas) | Fails under stress | < 1s |
| Mixed load (Tier-1 APIs) | Fails to achieve P99 < 1s | P99 < 1s |

### Scale/Volume Information

Based on testing configurations:
- **Users**: 20,000 - 100,000
- **Groups**: 50
- **Personas**: 100
- **Auth Policies**: Thousands per tenant (varies)
- **Affected Tenants**: [Spreadsheet of affected tenants](https://docs.google.com/spreadsheets/d/1hzIPQmBT_0fqRgn3PdxN9J8CUYxH2z3imHLUckB7d6s/edit?usp=sharing)

---

## 5. Constraints & Dependencies

### Dependencies

| Component | Role | Notes |
|-----------|------|-------|
| **Heracles** | Auth service | Entry point for `/whoami` |
| **Keycloak** | Identity provider | User/role/group management |
| **Atlas (Metastore)** | Entity storage | Stores Purpose and AuthPolicy entities |
| **Elasticsearch** | Index store | Purpose and AuthPolicy stored as separate entities |
| **CNPG (Postgres)** | Database | Keycloak backend |
| **Redis** | Cache | Used by `/tenants/default` |

### Resource Configuration (Standard Hardware)

| Service | CPU | Memory |
|---------|-----|--------|
| Heracles | 300m | 300MiB |
| Keycloak | 2000m | 1.5GiB (min for 20K users) |
| CNPG | 1500m | 3.6GiB |
| Atlas | 3000m | 8GiB |

### Constraints

1. **Metastore team bandwidth required** for Approach 1 (New API in Atlas)
2. Keycloak requires minimum 1.5GiB memory for 20K users
3. Rate limits on Heracles (per-minute rate limits observed during stress tests)
4. Cannot modify IndexSearch API significantly (high blast radius)

---

## 6. Related Issues

### Linear/Jira Issues

| Issue | Title | Status | Description |
|-------|-------|--------|-------------|
| [PLTS-696](https://atlanhq.atlassian.net/browse/PLTS-696) | Refactor /whoami API | In Review | Solve N+1 query pattern, decouple persona/purpose |
| [PLTS-700](https://atlanhq.atlassian.net/browse/PLTS-700) | Decouple persona and purpose logic from WhoAmI API | Blocked | FE can directly perform indexsearch calls |
| [PLTS-655](https://atlanhq.atlassian.net/browse/PLTS-655) | Scale IAM stack to 100K users | In Progress | Parent epic |
| [PLTS-684](https://atlanhq.atlassian.net/browse/PLTS-684) | Cargill - Login Failure Due to /whoami 400 | Done | Related customer issue |

### Confluence Documentation

| Document | Link |
|----------|------|
| Purpose Fetching API: Problem Statement & Approaches | [Confluence](https://atlanhq.atlassian.net/wiki/spaces/Platform/pages/1418920084) |
| Tier-1 API Performance Test (Standard Hardware) | [Confluence](https://atlanhq.atlassian.net/wiki/spaces/Platform/pages/1361903649) |
| Auth Stack Scaling: API Tiering | [Confluence](https://atlanhq.atlassian.net/wiki/spaces/Platform/pages/1355055141) |

### Slack Thread

- [Discussion Thread](https://atlanhq.slack.com/archives/C09SEEUK9DK/p1765439594058589)

---

## 7. Architecture Overview

### Current whoami/Purpose/Persona Fetching Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Frontend  │────▶│   Heracles  │────▶│   Keycloak  │────▶│    CNPG     │
│             │     │  (/whoami)  │     │             │     │  (Postgres) │
└─────────────┘     └──────┬──────┘     └─────────────┘     └─────────────┘
                           │
                           │ (200+ RPS for persona/purpose)
                           ▼
                    ┌─────────────┐     ┌─────────────┐
                    │    Atlas    │────▶│ Elasticsearch│
                    │ (Metastore) │     │             │
                    └─────────────┘     └─────────────┘
```

### Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| **Frontend** | Orchestrates API calls, aggregates Purpose from AuthPolicies |
| **Heracles** | Auth service, exposes `/whoami`, `/evaluates` endpoints |
| **Keycloak** | User authentication, role/group management |
| **Atlas** | Stores Purpose, Persona, AuthPolicy as separate entities |
| **Elasticsearch** | Index search for Purpose/Persona/AuthPolicy entities |

---

## 8. Data Flow

### Current Flow (Inefficient)

```
1. User logs in
2. Frontend calls /whoami → Heracles
3. Heracles calls Keycloak (N+1 pattern for roles)
   - Bulk fetch role IDs
   - Individual calls for role details (350 RPS)
   - Individual calls for group roles
   - Individual calls for composite roles
4. Heracles calls Atlas for persona/purpose (200+ RPS)
5. Frontend receives user data with persona list
6. Frontend fetches AuthPolicies for user
7. Frontend aggregates unique Purposes from AuthPolicies (BOTTLENECK)
8. Frontend fetches Purpose details for each unique Purpose
```

### Proposed Flow (Optimized)

```
1. User logs in
2. Frontend calls /whoami → Returns username, groups, basic permissions
3. Frontend calls NEW Purpose API → Backend aggregates and returns all accessible Purposes
4. Frontend calls separate Persona API if needed
```

---

## 9. Current Performance Metrics

### Test Environment: 20K Users, Standard Hardware

#### /whoami Isolation Tests (20K users, 50 groups)

| Test Type | Concurrent Users | Duration | P99 Latency | Notes |
|-----------|------------------|----------|-------------|-------|
| Baseline | 1 | 3 min | Baseline | [Report](https://drive.google.com/file/d/1wRK8Lh8yIdKMIlhPFIv-YPvqaXXJnczt/view) |
| Steady | 5 | 10 min | - | [Report](https://drive.google.com/file/d/1HmbIkv0EVn_u8C9VgWjiT1vvvBYGRVvS/view) |
| Stress | 10 | 10 min | Rate limit hit | [Report](https://drive.google.com/file/d/1PgJ358-O4YddNRKQa4d2gDxNISw4Frqk/view) |

#### /whoami with Personas (20K users, 50 groups, 100 personas)

| Test Type | Concurrent Users | Duration | Observations |
|-----------|------------------|----------|--------------|
| Steady | 5 | 10 min | [Report](https://drive.google.com/file/d/1O6xgoUV2dBIt48TuJRaYvIyIYP6bfuv8/view) |
| Stress | 10 | 10 min | [Report](https://drive.google.com/file/d/1Tq9r7x_CqEadaXgmm80SyPXbKzuiZflv/view) |

### Observed Patterns

1. **N+1 Query Pattern**: 350+ RPS to Keycloak from Heracles
2. **Atlas RPS**: 200+ RPS for persona/purpose listing
3. **CNPG CPU Throttling**: Database CPU maxes out under load
4. **Mixed Load Failure**: Failed to achieve P99 < 1s in mixed workload

### Post-Refactoring Results (N+1 Fix)

After eliminating the N+1 query pattern:
- Keycloak RPS reduced from **350+ RPS** to **~40 RPS** for user detail fetching
- Significant improvement but Purpose aggregation bottleneck remains

---

## 10. API Contracts

### Current /whoami Response Structure

```json
{
  "assignedRole": {
    "id": "bb6a3bef-9a85-45d1-ade8-b90f0f92c569",
    "name": "$guest",
    "description": "Guest",
    "level": "workspace",
    "summary": ""
  },
  "decentralizedRoles": [],
  "defaultRoles": ["offline_access", "uma_authorization", "$guest"],
  "designation": "",
  "groups": [],
  "permissions": ["READ_WORKSPACE", ...],
  "personas": [
    {
      "id": "1b619fd1-4c54-4ed6-a0ff-460c1301c26b",
      "roleName": "persona_HT9JeNRMPy2yOH2Cng12TD",
      "name": "Test_46_1764589415"
    }
  ],
  "profileRole": "",
  "profileRoleOther": "",
  "purposes": [],
  "roles": [
    {
      "id": "bb6a3bef-9a85-45d1-ade8-b90f0f92c569",
      "name": "$guest"
    }
  ],
  "userId": "6e552e24-b963-446b-913e-6c3d7a3b67f5",
  "username": "testuser10001_1764572446",
  "workspaceRole": "$guest"
}
```

### Proposed New Purpose API (Approach 1)

**Endpoint**: `GET /api/meta/purposes/accessible` (TBD)

**Request Parameters**:
- `username` (string): Current user's username
- `groups` (array): User's group IDs

**Expected Response**:
```json
{
  "purposes": [
    {
      "id": "purpose-guid",
      "name": "Purpose Name",
      "description": "Purpose description",
      "classifications": [...],
      "policies": [...]
    }
  ],
  "total": 10
}
```

---

## 11. Proposed Solutions

### Approach 1: New Dedicated API in Atlas (Recommended)

Create a new API endpoint specifically for Purpose filtering by user/groups.

| Pros | Cons |
|------|------|
| Clean separation of concerns | Requires Metastore team development effort |
| Backend handles all aggregation logic | |
| Single API call for all purposes | |

### Approach 2: Enhanced Index Search

Modify the existing Index Search API to support joining Purpose with AuthPolicy with deduplication support.

| Pros | Cons |
|------|------|
| Reuses existing API infrastructure | Adds complexity to indexsearch API |
| | High blast radius change |

### Approach 3: New API in Heracles

Create a new API in Heracles to fetch AuthPolicy and aggregate unique Purposes.

| Pros | Cons |
|------|------|
| Faster to release | Introduces coupling between auth service and Metastore |
| Doesn't require metastore team bandwidth | Adds additional network hop, increasing latency |

### N+1 Query Solution (Implemented in PLTS-696)

Single query to fetch roles with attributes:

```sql
WITH user_direct_roles AS (
    SELECT role_id FROM user_role_mapping WHERE user_id = ?
),
user_group_roles AS (
    SELECT grm.role_id
    FROM group_role_mapping grm
    INNER JOIN user_group_membership ugm ON grm.group_id = ugm.group_id
    WHERE ugm.user_id = ?
),
all_role_ids AS (
    SELECT role_id, 'direct' AS assignment_type FROM user_direct_roles
    UNION
    SELECT role_id, 'group' AS assignment_type FROM user_group_roles
),
filtered_role_attributes AS (
    SELECT ra.role_id, ra.name AS attr_name, array_agg(ra.value) AS attr_values
    FROM role_attribute ra
    WHERE ra.role_id IN (SELECT role_id FROM all_role_ids)
    GROUP BY ra.role_id, ra.name
)
SELECT
    r.id AS role_id, r.name AS role_name, r.description, r.client_role,
    to_jsonb(array_agg(DISTINCT ari.assignment_type)) AS assignment_types,
    COALESCE(jsonb_object_agg(fra.attr_name, to_jsonb(fra.attr_values))
             FILTER (WHERE fra.attr_name IS NOT NULL), '{}'::jsonb) AS attributes
FROM all_role_ids ari
INNER JOIN keycloak_role r ON r.id = ari.role_id
LEFT JOIN filtered_role_attributes fra ON r.id = fra.role_id
GROUP BY r.id, r.name, r.description, r.client_role
ORDER BY r.name;
```

---

## 12. Code References

### Entry Points in Heracles

| Function | File | Line |
|----------|------|------|
| Persona Call | `handler/evaluate.go` | [L346](https://github.com/atlanhq/heracles/blob/11922bc955e15d078a5f9b17fe26a9e81958ae0c/handler/evaluate.go#L346) |
| Purpose Call | `handler/evaluate.go` | [L400](https://github.com/atlanhq/heracles/blob/11922bc955e15d078a5f9b17fe26a9e81958ae0c/handler/evaluate.go#L400) |

### Git Branch

```
arnabsaha/ms-546
```

---

## 13. Additional Context

### API Tiering Classification

| Tier | APIs | Success Criteria | Impact if Failed |
|------|------|------------------|------------------|
| **Tier 1** | `/whoami`, `/tenants/default`, `/evaluates`, `/indexsearch` | P99 < 1s | Product DOWN |
| **Tier 2** | `/users`, `/groups`, `/roles`, `/users/{id}` | - | Major functionality broken |
| **Tier 3** | Low priority APIs | - | Small feature impact |

### Frontend Changes Already Implemented

- Frontend has implemented logic to fetch persona and purpose independently
- Removes dependency on `/whoami` to return these fields
- Backend API (this ticket) is the blocker

### Affected Customers (Pattern)

- **Itau**: 15K users with whoami slowness
- **Alcon**: Performance challenges
- **Marriott**: Performance challenges
- **Palo Alto**: Performance challenges
- **Autodesk**: 200+ personas, timing out on assets page

---

## Document Metadata

| Field | Value |
|-------|-------|
| **Created** | 2026-02-02 |
| **Author** | Generated from Linear/Confluence/Jira |
| **Last Updated** | 2026-02-02 |
| **Sources** | Linear MS-546, Confluence, PLTS-696, PLTS-700 |
