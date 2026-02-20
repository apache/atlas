# MS-546: Implementation Plan

> **New API: Purpose Discovery for User**
> **Target**: Atlas-Metastore
> **Approach**: ES Aggregation-Based (Option A)

---

## Table of Contents

1. [Overview](#1-overview)
2. [API Specification](#2-api-specification)
3. [Component Design](#3-component-design)
4. [Implementation Tasks](#4-implementation-tasks)
5. [ES Query Design](#5-es-query-design)
6. [Code Implementation](#6-code-implementation)
7. [Testing Strategy](#7-testing-strategy)
8. [Rollout Plan](#8-rollout-plan)
9. [Open Questions](#9-open-questions)

---

## 1. Overview

### Objective

Create a new REST API endpoint in Atlas-Metastore that returns all Purpose entities accessible to a given user, eliminating the need for frontend aggregation of AuthPolicies.

### Key Benefits

| Benefit | Impact |
|---------|--------|
| Single API call | Reduces frontend complexity |
| Backend aggregation | Eliminates N+1 pattern |
| ES aggregation | Efficient deduplication at query level |
| Reduced data transfer | Only returns Purpose entities, not all AuthPolicies |

### Architecture Decision

**Chosen Approach**: ES Aggregation-Based Query

```
Request → Atlas API → ES Aggregation (get unique Purpose GUIDs)
                   → ES Multi-Get (fetch Purpose details) → Response
```

---

## 2. API Specification

### Endpoint

```
POST /api/meta/purposes/user
```

### Request

```json
{
  "username": "string",           // Required: User's username
  "groups": ["string"],           // Required: User's group names
  "attributes": ["string"],       // Optional: Specific attributes to return
  "limit": 100,                   // Optional: Max purposes to return (default: 100, max: 500)
  "offset": 0                     // Optional: Pagination offset (default: 0)
}
```

### Authentication

Same authentication as `/whoami`:
- Requires valid JWT token in `Authorization` header
- User context extracted from token for audit logging
- No additional RBAC checks (user can only query their own purposes)

### Response

```json
{
  "purposes": [
    {
      "guid": "string",
      "name": "string",
      "displayName": "string",
      "description": "string",
      "qualifiedName": "string",
      "classifications": ["string"],
      "isEnabled": true,
      "createdBy": "string",
      "updatedBy": "string",
      "createTime": 1234567890,
      "updateTime": 1234567890
    }
  ],
  "count": 10,
  "totalCount": 50,
  "hasMore": true
}
```

### Error Responses

| Status Code | Error | Description |
|-------------|-------|-------------|
| 400 | BAD_REQUEST | Missing required parameters |
| 401 | UNAUTHORIZED | Invalid or missing authentication |
| 500 | INTERNAL_ERROR | Server-side error |

### Headers

```
Authorization: Bearer <token>
Content-Type: application/json
```

---

## 3. Component Design

### 3.1 Package Structure

```
atlas-metastore/
├── intg/src/main/java/org/apache/atlas/model/discovery/
│   ├── PurposeUserRequest.java          # Request DTO
│   └── PurposeUserResponse.java         # Response DTO
│
├── repository/src/main/java/org/apache/atlas/discovery/
│   ├── PurposeDiscoveryService.java     # Service interface
│   └── PurposeDiscoveryServiceImpl.java # Service implementation
│
└── webapp/src/main/java/org/apache/atlas/web/rest/
    └── PurposeDiscoveryREST.java        # REST endpoint
```

### 3.2 Class Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    PurposeDiscoveryREST                         │
│  @Path("/api/meta/purposes")                                    │
├─────────────────────────────────────────────────────────────────┤
│  + getUserPurposes(PurposeUserRequest): PurposeUserResponse     │
└───────────────────────────┬─────────────────────────────────────┘
                            │ uses
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                  PurposeDiscoveryService                        │
│  <<interface>>                                                  │
├─────────────────────────────────────────────────────────────────┤
│  + discoverPurposesForUser(request): PurposeUserResponse        │
└───────────────────────────┬─────────────────────────────────────┘
                            │ implements
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│               PurposeDiscoveryServiceImpl                       │
├─────────────────────────────────────────────────────────────────┤
│  - graph: AtlasGraph                                            │
│  - entityRetriever: EntityGraphRetriever                        │
│  - indexSearchService: AtlasDiscoveryService                    │
├─────────────────────────────────────────────────────────────────┤
│  + discoverPurposesForUser(request): PurposeUserResponse        │
│  - buildPolicyAggregationQuery(request): String                 │
│  - extractPurposeGuids(aggregationResult): Set<String>          │
│  - fetchPurposeDetails(guids): List<AtlasEntityHeader>          │
└─────────────────────────────────────────────────────────────────┘
```

### 3.3 Sequence Diagram

```
┌────────┐     ┌─────────────────┐     ┌──────────────────┐     ┌────────────┐
│ Client │     │ PurposeDiscovery│     │ PurposeDiscovery │     │Elasticsearch│
│        │     │     REST        │     │    Service       │     │            │
└───┬────┘     └────────┬────────┘     └────────┬─────────┘     └─────┬──────┘
    │                   │                       │                     │
    │ POST /purposes/user                       │                     │
    │ {username, groups}│                       │                     │
    │──────────────────▶│                       │                     │
    │                   │                       │                     │
    │                   │ discoverPurposesForUser                     │
    │                   │──────────────────────▶│                     │
    │                   │                       │                     │
    │                   │                       │ Aggregation Query   │
    │                   │                       │ (AuthPolicy → guid) │
    │                   │                       │────────────────────▶│
    │                   │                       │                     │
    │                   │                       │   Unique Purpose    │
    │                   │                       │   GUIDs             │
    │                   │                       │◀────────────────────│
    │                   │                       │                     │
    │                   │                       │ Multi-Get Query     │
    │                   │                       │ (Purpose details)   │
    │                   │                       │────────────────────▶│
    │                   │                       │                     │
    │                   │                       │   Purpose Entities  │
    │                   │                       │◀────────────────────│
    │                   │                       │                     │
    │                   │   PurposeUserResponse │                     │
    │                   │◀──────────────────────│                     │
    │                   │                       │                     │
    │ 200 OK            │                       │                     │
    │ {purposes: [...]} │                       │                     │
    │◀──────────────────│                       │                     │
    │                   │                       │                     │
```

---

## 4. Implementation Tasks

### Phase 1: Core Implementation (3 days)

| Task | File | Description | Priority |
|------|------|-------------|----------|
| 1.1 | `PurposeUserRequest.java` | Create request DTO | P0 |
| 1.2 | `PurposeUserResponse.java` | Create response DTO | P0 |
| 1.3 | `PurposeDiscoveryService.java` | Define service interface | P0 |
| 1.4 | `PurposeDiscoveryServiceImpl.java` | Implement service logic | P0 |
| 1.5 | `PurposeDiscoveryREST.java` | Create REST endpoint | P0 |
| 1.6 | Spring configuration | Wire up beans | P0 |

### Phase 2: Testing (1-2 days)

| Task | Description | Priority |
|------|-------------|----------|
| 2.1 | Unit tests for service layer | P0 |
| 2.2 | Integration tests for REST endpoint | P0 |
| 2.3 | Performance tests with mock data | P1 |

### Phase 3: Integration (1 day)

| Task | Description | Priority |
|------|-------------|----------|
| 3.1 | Update Heracles to use new API | P0 |
| 3.2 | Update API documentation | P1 |
| 3.3 | Add monitoring/metrics | P1 |

---

## 5. ES Query Design

### 5.0 Data Model Discovery (from live cluster investigation)

**Key Finding**: The `accessControl` relationship is stored in a **nested `relationshipList` field**, not as a top-level attribute.

**ES Mapping Structure:**
```json
{
  "relationshipList": {
    "type": "nested",
    "properties": {
      "endGuid": { "type": "keyword" },      // Purpose GUID
      "endTypeName": { "type": "keyword" },  // "Purpose" or "AccessControl"
      "label": { "type": "keyword" },        // "__AccessControl.policies"
      "status": { "type": "keyword" }        // "ACTIVE"
    }
  }
}
```

**Verified Index Mappings:**
| Field | Type | Status |
|-------|------|--------|
| `policyCategory` | keyword | ✅ Indexed |
| `policyGroups` | keyword | ✅ Indexed |
| `policyUsers` | keyword | ✅ Indexed |
| `relationshipList` | nested | ✅ Indexed |
| `relationshipList.endGuid` | keyword | ✅ Indexed |

### 5.1 Step 1: Nested Aggregation Query for Unique Purpose GUIDs

Since relationships are stored in a nested field, we need a **nested aggregation**:

```json
{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "__state": "ACTIVE"
          }
        },
        {
          "term": {
            "__typeName.keyword": "AuthPolicy"
          }
        },
        {
          "term": {
            "policyCategory": "purpose"
          }
        },
        {
          "bool": {
            "should": [
              {
                "term": {
                  "policyGroups": "public"
                }
              },
              {
                "terms": {
                  "policyGroups": "${groups}"
                }
              },
              {
                "term": {
                  "policyUsers": "${username}"
                }
              }
            ],
            "minimum_should_match": 1
          }
        }
      ]
    }
  },
  "aggs": {
    "relationships": {
      "nested": {
        "path": "relationshipList"
      },
      "aggs": {
        "access_control_relations": {
          "filter": {
            "bool": {
              "must": [
                {
                  "term": {
                    "relationshipList.label": "__AccessControl.policies"
                  }
                },
                {
                  "term": {
                    "relationshipList.status": "ACTIVE"
                  }
                }
              ]
            }
          },
          "aggs": {
            "unique_purposes": {
              "terms": {
                "field": "relationshipList.endGuid",
                "size": 1000
              }
            }
          }
        }
      }
    }
  }
}
```

**Key Points:**
- `size: 0` - We don't need AuthPolicy documents, just the aggregation
- `nested` aggregation on `relationshipList` path
- Filter within nested agg to only get `__AccessControl.policies` relationships
- `terms` aggregation on `relationshipList.endGuid` - Gets unique Purpose GUIDs
- `size: 1000` in aggregation - Adjust based on max expected purposes per tenant

**Expected Response Structure:**
```json
{
  "aggregations": {
    "relationships": {
      "access_control_relations": {
        "unique_purposes": {
          "buckets": [
            { "key": "purpose-guid-1", "doc_count": 5 },
            { "key": "purpose-guid-2", "doc_count": 3 }
          ]
        }
      }
    }
  }
}
```

### 5.2 Alternative: Use Existing Atlas IndexSearch with relationAttributes

If nested aggregation proves complex, we can leverage the existing Atlas mechanism:

1. Use `directIndexSearch` with `relationAttributes: ["accessControl"]`
2. Atlas will hydrate the relationship data from the graph
3. Process results in Java to extract unique Purpose GUIDs

**Pros:** Uses existing, tested code paths
**Cons:** Fetches all matching AuthPolicies (less efficient)

**Recommendation:** Start with Alternative approach for faster delivery, then optimize with nested aggregation if performance is insufficient.

### 5.3 Step 2: Multi-Get Purpose Details

```json
{
  "size": 100,
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "__state": "ACTIVE"
          }
        },
        {
          "term": {
            "__typeName.keyword": "Purpose"
          }
        },
        {
          "terms": {
            "__guid": ["${purposeGuid1}", "${purposeGuid2}", "..."]
          }
        }
      ]
    }
  },
  "_source": {
    "includes": [
      "__guid",
      "name",
      "displayName",
      "description",
      "qualifiedName",
      "__classificationNames",
      "isAccessControlEnabled",
      "__createdBy",
      "__modifiedBy",
      "__timestamp",
      "__modificationTimestamp"
    ]
  }
}
```

### 5.4 Index Requirements Summary

All required fields are **confirmed indexed** in production ES:

| Field | Index Type | Status |
|-------|------------|--------|
| `__typeName.keyword` | keyword | ✅ Verified |
| `__state` | keyword | ✅ Verified |
| `policyCategory` | keyword | ✅ Verified |
| `policyGroups` | keyword (array) | ✅ Verified |
| `policyUsers` | keyword (array) | ✅ Verified |
| `relationshipList.endGuid` | keyword (nested) | ✅ Verified |
| `relationshipList.label` | keyword (nested) | ✅ Verified |

---

## 6. Code Implementation

### 6.1 PurposeUserRequest.java

```java
package org.apache.atlas.model.discovery;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY,
                setterVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY,
                fieldVisibility = JsonAutoDetect.Visibility.NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PurposeUserRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private String username;
    private List<String> groups;
    private Set<String> attributes;
    private int limit = 100;
    private int offset = 0;

    public PurposeUserRequest() {}

    // Getters and Setters
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public List<String> getGroups() { return groups; }
    public void setGroups(List<String> groups) { this.groups = groups; }

    public Set<String> getAttributes() { return attributes; }
    public void setAttributes(Set<String> attributes) { this.attributes = attributes; }

    public int getLimit() { return limit; }
    public void setLimit(int limit) { this.limit = limit; }

    public int getOffset() { return offset; }
    public void setOffset(int offset) { this.offset = offset; }

    // Validation
    public void validate() throws IllegalArgumentException {
        if (username == null || username.trim().isEmpty()) {
            throw new IllegalArgumentException("username is required");
        }
        if (groups == null) {
            throw new IllegalArgumentException("groups is required (can be empty list)");
        }
        if (limit < 1 || limit > 1000) {
            throw new IllegalArgumentException("limit must be between 1 and 1000");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
    }
}
```

### 6.2 PurposeUserResponse.java

```java
package org.apache.atlas.model.discovery;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.instance.AtlasEntityHeader;

import java.io.Serializable;
import java.util.List;

@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY,
                setterVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY,
                fieldVisibility = JsonAutoDetect.Visibility.NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PurposeUserResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<AtlasEntityHeader> purposes;
    private int count;
    private long totalCount;
    private boolean hasMore;

    public PurposeUserResponse() {}

    public PurposeUserResponse(List<AtlasEntityHeader> purposes, long totalCount, int limit, int offset) {
        this.purposes = purposes;
        this.count = purposes != null ? purposes.size() : 0;
        this.totalCount = totalCount;
        this.hasMore = (offset + this.count) < totalCount;
    }

    // Getters and Setters
    public List<AtlasEntityHeader> getPurposes() { return purposes; }
    public void setPurposes(List<AtlasEntityHeader> purposes) { this.purposes = purposes; }

    public int getCount() { return count; }
    public void setCount(int count) { this.count = count; }

    public long getTotalCount() { return totalCount; }
    public void setTotalCount(long totalCount) { this.totalCount = totalCount; }

    public boolean isHasMore() { return hasMore; }
    public void setHasMore(boolean hasMore) { this.hasMore = hasMore; }
}
```

### 6.3 PurposeDiscoveryService.java

```java
package org.apache.atlas.discovery;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.PurposeUserRequest;
import org.apache.atlas.model.discovery.PurposeUserResponse;

public interface PurposeDiscoveryService {

    /**
     * Discover all Purpose entities accessible to a user based on their
     * username and group memberships.
     *
     * @param request The request containing username and groups
     * @return Response containing accessible Purpose entities
     * @throws AtlasBaseException if discovery fails
     */
    PurposeUserResponse discoverPurposesForUser(PurposeUserRequest request) throws AtlasBaseException;
}
```

### 6.4 Configuration Properties

Add to `atlas-application.properties`:

```properties
# Purpose Discovery API Configuration
atlas.discovery.purpose.max-aggregation-size=500
atlas.discovery.purpose.max-results=1000
atlas.discovery.purpose.default-limit=100
```

### 6.5 PurposeDiscoveryServiceImpl.java (Skeleton)

```java
package org.apache.atlas.discovery;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.PurposeUserRequest;
import org.apache.atlas.model.discovery.PurposeUserResponse;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.*;

@Service
public class PurposeDiscoveryServiceImpl implements PurposeDiscoveryService {
    private static final Logger LOG = LoggerFactory.getLogger(PurposeDiscoveryServiceImpl.class);

    private static final String PURPOSE_TYPE = "Purpose";
    private static final String AUTH_POLICY_TYPE = "AuthPolicy";
    private static final String POLICY_CATEGORY_PURPOSE = "purpose";
    private static final String PUBLIC_GROUP = "public";

    // Configuration keys
    private static final String CONFIG_MAX_AGGREGATION_SIZE = "atlas.discovery.purpose.max-aggregation-size";
    private static final String CONFIG_MAX_RESULTS = "atlas.discovery.purpose.max-results";
    private static final String CONFIG_DEFAULT_LIMIT = "atlas.discovery.purpose.default-limit";

    // Defaults
    private static final int DEFAULT_MAX_AGGREGATION_SIZE = 500;
    private static final int DEFAULT_MAX_RESULTS = 1000;
    private static final int DEFAULT_LIMIT = 100;

    private final AtlasGraph graph;
    private final EntityGraphRetriever entityRetriever;
    private final AtlasDiscoveryService discoveryService;
    private final int maxAggregationSize;
    private final int maxResults;

    @Inject
    public PurposeDiscoveryServiceImpl(AtlasGraph graph,
                                        EntityGraphRetriever entityRetriever,
                                        AtlasDiscoveryService discoveryService) throws AtlasBaseException {
        this.graph = graph;
        this.entityRetriever = entityRetriever;
        this.discoveryService = discoveryService;

        // Load configuration
        Configuration config = ApplicationProperties.get();
        this.maxAggregationSize = config.getInt(CONFIG_MAX_AGGREGATION_SIZE, DEFAULT_MAX_AGGREGATION_SIZE);
        this.maxResults = config.getInt(CONFIG_MAX_RESULTS, DEFAULT_MAX_RESULTS);
    }

    @Override
    public PurposeUserResponse discoverPurposesForUser(PurposeUserRequest request) throws AtlasBaseException {
        LOG.debug("Discovering purposes for user: {}, groups: {}",
                  request.getUsername(), request.getGroups());

        // Step 1: Get unique Purpose GUIDs from AuthPolicies via aggregation
        Set<String> purposeGuids = getUniquePurposeGuids(request);

        if (CollectionUtils.isEmpty(purposeGuids)) {
            LOG.debug("No purposes found for user: {}", request.getUsername());
            return new PurposeUserResponse(Collections.emptyList(), 0, request.getLimit(), request.getOffset());
        }

        long totalCount = purposeGuids.size();
        LOG.debug("Found {} unique purpose GUIDs for user: {}", totalCount, request.getUsername());

        // Step 2: Apply pagination to GUIDs
        List<String> paginatedGuids = applyPagination(purposeGuids, request.getOffset(), request.getLimit());

        // Step 3: Fetch Purpose details
        List<AtlasEntityHeader> purposes = fetchPurposeDetails(paginatedGuids, request.getAttributes());

        return new PurposeUserResponse(purposes, totalCount, request.getLimit(), request.getOffset());
    }

    private Set<String> getUniquePurposeGuids(PurposeUserRequest request) throws AtlasBaseException {
        // TODO: Implement ES aggregation query
        // 1. Build aggregation query for AuthPolicy with policyCategory=purpose
        // 2. Filter by policyUsers (username) OR policyGroups (user's groups + "public")
        // 3. Aggregate on accessControl.guid to get unique Purpose GUIDs

        throw new UnsupportedOperationException("Not yet implemented");
    }

    private List<String> applyPagination(Set<String> guids, int offset, int limit) {
        List<String> guidList = new ArrayList<>(guids);

        if (offset >= guidList.size()) {
            return Collections.emptyList();
        }

        int endIndex = Math.min(offset + limit, guidList.size());
        return guidList.subList(offset, endIndex);
    }

    private List<AtlasEntityHeader> fetchPurposeDetails(List<String> guids, Set<String> attributes)
            throws AtlasBaseException {
        // TODO: Implement multi-get for Purpose entities
        // 1. Query ES for Purpose entities by GUIDs
        // 2. Return AtlasEntityHeader list with requested attributes

        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### 6.6 PurposeDiscoveryREST.java

```java
package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.discovery.PurposeDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.PurposeUserRequest;
import org.apache.atlas.model.discovery.PurposeUserResponse;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("meta/purposes")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class PurposeDiscoveryREST {
    private static final Logger LOG = LoggerFactory.getLogger(PurposeDiscoveryREST.class);

    private final PurposeDiscoveryService purposeDiscoveryService;

    @Inject
    public PurposeDiscoveryREST(PurposeDiscoveryService purposeDiscoveryService) {
        this.purposeDiscoveryService = purposeDiscoveryService;
    }

    /**
     * Discover all Purpose entities accessible to a user.
     *
     * @param request Request containing username and groups
     * @return Response containing accessible Purpose entities
     * @throws AtlasBaseException if discovery fails
     */
    @POST
    @Path("/user")
    public PurposeUserResponse getUserPurposes(PurposeUserRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getUserPurposes");

        try {
            if (request == null) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Request body is required");
            }

            request.validate();

            LOG.info("getUserPurposes: username={}, groupCount={}",
                     request.getUsername(),
                     request.getGroups() != null ? request.getGroups().size() : 0);

            return purposeDiscoveryService.discoverPurposesForUser(request);

        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }
}
```

---

## 7. Testing Strategy

### 7.1 Unit Tests

**File**: `repository/src/test/java/org/apache/atlas/discovery/PurposeDiscoveryServiceTest.java`

| Test Case | Description |
|-----------|-------------|
| `testDiscoverPurposes_UserWithDirectAccess` | User has direct policy access to purposes |
| `testDiscoverPurposes_UserWithGroupAccess` | User has access via group membership |
| `testDiscoverPurposes_UserWithPublicAccess` | User can see public purposes |
| `testDiscoverPurposes_UserWithMixedAccess` | Combination of direct, group, and public |
| `testDiscoverPurposes_NoPurposes` | User has no accessible purposes |
| `testDiscoverPurposes_Deduplication` | Same purpose via multiple policies |
| `testDiscoverPurposes_Pagination` | Pagination works correctly |
| `testDiscoverPurposes_InvalidRequest` | Validation errors handled |

### 7.2 Integration Tests

**File**: `webapp/src/test/java/org/apache/atlas/web/rest/PurposeDiscoveryRESTTest.java`

| Test Case | Description |
|-----------|-------------|
| `testEndpoint_Success` | Full flow with real ES |
| `testEndpoint_Unauthorized` | Auth validation |
| `testEndpoint_BadRequest` | Invalid request handling |
| `testEndpoint_Performance` | Response time < 500ms |

### 7.3 Performance Tests

**Setup**:
- 100 Purposes
- 1000 AuthPolicies
- 50 Groups
- Various user scenarios

**Metrics to Capture**:
- P50, P95, P99 latency
- ES query time breakdown
- Memory usage

---

## 8. Rollout Plan

### Phase 1: Development (Week 1)

- [ ] Implement DTOs (`intg` module)
- [ ] Implement service layer (`repository` module)
- [ ] Implement REST endpoint (`webapp` module)
- [ ] Unit tests

### Phase 2: Testing (Week 2)

- [ ] Integration tests
- [ ] Performance tests
- [ ] Code review
- [ ] Security review

### Phase 3: Staging Deployment (Week 2-3)

- [ ] Deploy to staging environment
- [ ] Heracles integration in staging
- [ ] Load testing with realistic data
- [ ] Monitor metrics

### Phase 4: Production Rollout (Week 3-4)

- [ ] Canary deployment (1 tenant)
- [ ] Monitor for 24-48 hours
- [ ] Gradual rollout (10% → 50% → 100%)
- [ ] Update Heracles to use new API
- [ ] Deprecate old flow

---

## 9. Open Questions

| # | Question | Owner | Status | Resolution |
|---|----------|-------|--------|------------|
| 1 | Is `accessControl.guid` field indexed in ES? | @backend-team | ✅ Resolved | **Yes** - stored in `relationshipList.endGuid` (nested field) |
| 2 | Max purposes per tenant? (for aggregation size) | @product-team | ✅ Resolved | **500 default, 1000 max** - based on enterprise scale analysis |
| 3 | Should we add Redis caching layer? | @arnab | ✅ Resolved | **No** - Heracles team will implement caching independently |
| 4 | Auth requirements - same as /whoami? | @security-team | ✅ Resolved | **Yes** - same auth as `/whoami` |
| 5 | Should Purpose include nested policy details? | @frontend-team | ✅ Resolved | **No** - return only Purpose entities, policies kept separate |

### Resolved Questions Detail

**Q1: accessControl.guid indexing**

Investigated on live cluster (`sonarsource`). The relationship is stored in:
```
relationshipList (nested) → endGuid (keyword)
```

The relationship label is `__AccessControl.policies`. Query approach updated to use nested aggregation.

**Q2: Max purposes per tenant**

Based on enterprise customer data from README:
- Autodesk: 200+ personas (purposes likely similar scale)
- Test configurations: 100 personas
- Typical enterprise: 50-200 purposes

**Decision:**
- Default aggregation size: **500**
- Maximum configurable: **1000**
- Make this configurable via application properties: `atlas.discovery.purpose.max-results=500`

**Q3: Caching strategy**

**Decision:** Skip Redis caching in Atlas. The Heracles team will implement caching at their layer, which is more appropriate since:
- They control the `/whoami` flow
- They can cache the full response including other data
- Avoids duplicate caching logic

**Q4: Auth requirements**

**Decision:** Use same authentication as `/whoami`:
- Requires valid JWT token
- User context extracted from token
- No additional permission checks needed (user can only see their own purposes)

**Q5: Nested policy details**

**Decision:** No - return only Purpose entities without nested policies.
- Primary ask is to have purposes available
- Keeps response lean and fast
- Policies can be fetched separately if needed via existing APIs

---

## Appendix A: Related Files to Reference

| File | Purpose |
|------|---------|
| `repository/src/main/java/org/apache/atlas/discovery/AtlasDiscoveryService.java` | Existing discovery service interface |
| `repository/src/main/java/org/apache/atlas/discovery/EntityDiscoveryService.java` | Reference for ES query patterns |
| `webapp/src/main/java/org/apache/atlas/web/rest/DiscoveryREST.java` | Reference for REST patterns |
| `intg/src/main/java/org/apache/atlas/model/discovery/SearchParameters.java` | Reference for request DTOs |

---

## Appendix B: ES Index Verification Script

Run this to verify required fields are indexed:

```bash
curl -X GET "localhost:9200/janusgraph_vertex_index/_mapping" | jq '.[] | .mappings.properties | {
  policyCategory: .policyCategory,
  policyGroups: .policyGroups,
  policyUsers: .policyUsers,
  accessControl: .accessControl
}'
```

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-02-02 | Arnab Saha | Initial draft |
