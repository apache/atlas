# MS-546: Implementation Summary for Team Validation

> **TL;DR**: New API endpoint `POST /api/meta/purposes/user` in Atlas that returns all accessible Purposes for a user in a single call, using ES aggregation to eliminate frontend bottleneck.

---


## Proposed Solution

**Endpoint**: `POST /api/meta/purposes/user`

**Approach**: ES Aggregation-Based (2-step query)

```
Step 1: Aggregate AuthPolicies → Get unique Purpose GUIDs
Step 2: Multi-Get → Fetch Purpose details by GUIDs
```

---

## API Contract

### Request
```json
{
  "username": "john.doe",
  "groups": ["data-team", "analysts"],
  "limit": 100,
  "offset": 0
}
```

### Response
```json
{
  "purposes": [{ "guid": "...", "name": "...", "displayName": "..." }],
  "count": 10,
  "totalCount": 50,
  "hasMore": true
}
```

---

## Implementation Scope

| Component | Files | Effort |
|-----------|-------|--------|
| DTOs | `PurposeUserRequest.java`, `PurposeUserResponse.java` | 0.5 day |
| Service | `PurposeDiscoveryService.java`, `PurposeDiscoveryServiceImpl.java` | 1.5 days |
| REST | `PurposeDiscoveryREST.java` | 0.5 day |
| Tests | Unit + Integration | 1-2 days |

**Total Estimate**: ~5 days (dev + testing)

---

## ES Query Strategy

### Step 1: Aggregation (get unique Purpose GUIDs)
- Query `AuthPolicy` entities where `policyCategory = "purpose"`
- Filter by `policyUsers` (username) OR `policyGroups` (user groups + "public")
- Aggregate on `accessControl.guid` for deduplication

### Step 2: Multi-Get (fetch details)
- Query `Purpose` entities by GUIDs from Step 1
- Return only requested attributes

---

## Timeline

| Week | Milestone |
|------|-----------|
| Week 1 | Core implementation + unit tests |
| Week 2 | Integration tests + staging deployment |
| Week 2-3 | Load testing + Heracles integration |
| Week 3-4 | Production rollout (canary → gradual) |

---

## Open Questions (Need Team Input)

| # | Question | Stakeholder |
|---|----------|-------------|
| 1 | **Is `accessControl.guid` indexed in ES?** (Critical for aggregation) | Backend |
| 2 | **Max purposes per tenant?** (Affects aggregation `size` parameter) | Product |
| 3 | **Should we add Redis caching?** | Arnab/Backend |
| 4 | **Auth requirements** - same as `/whoami`? | Security |
| 5 | **Should response include nested policy details?** | Frontend |

---

## Dependencies

- **Heracles**: Will need update to call new API instead of orchestrating multiple calls
- **Frontend**: Already implemented independent persona/purpose fetching logic (waiting on this API)

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| `accessControl.guid` not indexed | Verify before starting; may need index update |
| Large aggregation results | Set reasonable `size` limit; add pagination |
| Performance regression | Load test before production; canary deployment |

---

## Validation Checklist

Please confirm:

- [ ] API contract looks correct for frontend needs
- [ ] ES aggregation approach is acceptable (vs. alternative approaches)
- [ ] Timeline is realistic
- [ ] Open questions are assigned to correct owners
- [ ] No missing dependencies or blockers

---

## Links

- [Full Implementation Plan](./IMPLEMENTATION_PLAN.md)
- [Requirements Doc](./README.md)
- [Linear Issue](https://linear.app/atlan-epd/issue/MS-546)
- [Confluence: Purpose Fetching API](https://atlanhq.atlassian.net/wiki/spaces/Platform/pages/1418920084)
