# Ring Release Report: PR #6191

**Repository:** atlanhq/atlas-metastore  
**PR:** [#6191 – trigger ring release](https://github.com/atlanhq/atlas-metastore/pull/6191)  
**Ring branch:** `ring-testing`  
**Date:** 25 February 2026  
**Author:** krishnanunni-atlan  

---

## Executive Summary

Ring release was executed for PR #6191 using the cohort release workflow. Two cohort labels were applied sequentially. The first release had one tenant failure (markeznp20) due to a Temporal activity heartbeat timeout; a follow-up release after a new commit succeeded for all tenants. Cleanup ran successfully when the PR was closed without merge. The run validated sequential VPN processing, retry behaviour, and cleanup flow.

---

## 1. Configuration

| Item | Value |
|------|--------|
| Ring branch | `ring-testing` |
| Base branch | `master` |
| Cohorts | `atlas-dummy-1`, `atlas-internal-level-1` |
| Tenants | linea1rp01 (atlas-dummy-1); markeznp28, markeznp20 (atlas-internal-level-1) |
| PR outcome | Closed without merge (abandoned ring) |

---

## 2. Release Timeline

### 2.1 First release (image tag: `e65a995abcd`)

| Time (IST) | Event |
|------------|--------|
| — | Label `cohort:github:path:atlas-dummy-1` added. |
| — | GitHub Actions triggered; atlan-ci posted “Ring Release Triggered” for atlas-dummy-1 (tenant: linea1rp01). |
| ~11:10 | atlan-ci posted “Service Release Result”: **success** for atlas-dummy-1 (1/1 tenant). |
| — | Label `cohort:github:path:atlas-internal-level-1` added. |
| — | atlan-ci posted “Ring Release Triggered” for atlas-internal-level-1 (tenants: markeznp28, markeznp20). |
| ~11:29 | atlan-ci posted “Service Release Result”: **partial_success** for atlas-internal-level-1 — markeznp28 ✅, markeznp20 ❌ (WaitForStatefulSetRolloutActivity heartbeat timeout). |

### 2.2 Second release (image tag: `c544a51abcd`)

| Time (IST) | Event |
|------------|--------|
| — | New commit: “Remove commented-out license section in build.sh” (`c544a51`) — triggered rebuild and re-release to all labeled cohorts. |
| — | atlan-ci posted “Ring Release Triggered” for **both** cohorts in a single run (sequential processing; one VPN connection). |
| ~12:20 | atlan-ci posted “Service Release Result”: **success** for atlas-dummy-1 (1/1 tenant). |
| ~12:57 | atlan-ci posted “Service Release Result”: **success** for atlas-internal-level-1 (2/2 tenants). |

### 2.3 Cleanup

| Time (IST) | Event |
|------------|--------|
| — | PR closed without merge; branch `ring-testing` deleted. |
| ~13:19 | atlan-ci posted “Service Release Result”: **success** (cleanup) — empty image tag, 3 tenants; overrides removed. |

---

## 3. Results Summary

| Phase | Image tag | Cohorts | Tenants | Result |
|-------|-----------|---------|---------|--------|
| First release | e65a995abcd | atlas-dummy-1 | linea1rp01 | Success |
| First release | e65a995abcd | atlas-internal-level-1 | markeznp28, markeznp20 | Partial (1 success, 1 failure) |
| Second release | c544a51abcd | atlas-dummy-1 | linea1rp01 | Success |
| Second release | c544a51abcd | atlas-internal-level-1 | markeznp28, markeznp20 | Success |
| Cleanup | (empty) | All | 3 | Success |

---

## 4. Issues and Resolutions

### 4.1 Heartbeat timeout (markeznp20)

- **Symptom:** WaitForStatefulSetRolloutActivity failed with “activity Heartbeat timeout” for tenant markeznp20 on the first release.
- **Cause:** Tenant worker restarted while the activity was running; no heartbeat was sent in time.
- **Resolution:** Retry policy for verification activities was set to `MaximumAttempts: 3` in platform-temporal-workflows so Temporal retries after worker restart.

### 4.2 Sequential VPN processing

- Second release used a single job that processed both cohorts sequentially after one VPN connection, avoiding the previous “one VPN connection at a time” limitation and eliminating VPN-related failures for multi-cohort runs.

---

## 5. Validation Checklist

- [x] Label-based trigger (atlas-dummy-1, then atlas-internal-level-1)
- [x] Fire-and-forget GitHub Actions; Temporal posts final result comments
- [x] Two PR comments per cohort: “Ring Release Triggered” and “Service Release Result”
- [x] State file `atlas-ring-testing.json` updated and used for cleanup
- [x] Cleanup on PR close (no merge): overrides removed for all 3 tenants
- [x] Sequential cohort processing with single VPN connection
- [x] Retry handling for verification activities (post-report change)

---

## 6. Limitations (ArgoCD behaviour)

This implementation depends on the tenant’s ArgoCD application. Atlas is currently part of a large umbrella chart, which implies:

- If the tenant Argo app is **hung or times out**, verification fails even when the patch was applied.
- **Timing:** For a smoother run, add the cohort label when **no other syncs are in progress** on the target tenant app.

**Phase 2:** These limitations are expected to be addressed when metastore components are split into a **separate ArgoCD application**, so atlas is no longer under the large umbrella chart and ArgoCD can sync it independently. See [atlas-metastore-cohort-release-implementation.md](./atlas-metastore-cohort-release-implementation.md#limitation-argocd-behaviour-tenant-umbrella-app).

---

## 7. References

- PR: https://github.com/atlanhq/atlas-metastore/pull/6191  
- State file: [atlas-ring-testing.json](https://github.com/atlanhq/atlan-releases/blob/main/atlas-ring-testing.json) (atlan-releases)  
- Implementation doc: [atlas-metastore-cohort-release-implementation.md](./atlas-metastore-cohort-release-implementation.md)  

---

*Report generated from PR #6191 activity. Last updated: February 2026.*
