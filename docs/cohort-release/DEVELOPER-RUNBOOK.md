# Atlas-Metastore Ring Release: Developer Runbook

> Quick reference for deploying changes to production tenants via ring releases.

---

## TL;DR Flow

```
feature-branch → ring-branch → PR to master → add label → monitor → merge → cleanup
```

![Ring Release Flow](metastore-ring-based-cohort-release.png)

---

## Step-by-Step

### 1. Create Ring Branch (from tested feature)

```bash
git checkout master && git pull
git checkout -b ring-<ticket>-<short-desc>   # e.g., ring-ms-732-remove-perf-metrics
git merge <your-feature-branch>
git push origin ring-<ticket>-<short-desc>
```

**Wait ~30 min** for the Docker image build to complete.

---

### 2. Open PR: `ring-*` → `master`

This PR is your **control surface** — labels control deployments, results appear as PR comments.

---

### 3. Add Cohort Label

Add a label to deploy to specific tenants:

| Label | Ring | Tenants | Asset Range |
|-------|------|---------|-------------|
| `cohort:github:path:atlas-ring-0-empty` | Ring 0 | ~44 | 0 assets |
| `cohort:github:path:atlas-ring-1-tiny` | Ring 1 | ~124 | 1 - 100K |
| `cohort:github:path:atlas-ring-2-small` | Ring 2 | ~217 | 100K - 1M |
| `cohort:github:path:atlas-ring-3-medium` | Ring 3 | ~163 | 1M - 10M |
| `cohort:github:path:atlas-ring-4-large` | Ring 4 | ~28 | 10M - 50M |
| `cohort:github:path:atlas-ring-5-very-large` | Ring 5 | ~4 | 50M+ |

> **Note:** Tenant counts are approximate. Rings are dynamically redistributed quarterly based on asset counts from Vitally/Snowflake. See [atlan-releases/cohorts](https://github.com/atlanhq/atlan-releases/tree/main/cohorts) for current counts.

**Start with Ring 0**, then progressively expand.

---

### 4. Monitor Release

| Where | What |
|-------|------|
| **GitHub Actions** | `PR Label Release` workflow (exits quickly after triggering) |
| **Temporal UI** | [ServiceReleaseWorkflow](https://temporal.atlan.com/namespaces/default/workflows) |
| **PR Comments** | "Ring Release Triggered" → "Service Release Result" |
| **Slack** | `#testing_notifications` |

**Timeline:** Varies significantly based on each tenant's ArgoCD app state. Can complete in 30 min or take over an hour. Temporal processes up to 100 tenants in parallel.

---

### 5. Verify on Tenants

Check your feature works on the ring tenants. Use the observability dashboard:
- [Atlas Ring Release Health](https://observability.atlan.com/d/atlas-ring-release-health)

---

### 6. Expand Rings (Progressive Rollout)

If Ring 0 looks good, add the next label:

```
cohort:github:path:atlas-ring-1-tiny
```

**Gate criteria before expanding:**
- [ ] No new errors in ring tenants
- [ ] Feature behaves as expected
- [ ] No performance degradation

---

### 7. Go GA (Merge to Master)

When confident:

1. **Merge the ring PR** to master
2. **Wait for ArgoCD to recognize the GA image** (~15-30 min after `atlan` chart PR is merged)
   - You don't need to wait for full sync/rollout to all tenants
   - Just ensure ArgoCD has picked up the new chart version
   - Check: ArgoCD app shows the new image tag as "desired" state
3. **Run cleanup** (Actions → Manual Cohort Cleanup → enter PR number)

> **Why wait?** Cleanup removes overrides. If you cleanup before ArgoCD recognizes the GA image, ring tenants briefly revert to the old master image until ArgoCD catches up.

**What cleanup does:**
- Auto-detects all `cohort:github:path:*` labels on the PR
- Triggers a separate Temporal cleanup workflow per cohort
- Temporal reads the state file to find which tenants were actually released
- Removes the image overrides from each tenant's ArgoCD manifest
- Tenants then pick up the GA master image via normal ArgoCD sync

---

### 8. Abandon Ring (Close PR Without Merging)

If you decide not to proceed:

1. **Close PR without merging**
2. `pr-close-release.yml` auto-triggers cleanup
3. Temporal reads the state file → reverts ALL tenants across ALL cohorts
4. Tenants revert to master image
5. Delete the ring branch

> **Note:** This is fully automatic. No manual action needed beyond closing the PR.

---

### 9. Rollback a Specific Cohort (PR Still Open)

If something goes wrong with one ring but you want to keep the PR open for other rings:

1. Go to **Actions → Manual Cohort Cleanup**
2. Fill in:
   - `pr_number`: Your PR number
   - `path`: `cohorts/atlas-ring-3-medium.json` (the specific cohort to revert)
   - `allow_open_pr`: ✓ checked
3. Only the specified cohort is reverted; other rings are untouched

**To rollback specific tenants only:**
1. Same workflow, but use:
   - `source`: `custom`
   - `tenant_names`: `tenant1, tenant2, tenant3`
   - `allow_open_pr`: ✓ checked

---

## Quick Reference

| Action | Result |
|--------|--------|
| Push to `ring-*` branch | Triggers ~30 min build |
| Add cohort label | Deploys to those tenants (after build completes) |
| Push fix to ring branch | Rebuilds + re-deploys to all labeled cohorts |
| Add another label | Deploys to additional cohort |
| Merge ring PR | Starts GA flow; wait ~15-30 min for ArgoCD to recognize, then run Manual Cohort Cleanup |
| Close PR without merge | Auto-cleanup triggers, ALL tenants across ALL cohorts revert to master |
| Manual Cohort Cleanup (no path) | Detects all cohort labels on PR, cleans up each one (GA flow) |
| Manual Cohort Cleanup (with path) | Reverts only the specified cohort (selective rollback) |
| Manual Cohort Cleanup (custom) | Reverts only the specified tenants (surgical rollback) |

---

## Gotchas

| Issue | Solution |
|-------|----------|
| **Build takes ~30 min** | Plan ahead. Don't expect instant releases. |
| **Only Docker image is overridden** | Helm/config changes won't reach ring tenants. Use feature flags. |
| **Tenant has different image override** | `ValidateManifestForReleaseActivity` fails if tenant's ArgoCD manifest already has a different ring image. Remove the override first or coordinate with the other ring owner. |
| **`atlas-read` not overridden** | Only `atlas` (write path) is patched. Read path stays on master. |
| **ArgoCD sync slow/stuck** | Release may timeout. Check tenant's ArgoCD app health. |
| **Large cohorts (>100 tenants)** | Temporal processes 100 tenants in parallel. Larger cohorts are batched. |
| **Tenant skipped (auto-sync disabled)** | Tenants without ArgoCD auto-sync are skipped, not failed. Check PR comment for skipped list. |
| **Tenant not in ring** | Only tenants on MAIN-BASE/GOLDEN-MAIN-BASE release channels are included. Beta/staging tenants are excluded. |
| **Tenant in multiple cohorts** | Tenant can exist in both a custom and standard ring. The `ValidateManifestForReleaseActivity` prevents accidental overwrites — whichever ring deploys first "locks" the tenant. Remove from the other cohort file + state file to avoid confusion. |
| **Changes to `.github/` only** | `maven.yml` has `paths-ignore: '.github/**'`. Workflow-only changes won't trigger a build. Include a dummy change to a source file if you need a new build on the same commit. |
| **First push to new ring branch** | Build should trigger automatically. If it doesn't, check if the `paths-filter` detected changes correctly. |

---

## Custom Rings

Use a custom ring when you need to deploy to specific tenants not covered by the standard rings (e.g., a customer-specific fix, testing on a particular tenant setup).

### 1. Create Custom Cohort File

Create a JSON file in `atlan-releases/cohorts/`:

```json
{
  "description": "Custom ring for MS-999 feature validation",
  "ring": "custom",
  "generatedAt": "2026-03-10",
  "tenantCount": 3,
  "tenants": [
    {
      "name": "tenant-cluster-name-1",
      "domain": "customer1.atlan.com",
      "assetCount": 0,
      "cloudProvider": "aws",
      "deploymentType": "Production"
    },
    {
      "name": "tenant-cluster-name-2",
      "domain": "customer2.atlan.com",
      "assetCount": 0,
      "cloudProvider": "azure",
      "deploymentType": "Production"
    },
    {
      "name": "tenant-cluster-name-3",
      "domain": "customer3.atlan.com",
      "assetCount": 0,
      "cloudProvider": "gcp",
      "deploymentType": "Trial"
    }
  ]
}
```

**File naming:** `atlas-custom-<ticket>-<desc>.json` (e.g., `atlas-custom-ms-999-dq-fix.json`)

**Important:** The `name` field must be the **cluster name** (e.g., `affirm-mt`, `gcphft`), not the domain prefix.

### 2. Commit to atlan-releases

```bash
cd atlan-releases
git checkout main && git pull
# Create your cohort file
git add cohorts/atlas-custom-<your-file>.json
git commit -m "Add custom cohort for <ticket>"
git push origin main
```

### 3. Use the Custom Ring Label

Add this label to your ring PR:

```
cohort:github:path:atlas-custom-<your-file>
```

For example: `cohort:github:path:atlas-custom-ms-999-dq-fix`

The label format is `cohort:github:path:<filename-without-json>`.

### 4. Clean Up After

Once your ring release is complete (merged to GA or abandoned):

1. Delete the custom cohort file from `atlan-releases`
2. Or keep it if you plan to reuse for future releases

### Example: Deploy to 2 Specific Customers

```bash
# 1. Find cluster names
# affirm.atlan.com → affirm-mt
# sheetz.atlan.com → sheetz01

# 2. Create cohort file: atlan-releases/cohorts/atlas-custom-dq-validation.json
{
  "description": "DQ feature validation on Affirm and Sheetz",
  "ring": "custom",
  "generatedAt": "2026-03-10",
  "tenantCount": 2,
  "tenants": [
    {"name": "affirm-mt", "domain": "affirm.atlan.com", "assetCount": 0, "cloudProvider": "aws", "deploymentType": "Production"},
    {"name": "sheetz01", "domain": "sheetz.atlan.com", "assetCount": 0, "cloudProvider": "aws", "deploymentType": "Trial"}
  ]
}

# 3. Commit and push to atlan-releases

# 4. Add label to your ring PR:
#    cohort:github:path:atlas-custom-dq-validation
```

---

## Release Gates (Automated)

Before a ring release proceeds, these are checked:

- [ ] Docker build succeeded for current commit SHA **and** branch
- [ ] Integration tests passed for current commit SHA **and** branch
- [ ] At least 1 PR approval on current HEAD SHA (stale approvals from previous commits don't count)

If any gate fails, the release is blocked with a PR comment explaining why.

> **Note:** The build and test gates validate both SHA and branch name. This prevents a build from a different branch with the same commit SHA from being accepted.

---

## Release Result States

The "Service Release Result" PR comment shows one of these statuses:

| Status | Meaning |
|--------|---------|
| ✅ **success** | All tenants deployed successfully |
| ⚠️ **partial_success** | Some tenants succeeded, some failed |
| ❌ **failed** | All tenants failed |

**Skipped tenants** are reported separately and don't count as failures:
- Tenants without ArgoCD auto-sync enabled are skipped
- The release is still marked **success** if all non-skipped tenants succeed

---

## Emergency: Rollback a Ring

If something goes wrong after deploying to a ring:

**Option A: Rollback ALL cohorts (close PR)**
```
Close the ring PR without merging → auto-cleanup triggers for all cohorts
```

**Option B: Rollback a SPECIFIC cohort (keep PR open)**
```
Actions → Manual Cohort Cleanup → PR number + path + allow_open_pr ✓
```
Only the specified cohort is reverted. Other rings stay deployed.

**Option C: Rollback SPECIFIC tenants**
```
Actions → Manual Cohort Cleanup → PR number + source=custom + tenant_names + allow_open_pr ✓
```

**Option D: Fix forward**
```bash
git checkout ring-<name>
# fix the issue
git push origin ring-<name>
```
New build triggers, then auto-deploys fix to all labeled cohorts.

| Rollback Method | Speed | Scope | PR State |
|----------------|-------|-------|----------|
| Close PR | ~30 min | All cohorts | Closed |
| Manual cleanup (path) | ~30 min | One cohort | Open or Closed |
| Manual cleanup (custom) | ~15 min | Specific tenants | Open or Closed |
| Fix forward | ~45 min (build + deploy) | All labeled cohorts | Open |

---

## Links

| Resource | URL |
|----------|-----|
| Ring Cohort Files | [atlan-releases/cohorts/atlas-ring-*.json](https://github.com/atlanhq/atlan-releases/tree/main/cohorts) |
| Temporal UI | [temporal.atlan.com](https://temporal.atlan.com/namespaces/default/workflows) |
| Observability Dashboard | [Atlas Ring Release Health](https://observability.atlan.com/d/atlas-ring-release-health) |
| Full Implementation Doc | [atlas-metastore-cohort-release-implementation.md](./atlas-metastore-cohort-release-implementation.md) |

---

## Checklist: First Ring Release

- [ ] Feature tested on staging/preprod
- [ ] Ring branch created from master + feature merged in
- [ ] Build completed (~30 min)
- [ ] PR opened: ring-* → master
- [ ] Integration tests passing
- [ ] 1+ approval on PR
- [ ] Ring 0 label added, release successful
- [ ] Verified on Ring 0 tenants
- [ ] Progressive rollout to Ring 1, 2, ...
- [ ] All rings healthy
- [ ] Merged to master
- [ ] Waited for ArgoCD to recognize GA image (~15-30 min after atlan PR merge)
- [ ] Manual cleanup run
