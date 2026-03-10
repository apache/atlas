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

| Label | Ring | Tenants |
|-------|------|---------|
| `cohort:github:path:atlas-ring-0-empty` | Ring 0 | 38 internal/empty tenants |
| `cohort:github:path:atlas-ring-1-tiny` | Ring 1 | 118 tiny tenants (<100K assets) |
| `cohort:github:path:atlas-ring-2-small` | Ring 2 | 223 small tenants |
| `cohort:github:path:atlas-ring-3-medium` | Ring 3 | 169 medium tenants |
| `cohort:github:path:atlas-ring-4-large` | Ring 4 | 27 large tenants |
| `cohort:github:path:atlas-ring-5-very-large` | Ring 5 | 4 very large tenants |

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
3. **Remove the label** from the (now merged) PR
4. **Run cleanup** (Actions → Manual Cohort Cleanup → enter PR number)

> **Why wait?** Cleanup removes overrides. If you cleanup before ArgoCD recognizes the GA image, ring tenants briefly revert to the old master image until ArgoCD catches up.

---

### 8. Abandon Ring (If Needed)

If you decide not to proceed:

1. **Close PR without merging**
2. Overrides are **auto-removed** — tenants revert to master
3. Delete the ring branch

---

## Quick Reference

| Action | Result |
|--------|--------|
| Push to `ring-*` branch | Triggers ~30 min build |
| Add cohort label | Deploys to those tenants (after build completes) |
| Push fix to ring branch | Rebuilds + re-deploys to all labeled cohorts |
| Add another label | Deploys to additional cohort |
| Merge ring PR | Starts GA flow; wait ~15-30 min for ArgoCD to recognize, then cleanup |
| Close PR without merge | Auto-cleanup, tenants revert |
| Manual Cohort Cleanup | Removes overrides for specified PR |

---

## Gotchas

| Issue | Solution |
|-------|----------|
| **Build takes ~30 min** | Plan ahead. Don't expect instant releases. |
| **Only Docker image is overridden** | Helm/config changes won't reach ring tenants. Use feature flags. |
| **Another ring already on same cohort** | Release fails. Use different cohort or coordinate. |
| **`atlas-read` not overridden** | Only `atlas` (write path) is patched. Read path stays on master. |
| **ArgoCD sync slow/stuck** | Release may timeout. Check tenant's ArgoCD app health. |
| **Large cohorts (>100 tenants)** | Temporal processes 100 tenants in parallel. Larger cohorts are batched. |

---

## Release Gates (Automated)

Before a ring release proceeds, these are checked:

- [ ] Docker build succeeded for current commit
- [ ] Integration tests passed
- [ ] At least 1 PR approval (on latest commit)

If any gate fails, the release is blocked with a PR comment explaining why.

---

## Emergency: Rollback a Ring

If something goes wrong after deploying to a ring:

**Option A: Quick rollback**
```
Run: Actions → Manual Cohort Cleanup → PR number
```
This removes overrides immediately. Tenants revert to master image.

**Option B: Fix forward**
```bash
git checkout ring-<name>
# fix the issue
git push origin ring-<name>
```
New build triggers, then auto-deploys fix to all labeled cohorts.

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
