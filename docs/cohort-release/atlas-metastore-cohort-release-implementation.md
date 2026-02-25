# Atlas-Metastore Cohort Release Implementation

This document describes the service-based ring release (cohort release) system implemented for `atlas-metastore`, modeled after the existing implementation in `heracles`.

## Background

The cohort release system allows developers to deploy changes to a limited set of tenants via `ring-*` branches before going GA (merging to `master`). The flow is:

1. Feature branch merged to `ring-*` branch
2. PR opened from `ring-*` to `master`
3. Docker image built on push to `ring-*`
4. Cohort label added to PR (e.g., `cohort:github:path:internal-level-1`)
5. GitHub Action triggers a Temporal workflow and exits immediately (fire-and-forget for cost optimization)
6. Temporal patches ArgoCD Application manifests per tenant to override the image
7. Temporal waits for ArgoCD sync, then verifies StatefulSet rollout on tenant cluster
8. Temporal posts final release status as PR comment
9. On PR close without merge (abandoned ring), overrides are auto-cleaned up
10. On PR merge (GA), developer manually cleans up after GA chart rolls out

## Workflow Diagram

![Atlas-Metastore Ring-Based Cohort Release](metastore-ring-based-cohort-release.png)

---

## How Heracles Implements It (Reference)

### GitHub Actions (in `heracles` repo)

| Workflow | File | Purpose |
|----------|------|---------|
| Package Docker Image | `main.yml` | Builds multi-arch Docker images; triggers on `ring-*` branches |
| PR Label Release | `pr-label-release.yml` | Orchestrates release on label add or build completion |
| PR Close Release | `pr-close-release.yml` | Triggers cleanup when ring PR is merged/closed |
| Manual Cohort Cleanup | `manual-cohort-cleanup.yml` | Manual override cleanup for edge cases |

Heracles uses **reusable workflows** from `atlan-releases` repo (`get-pr-no.yml`, `check-ring-release-conditions.yml`, `parse-cohort-labels.yml`, `trigger-temporal-release.yml`).

### Key Difference: Atlas-metastore Cannot Use Reusable Workflows

Atlas-metastore is a **public** repository. The `atlan-releases` repo is **private**. GitHub Actions does not allow public repos to call reusable workflows from private repos, even within the same organization. The atlan-releases settings allow access "from repositories in the atlanhq organization" but restrict it to **private repositories only**.

Therefore, all workflow logic from the four reusable workflows is **inlined** directly into atlas-metastore's workflow files. VPN connectivity uses `openconnect` directly (same approach as `maven.yml` smoke tests) instead of `atlanhq/github-actions/globalprotect-connect-action` (also private).

---

## Key Architectural Differences: Heracles vs Atlas-Metastore

| Aspect | Heracles | Atlas-Metastore |
|--------|----------|-----------------|
| Language | Go | Java (Maven) |
| Build workflow file | `main.yml` | `maven.yml` |
| Build workflow name | `Package Docker Image` | `Java CI with Maven` |
| Default branch | `main` | `master` |
| Build architecture | Separate amd64/arm64 jobs + manifest | Single QEMU multi-arch build |
| Build time | ~5 min | ~25-30 min |
| Repo visibility | Private | Public |
| Workflow approach | Reusable workflows from atlan-releases | Inlined (public/private restriction) |
| Helm chart location | Local subchart in `atlan` repo (`file://../subcharts/heracles`) | OCI chart published from atlas-metastore repo (`oci://ghcr.io/atlanhq/helm-charts`) |
| Image in atlan values.yaml | `heracles.image.repository` / `heracles.image.tag` (directly set) | Not set — image is baked into the OCI chart during `helm-publish` |
| Chart values nesting | Flat: `.Values.image.repository` | Nested: `.Values.atlas.image.repository` |
| ArgoCD override path | `valuesObject.heracles.image` | `valuesObject.atlas.atlas.image` (extra nesting) |
| PR close behavior | Auto-cleanup on any close (merge or not) | Auto-cleanup only on close-without-merge (avoids GA rollback) |

---

## What Was Implemented

### Changes in `atlas-metastore`

| File | Change |
|------|--------|
| `.github/workflows/maven.yml` | Added `ring-*` to build trigger branches; skip `helm-publish` on ring branches |
| `.github/workflows/pr-label-release.yml` | New — triggers Temporal and exits (fire-and-forget for cost optimization) |
| `.github/workflows/pr-close-release.yml` | New — auto-cleanup on close-without-merge only |
| `.github/workflows/manual-cohort-cleanup.yml` | New — manual cleanup via workflow_dispatch |

### Changes in `platform-temporal-workflows`

| File | Change |
|------|--------|
| `service-release/types.go` | Added `ServiceImageValuePath` map and `GetServiceImagePath()` for per-service image path configuration |
| `service-release/activities.go` | Updated `buildValuesObjectPatchOperations` and `buildValuesStringPatchOperations` to use dynamic path |
| `service-release/argocd_activities.go` | Updated `validateImageConfiguration` to traverse nested paths; added `WaitForArgoCDSyncActivity` for StatefulSet sync verification |
| `service-release/app_workflow.go` | Added atlas-specific gates: ArgoCD sync wait + StatefulSet rollout verification (routed to tenant worker) |
| `service-release/statefulset_rollout_activity.go` | New — verifies StatefulSet rollout on tenant cluster with dynamic timeout (15 min/pod) |
| `ring-branch-sync/activities.go` | Added `"atlanhq/atlas-metastore": "master"` to `RingBranchSyncRepos` |
| `cmd/tenants-worker/main.go` | Registered `WaitForStatefulSetRolloutActivity` for tenant workers |
| `pkg/k8/client.go` | Added `GetStatefulSetRolloutStatus()` method |

### Changes in `atlan-releases`

| File | Change |
|------|--------|
| `.github/workflows/check-ring-release-conditions.yml` | Added `build_workflow_id` input parameter (default: `main.yml`) |
| `.github/workflows/trigger-temporal-release.yml` | Added `service_name` input parameter; decoupled from `imageRepo` |
| `cohorts/atlas-dummy-1.json` | New — test cohort for atlas-metastore |

Note: The atlan-releases changes are for potential future use by other private service repos. Atlas-metastore does not use these reusable workflows due to the public/private restriction.

---

## GitHub Actions Cost Optimization

GitHub Actions charges per minute of execution time. Initial implementations had GitHub Actions wait for Temporal to complete (potentially 20+ minutes per release), accumulating significant costs.

### Solution: Fire-and-Forget with Temporal Verification

The `pr-label-release.yml` workflow now:
1. Triggers the Temporal workflow
2. Posts an initial "Ring Release Triggered" comment
3. Exits immediately (~1 min total)

Temporal handles the full release lifecycle:
1. Patches ArgoCD Application manifests
2. Waits for ArgoCD to sync the StatefulSet resource (5 min initial delay + 60 min polling)
3. Routes `WaitForStatefulSetRolloutActivity` to tenant worker for direct K8s access
4. Verifies all pods are updated with correct image (dynamic timeout: 15 min/pod, max 120 min)
5. Posts final "Service Release Result" comment to PR

**Note:** For cleanup flows (PR close or manual cleanup), verification is skipped. Once the image override is removed from ArgoCD, the workflow completes immediately. ArgoCD handles the actual rollback asynchronously.

### Multi-Worker Architecture

The release verification uses Temporal's task queue routing:

```
Control-Plane Worker (platform cluster)
├── ServiceReleaseWorkflow (parent)
├── ServiceReleasePatchWorkflow (per tenant)
├── GetArgoCDAppManifestActivity
├── CreateImagePatchActivity
├── ApplyKubectlPatchActivity
├── WaitForArgoCDSyncActivity
└── (routes to tenant worker) ──┐
                                │
Tenant Worker (tenant vCluster) ◄┘
└── WaitForStatefulSetRolloutActivity
    └── Direct K8s access to verify StatefulSet rollout
```

This architecture ensures:
- ArgoCD patching happens on control-plane (has ArgoCD access)
- Pod verification happens on tenant worker (has direct K8s access to tenant cluster)
- GitHub Actions billing is minimized (~1 min per release)

### Timeout Configuration

| Stage | Timeout | Notes |
|-------|---------|-------|
| ArgoCD sync initial delay | 5 min | Wait for ArgoCD to process refresh |
| ArgoCD sync polling | 60 min | Polls every 15s for StatefulSet sync status |
| StatefulSet rollout per pod | 15 min | Dynamic: `replicas × 15 min` |
| StatefulSet rollout minimum | 15 min | Floor for small deployments |
| StatefulSet rollout maximum | 120 min | Cap for large deployments |

**Cleanup flows** (PR close, manual cleanup): Verification is skipped entirely. The workflow removes the image override and exits immediately without waiting for ArgoCD sync or pod rollout.

---

## Bugs Found and Fixed During Testing

### 1. Build check race condition (removed push trigger)

The `push` trigger on `pr-label-release.yml` used `lewagon/wait-on-check-action` to wait for the `build` check. But atlas-metastore's `build` job depends on `changes` and `helm-lint` completing first, so the check doesn't exist immediately. The wait action would fail with "The requested check was never run against this ref."

**Fix:** Removed the `push` trigger entirely. The `workflow_run` trigger (fires after Maven completes) is the reliable path. The `labeled` trigger handles label-after-build scenarios.

### 2. SHA validation — premature release from stale build

The `check-prerequisites` step filtered workflow runs by `branch` name, which could match old completed runs from a previous branch with the same name (e.g., reusing `ring-test`). This allowed releases to trigger before the current commit's build completed, patching ArgoCD with a non-existent image tag.

**Fix:** Changed to filter by `head_sha` instead of `branch`, ensuring only the current commit's build is considered.

### 3. `workflow_run` context — wrong branch name

For `workflow_run` events, `context.ref` resolves to `refs/heads/master` (the default branch), not the ring branch that triggered the build. The `get-pr-number` job used `context.ref` to find the PR, resulting in looking for a PR from `master` instead of `ring-test`.

**Fix:** For `workflow_run` events, use `context.payload.workflow_run.head_branch` to get the actual ring branch name.

---

## GA Rollback Race Condition

This is a design-level concern that affects all services using the cohort release flow, but is significantly amplified for atlas-metastore due to its longer build time.

### The Problem

When a ring PR is merged to `master`, two things fire simultaneously:

**Path A — Cleanup (fast, ~2-3 min):**

1. `pr-close-release.yml` triggers on PR close
2. Temporal removes image overrides from all cohort tenants
3. Cohort tenants revert to the **old master image** (the one before the merge)
4. The feature under test is **gone** from these tenants

**Path B — GA release (slow, 30 min to hours):**

1. Push to `master` triggers `maven.yml` (~30 min build)
2. `chart-release-dispatcher` fires after build completes
3. PR created on `atlan` repo to update the OCI chart version
4. PR needs **manual review and merge**
5. ArgoCD syncs the new chart version to tenants
6. Feature is **restored** via the GA image

### Our Solution

Atlas-metastore's `pr-close-release.yml` only auto-cleans up on **close without merge** (abandoned ring PRs). On merge, overrides stay until the developer runs `manual-cohort-cleanup.yml` after confirming the GA chart has rolled out.

```yaml
if: startsWith(github.head_ref, 'ring-') && github.event.pull_request.merged == false
```

Note: Heracles still auto-cleans up on any close (merge or not). The window is smaller (~5 min build) but the risk exists there too.

---

## Edge Cases and Operational Risks

### Edge Case 1: `atlas-read` not being overridden

Atlas-metastore publishes **two** application charts — `atlas` (write path) and `atlas-read` (read path) — both built from the same Java codebase. The Temporal workflow with `serviceName = "atlas"` only patches the `atlas` subchart's image. `atlas-read` is left untouched.

**Impact:** High. Most atlas-metastore features touch both write and read paths. Cohort tenants would run the ring image on atlas but the old master image on atlas-read.

**Possible solutions:**
- Trigger two Temporal releases per cohort — one for `atlas`, one for `atlas-read`
- Extend the Temporal workflow to accept a list of service names
- Handle it in `pr-label-release.yml` by triggering twice with different service names

### Edge Case 2: Helm template or config changes not reaching cohort tenants

The cohort release only overrides the Docker **image**. The **chart version** remains whatever `master` last published. Helm template changes in the ring branch (new env vars, volume mounts, config) won't reach cohort tenants.

**Mitigation:** Design features to be backward-compatible with master chart templates (e.g., use runtime feature flags rather than helm-injected env vars).

### Edge Case 3: Two ring branches targeting the same cohort

If two ring branches try to release to the same cohort, the second one fails with: `image already overridden for service atlas: found .../ring-A, want .../ring-B`

**Workaround:** Use different cohorts for different ring branches, or merge both features into the same ring branch.

### Edge Case 4: Cohort label accidentally removed from PR

Removing a label doesn't trigger cleanup. Tenants from that cohort stay on the ring image until the PR is closed or cleanup is run manually.

**Mitigation:** Re-add the label, or use `manual-cohort-cleanup.yml`.

### Limitation: ArgoCD behaviour (tenant umbrella app)

Verification depends on the tenant's ArgoCD application. Today, atlas is deployed as part of a **large umbrella chart** (many subcharts: redis, kafka, cassandra, keycloak, etc.). This leads to two limitations:

1. **Hung or slow Argo app:** If the tenant Argo app is hung or does not sync the StatefulSet within the allowed timeout (5 min initial delay + 60 min polling), `WaitForArgoCDSyncActivity` fails and the release is reported as failed even if the patch was applied.

2. **Sync contention:** When many resources are syncing, ArgoCD may take a long time to reach the atlas StatefulSet. For a smoother flow, **add the cohort label when no other syncs are in progress** on the target tenant app (e.g., avoid adding a label right after a large chart or config change).

**Planned resolution (Phase 2):** Split metastore components into a **separate ArgoCD application** so that atlas is not under the huge umbrella chart. ArgoCD will then sync a smaller, focused app and verification will be more reliable and faster.

---

## Developer Guide: How to Use Cohort Releases

### When to Use

Use a cohort release when:
- Your change is large or high-risk and preprod alone isn't enough confidence
- You want to validate on real production tenants before going GA
- The change touches critical paths (entity processing, search, auth, etc.)

You do **not** need a cohort release for every change. Small, well-tested changes can go directly to `master` through the normal flow.

### Important: Don't Develop on Ring Branches

Every push to a `ring-*` branch triggers a full Maven build (~30 min) and, if cohort labels are present, a release to all labeled cohorts. Do not use ring branches for active development.

```
Correct flow:
  feature branch → test on beta/staging → merge to ring → one release

Wrong flow:
  ring branch → push, push, push (each triggers 30 min build + release)
```

### Step-by-Step Process

#### 1. Develop and Test Your Feature

```bash
# Work on your feature branch as usual
git checkout -b feat/my-feature
# ... develop, test locally, push ...
# Get it tested on beta/staging through the normal flow
```

#### 2. Create a Ring Branch

Once your feature is tested and ready for cohort release:

```bash
git checkout master && git pull
git checkout -b ring-my-feature
git merge feat/my-feature
git push origin ring-my-feature
```

This triggers the Maven build. Wait ~30 min for the Docker image to be built and pushed.

#### 3. Open a PR to Master

Open a PR: `ring-my-feature → master`

This PR is your **control surface** — labels on it control where the image is deployed, and results are posted as PR comments.

#### 4. Add a Cohort Label

Add a label to the PR to specify which tenants to release to:

```
cohort:github:path:internal-level-1
```

Label format: `cohort:<source>:<key>:<value>`

Available cohorts (defined in `atlan-releases/cohorts/`):
- `internal-level-1` — Internal low-risk tenants
- `internal-level-2` — Internal broader set
- `partner-level-1` — Partner tenants
- `enterprise-level-1` — Enterprise tenants (use with caution)

You can add the label before or after the build completes. The release only proceeds once the build succeeds.

#### 5. Monitor the Release

- **GitHub Actions tab:** Watch `PR Label Release` workflow (triggers Temporal and exits quickly)
- **Temporal UI:** Check `ServiceReleaseWorkflow` at https://temporal.atlan.com/namespaces/default/workflows
- **PR comments:** Two comments from `atlan-ci`:
  1. **"Ring Release Triggered"** — posted immediately when GitHub Actions triggers Temporal
  2. **"Service Release Result"** — posted by Temporal after rollout verification completes
- **Slack:** Results posted to `#testing_notifications`

#### 6. Verify on Tenants

The cohort tenants are now running your ring image. Verify your feature works as expected on real production data.

#### 7. Expand to More Cohorts (Optional)

If the initial cohort looks good, add more labels:

```
cohort:github:path:partner-level-1
```

Each new label triggers a release to that cohort. Existing cohort overrides remain untouched.

#### 8. Fix Issues (If Needed)

If you find a bug during cohort testing:

```bash
# Fix on your feature branch first
git checkout feat/my-feature
# ... fix the bug ...
git push origin feat/my-feature

# Then merge the fix into the ring branch
git checkout ring-my-feature
git merge feat/my-feature
git push origin ring-my-feature
```

The push triggers a new build. Once complete, the updated image is automatically released to all labeled cohorts.

#### 9. Go GA

When you're confident the feature is ready:

1. **Merge the ring PR to `master`** — this starts the normal GA flow (master build → chart update → ArgoCD sync to all tenants)
2. **Wait for the GA chart to roll out** — check the `atlan` repo for the chart update PR, ensure it's merged and ArgoCD has synced
3. **Clean up overrides** — go to the atlas-metastore repo Actions tab, run `Manual Cohort Cleanup` workflow with the PR number

> **Why manual cleanup?** If overrides were auto-removed on merge, cohort tenants would briefly revert to the old master image during the ~30 min gap before the new GA build completes. Manual cleanup avoids this rollback.

#### 10. Abandon a Ring (If Needed)

If you decide not to proceed with the feature:

1. **Close the PR without merging**
2. Overrides are automatically removed from all cohort tenants
3. Tenants revert to the current master image
4. Delete the ring branch

### Quick Reference

| Action | What Happens |
|--------|-------------|
| Push to `ring-*` | Maven build triggers (~30 min) |
| Add cohort label to PR | Release triggers (after build completes) |
| Push new commit to ring | Rebuild + re-release to all labeled cohorts |
| Add another cohort label | Release to new cohort (existing ones untouched) |
| Merge ring PR | GA flow starts, overrides stay until manual cleanup |
| Close ring PR without merge | Auto-cleanup, tenants revert to master |
| Run Manual Cohort Cleanup | Removes overrides from specified ring's tenants |

### Things to Know

- **Build time is ~30 min.** Plan accordingly. Don't expect instant releases.
- **Only the Docker image is overridden.** Helm template changes, config changes, new env vars in your ring branch won't reach cohort tenants. Design features to be backward-compatible with master chart templates.
- **One ring per cohort.** If another ring is already deployed to a cohort, your release to that cohort will fail. Use a different cohort or coordinate with the other developer.
- **Ring branches auto-sync with master.** The `RingBranchSyncWorkflow` periodically merges master into ring branches (using `-X ours` strategy — your ring changes win on conflicts).
- **Max 20 tenants per cohort.** The Temporal workflow caps cohort size to keep releases manageable.
- **`atlas-read` is not overridden.** Currently only the `atlas` chart image is patched. If your feature affects the read path, be aware that `atlas-read` will still run the master image.

### Required Secrets

| Secret | Purpose |
|--------|---------|
| `ORG_PAT_GITHUB` | GitHub API access for PR details, label parsing, workflow run checks |
| `GLOBALPROTECT_USERNAME` | VPN credentials for GitHub Actions runner to reach Temporal server |
| `GLOBALPROTECT_PASSWORD` | VPN credentials (paired with above) |

---
