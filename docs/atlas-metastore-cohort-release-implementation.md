# Atlas-Metastore Cohort Release Implementation Plan

This document captures the analysis and required changes to enable the service-based ring release (cohort release) for `atlas-metastore`, modeled after the existing implementation in `heracles`.

## Background

The cohort release system allows developers to deploy changes to a limited set of tenants via `ring-*` branches before going GA (merging to `master`/`main`). The flow is:

1. Feature branch merged to `ring-*` branch
2. PR opened from `ring-*` to default branch
3. Docker image built on push to `ring-*`
4. Cohort label added to PR (e.g., `cohort:github:path:internal-level-1`)
5. GitHub Action triggers a Temporal workflow (`ServiceReleaseWorkflow`)
6. Temporal patches ArgoCD Application manifests per tenant to override the image
7. On PR merge/close, overrides are cleaned up

This flow works for heracles today. Enabling it for atlas-metastore requires changes in atlas-metastore (which we own) and coordination with two other repositories.

---

## How Heracles Implements It

### GitHub Actions (in `heracles` repo)

| Workflow | File | Purpose |
|----------|------|---------|
| Package Docker Image | `main.yml` | Builds multi-arch Docker images; triggers on `ring-*` branches |
| PR Label Release | `pr-label-release.yml` | Orchestrates release on label add or build completion |
| PR Close Release | `pr-close-release.yml` | Triggers cleanup when ring PR is merged/closed |
| Manual Cohort Cleanup | `manual-cohort-cleanup.yml` | Manual override cleanup for edge cases |

### Reusable Workflows (in `atlan-releases` repo)

| Workflow | Purpose |
|----------|---------|
| `get-pr-no.yml` | Finds PR number from branch context |
| `check-ring-release-conditions.yml` | Validates: ring branch? cohort labels? successful build? |
| `parse-cohort-labels.yml` | Parses `cohort:source:key:value` labels into JSON |
| `trigger-temporal-release.yml` | Constructs image details and triggers Temporal workflow |

### Temporal Workflows (in `platform-temporal-workflows` repo)

| Workflow | Purpose |
|----------|---------|
| `ServiceReleaseWorkflow` | Parent: resolves tenants, spawns child per tenant, aggregates results |
| `ServiceReleasePatchWorkflow` | Child: fetches ArgoCD manifest, creates JSON patch, applies via kubectl |
| `RingBranchSyncWorkflow` | Scheduled: keeps ring branches synced with default branch |

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
| Helm chart location | External subchart in `atlan` repo (`file://../subcharts/heracles`) | OCI chart published from atlas-metastore repo (`oci://ghcr.io/atlanhq/helm-charts`) |
| Image in atlan values.yaml | `heracles.image.repository` / `heracles.image.tag` (directly set) | Not set — image is baked into the OCI chart during `helm-publish` |
| Chart values nesting | Flat: `.Values.image.repository` | Nested: `.Values.atlas.image.repository` |
| ArgoCD override path | `valuesObject.heracles.image` | `valuesObject.atlas.atlas.image` (extra nesting) |

---

## Part 1: Implementation in `atlas-metastore`

These are straightforward implementation tasks. We have full control over this repo.

### 1.1 Add `ring-*` to build trigger branches

**File:** `.github/workflows/maven.yml`

Add `- ring-*` to the `on.push.branches` list so Docker images get built for ring branches.

### 1.2 Skip `helm-publish` on `ring-*` branches

**File:** `.github/workflows/maven.yml`

The `helm-publish` job currently runs on any successful build. For ring branches, this unnecessarily publishes OCI charts and creates GitHub Releases. The cohort release patches ArgoCD directly and doesn't use helm charts.

Add a condition to skip `helm-publish` when the branch matches `ring-*`:

```yaml
if: |
  always() &&
  !startsWith(github.ref_name, 'ring-') &&
  needs.helm-lint.result == 'success' && ...
```

### 1.3 Create `pr-label-release.yml`

**File:** `.github/workflows/pr-label-release.yml` (new)

Adapted from heracles with these atlas-metastore specific adjustments:
- **Base branch:** `master` (not `main`)
- **`workflow_run` trigger:** watches `"Java CI with Maven"` (not `"Package Docker Image"`)
- **`wait-for-docker-build`:** waits for a single `build` check (not separate `build (amd64)`, `build (arm64)`, `create-manifest`)
- **`check-prerequisites`:** needs to pass `build_workflow_id: 'maven.yml'` (see Part 2, Problem 1)
- **`trigger-release`:** needs to pass `service_name: 'atlas'` (see Part 2, Problem 2)

### 1.4 Create `pr-close-release.yml`

**File:** `.github/workflows/pr-close-release.yml` (new)

Adapted from heracles. Only change: target `master` instead of `main`.

### 1.5 Create `manual-cohort-cleanup.yml`

**File:** `.github/workflows/manual-cohort-cleanup.yml` (new)

Can be copied from heracles mostly as-is. Provides a manual trigger to clean up overrides for a specific ring.

### 1.6 Repository setup (manual)

- Create labels: `cohort:github:path:internal-level-1`, `cohort:github:path:partner-level-1`, etc.
- Verify secrets exist: `ORG_PAT_GITHUB`, `GLOBALPROTECT_USERNAME`, `GLOBALPROTECT_PASSWORD`
- Verify `SERVICE_RELEASE_GITHUB_TOKEN` on Temporal side has read/write access to atlas-metastore (the reference doc says it's already added)

---

## Part 2: Changes needed in `atlan-releases` (needs coordination with repo owners)

These are changes to shared reusable workflows. They must be backward-compatible with heracles.

### Problem 1: Hardcoded `workflow_id: 'main.yml'` in `check-ring-release-conditions.yml`

**File:** `.github/workflows/check-ring-release-conditions.yml` (line 69)

The reusable workflow checks if the build has completed successfully by looking for workflow runs of `main.yml`:

```javascript
const { data: workflows } = await github.rest.actions.listWorkflowRuns({
    owner: context.repo.owner,
    repo: context.repo.repo,
    workflow_id: 'main.yml',     // <-- hardcoded
    branch: headRef,
    status: 'completed',
    per_page: 1
});
```

Atlas-metastore's build workflow file is `maven.yml`. This check will always return zero results, setting `should_proceed = false` and blocking the entire release.

**Suggested fix:** Add an input parameter `build_workflow_id` (default: `'main.yml'`) so callers can override it. This is backward-compatible — heracles doesn't pass it and gets the default.

```yaml
# In workflow_call inputs:
build_workflow_id:
  type: string
  required: false
  default: 'main.yml'

# In the script:
workflow_id: '${{ inputs.build_workflow_id }}',
```

### Problem 2: `serviceName` derived from repo name in `trigger-temporal-release.yml`

**File:** `.github/workflows/trigger-temporal-release.yml` (line 87)

The workflow derives the service name from `context.repo.repo`:

```javascript
const serviceName = context.repo.repo;  // produces "atlas-metastore"
```

This causes two downstream failures:

1. **Temporal validation failure:** `SupportedServices = ["heracles", "atlas"]` in the Temporal code. `"atlas-metastore"` is not in the list. The workflow immediately errors with `serviceName must be one of: [heracles atlas]`.

2. **Wrong ArgoCD patch path:** Even if validation passed, the patch path would target `valuesObject.atlas-metastore.image` which doesn't exist in any ArgoCD Application manifest. The subchart key in the atlan chart is `atlas`, not `atlas-metastore`.

**Suggested fix:** Add an optional `service_name` input. If provided, use it instead of `context.repo.repo`. Importantly, `imageRepo` must still use the actual repo name for the Docker image path, so the two need to be decoupled:

```javascript
const repoName = context.repo.repo;                             // "atlas-metastore" — for image path
const serviceName = '${{ inputs.service_name }}' || repoName;   // "atlas" — for ArgoCD patching
const imageRepo = `ghcr.io/atlanhq/${repoName}-${ringName}`;   // correct image path
```

This is backward-compatible — heracles doesn't pass `service_name` and gets `context.repo.repo` = `"heracles"` which matches its subchart name.

---

## Part 3: Changes needed in `platform-temporal-workflows` (needs coordination with repo owners)

These are changes to the Temporal workflow's ArgoCD patching logic and ring branch sync configuration.

### Problem 3: Wrong ArgoCD patch path for nested chart values

**File:** `go/workflows/control-plane/service-release/activities.go` (line 196)

The `buildValuesObjectPatchOperations` function constructs the patch path as:

```go
basePath := fmt.Sprintf("/spec/source/helm/valuesObject/%s/image", params.ServiceName)
```

For heracles (`serviceName = "heracles"`), this produces `/spec/source/helm/valuesObject/heracles/image`, which correctly maps to the Helm value `heracles.image` because the heracles chart uses `.Values.image.repository` at its root level.

For atlas (`serviceName = "atlas"`), this produces `/spec/source/helm/valuesObject/atlas/image`. But the atlas chart's StatefulSet template reads:

```yaml
image: "{{ .Values.atlas.image.repository }}:{{ .Values.atlas.image.tag }}"
```

In ArgoCD, subchart values are scoped under the subchart name. The atlas subchart's `.Values.atlas.image` maps to `valuesObject.atlas.atlas.image` in the ArgoCD Application. The correct patch path should be:

```
/spec/source/helm/valuesObject/atlas/atlas/image
```

The current code patches one level too shallow. The override won't reach the StatefulSet — it creates a dangling `atlas.image` key at the chart root that nothing reads.

**This same nesting issue affects three places:**

| Location | File | Line | Current path | Needed path for atlas |
|----------|------|------|-------------|----------------------|
| `buildValuesObjectPatchOperations` | `activities.go` | 196 | `/{serviceName}/image` | `/{serviceName}/atlas/image` |
| `buildValuesStringPatchOperations` | `activities.go` | 229 | `values[serviceName]["image"]` | `values[serviceName]["atlas"]["image"]` |
| `validateImageConfiguration` | `argocd_activities.go` | 67 | `valuesObject[serviceName]["image"]` | `valuesObject[serviceName]["atlas"]["image"]` |

**Suggested fix:** Introduce a per-service image path configuration:

**Option A — Path mapping in code (simple, low-risk):**

```go
// Map of service name to the values sub-path where image config lives
var serviceImagePath = map[string]string{
    "heracles": "image",         // flat: valuesObject.heracles.image
    "atlas":    "atlas/image",   // nested: valuesObject.atlas.atlas.image
}

func buildValuesObjectPatchOperations(params ImagePatchParams) []PatchOperation {
    imageSubPath := serviceImagePath[params.ServiceName]
    if imageSubPath == "" {
        imageSubPath = "image" // default: flat structure
    }
    basePath := fmt.Sprintf("/spec/source/helm/valuesObject/%s/%s", params.ServiceName, imageSubPath)
    // ...
}
```

**Option B — Add image path to workflow params (more flexible, more changes):**

Add an `ImageBasePath` field to `ServiceReleaseWorkflowParams` that gets passed from the GitHub Action. More flexible for future services but requires changes in the trigger workflow too.

### Problem 4: Atlas-metastore not registered for ring branch auto-sync

**File:** `go/workflows/control-plane/ring-branch-sync/activities.go` (line 23)

```go
var RingBranchSyncRepos = map[string]string{
    "atlanhq/heracles": "main",
}
```

Only heracles is registered. Ring branches in atlas-metastore won't be auto-synced with `master`, meaning they'll diverge over time as other PRs merge to master.

**Suggested fix:** Add atlas-metastore:

```go
var RingBranchSyncRepos = map[string]string{
    "atlanhq/heracles":        "main",
    "atlanhq/atlas-metastore": "master",
}
```

---

## Summary

### What we do (atlas-metastore)

| # | Task | File | Type |
|---|------|------|------|
| 1 | Add `ring-*` to build trigger branches | `.github/workflows/maven.yml` | Modify |
| 2 | Skip `helm-publish` on `ring-*` branches | `.github/workflows/maven.yml` | Modify |
| 3 | Create PR label release workflow | `.github/workflows/pr-label-release.yml` | New |
| 4 | Create PR close release workflow | `.github/workflows/pr-close-release.yml` | New |
| 5 | Create manual cohort cleanup workflow | `.github/workflows/manual-cohort-cleanup.yml` | New |
| 6 | Create cohort labels, verify secrets | Repository settings | Manual |

### What we check with `atlan-releases` owners

| # | Problem | Suggested Fix |
|---|---------|---------------|
| 1 | `check-ring-release-conditions.yml` has hardcoded `workflow_id: 'main.yml'` | Add `build_workflow_id` input parameter (default: `'main.yml'`) |
| 2 | `trigger-temporal-release.yml` uses `context.repo.repo` as `serviceName` | Add `service_name` input parameter, decouple from `imageRepo` |

Both fixes are backward-compatible with heracles.

### What we check with `platform-temporal-workflows` owners

| # | Problem | Suggested Fix |
|---|---------|---------------|
| 3 | ArgoCD patch path assumes flat chart structure (`/{serviceName}/image`) — atlas needs `/{serviceName}/atlas/image` | Add per-service image path mapping (affects `activities.go` and `argocd_activities.go`) |
| 4 | Ring branch sync doesn't include atlas-metastore | Add `"atlanhq/atlas-metastore": "master"` to `RingBranchSyncRepos` |

---

## Recommended Implementation Order

1. **Coordinate with `platform-temporal-workflows` owners** — The image path mapping (Problem 3) is foundational. Nothing works without it.
2. **Coordinate with `atlan-releases` owners** — The input parameters (Problems 1-2) are required for our workflows to function.
3. **Implement in `atlas-metastore`** — Once the above are in place, modify `maven.yml` and create the new workflow files.
4. **Test** — Create a `ring-test` branch, push a dummy commit, add a `cohort:github:path:dummy-1` label, verify the end-to-end flow with dry-run first.

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

The gap between override removal (Path A) and GA image reaching tenants (Path B) is **at minimum 30 minutes** (build time alone), but realistically **1-2+ hours** including the manual atlan PR merge. During this window, cohort tenants that were running the feature are **rolled back** to the previous master image.

If the feature introduced schema changes, migrations, new API contracts, or state changes that aren't backward-compatible, this rollback can **actively break** those tenants.

Note: This affects heracles too, but the window is much smaller (~5 min build + local subchart update).

### Mitigation Options

**Option 1 (recommended): Don't auto-cleanup on merge — only on close-without-merge.**

Modify `pr-close-release.yml` to only trigger cleanup when the PR is **closed without merging** (abandoned ring). On merge, the developer uses `manual-cohort-cleanup.yml` after confirming the GA release has rolled out to all tenants.

```yaml
if: startsWith(github.head_ref, 'ring-') && github.event.pull_request.merged == false
```

- Pro: No race condition. Developer controls cleanup timing.
- Con: If developer forgets cleanup, overrides linger. Needs process discipline.

**Option 2: Delay cleanup in Temporal.**

When the Temporal workflow receives an empty `imageTag` (cleanup signal), add a wait/poll loop that checks whether the new master image has been built and the atlan chart PR has been merged before removing overrides.

- Pro: Fully automated, no gap.
- Con: Complex implementation. How long to poll? Temporal workflow sits idle for potentially hours.

**Option 3: Reverse the order — GA first, then close ring PR.**

Don't merge the ring PR directly. Instead:

1. Merge the feature to `master` through a separate PR
2. Wait for the master build + atlan chart PR to merge + ArgoCD sync
3. Close the ring PR (triggers cleanup, but tenants already have the GA image)

- Pro: Zero gap — tenants go from ring image directly to GA image.
- Con: Breaks the clean "merge ring PR = go GA" model. More manual steps.

**Option 4: Document the gap, let developers choose.**

No automation changes. Document clearly: "After merging the ring PR, cohort tenants will briefly revert to the previous master image. If this is unacceptable, ensure the GA chart PR is merged on atlan before merging the ring PR."

- Pro: No code changes.
- Con: Relies on developer awareness.

### Recommendation

Option 1 is the safest quick win for atlas-metastore. The manual cleanup step is a small price for avoiding a potentially dangerous rollback window. This should also be raised with the heracles/platform-services team as a general improvement to the cohort release system.

---

## Edge Cases and Operational Risks

### Edge Case 1: `atlas-read` not being overridden

Atlas-metastore publishes **two** application charts — `atlas` (write path) and `atlas-read` (read path) — both built from the same Java codebase:

```yaml
# atlan Chart.yaml
- name: atlas
  repository: oci://ghcr.io/atlanhq/helm-charts
  version: "1.0.0-master.b34e72eabcd"
- name: atlas-read
  repository: oci://ghcr.io/atlanhq/helm-charts
  version: "1.0.0-master.b34e72eabcd"
```

The Temporal workflow with `serviceName = "atlas"` only patches the `atlas` subchart's image. `atlas-read` is left untouched. This means cohort tenants would run the **ring image on atlas** but the **old master image on atlas-read**.

Since both are the same Java application with the same codebase, any feature that affects shared behavior (API contracts, data model, serialization, entity processing) will create a mismatch between the write and read paths within the same tenant. This could cause subtle bugs, data inconsistencies, or failures.

**Impact:** High. Most atlas-metastore features touch both write and read paths.

**Possible solutions:**
- Trigger two Temporal releases per cohort — one for `atlas`, one for `atlas-read`. This requires either a new workflow parameter or running the release twice with different service names.
- Extend the Temporal workflow to accept a list of service names to patch (e.g., `["atlas", "atlas-read"]`).
- Handle it in the atlas-metastore `pr-label-release.yml` by calling `trigger-temporal-release` twice with different `service_name` values.

The third option (calling the trigger twice from our workflow) is the simplest and doesn't require Temporal changes, but it doubles the release time and creates two separate state files.

### Edge Case 2: Helm template or config changes not reaching cohort tenants

The cohort release only overrides the Docker **image** in the ArgoCD Application. The **chart version** remains whatever `master` last published. If a ring branch also modifies helm templates or config, those changes won't be reflected on cohort tenants.

For example, if the leangraph feature requires a new environment variable:

```yaml
# In helm/atlas/templates/statefulset.yaml (ring branch change)
- name: LEANGRAPH_ENABLED
  value: "true"
```

Cohort tenants would run the leangraph image but with the master chart templates — the env var doesn't exist. The feature could silently not work, or the application could crash if it requires the variable.

This applies to any ring branch change that touches:
- StatefulSet templates (env vars, volume mounts, probes, resource limits)
- ConfigMaps
- Services, ingress rules
- Any other helm template

**Impact:** Medium-High. Atlas-metastore features often require config changes alongside code changes.

**Mitigation:** Developers must be aware that only image changes are tested in cohort releases. Any feature that requires helm template changes should be designed to be backward-compatible with the master chart templates (e.g., use feature flags checked at runtime rather than env vars injected by helm).

### Edge Case 3: Two ring branches targeting the same cohort

If `ring-leangraph` is deployed to `internal-level-1` and another developer tries to deploy `ring-search-fix` to the same cohort, the Temporal workflow's validation detects that the image is already overridden with a different repository:

```
image already overridden for service atlas: found .../atlas-metastore-ring-leangraph, want .../atlas-metastore-ring-search-fix
```

The second ring's release **fails** on those tenants. This is a known limitation mentioned in the reference doc — a ring branch effectively "locks" the cohorts it's deployed to.

**Impact:** Medium. Blocks parallel ring testing on overlapping tenant sets.

**Workarounds:**
- Use different cohorts for different ring branches (e.g., `internal-level-1` for one, `internal-level-2` for another).
- Merge both features into the same ring branch if they need the same cohort.

### Edge Case 4: Cohort label accidentally removed from PR

If someone removes a cohort label (e.g., `cohort:github:path:internal-level-1`) from the PR:

- **Nothing happens immediately.** The `pull_request: [labeled]` event only fires on label *add*, not removal. The existing image overrides on tenants remain in place.
- **On next ring push:** If a new commit is pushed to the ring branch, the release workflow re-triggers but only deploys to whatever labels are still on the PR. The tenants from the removed label's cohort are now stuck on a **stale ring image** — they won't get new ring updates, and they won't revert to master either.
- **Cleanup:** The stale overrides are only cleaned up when the PR is merged or closed (which triggers cleanup for all tenants across all previously released cohorts). Until then, those tenants are in a "ghost" state — detached from both the ring release and the GA track.

**Impact:** Low-Medium. Labels are fragile (no access control on who can add/remove them), but accidental removal is uncommon.

**Mitigation:** If a label is accidentally removed, re-add it. If overrides need to be removed from the "orphaned" cohort, use the `manual-cohort-cleanup.yml` workflow.

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
✅ Correct flow:
  feature branch → test on beta/staging → merge to ring → one release

❌ Wrong flow:
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

- **GitHub Actions tab:** Watch `PR Label Release` workflow progress
- **Temporal UI:** Check `ServiceReleaseWorkflow` at https://temporal.atlan.com/namespaces/default/workflows
- **PR comment:** `atlan-ci` posts a summary with status, tenant results, and workflow link
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

---
