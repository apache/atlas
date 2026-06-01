/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

# Atlas Git hooks (dashboard, dashboardv2, docs)

Hooks run **locally** before `git commit` so common issues are caught early.
**Pre-push runs no checks** (all dashboard verification is on pre-commit).
**CI** on the server is still required to enforce merges.

## One-time setup (per clone)

From the **Atlas repo root** (`atlas/`, where `.git` lives):

```bash
git config core.hooksPath .githooks
```

Or run **`npm install`** inside **`dashboard/`** — the **`prepare`** script runs
`dashboard/scripts/install-git-hooks.mjs`, which sets **`core.hooksPath=.githooks`**
when Git config is writable.

Verify:

```bash
git config --get core.hooksPath
# expect: .githooks
```

The active hook scripts live in **`.githooks/`** at the **repository root**.
`dashboard/.githooks/*` only forwards to the root hooks (legacy path compat).

**This repo does not use Husky.** Git invokes hooks via **`core.hooksPath=.githooks`**
only (`prepare`/`install-git-hooks.mjs` or manual `git config`); there is no
`husky` npm package.

## What runs when

### `pre-commit` (root: `scripts/git-hooks/run-precommit.mjs`)

Runs **only for packages that have staged paths** under that prefix.

| Area | When staged under … | Checks |
|------|---------------------|--------|
| **dashboard** | `dashboard/` | (1) **UI test guard** — staged UI changes must include a **staged** test file; staged UI files must have colocated `__tests__` or `*.test.ts(x)` on disk; (2) **RAT-aligned ASF license** on **new** files under `dashboard/src/`; (3) **`jest --findRelatedTests`** on staged `.ts`/`.tsx` under `src/`; (4) **`eslint src`** (full `src/` tree); (5) **`npm run typecheck`** (`tsc --noEmit`). |
| **dashboardv2** | `dashboardv2/` | (1) **ASF license** on **new** `.js`/`.jsx`/`.ts`/`.tsx` (skips `node_modules`, `bin/`, `external_lib`, `.min.js`); (2) **`node --check`** on staged plain `.js` under `dashboardv2/public/js/` (syntax). |
| **docs** | `docs/` | (1) **ASF license** on **new** sources (skips `node_modules`, `site/`, `bin/`, `docz-lib/`); (2) **`node --check`** on staged **plain** `docs/**/*.js` outside theme/webapp JSX trees. |

### `pre-push` (root: `scripts/git-hooks/run-prepush.mjs`)

**No checks.** Exits immediately. Use pre-commit before each commit, or CI on
the server for push/merge validation.

## Skip hooks (emergency / slow machines)

Disable **everything**:

```bash
SKIP_ATLAS_HOOKS=1 git commit ...
```

Per **package**:

```bash
SKIP_DASHBOARD_HOOKS=1 git commit ...
SKIP_DASHBOARDV2_HOOKS=1 git commit ...
SKIP_DOCS_HOOKS=1 git commit ...
```

**dashboard** only:

```bash
SKIP_DASHBOARD_TEST_GUARD=1 git commit ...   # UI ↔ test rules
SKIP_DASHBOARD_LICENSE_CHECK=1 git commit ... # RAT-aligned ASF on new dashboard/src
SKIP_DASHBOARD_TYPECHECK=1 git commit ...     # tsc on commit
```

**dashboardv2 / docs** ASF license on new files:

```bash
SKIP_ATLAS_LICENSE_CHECK=1 git commit ...
```

## Manual run (no Git hook)

From **repo root** `atlas/`:

```bash
node scripts/git-hooks/run-precommit.mjs
```

**dashboard**-only local verify:

```bash
cd dashboard && npm run verify:precommit
```

(`verify:precommit` requires the npm script in `dashboard/package.json`.)

## Limitations

- **dashboardv2** has no ESLint in-repo; **`node --check`** only catches **syntax** on selected `.js` paths, not style.
- **docs** JSX/theme files are not run through `node --check`.
- Hooks can be bypassed with env vars; **rely on CI** for PR enforcement.

## Files (reference)

| Path | Role |
|------|------|
| `.githooks/pre-commit` | Root hook → `run-precommit.mjs` |
| `.githooks/pre-push` | Root hook → `run-prepush.mjs` (no-op) |
| `scripts/git-hooks/run-precommit.mjs` | Monorepo pre-commit orchestration |
| `scripts/git-hooks/run-prepush.mjs` | No-op (checks moved to pre-commit) |
| `scripts/git-hooks/check-added-license-generic.mjs` | ASF header for v2/docs new files |
| `scripts/git-hooks/syntax-check-staged.mjs` | `node --check` for v2/docs |
| `scripts/git-hooks/lib/git-helpers.mjs` | `git diff` helpers |
| `scripts/git-hooks/lib/extra-license-skip.mjs` | Path skip rules for v2/docs |
| `dashboard/scripts/install-git-hooks.mjs` | Sets `core.hooksPath=.githooks` |
| `dashboard/scripts/git-precommit-verify.mjs` | Dashboard UI ↔ test guard |
| `dashboard/scripts/check-staged-new-file-license.mjs` | Dashboard ASF on new files |
| `dashboard/scripts/git-precommit-tests-lint.mjs` | Dashboard Jest + ESLint |
| `dashboard/scripts/run-precommit-local.mjs` | Manual pre-commit verify (dashboard) |
| `dashboard/lint-staged.config.mjs` | Legacy ESLint-on-staged config (unused by hooks) |
