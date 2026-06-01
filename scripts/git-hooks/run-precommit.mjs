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

/**
 * Repo-wide pre-commit: dashboard (full), dashboardv2 + docs (license, syntax).
 */

import { execFileSync } from 'node:child_process'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { verifyAddedFilesAspLicense } from './check-added-license-generic.mjs'
import {
	shouldSkipLicenseDashboardv2,
	shouldSkipLicenseDocs,
} from './lib/extra-license-skip.mjs'
import { getRepoRoot, getStagedFiles } from './lib/git-helpers.mjs'
import {
	syntaxCheckDashboardv2Staged,
	syntaxCheckDocsStaged,
} from './syntax-check-staged.mjs'

const __dirname = dirname(fileURLToPath(import.meta.url))
const scriptsDir = join(__dirname, '..')
const repoRoot = getRepoRoot(scriptsDir)

if (
	process.env.SKIP_ATLAS_HOOKS === '1' ||
	process.env.SKIP_ALL_ATLAS_GIT_HOOKS === '1'
) {
	process.exit(0)
}

const staged = getStagedFiles(repoRoot)
const touchDashboard = staged.some((p) => p.startsWith('dashboard/'))
const touchV2 = staged.some((p) => p.startsWith('dashboardv2/'))
const touchDocs = staged.some((p) => p.startsWith('docs/'))

const runDash = (title, file) => {
	console.log(`\x1b[36m▶\x1b[0m [dashboard] ${title}`)
	execFileSync(process.execPath, [join(repoRoot, 'dashboard', 'scripts', file)], {
		cwd: join(repoRoot, 'dashboard'),
		stdio: 'inherit',
	})
}

if (touchDashboard && process.env.SKIP_DASHBOARD_HOOKS !== '1') {
	runDash('UI ↔ staged test guard', 'git-precommit-verify.mjs')
	runDash('ASF license (new staged files under src/)', 'check-staged-new-file-license.mjs')
	runDash('Jest (related tests) + ESLint (src/)', 'git-precommit-tests-lint.mjs')

	if (process.env.SKIP_DASHBOARD_TYPECHECK !== '1') {
		execFileSync(
			process.platform === 'win32' ? 'npm.cmd' : 'npm',
			['run', 'typecheck'],
			{
				cwd: join(repoRoot, 'dashboard'),
				stdio: 'inherit',
				shell: process.platform === 'win32',
			},
		)
	}
}

if (touchV2 && process.env.SKIP_DASHBOARDV2_HOOKS !== '1') {
	if (process.env.SKIP_ATLAS_LICENSE_CHECK !== '1') {
		verifyAddedFilesAspLicense({
			label: 'dashboardv2',
			shouldSkip: (p) => shouldSkipLicenseDashboardv2(p),
			extensions: new Set(['.js', '.jsx', '.ts', '.tsx']),
			repoRoot,
		})
	}
	syntaxCheckDashboardv2Staged(repoRoot)
}

if (touchDocs && process.env.SKIP_DOCS_HOOKS !== '1') {
	if (process.env.SKIP_ATLAS_LICENSE_CHECK !== '1') {
		verifyAddedFilesAspLicense({
			label: 'docs',
			shouldSkip: (p) => shouldSkipLicenseDocs(p),
			extensions: new Set(['.js', '.jsx', '.ts', '.tsx']),
			repoRoot,
		})
	}
	syntaxCheckDocsStaged(repoRoot)
}

process.exit(0)
