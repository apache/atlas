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
 * Pre-push: any *new* dashboard/src source files in the push range must carry
 * a RAT-aligned Apache license header at HEAD (same policy as pre-commit).
 *
 * Skip: SKIP_DASHBOARD_HOOKS=1 | SKIP_DASHBOARD_LICENSE_CHECK=1
 */

import { execFileSync } from 'node:child_process'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { getPushAddedRepoPaths } from './lib/git-changed-files.mjs'
import {
	gitShowUtf8,
	listDashboardSrcAddedMissingLicense,
} from './lib/verify-dashboard-src-license.mjs'

if (
	process.env.SKIP_DASHBOARD_HOOKS === '1' ||
	process.env.SKIP_DASHBOARD_LICENSE_CHECK === '1'
) {
	process.exit(0)
}

const __dirname = fileURLToPath(new URL('.', import.meta.url))
const dashboardDir = join(__dirname, '..')

let repoRoot
try {
	repoRoot = String(
		execFileSync('git', ['rev-parse', '--show-toplevel'], {
			encoding: 'utf8',
			cwd: dashboardDir,
		}),
	).trim()
} catch {
	console.warn('[license-check push] Not in a Git work tree; skipping.')
	process.exit(0)
}

const added = getPushAddedRepoPaths(repoRoot)
const missing = listDashboardSrcAddedMissingLicense(
	added,
	(norm) => gitShowUtf8(repoRoot, `HEAD:${norm}`),
)

if (missing.length > 0) {
	console.error(
		'\x1b[31m[dashboard pre-push]\x1b[0m Added file(s) lack a RAT-aligned Apache license header:',
	)
	for (const m of missing.sort()) {
		console.error(`  - ${m}`)
	}
	console.error(
		'\nInclude the full standard ASF block at the top (see license-header-policy.mjs markers).',
		'Or set SKIP_DASHBOARD_LICENSE_CHECK=1 only for rare exceptions.\n',
	)
	process.exit(1)
}

process.exit(0)
