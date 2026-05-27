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
 * Pre-commit: newly added (staged) source files under dashboard/src must carry
 * a RAT-aligned Apache license header (see license-header-policy.mjs).
 *
 * Skip: SKIP_DASHBOARD_HOOKS=1 | SKIP_DASHBOARD_LICENSE_CHECK=1
 */

import { execFileSync } from 'node:child_process'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

import {
	gitShowUtf8,
	listDashboardSrcAddedMissingLicense,
} from './lib/verify-dashboard-src-license.mjs'

if (
	process.env.SKIP_DASHBOARD_HOOKS === '1' ||
	process.env.SKIP_DASHBOARD_LICENSE_CHECK === '1' ||
	process.env.HUSKY === '0'
) {
	process.exit(0)
}

const __dirname = fileURLToPath(new URL('.', import.meta.url))
const dashboardDir = join(__dirname, '..')

const shLines = (args) => {
	try {
		return String(
			execFileSync('git', args, {
				encoding: 'utf8',
				cwd: dashboardDir,
				maxBuffer: 20 * 1024 * 1024,
			}),
		)
			.trim()
			.split('\n')
			.map((l) => l.trim())
			.filter(Boolean)
	} catch {
		return []
	}
}

let repoRoot
try {
	repoRoot = String(
		execFileSync('git', ['rev-parse', '--show-toplevel'], {
			encoding: 'utf8',
			cwd: dashboardDir,
		}),
	).trim()
} catch {
	console.warn('[license-check] Not in a Git work tree; skipping.')
	process.exit(0)
}

const added = shLines(['-C', repoRoot, 'diff', '--cached', '--name-only', '--diff-filter=A'])

const missing = listDashboardSrcAddedMissingLicense(
	added,
	(norm) => gitShowUtf8(repoRoot, `:${norm}`),
)

if (missing.length > 0) {
	console.error(
		'\x1b[31m[dashboard pre-commit]\x1b[0m New file(s) lack a RAT-aligned Apache license header:',
	)
	for (const m of missing.sort()) {
		console.error(`  - ${m}`)
	}
	console.error(
		'\nInclude the full standard ASF block at the top (see CreateDropdown.tsx or audit sibling files).',
		'Required markers match dashboard/scripts/lib/license-header-policy.mjs.',
		'Or set SKIP_DASHBOARD_LICENSE_CHECK=1 only for rare exceptions.\n',
	)
	process.exit(1)
}

process.exit(0)
