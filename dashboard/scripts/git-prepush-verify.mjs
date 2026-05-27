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
 * Pre-push: impact-related Jest tests, ESLint (src), production build.
 * Skip: SKIP_DASHBOARD_HOOKS=1 or HUSKY=0
 */

import { execFileSync, execSync, spawnSync } from 'node:child_process'
import { existsSync } from 'node:fs'
import { join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'

import { getPushRangeFiles } from './lib/git-changed-files.mjs'
import {
	allUiChangesHaveTestHome,
	isUiSourcePath,
	toDashboardRelative,
} from './lib/test-path-helpers.mjs'

if (process.env.SKIP_DASHBOARD_HOOKS === '1' || process.env.HUSKY === '0') {
	process.exit(0)
}

const __dirname = fileURLToPath(new URL('.', import.meta.url))
const dashboardRoot = join(__dirname, '..')
if (!existsSync(join(dashboardRoot, 'package.json'))) {
	console.error('Could not find dashboard root', dashboardRoot)
	process.exit(1)
}

if (process.env.SKIP_DASHBOARD_LICENSE_CHECK !== '1') {
	console.log(
		'\x1b[35m[dashboard pre-push]\x1b[0m RAT-aligned ASF header on newly added dashboard/src files…',
	)
	execFileSync(process.execPath, ['scripts/check-push-new-file-license.mjs'], {
		cwd: dashboardRoot,
		stdio: 'inherit',
	})
}

const run = (cmd, opts = {}) => {
	console.log(`\x1b[36m▶\x1b[0m ${cmd}`)
	execSync(cmd, { stdio: 'inherit', cwd: dashboardRoot, ...opts })
}

const repoPaths = getPushRangeFiles()
const dashboardPaths = repoPaths.filter(
	(p) => p.startsWith('dashboard/') || p.startsWith('src/'),
)

const dashRelFiles = dashboardPaths.map(toDashboardRelative).filter((p) => {
	if (p.startsWith('..')) return false
	return existsSync(join(dashboardRoot, p))
})

/** Source files Jest can map to related tests */
const jestSourceArgs = dashRelFiles.filter((p) => {
	if (!p.startsWith('src/')) return false
	if (p.includes('__tests__')) return false
	if (/\.(test|spec)\.(tsx?)$/.test(p)) return false
	return /\.(ts|tsx)$/.test(p)
})

if (process.env.SKIP_DASHBOARD_TEST_GUARD !== '1') {
	const hasUi = dashboardPaths.map(toDashboardRelative).some(isUiSourcePath)
	if (hasUi) {
		const { ok, missing } = allUiChangesHaveTestHome(
			dashboardRoot,
			dashboardPaths,
		)
		if (!ok) {
			console.error(
				'\x1b[31m[dashboard pre-push]\x1b[0m These UI files have no colocated __tests__ or *.test.ts(x):',
			)
			for (const m of missing) {
				console.error(`  - ${m}`)
			}
			console.error(
				'Add tests or set SKIP_DASHBOARD_TEST_GUARD=1 only for exceptions.\n',
			)
			process.exit(1)
		}
	}
}

console.log('\x1b[35m[dashboard pre-push]\x1b[0m Changed paths in range (sample):')
console.log(
	dashRelFiles.slice(0, 20).join('\n') + (dashRelFiles.length > 20 ? '\n…' : ''),
)

if (jestSourceArgs.length > 0) {
	console.log(
		'\x1b[35m[dashboard pre-push]\x1b[0m Running Jest --findRelatedTests (impact surface):',
	)
	const rel = jestSourceArgs.map((f) =>
		relative(dashboardRoot, join(dashboardRoot, f)).replace(/\\/g, '/'),
	)
	const res = spawnSync(
		process.platform === 'win32' ? 'npx.cmd' : 'npx',
		['jest', '--bail', '--passWithNoTests', '--findRelatedTests', ...rel],
		{ cwd: dashboardRoot, stdio: 'inherit', shell: process.platform === 'win32' },
	)
	if (res.status !== 0) process.exit(res.status ?? 1)
} else {
	console.log(
		'\x1b[33m[dashboard pre-push]\x1b[0m No TS source files in diff for --findRelatedTests; skipping Jest.',
	)
}

run(
	'npx eslint src --ext ts,tsx --report-unused-disable-directives --max-warnings 200',
)
run('npm run build')

console.log('\x1b[32m[dashboard pre-push]\x1b[0m All checks passed.\n')
process.exit(0)
