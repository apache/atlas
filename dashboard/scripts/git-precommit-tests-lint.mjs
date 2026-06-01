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
 * Pre-commit: Jest --findRelatedTests on staged TS/TSX, then ESLint on src/.
 * Skip: SKIP_DASHBOARD_HOOKS=1
 */

import { execSync, spawnSync } from 'node:child_process'
import { existsSync } from 'node:fs'
import { join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'

import { getStagedFiles } from './lib/git-changed-files.mjs'
import { toDashboardRelative } from './lib/test-path-helpers.mjs'

if (process.env.SKIP_DASHBOARD_HOOKS === '1') {
	process.exit(0)
}

const __dirname = fileURLToPath(new URL('.', import.meta.url))
const dashboardRoot = join(__dirname, '..')
if (!existsSync(join(dashboardRoot, 'package.json'))) {
	console.error('Could not find dashboard root', dashboardRoot)
	process.exit(1)
}

const run = (cmd, opts = {}) => {
	console.log(`\x1b[36m▶\x1b[0m ${cmd}`)
	execSync(cmd, { stdio: 'inherit', cwd: dashboardRoot, ...opts })
}

const staged = getStagedFiles()
const dashboardPaths = staged.filter(
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

if (dashRelFiles.length > 0) {
	console.log('\x1b[35m[dashboard pre-commit]\x1b[0m Staged paths (sample):')
	console.log(
		dashRelFiles.slice(0, 20).join('\n') +
			(dashRelFiles.length > 20 ? '\n…' : ''),
	)
}

if (jestSourceArgs.length > 0) {
	console.log(
		'\x1b[35m[dashboard pre-commit]\x1b[0m Running Jest --findRelatedTests (staged sources):',
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
		'\x1b[33m[dashboard pre-commit]\x1b[0m No staged TS source files for --findRelatedTests; skipping Jest.',
	)
}

run(
	'npx eslint src --ext ts,tsx --report-unused-disable-directives --max-warnings 200',
)

console.log('\x1b[32m[dashboard pre-commit]\x1b[0m Jest and ESLint passed.\n')
process.exit(0)
