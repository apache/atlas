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
 * Run the same checks as .githooks/pre-commit (for manual verification).
 * Execute from dashboard/: npm run verify:precommit
 *
 * Order: test guard → ASF header on new files → lint-staged → tsc --noEmit
 */

import { execFileSync } from 'node:child_process'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = fileURLToPath(new URL('.', import.meta.url))
const dashboardDir = join(__dirname, '..')

const run = (title, fn) => {
	console.log(`\x1b[36m▶\x1b[0m ${title}`)
	fn()
}

try {
	run('UI ↔ staged test guard', () => {
		execFileSync(process.execPath, ['scripts/git-precommit-verify.mjs'], {
			cwd: dashboardDir,
			stdio: 'inherit',
		})
	})

	run('ASF license on newly added staged files', () => {
		execFileSync(process.execPath, ['scripts/check-staged-new-file-license.mjs'], {
			cwd: dashboardDir,
			stdio: 'inherit',
		})
	})

	const repoRoot = String(
		execFileSync('git', ['rev-parse', '--show-toplevel'], {
			encoding: 'utf8',
			cwd: dashboardDir,
		}),
	).trim()

	run('lint-staged (ESLint on staged dashboard/src)', () => {
		const lintStagedCli = join(
			dashboardDir,
			'node_modules/lint-staged/bin/lint-staged.js',
		)
		execFileSync(process.execPath, [lintStagedCli, '--config', 'dashboard/lint-staged.config.mjs'], {
			cwd: repoRoot,
			stdio: 'inherit',
		})
	})

	if (process.env.SKIP_DASHBOARD_TYPECHECK !== '1') {
		run('TypeScript project check (tsc --noEmit)', () => {
			execFileSync(process.platform === 'win32' ? 'npm.cmd' : 'npm', ['run', 'typecheck'], {
				cwd: dashboardDir,
				stdio: 'inherit',
				shell: process.platform === 'win32',
			})
		})
	}

	console.log('\x1b[32m[dashboard verify:precommit]\x1b[0m All steps passed.\n')
} catch (e) {
	process.exit(e.status ?? 1)
}
