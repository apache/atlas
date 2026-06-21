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
 * Pre-commit: staged UI test guard + colocated tests on disk.
 * Skip: SKIP_DASHBOARD_HOOKS=1 or SKIP_DASHBOARD_TEST_GUARD=1
 */

import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { getStagedFiles } from './lib/git-changed-files.mjs'
import {
	allUiChangesHaveTestHome,
	isUiSourcePath,
	stagedIncludesTestWhenUiChanges,
	toDashboardRelative,
} from './lib/test-path-helpers.mjs'

if (process.env.SKIP_DASHBOARD_HOOKS === '1') {
	process.exit(0)
}

if (process.env.SKIP_DASHBOARD_TEST_GUARD === '1') {
	process.exit(0)
}

const __dirname = fileURLToPath(new URL('.', import.meta.url))
const dashboardRoot = join(__dirname, '..')

const staged = getStagedFiles()
const dashboardPaths = staged.filter(
	(p) => p.startsWith('dashboard/') || p.startsWith('src/'),
)

if (dashboardPaths.length === 0) {
	process.exit(0)
}

const stagedGuard = stagedIncludesTestWhenUiChanges(dashboardPaths)
if (!stagedGuard.ok) {
	console.error('\x1b[31m[dashboard pre-commit]\x1b[0m', stagedGuard.message)
	process.exit(1)
}

const hasUi = dashboardPaths.map(toDashboardRelative).some(isUiSourcePath)
if (hasUi) {
	const { ok, missing } = allUiChangesHaveTestHome(
		dashboardRoot,
		dashboardPaths,
	)
	if (!ok) {
		console.error(
			'\x1b[31m[dashboard pre-commit]\x1b[0m These staged UI files have no colocated __tests__ or *.test.ts(x):',
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

process.exit(0)
