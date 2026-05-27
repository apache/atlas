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
 * Pre-commit: ensure UI changes stage tests; lint-staged runs ESLint after this.
 * Skip: SKIP_DASHBOARD_HOOKS=1 or HUSKY=0 or SKIP_DASHBOARD_TEST_GUARD=1
 */

import { stagedIncludesTestWhenUiChanges } from './lib/test-path-helpers.mjs'
import { getStagedFiles } from './lib/git-changed-files.mjs'

if (process.env.SKIP_DASHBOARD_HOOKS === '1' || process.env.HUSKY === '0') {
	process.exit(0)
}

if (process.env.SKIP_DASHBOARD_TEST_GUARD === '1') {
	process.exit(0)
}

const staged = getStagedFiles()
const dashboardPaths = staged.filter(
	(p) => p.startsWith('dashboard/') || p.startsWith('src/'),
)

if (dashboardPaths.length === 0) {
	process.exit(0)
}

const guard = stagedIncludesTestWhenUiChanges(dashboardPaths)
if (!guard.ok) {
	console.error('\x1b[31m[dashboard pre-commit]\x1b[0m', guard.message)
	process.exit(1)
}

process.exit(0)
