#!/usr/bin/env node
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
