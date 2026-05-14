#!/usr/bin/env node
/**
 * Repo-wide pre-push: dashboard (tests + eslint + build), dashboardv2 build, docs build.
 * SPDX-License-Identifier: Apache-2.0
 */

import { execFileSync } from 'node:child_process'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { getRepoRoot, getPushRangeFiles } from './lib/git-helpers.mjs'

const __dirname = dirname(fileURLToPath(import.meta.url))
const scriptsDir = join(__dirname, '..')
const repoRoot = getRepoRoot(scriptsDir)

if (
	process.env.SKIP_ATLAS_HOOKS === '1' ||
	process.env.SKIP_ALL_ATLAS_GIT_HOOKS === '1'
) {
	process.exit(0)
}

const changed = getPushRangeFiles(repoRoot)
const touchDashboard = changed.some((p) => p.startsWith('dashboard/'))
const touchV2 = changed.some((p) => p.startsWith('dashboardv2/'))
const touchDocs = changed.some((p) => p.startsWith('docs/'))

if (!touchDashboard && !touchV2 && !touchDocs) {
	process.exit(0)
}

if (touchDashboard && process.env.SKIP_DASHBOARD_HOOKS !== '1') {
	console.log('\x1b[35m[atlas pre-push]\x1b[0m dashboard package…')
	execFileSync(process.execPath, ['scripts/git-prepush-verify.mjs'], {
		cwd: join(repoRoot, 'dashboard'),
		stdio: 'inherit',
	})
}

if (touchV2 && process.env.SKIP_DASHBOARDV2_HOOKS !== '1') {
	if (process.env.SKIP_DASHBOARDV2_BUILD === '1') {
		console.log(
			'\x1b[33m[atlas pre-push]\x1b[0m SKIP_DASHBOARDV2_BUILD=1 — skipping dashboardv2 npm run build.',
		)
	} else {
		console.log('\x1b[35m[atlas pre-push]\x1b[0m dashboardv2 — npm run build…')
		execFileSync(process.platform === 'win32' ? 'npm.cmd' : 'npm', ['run', 'build'], {
			cwd: join(repoRoot, 'dashboardv2'),
			stdio: 'inherit',
			shell: process.platform === 'win32',
		})
	}
}

if (touchDocs && process.env.SKIP_DOCS_HOOKS !== '1') {
	if (process.env.SKIP_DOCS_BUILD === '1') {
		console.log(
			'\x1b[33m[atlas pre-push]\x1b[0m SKIP_DOCS_BUILD=1 — skipping docs npm run build.',
		)
	} else {
		console.log('\x1b[35m[atlas pre-push]\x1b[0m docs — npm run build…')
		execFileSync(process.platform === 'win32' ? 'npm.cmd' : 'npm', ['run', 'build'], {
			cwd: join(repoRoot, 'docs'),
			stdio: 'inherit',
			shell: process.platform === 'win32',
		})
	}
}

console.log('\x1b[32m[atlas pre-push]\x1b[0m Done.\n')
process.exit(0)
