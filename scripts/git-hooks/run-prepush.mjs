#!/usr/bin/env node
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
 * Repo-wide pre-push: dashboard (tests + eslint + build), dashboardv2 build, docs build.
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
