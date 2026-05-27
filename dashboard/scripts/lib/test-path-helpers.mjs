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
 * Helpers: detect UI source files and colocated / __tests__ coverage.
 */

import { existsSync, readdirSync } from 'node:fs'
import { basename, dirname, extname, join } from 'node:path'

/** Root-level React entry files (same risk as views/components). */
const ROOT_UI_FILES = new Set([
	'src/App.tsx',
	'src/Main.tsx',
	'src/ErrorBoundary.tsx',
])

/** Normalize to path relative to dashboard/ */
export const toDashboardRelative = (repoPath) => {
	const norm = repoPath.replace(/\\/g, '/')
	if (norm.startsWith('dashboard/')) return norm.slice('dashboard/'.length)
	return norm
}

/** Production React UI paths worth guarding with tests */
export const isUiSourcePath = (dashboardRel) => {
	if (!dashboardRel.startsWith('src/')) return false
	if (dashboardRel.includes('__tests__')) return false
	if (dashboardRel.includes('__mocks__')) return false
	if (/\.(test|spec)\.(tsx?|jsx?)$/.test(dashboardRel)) return false
	if (!/\.(tsx|jsx)$/.test(dashboardRel)) return false
	if (ROOT_UI_FILES.has(dashboardRel)) return true
	if (
		!dashboardRel.startsWith('src/views/') &&
		!dashboardRel.startsWith('src/components/')
	) {
		return false
	}
	return true
}

/**
 * @param {string} absFile absolute path to source file
 */
export const hasColocatedOrDirTests = (absFile) => {
	const dir = dirname(absFile)
	const ext = extname(absFile)
	const base = basename(absFile, ext)

	const candidates = [
		join(dir, `${base}.test.tsx`),
		join(dir, `${base}.test.ts`),
		join(dir, '__tests__', `${base}.test.tsx`),
		join(dir, '__tests__', `${base}.test.ts`),
	]

	for (const c of candidates) {
		if (existsSync(c)) return true
	}

	if (base === 'App') {
		const appTest = join(dir, 'components', '__tests__', 'App.test.tsx')
		if (existsSync(appTest)) return true
	}

	if (base === 'EntityForm') {
		const viewsDir = dirname(dir)
		const viewTests = join(viewsDir, '__tests__', 'EntityForm.test.tsx')
		if (existsSync(viewTests)) return true
		const viewTestsTs = join(viewsDir, '__tests__', 'EntityForm.test.ts')
		if (existsSync(viewTestsTs)) return true
	}

	const testsDir = join(dir, '__tests__')
	if (existsSync(testsDir)) {
		const entries = readdirSync(testsDir)
		if (entries.some((e) => /\.(test|spec)\.(tsx?|jsx?)$/.test(e))) return true
	}

	return false
}

/**
 * @param {string} dashboardRoot absolute path to dashboard package
 * @param {string} dashboardRel e.g. src/views/Foo/Bar.tsx
 */
export const hasTestsOnDisk = (dashboardRoot, dashboardRel) => {
	const abs = join(dashboardRoot, dashboardRel)
	if (!existsSync(abs)) return false
	return hasColocatedOrDirTests(abs)
}

/**
 * Staged files must include at least one test when UI sources are staged.
 * @param {string[]} repoPaths paths from git (dashboard/... or src/...)
 * @returns {{ ok: boolean, message?: string }}
 */
export const stagedIncludesTestWhenUiChanges = (repoPaths) => {
	const dashPaths = repoPaths
		.map(toDashboardRelative)
		.filter((p) => !p.startsWith('..'))

	const uiChanged = dashPaths.filter(isUiSourcePath)
	if (uiChanged.length === 0) return { ok: true }

	const testTouched = dashPaths.some(
		(p) =>
			p.includes('__tests__') || /\.(test|spec)\.(tsx?|jsx?)$/.test(p),
	)

	if (testTouched) return { ok: true }

	return {
		ok: false,
		message:
			'Staged changes touch src/views or src/components (.tsx/.jsx) but no test file was staged.\n' +
			'Add or update a matching *.test.ts(x) or __tests__/* file, or set SKIP_DASHBOARD_TEST_GUARD=1 for a rare exception.',
	}
}

/**
 * For each changed UI file, require on-disk test companion.
 * @param {string} dashboardRoot
 * @param {string[]} repoPaths
 * @returns {{ ok: boolean, missing: string[] }}
 */
export const allUiChangesHaveTestHome = (dashboardRoot, repoPaths) => {
	const dashPaths = repoPaths
		.map(toDashboardRelative)
		.filter((p) => p.startsWith('src/'))
	const ui = [...new Set(dashPaths.filter(isUiSourcePath))]
	const missing = ui.filter((p) => !hasTestsOnDisk(dashboardRoot, p))
	return { ok: missing.length === 0, missing }
}
