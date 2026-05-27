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
 * node --check on staged plain JS (not .jsx) under dashboardv2/public/js.
 */

import { execFileSync, spawnSync } from 'node:child_process'
import { join } from 'node:path'

import { getStagedFiles } from './lib/git-helpers.mjs'

/**
 * @param {string} repoRel
 */
const isV2CheckableJs = (repoRel) => {
	const n = repoRel.replace(/\\/g, '/')
	if (!n.startsWith('dashboardv2/public/js/')) return false
	if (!n.endsWith('.js')) return false
	if (n.includes('/external_lib/')) return false
	if (n.endsWith('.min.js')) return false
	return true
}

/**
 * @param {string} root
 */
export const syntaxCheckDashboardv2Staged = (root) => {
	const staged = getStagedFiles(root)
	const files = staged.filter(isV2CheckableJs)
	for (const f of files) {
		const abs = join(root, f)
		const r = spawnSync(process.execPath, ['--check', abs], {
			encoding: 'utf8',
		})
		if (r.status !== 0) {
			console.error(
				`\x1b[31m[dashboardv2 pre-commit]\x1b[0m Syntax error in ${f}:\n${r.stderr || r.stdout}`,
			)
			process.exit(r.status ?? 1)
		}
	}
}

/** Docs: only plain scripts (Node parses scripts/*.js, doczrc.js, webapp config). */
const isDocsCheckableJs = (repoRel) => {
	const n = repoRel.replace(/\\/g, '/')
	if (!n.startsWith('docs/')) return false
	if (!n.endsWith('.js')) return false
	if (n.includes('/node_modules/')) return false
	if (n.startsWith('docs/site/') || n.startsWith('docs/bin/')) return false
	if (n.startsWith('docs/docz-lib/')) return false
	// Avoid JSX-heavy paths (node cannot parse)
	if (n.startsWith('docs/theme/') || n.startsWith('docs/webapp/')) return false
	return true
}

/**
 * @param {string} root
 */
export const syntaxCheckDocsStaged = (root) => {
	const staged = getStagedFiles(root)
	const files = staged.filter(isDocsCheckableJs)
	for (const f of files) {
		const abs = join(root, f)
		try {
			execFileSync(process.execPath, ['--check', abs], { stdio: 'pipe' })
		} catch (e) {
			console.error(`\x1b[31m[docs pre-commit]\x1b[0m Syntax error in ${f}`)
			process.exit(1)
		}
	}
}
