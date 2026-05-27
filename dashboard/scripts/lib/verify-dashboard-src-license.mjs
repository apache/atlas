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
 * Shared ASF header verification for dashboard/src (pre-commit + pre-push).
 */

import { execFileSync } from 'node:child_process'

import {
	HEADER_READ_BYTES,
	contentHasAsfHeader,
	isLicenseCheckSkippedForSrcRel,
} from './license-header-policy.mjs'

const SOURCE_EXT = new Set(['.ts', '.tsx', '.js', '.jsx'])

/**
 * @param {string[]} addedRepoPaths paths from git (e.g. dashboard/src/Foo.tsx)
 * @param {(repoNormPath: string) => string} readBlob git show spec resolved to UTF-8
 * @returns {string[]} repo-relative paths missing a RAT-aligned header
 */
export const listDashboardSrcAddedMissingLicense = (addedRepoPaths, readBlob) => {
	const missing = []

	for (const repoPath of addedRepoPaths) {
		const norm = repoPath.replace(/\\/g, '/')
		if (!norm.startsWith('dashboard/src/')) continue

		const ext = norm.slice(norm.lastIndexOf('.'))
		if (!SOURCE_EXT.has(ext)) continue

		const srcRel = norm.slice('dashboard/src/'.length)
		if (isLicenseCheckSkippedForSrcRel(srcRel)) continue

		let content
		try {
			content = readBlob(norm)
		} catch {
			continue
		}

		const head = content.slice(0, HEADER_READ_BYTES)
		if (!contentHasAsfHeader(head)) {
			missing.push(norm)
		}
	}

	return missing
}

/**
 * @param {string} repoRoot
 * @param {string} gitShowArg e.g. `:${path}` (index) or `HEAD:${path}`
 * @returns {string}
 */
export const gitShowUtf8 = (repoRoot, gitShowArg) =>
	String(
		execFileSync('git', ['-C', repoRoot, 'show', gitShowArg], {
			encoding: 'utf8',
			maxBuffer: HEADER_READ_BYTES + 64_000,
		}),
	)
