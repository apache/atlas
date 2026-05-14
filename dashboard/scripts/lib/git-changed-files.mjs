/**
 * Resolve paths changed in git (staged, committed range, or vs base branch).
 * SPDX: Apache-2.0 (match dashboard)
 */

import { execSync } from 'node:child_process'

/**
 * @param {string} cmd
 * @returns {string}
 */
const sh = (cmd) =>
	String(execSync(cmd, { encoding: 'utf8', maxBuffer: 10 * 1024 * 1024 })).trim()

/**
 * @param {string} raw
 * @returns {string[]}
 */
export const splitLines = (raw) =>
	raw
		.split('\n')
		.map((l) => l.trim())
		.filter(Boolean)

/**
 * Files staged for commit.
 * @returns {string[]}
 */
export const getStagedFiles = () => {
	try {
		return splitLines(sh('git diff --cached --name-only --diff-filter=ACM'))
	} catch {
		return []
	}
}

/**
 * Files changed between remote tracking branch and HEAD (for pre-push).
 * Falls back to merge-base with main/master or single last commit.
 * @returns {string[]}
 */
export const getPushRangeFiles = () => {
	const tryRange = (range) => {
		try {
			return splitLines(sh(`git diff --name-only ${range}`))
		} catch {
			return null
		}
	}

	let files = tryRange('@{u}..HEAD')
	if (files && files.length > 0) return files

	for (const base of ['origin/main', 'origin/master', 'main', 'master']) {
		try {
			const mergeBase = sh(`git merge-base HEAD ${base} 2>/dev/null`)
			if (mergeBase) {
				files = tryRange(`${mergeBase}..HEAD`)
				if (files && files.length > 0) return files
			}
		} catch {
			// continue
		}
	}

	try {
		return splitLines(sh('git diff --name-only HEAD~1..HEAD'))
	} catch {
		return []
	}
}
