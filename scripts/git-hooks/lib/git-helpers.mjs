/**
 * Git helpers for repo-root hook orchestration.
 * SPDX-License-Identifier: Apache-2.0
 */

import { execFileSync } from 'node:child_process'

/**
 * @param {string} cwd
 * @returns {string}
 */
export const getRepoRoot = (cwd) =>
	String(
		execFileSync('git', ['rev-parse', '--show-toplevel'], {
			encoding: 'utf8',
			cwd,
		}),
	).trim()

/**
 * @param {string} root
 * @param {...string} gitArgs
 * @returns {string}
 */
export const git = (root, ...gitArgs) =>
	String(
		execFileSync('git', ['-C', root, ...gitArgs], {
			encoding: 'utf8',
			maxBuffer: 20 * 1024 * 1024,
		}),
	).trim()

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
 * @param {string} root
 * @returns {string[]}
 */
export const getStagedFiles = (root) => {
	try {
		return splitLines(git(root, 'diff', '--cached', '--name-only', '--diff-filter=ACM'))
	} catch {
		return []
	}
}

/**
 * @param {string} root
 * @returns {string[]}
 */
export const getStagedAddedFiles = (root) => {
	try {
		return splitLines(git(root, 'diff', '--cached', '--name-only', '--diff-filter=A'))
	} catch {
		return []
	}
}

/**
 * @param {string} root
 * @returns {string[]}
 */
export const getPushRangeFiles = (root) => {
	const tryRange = (range) => {
		try {
			return splitLines(git(root, 'diff', '--name-only', range))
		} catch {
			return null
		}
	}

	let files = tryRange('@{u}..HEAD')
	if (files && files.length > 0) return files

	for (const base of ['origin/master', 'origin/main', 'master', 'main']) {
		try {
			const mergeBase = String(
				execFileSync('git', ['-C', root, 'merge-base', 'HEAD', base], {
					encoding: 'utf8',
				}),
			).trim()
			if (mergeBase) {
				files = tryRange(`${mergeBase}..HEAD`)
				if (files && files.length > 0) return files
			}
		} catch {
			// continue
		}
	}

	try {
		return splitLines(git(root, 'diff', '--name-only', 'HEAD~1..HEAD'))
	} catch {
		return []
	}
}
