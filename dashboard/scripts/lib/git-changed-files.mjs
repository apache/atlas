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
 * Resolve paths changed in git (staged, committed range, or vs base branch).
 */

import { execFileSync, execSync } from 'node:child_process'

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

	for (const base of ['origin/master', 'origin/main', 'main', 'master']) {
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

/**
 * Revision range for `git diff` comparing this branch to its upstream / mainline.
 * @param {string} repoRoot absolute repo root
 * @returns {string} revRange e.g. `@{u}..HEAD`, `abc..HEAD`, `HEAD~1..HEAD`
 */
export const resolvePushRevRange = (repoRoot) => {
	const execGit = (args) =>
		String(
			execFileSync('git', ['-C', repoRoot, ...args], {
				encoding: 'utf8',
				maxBuffer: 10 * 1024 * 1024,
			}),
		).trim()

	const tryRange = (range) => {
		try {
			execGit(['diff', '--name-only', range])
			return range
		} catch {
			return null
		}
	}

	let range = tryRange('@{u}..HEAD')
	if (range) return range

	for (const base of ['origin/master', 'origin/main', 'main', 'master']) {
		try {
			const mergeBase = execGit(['merge-base', 'HEAD', base])
			if (mergeBase) {
				const r = `${mergeBase}..HEAD`
				if (tryRange(r)) return r
			}
		} catch {
			// continue
		}
	}

	return 'HEAD~1..HEAD'
}

/**
 * Repo-relative paths added (not present at merge-base / range start) in push range.
 * @param {string} repoRoot
 * @returns {string[]}
 */
export const getPushAddedRepoPaths = (repoRoot) => {
	const range = resolvePushRevRange(repoRoot)
	try {
		return splitLines(
			String(
				execFileSync(
					'git',
					['-C', repoRoot, 'diff', '--name-only', '--diff-filter=A', range],
					{
						encoding: 'utf8',
						maxBuffer: 20 * 1024 * 1024,
					},
				),
			).trim(),
		)
	} catch {
		return []
	}
}
