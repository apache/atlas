#!/usr/bin/env node
/**
 * ASF license on newly staged files for a path prefix (dashboardv2, docs).
 * Reuses marker rules from dashboard/scripts/lib/license-header-policy.mjs
 */

import { execFileSync } from 'node:child_process'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import {
	HEADER_READ_BYTES,
	contentHasAsfHeader,
} from '../../dashboard/scripts/lib/license-header-policy.mjs'
import { getRepoRoot, getStagedAddedFiles } from './lib/git-helpers.mjs'

const __dirname = dirname(fileURLToPath(import.meta.url))
const scriptDir = join(__dirname, '..') // git-hooks
const repoRootDefault = getRepoRoot(scriptDir)

/**
 * @param {object} opts
 * @param {string} opts.label
 * @param {(repoRel: string) => boolean} opts.shouldSkip
 * @param {Set<string>} opts.extensions
 * @param {string} [opts.repoRoot]
 */
export const verifyAddedFilesAspLicense = (opts) => {
	const root = opts.repoRoot ?? repoRootDefault
	const added = getStagedAddedFiles(root)
	const missing = []

	for (const repoPath of added) {
		const norm = repoPath.replace(/\\/g, '/')
		if (opts.shouldSkip(norm)) continue
		const ext = norm.slice(norm.lastIndexOf('.'))
		if (!opts.extensions.has(ext)) continue

		let content
		try {
			content = String(
				execFileSync('git', ['-C', root, 'show', `:${norm}`], {
					encoding: 'utf8',
					maxBuffer: HEADER_READ_BYTES + 64_000,
				}),
			)
		} catch {
			continue
		}

		const head = content.slice(0, HEADER_READ_BYTES)
		if (!contentHasAsfHeader(head)) missing.push(norm)
	}

	if (missing.length > 0) {
		console.error(
			`\x1b[31m[${opts.label} pre-commit]\x1b[0m New file(s) lack the Apache license header:`,
		)
		for (const m of missing.sort()) console.error(`  - ${m}`)
		console.error(
			'\nAdd the standard ASF block at the top (match sibling files), or set SKIP_ATLAS_LICENSE_CHECK=1 (emergency only).\n',
		)
		process.exit(1)
	}
}
