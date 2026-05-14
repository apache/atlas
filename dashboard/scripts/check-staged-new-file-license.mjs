#!/usr/bin/env node
/**
 * Pre-commit: newly added (staged) source files under dashboard/src must carry
 * the standard ASF header (same policy as apache-license-header.test.ts).
 *
 * Skip: SKIP_DASHBOARD_HOOKS=1 | SKIP_DASHBOARD_LICENSE_CHECK=1
 */

import { execFileSync } from 'node:child_process'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

import {
	HEADER_READ_BYTES,
	contentHasAsfHeader,
	isLicenseCheckSkippedForSrcRel,
} from './lib/license-header-policy.mjs'

if (
	process.env.SKIP_DASHBOARD_HOOKS === '1' ||
	process.env.SKIP_DASHBOARD_LICENSE_CHECK === '1' ||
	process.env.HUSKY === '0'
) {
	process.exit(0)
}

const __dirname = fileURLToPath(new URL('.', import.meta.url))
const dashboardDir = join(__dirname, '..')

const shLines = (args) => {
	try {
		return String(
			execFileSync('git', args, {
				encoding: 'utf8',
				cwd: dashboardDir,
				maxBuffer: 20 * 1024 * 1024,
			}),
		)
			.trim()
			.split('\n')
			.map((l) => l.trim())
			.filter(Boolean)
	} catch {
		return []
	}
}

let repoRoot
try {
	repoRoot = String(
		execFileSync('git', ['rev-parse', '--show-toplevel'], {
			encoding: 'utf8',
			cwd: dashboardDir,
		}),
	).trim()
} catch {
	console.warn('[license-check] Not in a Git work tree; skipping.')
	process.exit(0)
}

const added = shLines(['-C', repoRoot, 'diff', '--cached', '--name-only', '--diff-filter=A'])

const SOURCE_EXT = new Set(['.ts', '.tsx', '.js', '.jsx'])
const missing = []

for (const repoPath of added) {
	const norm = repoPath.replace(/\\/g, '/')
	if (!norm.startsWith('dashboard/src/')) continue
	const ext = norm.slice(norm.lastIndexOf('.'))
	if (!SOURCE_EXT.has(ext)) continue

	const srcRel = norm.slice('dashboard/src/'.length)
	if (isLicenseCheckSkippedForSrcRel(srcRel)) continue

	let content
	try {
		content = String(
			execFileSync('git', ['-C', repoRoot, 'show', `:${norm}`], {
				encoding: 'utf8',
				maxBuffer: HEADER_READ_BYTES + 64_000,
			}),
		)
	} catch {
		continue
	}

	const head = content.slice(0, HEADER_READ_BYTES)
	if (!contentHasAsfHeader(head)) {
		missing.push(norm)
	}
}

if (missing.length > 0) {
	console.error(
		'\x1b[31m[dashboard pre-commit]\x1b[0m New file(s) lack the Apache license header:',
	)
	for (const m of missing.sort()) {
		console.error(`  - ${m}`)
	}
	console.error(
		'\nAdd the standard ASF block at the top (see nearby files), or set SKIP_DASHBOARD_LICENSE_CHECK=1 only for rare exceptions.\n',
	)
	process.exit(1)
}

process.exit(0)
