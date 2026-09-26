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

import * as fs from 'fs'
import * as path from 'path'

const SCAN_ROOT = path.join(__dirname, '..')

/** Substrings required in the first HEADER_READ_BYTES of each scanned file. */
const RAT_ALIGNED_REQUIRED_MARKERS = [
	'Licensed to the Apache Software Foundation',
	'Apache License, Version 2.0',
	'http://www.apache.org/licenses/LICENSE-2.0',
	'Unless required by applicable law or agreed to in writing',
	'WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND',
	'limitations under the License',
]

const contentHasAsfHeader = (head: string): boolean => {
	const normalized = head
		.slice(0, HEADER_READ_BYTES)
		.replace(/\s+/gu, ' ')
		.trim()
	return RAT_ALIGNED_REQUIRED_MARKERS.every((marker) =>
		normalized.includes(marker),
	)
}

const SOURCE_EXTENSIONS = new Set(['.ts', '.tsx', '.js', '.jsx'])

const IGNORE_DIR_NAMES = new Set([
	'node_modules',
	'dist',
	'build',
	'coverage',
])

const HEADER_READ_BYTES = 12_000

/** Test helpers, mocks, and ambient typings are out of scope for this check. */
const isLicenseCheckSkipped = (relativePosix: string): boolean => {
	const segments = relativePosix.split('/')
	if (segments.includes('__tests__')) return true
	if (segments.includes('__mocks__')) return true
	if (/\.test\.(ts|tsx|js|jsx)$/.test(relativePosix)) return true
	if (relativePosix.endsWith('.d.ts')) return true
	if (
		relativePosix === 'setupTests.ts' ||
		relativePosix === 'setupTests.simple.ts'
	) {
		return true
	}
	if (relativePosix === 'utils/test-utils.tsx') return true
	return false
}

const collectSourceFiles = (dir: string): string[] => {
	const entries = fs.readdirSync(dir, { withFileTypes: true })
	const out: string[] = []

	for (const entry of entries) {
		const full = path.join(dir, entry.name)

		if (entry.isDirectory()) {
			if (IGNORE_DIR_NAMES.has(entry.name)) {
				continue
			}
			out.push(...collectSourceFiles(full))
			continue
		}

		if (!entry.isFile()) {
			continue
		}

		const ext = path.extname(entry.name)
		if (!SOURCE_EXTENSIONS.has(ext)) {
			continue
		}

		out.push(full)
	}

	return out
}

const fileStartsWithRatAlignedHeader = (filePath: string): boolean => {
	const fd = fs.openSync(filePath, 'r')
	try {
		const buf = Buffer.allocUnsafe(HEADER_READ_BYTES)
		const read = fs.readSync(fd, buf, 0, buf.length, 0)
		const head = buf.subarray(0, read).toString('utf8')
		return contentHasAsfHeader(head)
	} finally {
		fs.closeSync(fd)
	}
}

describe('Apache license header in source files', () => {
	it('requires RAT-aligned ASF license header markers', () => {
		expect(RAT_ALIGNED_REQUIRED_MARKERS.length).toBeGreaterThanOrEqual(6)
		expect(RAT_ALIGNED_REQUIRED_MARKERS.join(' ')).toMatch(/LICENSE-2\.0/)
	})

	it('rejects headers that only include the legacy two marker phrases', () => {
		const minimalFake = [
			'/*',
			' * Licensed to the Apache Software Foundation',
			' * Apache License, Version 2.0',
			' */',
		].join('\n')
		expect(contentHasAsfHeader(minimalFake)).toBe(false)
	})

	it('includes ASF license block at top of every scanned .ts, .tsx, .js, .jsx file under src/', () => {
		const allFiles = collectSourceFiles(SCAN_ROOT)
		const missing = allFiles
			.map((p) => ({ abs: p, rel: path.relative(SCAN_ROOT, p) }))
			.filter(({ rel }) =>
				!isLicenseCheckSkipped(rel.split(path.sep).join('/')),
			)
			.filter(({ abs }) => !fileStartsWithRatAlignedHeader(abs))
			.map(({ rel }) => rel)

		if (missing.length > 0) {
			const relative = [...missing].sort()
			const message = [
				`${missing.length} file(s) under src/ are missing the RAT-aligned Apache License header (markers not in first ${HEADER_READ_BYTES} bytes):`,
				'',
				...relative.map((r) => `  - ${r}`),
				'',
				'Required substrings are listed in RAT_ALIGNED_REQUIRED_MARKERS',
				'in apache-license-header.test.ts.',
			].join('\n')
			throw new Error(message)
		}
	})
})
