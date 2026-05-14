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
 * ASF license header policy aligned with src/__tests__/apache-license-header.test.ts
 * (paths here are relative to dashboard/src/).
 */

/**
 * Substrings that must all appear in the first HEADER_READ_BYTES bytes of the file.
 * Aligns with the standard Atlas dashboard header and Apache RAT expectations
 * (ASL2-style notice), so CI RAT and local hooks enforce the same bar.
 */
export const RAT_ALIGNED_REQUIRED_MARKERS = [
	'Licensed to the Apache Software Foundation',
	'Apache License, Version 2.0',
	'http://www.apache.org/licenses/LICENSE-2.0',
	'Unless required by applicable law or agreed to in writing',
	'WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND',
	'limitations under the License',
]

/** @deprecated use {@link RAT_ALIGNED_REQUIRED_MARKERS} — kept for callers that only need the lead-in */
export const LICENSE_MARKERS = RAT_ALIGNED_REQUIRED_MARKERS.slice(0, 2)

export const HEADER_READ_BYTES = 12_000

/**
 * @param {string} relativePosix path relative to src/, forward slashes
 * @returns {boolean}
 */
export const isLicenseCheckSkippedForSrcRel = (relativePosix) => {
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

/**
 * Collapse whitespace so wrapped ASF headers still match marker substrings.
 * @param {string} head
 * @returns {string}
 */
const normalizeHeaderWhitespace = (head) => head.replace(/\s+/gu, ' ').trim()

/**
 * @param {string} head first bytes of file as string
 * @returns {boolean}
 */
export const contentHasAsfHeader = (head) => {
	const n = normalizeHeaderWhitespace(head.slice(0, HEADER_READ_BYTES))
	return RAT_ALIGNED_REQUIRED_MARKERS.every((marker) => n.includes(marker))
}

/**
 * Alias for {@link contentHasAsfHeader} (explicit RAT-oriented name for tools/tests).
 * @param {string} head
 * @returns {boolean}
 */
export const contentHasRatApprovedAsfHeader = (head) => contentHasAsfHeader(head)
