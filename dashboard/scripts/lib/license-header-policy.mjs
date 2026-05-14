/**
 * ASF license header policy aligned with src/__tests__/apache-license-header.test.ts
 * (paths here are relative to dashboard/src/).
 */

export const LICENSE_MARKERS = [
	'Licensed to the Apache Software Foundation',
	'Apache License, Version 2.0',
]

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
 * @param {string} head first bytes of file as string
 * @returns {boolean}
 */
export const contentHasAsfHeader = (head) =>
	LICENSE_MARKERS.every((marker) => head.includes(marker))
