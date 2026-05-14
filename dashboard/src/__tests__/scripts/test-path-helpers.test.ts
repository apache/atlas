/**
 * Pre-commit / pre-push test-path guard helpers (dashboard/scripts).
 *
 * @jest-environment node
 */

import path from 'path'

const dashboardRoot = path.resolve(__dirname, '../../..')

describe('test-path-helpers (EntityForm vs views/__tests__)', () => {
	let hasTestsOnDisk: (root: string, rel: string) => boolean
	let isUiSourcePath: (rel: string) => boolean

	beforeAll(async () => {
		const mod = await import('../../../scripts/lib/test-path-helpers.mjs')
		hasTestsOnDisk = mod.hasTestsOnDisk
		isUiSourcePath = mod.isUiSourcePath
	})

	it('covers src/views/Entity/EntityForm.tsx with src/views/__tests__/EntityForm.test.tsx', () => {
		expect(
			hasTestsOnDisk(dashboardRoot, 'src/views/Entity/EntityForm.tsx'),
		).toBe(true)
	})

	it('treats EntityForm under views as a guarded UI source path', () => {
		expect(isUiSourcePath('src/views/Entity/EntityForm.tsx')).toBe(true)
	})
})
