/**
 * Unit tests for lineageApiUrl.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100%
 * - Branches: 100%
 * - Functions: 100%
 * - Lines: 100%
 */

// Mock dependencies before imports
jest.mock('../commonApiUrl', () => ({
	getBaseApiUrl: jest.fn((url: string) => {
		if (url === 'urlV2') return '/mock-base-url/api/atlas/v2'
		return '/mock-base-url/api/atlas'
	})
}))

import { lineageApiUrl, relationsApiUrl } from '../lineageApiUrl'
import { getBaseApiUrl } from '../commonApiUrl'

const mockGetBaseApiUrl = getBaseApiUrl as jest.MockedFunction<typeof getBaseApiUrl>

describe('lineageApiUrl', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetBaseApiUrl.mockImplementation((url: string) => {
			if (url === 'urlV2') return '/mock-base-url/api/atlas/v2'
			return '/mock-base-url/api/atlas'
		})
	})

	describe('lineageApiUrl', () => {
		it('should return correct URL for lineage', () => {
			const guid = 'test-guid-123'
			const result = lineageApiUrl(guid)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/lineage/test-guid-123')
		})

		it('should handle different guid values', () => {
			const result1 = lineageApiUrl('guid-456')
			expect(result1).toBe('/mock-base-url/api/atlas/v2/lineage/guid-456')

			const result2 = lineageApiUrl('guid-789')
			expect(result2).toBe('/mock-base-url/api/atlas/v2/lineage/guid-789')
		})

		it('should handle empty guid', () => {
			const result = lineageApiUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/lineage/')
		})
	})

	describe('relationsApiUrl', () => {
		it('should return base relationship URL when options is null', () => {
			const result = relationsApiUrl(null as any)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/relationship')
		})

		it('should return base relationship URL when options is undefined', () => {
			const result = relationsApiUrl(undefined as any)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/relationship')
		})

		it('should return base relationship URL when options is empty object', () => {
			const result = relationsApiUrl({})

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/relationship')
		})

		it('should return URL with guid when options has guid', () => {
			const options = { guid: 'test-guid-123' }
			const result = relationsApiUrl(options)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/relationship/guid/test-guid-123')
		})

		it('should handle different guid values in options', () => {
			const result1 = relationsApiUrl({ guid: 'guid-456' })
			expect(result1).toBe('/mock-base-url/api/atlas/v2/relationship/guid/guid-456')

			const result2 = relationsApiUrl({ guid: 'guid-789' })
			expect(result2).toBe('/mock-base-url/api/atlas/v2/relationship/guid/guid-789')
		})

		it('should handle empty guid in options', () => {
			// Empty string is falsy, so if (guid) won't execute
			const result = relationsApiUrl({ guid: '' })
			expect(result).toBe('/mock-base-url/api/atlas/v2/relationship')
		})

		it('should handle options with other properties but no guid', () => {
			const result = relationsApiUrl({ type: 'input', other: 'value' } as any)
			expect(result).toBe('/mock-base-url/api/atlas/v2/relationship')
		})

		it('should handle options with falsy guid', () => {
			const result1 = relationsApiUrl({ guid: null } as any)
			expect(result1).toBe('/mock-base-url/api/atlas/v2/relationship')

			const result2 = relationsApiUrl({ guid: undefined } as any)
			expect(result2).toBe('/mock-base-url/api/atlas/v2/relationship')
		})
	})
})
