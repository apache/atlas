/**
 * Unit tests for typeDefApiUrl.ts
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
	}),
	getDefApiUrl: jest.fn((name: string) => {
		if (name) {
			return `/mock-base-url/api/atlas/v2/types/typedef/name/${name}`
		}
		return '/mock-base-url/api/atlas/v2/types/typedefs'
	}),
	typedefsUrl: jest.fn(() => ({
		defs: '/mock-base-url/api/atlas/v2/types/typedefs',
		def: '/mock-base-url/api/atlas/v2/types/typedef'
	}))
}))

import {
	typeDefApiUrl,
	rootEntityDefUrl,
	typeDefHeaderApiUrl
} from '../typeDefApiUrl'
import { getBaseApiUrl, getDefApiUrl, typedefsUrl } from '../commonApiUrl'

const mockGetBaseApiUrl = getBaseApiUrl as jest.MockedFunction<typeof getBaseApiUrl>
const mockGetDefApiUrl = getDefApiUrl as jest.MockedFunction<typeof getDefApiUrl>
const mockTypedefsUrl = typedefsUrl as jest.MockedFunction<typeof typedefsUrl>

describe('typeDefApiUrl', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetBaseApiUrl.mockImplementation((url: string) => {
			if (url === 'urlV2') return '/mock-base-url/api/atlas/v2'
			return '/mock-base-url/api/atlas'
		})
		mockGetDefApiUrl.mockImplementation((name: string) => {
			if (name) {
				return `/mock-base-url/api/atlas/v2/types/typedef/name/${name}`
			}
			return '/mock-base-url/api/atlas/v2/types/typedefs'
		})
		mockTypedefsUrl.mockReturnValue({
			defs: '/mock-base-url/api/atlas/v2/types/typedefs',
			def: '/mock-base-url/api/atlas/v2/types/typedef'
		})
	})

	describe('typeDefApiUrl', () => {
		it('should return URL from getDefApiUrl when name is provided', () => {
			const name = 'TestType'
			const result = typeDefApiUrl(name)

			expect(mockGetDefApiUrl).toHaveBeenCalledWith(name)
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/TestType')
		})

		it('should return defs URL when name is empty', () => {
			const result = typeDefApiUrl('')

			expect(mockGetDefApiUrl).toHaveBeenCalledWith('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedefs')
		})

		it('should handle different type names', () => {
			const result1 = typeDefApiUrl('DataSet')
			expect(result1).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/DataSet')

			const result2 = typeDefApiUrl('Process')
			expect(result2).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/Process')
		})

		it('should handle names with special characters', () => {
			const result = typeDefApiUrl('Test-Type_123')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/Test-Type_123')
		})
	})

	describe('rootEntityDefUrl', () => {
		it('should return correct URL for root entity definition', () => {
			const name = 'TestEntity'
			const result = rootEntityDefUrl(name)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/entitydef/name/TestEntity')
		})

		it('should handle different entity names', () => {
			const result1 = rootEntityDefUrl('DataSet')
			expect(result1).toBe('/mock-base-url/api/atlas/v2/types/entitydef/name/DataSet')

			const result2 = rootEntityDefUrl('Table')
			expect(result2).toBe('/mock-base-url/api/atlas/v2/types/entitydef/name/Table')
		})

		it('should handle empty name', () => {
			const result = rootEntityDefUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/entitydef/name/')
		})

		it('should handle names with special characters', () => {
			const result = rootEntityDefUrl('Test-Entity_123')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/entitydef/name/Test-Entity_123')
		})
	})

	describe('typeDefHeaderApiUrl', () => {
		it('should return correct URL for type definition headers', () => {
			const result = typeDefHeaderApiUrl()

			expect(mockTypedefsUrl).toHaveBeenCalled()
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedefs/headers')
		})

		it('should always return the same URL', () => {
			const result1 = typeDefHeaderApiUrl()
			const result2 = typeDefHeaderApiUrl()
			expect(result1).toBe(result2)
			expect(result1).toBe('/mock-base-url/api/atlas/v2/types/typedefs/headers')
		})

		it('should use defs property from typedefsUrl', () => {
			const result = typeDefHeaderApiUrl()
			expect(mockTypedefsUrl).toHaveBeenCalled()
			expect(result).toContain('/headers')
		})
	})
})
