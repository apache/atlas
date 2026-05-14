/**
 * Unit tests for metricsApiUrl.ts
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
		if (url === 'url') return '/mock-base-url/api/atlas'
		return '/mock-base-url/api/atlas/v2'
	})
}))

import {
	metricsApiUrl,
	metricsAllCollectionTimeApiUrl,
	metricsCollectionTimeApiUrl,
	metricsGraphUrl,
	debugMetricsUrl
} from '../metricsApiUrl'
import { getBaseApiUrl } from '../commonApiUrl'

const mockGetBaseApiUrl = getBaseApiUrl as jest.MockedFunction<typeof getBaseApiUrl>

describe('metricsApiUrl', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetBaseApiUrl.mockImplementation((url: string) => {
			if (url === 'url') return '/mock-base-url/api/atlas'
			return '/mock-base-url/api/atlas/v2'
		})
	})

	describe('metricsApiUrl', () => {
		it('should return correct metrics API URL', () => {
			const result = metricsApiUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('url')
			expect(result).toBe('/mock-base-url/api/atlas/admin/metrics')
		})

		it('should always return the same URL', () => {
			const result1 = metricsApiUrl()
			const result2 = metricsApiUrl()
			expect(result1).toBe(result2)
			expect(result1).toBe('/mock-base-url/api/atlas/admin/metrics')
		})
	})

	describe('metricsAllCollectionTimeApiUrl', () => {
		it('should return correct URL for all collection time metrics', () => {
			const result = metricsAllCollectionTimeApiUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('url')
			expect(result).toBe('/mock-base-url/api/atlas/admin/metricsstats')
		})

		it('should always return the same URL', () => {
			const result1 = metricsAllCollectionTimeApiUrl()
			const result2 = metricsAllCollectionTimeApiUrl()
			expect(result1).toBe(result2)
		})
	})

	describe('metricsCollectionTimeApiUrl', () => {
		it('should return correct URL for collection time metrics', () => {
			const result = metricsCollectionTimeApiUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('url')
			expect(result).toBe('/mock-base-url/api/atlas/admin/metricsstat')
		})

		it('should always return the same URL', () => {
			const result1 = metricsCollectionTimeApiUrl()
			const result2 = metricsCollectionTimeApiUrl()
			expect(result1).toBe(result2)
		})
	})

	describe('metricsGraphUrl', () => {
		it('should return correct URL for metrics graph', () => {
			const result = metricsGraphUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('url')
			expect(result).toBe('/mock-base-url/api/atlas/admin/metricsstats/charts')
		})

		it('should always return the same URL', () => {
			const result1 = metricsGraphUrl()
			const result2 = metricsGraphUrl()
			expect(result1).toBe(result2)
		})
	})

	describe('debugMetricsUrl', () => {
		it('should return correct URL for debug metrics', () => {
			const result = debugMetricsUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('url')
			expect(result).toBe('/mock-base-url/api/atlas/admin/debug/metrics')
		})

		it('should always return the same URL', () => {
			const result1 = debugMetricsUrl()
			const result2 = debugMetricsUrl()
			expect(result1).toBe(result2)
		})
	})
})
