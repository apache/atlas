/**
 * Unit tests for headerApiMethods.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100% (6/6)
 * - Functions: 100% (1/1)
 * - Lines: 100% (6/6)
 */

import { getVersion } from '../headerApiMethods'
import { _get } from '../apiMethod'
import { versionUrl } from '../../../api/apiUrlLinks/headerUrl'

// Mock dependencies
jest.mock('../apiMethod', () => ({
	_get: jest.fn()
}))

const mockVersionUrl = jest.fn(() => '/api/version')

jest.mock('../../../api/apiUrlLinks/headerUrl', () => ({
	versionUrl: (...args: any[]) => mockVersionUrl(...args)
}))

describe('headerApiMethods', () => {
	const mockGet = _get as jest.MockedFunction<typeof _get>
	const mockResponse = {
		data: { version: '1.0.0' },
		status: 200,
		statusText: 'OK',
		headers: {},
		config: {}
	} as any

	beforeEach(() => {
		jest.clearAllMocks()
		mockGet.mockResolvedValue(mockResponse)
		mockVersionUrl.mockImplementation(() => '/api/version')
	})

	describe('getVersion', () => {
		it('should call _get with correct URL and config', async () => {
			const result = await getVersion()

			expect(mockVersionUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/version', {
				method: 'GET',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})

		it('should return version data', async () => {
			const result = await getVersion()

			expect(result.data.version).toBe('1.0.0')
		})
	})

	describe('Error Handling', () => {
		it('should propagate errors from getVersion', async () => {
			const error = new Error('Version Error')
			mockGet.mockRejectedValue(error)

			await expect(getVersion()).rejects.toThrow('Version Error')
		})
	})
})
