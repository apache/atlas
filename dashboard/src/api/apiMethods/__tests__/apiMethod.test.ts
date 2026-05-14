/**
 * Unit tests for apiMethod.ts
 * 
 * Coverage Target:
 * - Statements: ≥80% (target: 4/4 = 100%)
 * - Functions: ≥80% (target: 4/4 = 100%)
 * - Lines: ≥80% (target: 4/4 = 100%)
 */

import { _get, _post, _put, _delete } from '../apiMethod'
import { fetchApi } from '../fetchApi'

// Mock fetchApi
jest.mock('../fetchApi', () => ({
	fetchApi: jest.fn()
}))

describe('apiMethod', () => {
	const mockFetchApi = fetchApi as jest.MockedFunction<typeof fetchApi>
	const mockUrl = '/api/test'
	const mockConfig = {
		method: 'GET',
		params: { id: '123' },
		data: { name: 'test' }
	}
	const mockResponse = {
		data: { success: true },
		status: 200,
		statusText: 'OK',
		headers: {},
		config: {}
	} as any

	beforeEach(() => {
		jest.clearAllMocks()
		mockFetchApi.mockResolvedValue(mockResponse)
	})

	describe('_get', () => {
		it('should call fetchApi with GET method', async () => {
			const result = await _get(mockUrl, mockConfig)

			expect(mockFetchApi).toHaveBeenCalledWith(mockUrl, mockConfig)
			expect(result).toEqual(mockResponse)
		})

		it('should pass through all config parameters', async () => {
			const customConfig = {
				method: 'GET',
				params: { filter: 'active' },
				data: { query: 'test' }
			}

			await _get(mockUrl, customConfig)

			expect(mockFetchApi).toHaveBeenCalledWith(mockUrl, customConfig)
		})
	})

	describe('_post', () => {
		it('should call fetchApi with POST method', async () => {
			const postConfig = {
				...mockConfig,
				method: 'POST'
			}

			const result = await _post(mockUrl, postConfig)

			expect(mockFetchApi).toHaveBeenCalledWith(mockUrl, postConfig)
			expect(result).toEqual(mockResponse)
		})

		it('should pass through data in config', async () => {
			const postConfig = {
				method: 'POST',
				params: {},
				data: { name: 'new item' }
			}

			await _post(mockUrl, postConfig)

			expect(mockFetchApi).toHaveBeenCalledWith(mockUrl, postConfig)
		})
	})

	describe('_put', () => {
		it('should call fetchApi with PUT method', async () => {
			const putConfig = {
				...mockConfig,
				method: 'PUT'
			}

			const result = await _put(mockUrl, putConfig)

			expect(mockFetchApi).toHaveBeenCalledWith(mockUrl, putConfig)
			expect(result).toEqual(mockResponse)
		})

		it('should pass through update data', async () => {
			const putConfig = {
				method: 'PUT',
				params: { id: '123' },
				data: { name: 'updated item' }
			}

			await _put(mockUrl, putConfig)

			expect(mockFetchApi).toHaveBeenCalledWith(mockUrl, putConfig)
		})
	})

	describe('_delete', () => {
		it('should call fetchApi with DELETE method', async () => {
			const deleteConfig = {
				method: 'DELETE',
				params: { id: '123' }
			}

			const result = await _delete(mockUrl, deleteConfig)

			expect(mockFetchApi).toHaveBeenCalledWith(mockUrl, deleteConfig)
			expect(result).toEqual(mockResponse)
		})

		it('should handle delete without data', async () => {
			const deleteConfig = {
				method: 'DELETE',
				params: { id: '123' }
			}

			await _delete(mockUrl, deleteConfig)

			expect(mockFetchApi).toHaveBeenCalledWith(mockUrl, deleteConfig)
		})
	})

	describe('Error Handling', () => {
		it('should propagate errors from fetchApi', async () => {
			const error = new Error('API Error')
			mockFetchApi.mockRejectedValue(error)

			await expect(_get(mockUrl, mockConfig)).rejects.toThrow('API Error')
		})

		it('should propagate errors for POST', async () => {
			const error = new Error('POST Error')
			mockFetchApi.mockRejectedValue(error)

			await expect(_post(mockUrl, mockConfig)).rejects.toThrow('POST Error')
		})

		it('should propagate errors for PUT', async () => {
			const error = new Error('PUT Error')
			mockFetchApi.mockRejectedValue(error)

			await expect(_put(mockUrl, mockConfig)).rejects.toThrow('PUT Error')
		})

		it('should propagate errors for DELETE', async () => {
			const error = new Error('DELETE Error')
			mockFetchApi.mockRejectedValue(error)

			await expect(_delete(mockUrl, mockConfig)).rejects.toThrow('DELETE Error')
		})
	})
})
