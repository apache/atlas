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
 * Unit tests for fetchApi.ts
 * 
 * Coverage Target:
 * - Statements: ≥80% (target: 35+/40)
 * - Branches: ≥70% (target: 30+/43)
 * - Functions: ≥80% (target: 2/2 = 100%)
 * - Lines: ≥80% (target: 35+/40)
 */

import axios, { AxiosError, AxiosRequestConfig, AxiosResponse } from 'axios'
import { fetchApi } from '../fetchApi'
import { globalSessionData } from '../../../utils/Enum'
import { toast } from 'react-toastify'
import { serverErrorHandler } from '@utils/Utils'

// Mock dependencies - hoisted to top level
// Use a factory function that creates mocks and stores them globally for access
const mockAxios = jest.fn()
const mockIsAxiosError = jest.fn()

jest.mock('axios', () => {
	const axiosMock = jest.fn()
	const isAxiosErrorMock = jest.fn()
	;(axiosMock as any).isAxiosError = isAxiosErrorMock
	// Store references globally so we can access them in tests
	;(global as any).__mockAxios = axiosMock
	;(global as any).__mockIsAxiosError = isAxiosErrorMock
	return {
		__esModule: true,
		default: axiosMock,
		isAxiosError: isAxiosErrorMock
	}
})

jest.mock('react-toastify', () => ({
	toast: {
		error: jest.fn(),
		warning: jest.fn()
	}
}))

jest.mock('@utils/Utils', () => ({
	serverErrorHandler: jest.fn()
}))

jest.mock('../../../utils/Enum', () => ({
	globalSessionData: {
		restCrsfHeader: 'X-CSRF-TOKEN',
		crsfToken: 'test-token-123'
	}
}))

// Mock window.location
const mockReplace = jest.fn()
Object.defineProperty(window, 'location', {
	writable: true,
	value: {
		replace: mockReplace
	}
})

describe('fetchApi', () => {
	let mockTime = 0
	const mockUrl = '/api/test'
	const mockConfig: AxiosRequestConfig = {
		method: 'GET',
		params: { id: '123' },
		data: { name: 'test' }
	}
	const mockResponse: AxiosResponse = {
		data: { success: true },
		status: 200,
		statusText: 'OK',
		headers: {},
		config: {} as AxiosRequestConfig
	}

	// Get references to mocked functions
	const getMockAxios = () => (global as any).__mockAxios as jest.MockedFunction<typeof axios>
	const getMockIsAxiosError = () => (global as any).__mockIsAxiosError as jest.MockedFunction<any>

	beforeEach(() => {
		jest.clearAllMocks()
		jest.useFakeTimers()
		mockTime += 4000
		jest.setSystemTime(new Date(mockTime))
		mockReplace.mockClear()
		;(toast.error as jest.Mock).mockClear()
		;(toast.warning as jest.Mock).mockClear()
		;(serverErrorHandler as jest.Mock).mockClear()
		
		// Setup axios mocks
		getMockAxios().mockResolvedValue(mockResponse)
		
		// Setup isAxiosError mock
		getMockIsAxiosError().mockImplementation((error: any) => {
			return error && typeof error === 'object' && error.isAxiosError === true
		})
	})

	afterEach(() => {
		jest.useRealTimers()
	})

	describe('Successful Requests', () => {
		it('should make successful GET request', async () => {
			const result = await fetchApi(mockUrl, mockConfig)

			expect(getMockAxios()).toHaveBeenCalledWith(
				expect.objectContaining({
					url: mockUrl,
					method: 'GET',
					params: mockConfig.params,
					data: mockConfig.data,
					headers: expect.objectContaining({
						[globalSessionData.restCrsfHeader]: expect.anything()
					})
				})
			)
			expect(result).toEqual(mockResponse)
		}, 10000)

		it('should include CSRF token in headers when available', async () => {
			await fetchApi(mockUrl, mockConfig)

			expect(getMockAxios()).toHaveBeenCalledWith(
				expect.objectContaining({
					headers: expect.objectContaining({
						[globalSessionData.restCrsfHeader]: expect.anything()
					})
				})
			)
		}, 10000)

		it('should use empty string for CSRF token when not available', async () => {
			// Temporarily set crsfToken to null
			const originalToken = (globalSessionData as any).crsfToken
			;(globalSessionData as any).crsfToken = null

			await fetchApi(mockUrl, mockConfig)

			expect(getMockAxios()).toHaveBeenCalledWith(
				expect.objectContaining({
					headers: expect.objectContaining({
						[globalSessionData.restCrsfHeader]: '""'
					})
				})
			)

			// Restore original token
			;(globalSessionData as any).crsfToken = originalToken
		}, 10000)

		it('should pass through onUploadProgress callback', async () => {
			const onProgress = jest.fn()
			const configWithProgress = {
				...mockConfig,
				onUploadProgress: onProgress
			}

			await fetchApi(mockUrl, configWithProgress)

			expect(getMockAxios()).toHaveBeenCalledWith(
				expect.objectContaining({
					onUploadProgress: onProgress
				})
			)
		}, 10000)
	})

	describe('Error Handling - Status Codes', () => {
		const createAxiosError = (status: number, statusText?: string): AxiosError => {
			const error = new Error(`Request failed with status ${status}`) as AxiosError
			;(error as any).isAxiosError = true
			error.response = {
				data: { message: 'Error message' },
				status,
				statusText: statusText || 'Error',
				headers: {},
				config: {} as AxiosRequestConfig
			}
			return error
		}

		it('should handle status 0 (network error)', async () => {
			// Set system time to ensure diffTime > 3000
			// prevNetworkErrorTime starts at 0, so we need current time > 3000
			const mockNow = 5000
			jest.setSystemTime(mockNow)
			
			const error = createAxiosError(0, 'error')
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()

			// Status 0 is falsy, so it won't enter the switch, but will call errorHandelingForAbortAndStatus0
			// via the second check if statusText != "abort"
			expect(toast.error).toHaveBeenCalledWith(
				expect.stringContaining('Network Connection Failure')
			)
		}, 10000)

		it('should handle status 401 (unauthorized)', async () => {
			const error = createAxiosError(401)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()

			expect(mockReplace).toHaveBeenCalledWith('login.jsp')
		}, 10000)

		it('should handle status 403 (forbidden)', async () => {
			const error = createAxiosError(403)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()

			expect(serverErrorHandler).not.toHaveBeenCalled()
			expect(mockReplace).not.toHaveBeenCalled()
			jest.advanceTimersByTime(0)
			expect(toast.error).toHaveBeenCalledWith(
				'Error message',
				expect.objectContaining({
					toastId: 'fetch-api-http-403',
					autoClose: 5000
				})
			)
		}, 10000)

		it('should handle status 404 (not found)', async () => {
			const error = createAxiosError(404)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()

			expect(serverErrorHandler).toHaveBeenCalledWith(
				{ responseJSON: error.response?.data },
				'Resource not found'
			)
		}, 10000)

		it('should handle status 419 (session timeout)', async () => {
			const error = createAxiosError(419)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()

			expect(toast.warning).toHaveBeenCalledWith('Session Time Out !!')
			expect(mockReplace).toHaveBeenCalledWith('login.jsp')
		}, 10000)

		it('should handle status 500 (internal server error)', async () => {
			const error = createAxiosError(500)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()

			expect(serverErrorHandler).toHaveBeenCalledWith(
				{ responseJSON: error.response?.data },
				'Internal Server Error'
			)
		}, 10000)

		it('should handle status 503 (service unavailable)', async () => {
			const error = createAxiosError(503)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()

			expect(serverErrorHandler).toHaveBeenCalledWith(
				{ responseJSON: error.response?.data },
				'Service Unavailable'
			)
		}, 10000)

		it('should handle status 504 (gateway timeout)', async () => {
			const error = createAxiosError(504)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()

			expect(serverErrorHandler).toHaveBeenCalledWith(
				{ responseJSON: error.response?.data },
				'Gateway Timeout'
			)
		}, 10000)

		it('should handle other status codes (default case)', async () => {
			const error = createAxiosError(418)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
		}, 10000)
	})

	describe('Error Handling - Network Errors', () => {
		it('should handle abort status text', async () => {
			const error = new Error('Request aborted') as AxiosError
			;(error as any).isAxiosError = true
			error.response = {
				data: {},
				status: 0,
				statusText: 'abort',
				headers: {},
				config: {} as AxiosRequestConfig
			}
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()

			// Should not show network error toast for abort
			expect(toast.error).not.toHaveBeenCalled()
		}, 10000)

		it('should handle non-abort status text', async () => {
			const error = new Error('Network error') as AxiosError
			;(error as any).isAxiosError = true
			error.response = {
				data: {},
				status: 0,
				statusText: 'error',
				headers: {},
				config: {} as AxiosRequestConfig
			}
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()

			expect(toast.error).toHaveBeenCalled()
		}, 10000)

		it('should throttle network error messages (3 second window)', async () => {
			const error = new Error('Network error') as AxiosError
			;(error as any).isAxiosError = true
			error.response = {
				data: {},
				status: 0,
				statusText: 'error',
				headers: {},
				config: {} as AxiosRequestConfig
			}

			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			// First call
			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(toast.error).toHaveBeenCalledTimes(1)

			// Second call immediately (should be throttled)
			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(toast.error).toHaveBeenCalledTimes(1) // Still 1, throttled

			// Wait and call again (should show again)
			jest.advanceTimersByTime(4000)
			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(toast.error).toHaveBeenCalledTimes(2)
		}, 10000)

		it('should handle error without response status', async () => {
			const error = new Error('Network error') as AxiosError
			;(error as any).isAxiosError = true
			error.response = undefined
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
		}, 10000)

		it('should handle error with response but no status property', async () => {
			const error = new Error('Network error') as AxiosError
			;(error as any).isAxiosError = true
			error.response = {
				data: {},
				status: undefined as any,
				statusText: 'error',
				headers: {},
				config: {} as AxiosRequestConfig
			}
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()

			// No HTTP status: switch skipped; res exists and status is not 0 —
			// fetchApi does not show network toast for this shape.
			expect(toast.error).not.toHaveBeenCalled()
		}, 10000)

		it('should handle error with response.status = 0 and statusText = abort', async () => {
			const error = new Error('Aborted') as AxiosError
			;(error as any).isAxiosError = true
			error.response = {
				data: {},
				status: 0,
				statusText: 'abort',
				headers: {},
				config: {} as AxiosRequestConfig
			}
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()

			// Should not show error toast when statusText is "abort"
			expect(toast.error).not.toHaveBeenCalled()
		}, 10000)

		it('should handle error with response.status = 0 and statusText != abort', async () => {
			const error = new Error('Network error') as AxiosError
			;(error as any).isAxiosError = true
			error.response = {
				data: {},
				status: 0,
				statusText: 'error',
				headers: {},
				config: {} as AxiosRequestConfig
			}
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()

			// Should show error toast when statusText != "abort"
			expect(toast.error).toHaveBeenCalled()
		}, 10000)

		it('should throttle network error messages when called within 3 seconds', async () => {
			const error = new Error('Network error') as AxiosError
			;(error as any).isAxiosError = true
			error.response = {
				data: {},
				status: 0,
				statusText: 'error',
				headers: {},
				config: {} as AxiosRequestConfig
			}

			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			// First call
			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(toast.error).toHaveBeenCalledTimes(1)

			// Second call immediately (should be throttled)
			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(toast.error).toHaveBeenCalledTimes(1) // Still 1, throttled

			// Advance time by 4 seconds
			jest.advanceTimersByTime(4000)

			// Third call after 4 seconds (should show again)
			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(toast.error).toHaveBeenCalledTimes(2)
		}, 10000)

		it('should handle error with response.status = 0 and diffTime <= 3000', async () => {
			const error = new Error('Network error') as AxiosError
			;(error as any).isAxiosError = true
			error.response = {
				data: {},
				status: 0,
				statusText: 'error',
				headers: {},
				config: {} as AxiosRequestConfig
			}

			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			const initialCalls = (toast.error as jest.Mock).mock.calls.length

			// First call
			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			const firstCalls = (toast.error as jest.Mock).mock.calls.length

			// Advance time by 2 seconds (less than 3000ms)
			jest.advanceTimersByTime(2000)

			// Second call within 3 seconds (should be throttled)
			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect((toast.error as jest.Mock).mock.calls.length).toBe(firstCalls)
		}, 10000)
	})

	describe('Error Handling - Non-Axios Errors', () => {
		it('should throw non-axios errors', async () => {
			const error = new Error('Non-axios error')
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(false)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow('Non-axios error')
		}, 10000)
	})

	describe('Error Handling - All Status Code Branches', () => {
		const createAxiosError = (status: number, statusText?: string): AxiosError => {
			const error = new Error(`Request failed with status ${status}`) as AxiosError
			;(error as any).isAxiosError = true
			error.response = {
				data: { message: 'Error message' },
				status,
				statusText: statusText || 'Error',
				headers: {},
				config: {} as AxiosRequestConfig
			}
			return error
		}

		it('should handle status 0 branch', async () => {
			const error = createAxiosError(0, 'error')
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(toast.error).toHaveBeenCalled()
		}, 10000)

		it('should handle status 401 branch', async () => {
			const error = createAxiosError(401)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(mockReplace).toHaveBeenCalledWith('login.jsp')
		}, 10000)

		it('should handle status 403 branch', async () => {
			const error = createAxiosError(403)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(serverErrorHandler).not.toHaveBeenCalled()
			expect(mockReplace).not.toHaveBeenCalled()
			jest.advanceTimersByTime(0)
			expect(toast.error).toHaveBeenCalled()
		}, 10000)

		it('should handle status 404 branch', async () => {
			const error = createAxiosError(404)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(serverErrorHandler).toHaveBeenCalled()
		}, 10000)

		it('should handle status 419 branch', async () => {
			const error = createAxiosError(419)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(toast.warning).toHaveBeenCalledWith('Session Time Out !!')
			expect(mockReplace).toHaveBeenCalledWith('login.jsp')
		}, 10000)

		it('should handle status 500 branch', async () => {
			const error = createAxiosError(500)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(serverErrorHandler).toHaveBeenCalled()
		}, 10000)

		it('should handle status 503 branch', async () => {
			const error = createAxiosError(503)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(serverErrorHandler).toHaveBeenCalled()
		}, 10000)

		it('should handle status 504 branch', async () => {
			const error = createAxiosError(504)
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(serverErrorHandler).toHaveBeenCalled()
		}, 10000)

		it('should handle default case in switch statement', async () => {
			const error = createAxiosError(418) // I'm a teapot
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(serverErrorHandler).not.toHaveBeenCalled()
			expect(toast.error).not.toHaveBeenCalled()
		}, 10000)

		it('should handle error.response.status as falsy (null/undefined)', async () => {
			const error = new Error('Network error') as AxiosError
			;(error as any).isAxiosError = true
			error.response = {
				data: {},
				status: null as any,
				statusText: 'error',
				headers: {},
				config: {} as AxiosRequestConfig
			}
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			// Should call errorHandelingForAbortAndStatus0 when statusText != "abort"
			expect(toast.error).toHaveBeenCalled()
		}, 10000)

		it('should handle error.response.status as 0 (falsy but handled)', async () => {
			const error = createAxiosError(0, 'error')
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(toast.error).toHaveBeenCalled()
		}, 10000)

		it('should handle error.response.statusText === "abort" branch', async () => {
			const error = createAxiosError(0, 'abort')
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			// Should not call errorHandelingForAbortAndStatus0 when statusText === "abort"
			expect(toast.error).not.toHaveBeenCalled()
		}, 10000)

		it('should handle error.response.statusText !== "abort" branch', async () => {
			const error = createAxiosError(0, 'network-error')
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			// Should call errorHandelingForAbortAndStatus0 when statusText !== "abort"
			expect(toast.error).toHaveBeenCalled()
		}, 10000)

		it('should handle diffTime <= 3000 branch in errorHandelingForAbortAndStatus0', async () => {
			const error = createAxiosError(0, 'error')
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			// First call
			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(toast.error).toHaveBeenCalledTimes(1)

			// Advance time by 2 seconds (less than 3000ms)
			jest.advanceTimersByTime(2000)

			// Second call within 3 seconds (should be throttled - diffTime <= 3000)
			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(toast.error).toHaveBeenCalledTimes(1) // Still 1, throttled
		}, 10000)

		it('should handle diffTime > 3000 branch in errorHandelingForAbortAndStatus0', async () => {
			const error = createAxiosError(0, 'error')
			getMockAxios().mockRejectedValue(error)
			getMockIsAxiosError().mockReturnValue(true)

			// First call
			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(toast.error).toHaveBeenCalledTimes(1)

			// Advance time by 4 seconds (more than 3000ms)
			jest.advanceTimersByTime(4000)

			// Second call after 4 seconds (should show again - diffTime > 3000)
			await expect(fetchApi(mockUrl, mockConfig)).rejects.toThrow()
			expect(toast.error).toHaveBeenCalledTimes(2)
		}, 10000)
	})
})
