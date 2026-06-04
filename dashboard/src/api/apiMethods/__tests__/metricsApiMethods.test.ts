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
 * Unit tests for metricsApiMethods.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100% (18/18)
 * - Branches: 100% (2/2)
 * - Functions: 100% (4/4)
 * - Lines: 100% (15/15)
 */

import {
	getMetricsEntity,
	getMetricsStats,
	getMetricsGraph,
	getDebugMetrics
} from '../metricsApiMethods'
import { _get } from '../apiMethod'
import {
	metricsApiUrl,
	metricsAllCollectionTimeApiUrl,
	metricsCollectionTimeApiUrl,
	metricsGraphUrl,
	debugMetricsUrl
} from '../../apiUrlLinks/metricsApiUrl'

// Mock dependencies
jest.mock('../apiMethod', () => ({
	_get: jest.fn()
}))

const mockMetricsApiUrl = jest.fn(() => '/api/metrics')
const mockMetricsAllCollectionTimeApiUrl = jest.fn(() => '/api/metrics/collection-time/all')
const mockMetricsCollectionTimeApiUrl = jest.fn(() => '/api/metrics/collection-time')
const mockMetricsGraphUrl = jest.fn(() => '/api/metrics/graph')
const mockDebugMetricsUrl = jest.fn(() => '/api/metrics/debug')

jest.mock('../../apiUrlLinks/metricsApiUrl', () => ({
	metricsApiUrl: (...args: any[]) => mockMetricsApiUrl(...args),
	metricsAllCollectionTimeApiUrl: (...args: any[]) => mockMetricsAllCollectionTimeApiUrl(...args),
	metricsCollectionTimeApiUrl: (...args: any[]) => mockMetricsCollectionTimeApiUrl(...args),
	metricsGraphUrl: (...args: any[]) => mockMetricsGraphUrl(...args),
	debugMetricsUrl: (...args: any[]) => mockDebugMetricsUrl(...args)
}))

describe('metricsApiMethods', () => {
	const mockGet = _get as jest.MockedFunction<typeof _get>
	const mockResponse = {
		data: { success: true },
		status: 200,
		statusText: 'OK',
		headers: {},
		config: {}
	} as any

	beforeEach(() => {
		jest.clearAllMocks()
		mockGet.mockResolvedValue(mockResponse)
		
		// Setup URL mock implementations
		mockMetricsApiUrl.mockImplementation(() => '/api/metrics')
		mockMetricsAllCollectionTimeApiUrl.mockImplementation(() => '/api/metrics/collection-time/all')
		mockMetricsCollectionTimeApiUrl.mockImplementation(() => '/api/metrics/collection-time')
		mockMetricsGraphUrl.mockImplementation(() => '/api/metrics/graph')
		mockDebugMetricsUrl.mockImplementation(() => '/api/metrics/debug')
	})

	describe('getMetricsEntity', () => {
		it('should call _get with correct URL and params', async () => {
			const params = { type: 'entity' }
			const result = await getMetricsEntity(params)

			expect(mockMetricsApiUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/metrics', {
				method: 'GET',
				params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getMetricsStats', () => {
		it('should call _get with all collection time URL when dateValue is "Current"', async () => {
			const dateValue = 'Current'
			const result = await getMetricsStats(dateValue)

			expect(mockMetricsAllCollectionTimeApiUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/metrics/collection-time/all', {
				method: 'GET',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})

		it('should call _get with specific date URL when dateValue is not "Current"', async () => {
			const dateValue = '2024-01-01'
			const result = await getMetricsStats(dateValue)

			expect(mockMetricsCollectionTimeApiUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/metrics/collection-time/2024-01-01', {
				method: 'GET',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getMetricsGraph', () => {
		it('should call _get with correct URL and params', async () => {
			const params = { startDate: '2024-01-01', endDate: '2024-01-31' }
			const result = await getMetricsGraph(params)

			expect(mockMetricsGraphUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/metrics/graph', {
				method: 'GET',
				params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getDebugMetrics', () => {
		it('should call _get with correct URL', async () => {
			const result = await getDebugMetrics()

			expect(mockDebugMetricsUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/metrics/debug', {
				method: 'GET',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('Error Handling', () => {
		it('should propagate errors from getMetricsEntity', async () => {
			const error = new Error('Metrics Entity Error')
			mockGet.mockRejectedValue(error)

			await expect(getMetricsEntity({})).rejects.toThrow('Metrics Entity Error')
		})

		it('should propagate errors from getMetricsStats', async () => {
			const error = new Error('Metrics Stats Error')
			mockGet.mockRejectedValue(error)

			await expect(getMetricsStats('Current')).rejects.toThrow('Metrics Stats Error')
		})

		it('should propagate errors from getMetricsGraph', async () => {
			const error = new Error('Metrics Graph Error')
			mockGet.mockRejectedValue(error)

			await expect(getMetricsGraph({})).rejects.toThrow('Metrics Graph Error')
		})

		it('should propagate errors from getDebugMetrics', async () => {
			const error = new Error('Debug Metrics Error')
			mockGet.mockRejectedValue(error)

			await expect(getDebugMetrics()).rejects.toThrow('Debug Metrics Error')
		})
	})
})
