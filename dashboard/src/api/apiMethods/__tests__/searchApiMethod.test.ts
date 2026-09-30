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
 * Unit tests for searchApiMethod.ts
 * 
 * Coverage Target:
 * - Statements: ≥80% (target: 5+/6)
 * - Branches: ≥70% (target: 2/2 = 100%)
 * - Functions: ≥80% (target: 4/4 = 100%)
 * - Lines: ≥80% (target: 5+/6)
 */

import {
	getBasicSearchResult,
	getRelationShipResult,
	getGlobalSearchResult,
	getRelationShip
} from '../searchApiMethod'
import { fetchApi } from '../fetchApi'
import { searchApiUrl } from '../../apiUrlLinks/searchApiUrl'

// Mock dependencies
jest.mock('../fetchApi', () => ({
	fetchApi: jest.fn()
}))

const mockSearchApiUrl = jest.fn((type: string) => `/api/search/${type}`)

jest.mock('../../apiUrlLinks/searchApiUrl', () => ({
	searchApiUrl: (...args: any[]) => mockSearchApiUrl(...args)
}))

describe('searchApiMethod', () => {
	const mockFetchApi = fetchApi as jest.MockedFunction<typeof fetchApi>
	const mockResponse = {
		data: { results: [] },
		status: 200,
		statusText: 'OK',
		headers: {},
		config: {}
	} as any

	beforeEach(() => {
		jest.clearAllMocks()
		mockFetchApi.mockResolvedValue(mockResponse)
		mockSearchApiUrl.mockImplementation((type: string) => `/api/search/${type}`)
	})

	describe('getBasicSearchResult', () => {
		it('should use GET method when searchType is "dsl"', async () => {
			const params = { query: 'test', type: 'DataSet' }
			const result = await getBasicSearchResult(params, 'dsl')

			expect(mockSearchApiUrl).toHaveBeenCalledWith('dsl')
			expect(mockFetchApi).toHaveBeenCalledWith('/api/search/dsl', {
				method: 'GET',
				...params
			})
			expect(result).toEqual(mockResponse)
		})

		it('should use POST method when searchType is not "dsl"', async () => {
			const params = { query: 'test', type: 'DataSet' }
			const result = await getBasicSearchResult(params, 'basic')

			expect(mockSearchApiUrl).toHaveBeenCalledWith('basic')
			expect(mockFetchApi).toHaveBeenCalledWith('/api/search/basic', {
				method: 'POST',
				...params
			})
			expect(result).toEqual(mockResponse)
		})

		it('should use POST method when searchType is null', async () => {
			const params = { query: 'test' }
			const result = await getBasicSearchResult(params, null)

			expect(mockSearchApiUrl).toHaveBeenCalledWith('')
			expect(mockFetchApi).toHaveBeenCalledWith('/api/search/', {
				method: 'POST',
				...params
			})
			expect(result).toEqual(mockResponse)
		})

		it('should pass through all params', async () => {
			const params = {
				query: 'test',
				type: 'DataSet',
				limit: 10,
				offset: 0
			}
			await getBasicSearchResult(params, 'dsl')

			expect(mockFetchApi).toHaveBeenCalledWith(
				expect.any(String),
				expect.objectContaining(params)
			)
		})
	})

	describe('getRelationShipResult', () => {
		it('should call fetchApi with POST method', async () => {
			const params = { guid: '123', type: 'relations' }
			const result = await getRelationShipResult(params)

			expect(mockSearchApiUrl).toHaveBeenCalledWith('relations')
			expect(mockFetchApi).toHaveBeenCalledWith('/api/search/relations', {
				method: 'POST',
				...params
			})
			expect(result).toEqual(mockResponse)
		})

		it('should pass through all params', async () => {
			const params = { guid: '123', depth: 2 }
			await getRelationShipResult(params)

			expect(mockFetchApi).toHaveBeenCalledWith(
				expect.any(String),
				expect.objectContaining(params)
			)
		})
	})

	describe('getGlobalSearchResult', () => {
		it('should call fetchApi with GET method', async () => {
			const searchTerm = 'test search'
			const params = { limit: 10 }
			const result = await getGlobalSearchResult(searchTerm, params)

			expect(mockSearchApiUrl).toHaveBeenCalledWith(searchTerm)
			expect(mockFetchApi).toHaveBeenCalledWith('/api/search/test search', {
				method: 'GET',
				...params
			})
			expect(result).toEqual(mockResponse)
		})

		it('should pass through all params', async () => {
			const params = { limit: 20, offset: 0 }
			await getGlobalSearchResult('search', params)

			expect(mockFetchApi).toHaveBeenCalledWith(
				expect.any(String),
				expect.objectContaining(params)
			)
		})
	})

	describe('getRelationShip', () => {
		it('should call fetchApi with GET method', async () => {
			const params = { guid: '123' }
			const result = await getRelationShip(params)

			expect(mockSearchApiUrl).toHaveBeenCalledWith('relationship')
			expect(mockFetchApi).toHaveBeenCalledWith('/api/search/relationship', {
				method: 'GET',
				...params
			})
			expect(result).toEqual(mockResponse)
		})

		it('should pass through all params', async () => {
			const params = { guid: '123', type: 'output' }
			await getRelationShip(params)

			expect(mockFetchApi).toHaveBeenCalledWith(
				expect.any(String),
				expect.objectContaining(params)
			)
		})
	})

	describe('Error Handling', () => {
		it('should propagate errors from fetchApi', async () => {
			const error = new Error('API Error')
			mockFetchApi.mockRejectedValue(error)

			await expect(getBasicSearchResult({}, 'dsl')).rejects.toThrow('API Error')
		})

		it('should propagate errors for getRelationShipResult', async () => {
			const error = new Error('Relation Error')
			mockFetchApi.mockRejectedValue(error)

			await expect(getRelationShipResult({})).rejects.toThrow('Relation Error')
		})
	})
})
