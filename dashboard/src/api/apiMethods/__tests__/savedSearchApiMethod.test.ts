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
 * Unit tests for savedSearchApiMethod.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100% (12/12)
 * - Functions: 100% (3/3)
 * - Lines: 100% (10/10)
 */

import {
	getSavedSearch,
	removeSavedSearch,
	editSavedSearch
} from '../savedSearchApiMethod'
import { _get, _delete, _put } from '../apiMethod'
import { getSavedSearchUrl } from '../../../api/apiUrlLinks/savedSearchApiUrl'

const mockGetSavedSearchUrl = getSavedSearchUrl as jest.MockedFunction<typeof getSavedSearchUrl>

// Mock dependencies
jest.mock('../apiMethod', () => ({
	_get: jest.fn(),
	_delete: jest.fn(),
	_put: jest.fn()
}))

jest.mock('../../../api/apiUrlLinks/savedSearchApiUrl', () => ({
	getSavedSearchUrl: jest.fn()
}))

describe('savedSearchApiMethod', () => {
	const mockGet = _get as jest.MockedFunction<typeof _get>
	const mockDelete = _delete as jest.MockedFunction<typeof _delete>
	const mockPut = _put as jest.MockedFunction<typeof _put>
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
		mockDelete.mockResolvedValue(mockResponse)
		mockPut.mockResolvedValue(mockResponse)
		
		// Setup URL mock implementation
		mockGetSavedSearchUrl.mockImplementation(() => '/api/saved-search')
	})

	describe('getSavedSearch', () => {
		it('should call _get with correct URL and config', async () => {
			const result = await getSavedSearch()

			expect(mockGetSavedSearchUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/saved-search', {
				method: 'GET',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('removeSavedSearch', () => {
		it('should call _delete with correct URL', async () => {
			const guid = 'test-guid-123'
			const result = await removeSavedSearch(guid)

			expect(mockGetSavedSearchUrl).toHaveBeenCalled()
			expect(mockDelete).toHaveBeenCalledWith('/api/saved-search/test-guid-123', {
				method: 'DELETE',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('editSavedSearch', () => {
		it('should call _put with PUT method', async () => {
			const data = { name: 'My Search', query: 'test' }
			const method = 'PUT'
			const result = await editSavedSearch(data, method)

			expect(mockGetSavedSearchUrl).toHaveBeenCalled()
			expect(mockPut).toHaveBeenCalledWith('/api/saved-search', {
				method: 'PUT',
				params: {},
				data
			})
			expect(result).toEqual(mockResponse)
		})

		it('should call _put with POST method', async () => {
			const data = { name: 'New Search', query: 'test' }
			const method = 'POST'
			const result = await editSavedSearch(data, method)

			expect(mockPut).toHaveBeenCalledWith('/api/saved-search', {
				method: 'POST',
				params: {},
				data
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('Error Handling', () => {
		it('should propagate errors from getSavedSearch', async () => {
			const error = new Error('Get Saved Search Error')
			mockGet.mockRejectedValue(error)

			await expect(getSavedSearch()).rejects.toThrow('Get Saved Search Error')
		})

		it('should propagate errors from removeSavedSearch', async () => {
			const error = new Error('Remove Saved Search Error')
			mockDelete.mockRejectedValue(error)

			await expect(removeSavedSearch('guid')).rejects.toThrow('Remove Saved Search Error')
		})

		it('should propagate errors from editSavedSearch', async () => {
			const error = new Error('Edit Saved Search Error')
			mockPut.mockRejectedValue(error)

			await expect(editSavedSearch({}, 'PUT')).rejects.toThrow('Edit Saved Search Error')
		})
	})
})
