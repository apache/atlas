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
 * Unit tests for entityFormApiMethod.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100% (19/19)
 * - Functions: 100% (4/4)
 * - Lines: 100% (16/16)
 */

import {
	getAttributes,
	createEntity,
	getEntity,
	getTypedef
} from '../entityFormApiMethod'
import { _get } from '../apiMethod'
import {
	geAttributeUrl,
	getEntityUrl,
	getTypedefUrl
} from '../../../api/apiUrlLinks/entityFormApiUrl'
import { entitiesApiUrl } from '../../../api/apiUrlLinks/entitiesApiUrl'

// Mock dependencies
jest.mock('../apiMethod', () => ({
	_get: jest.fn()
}))

const mockGeAttributeUrl = jest.fn(() => '/api/entity/attributes')
const mockGetEntityUrl = jest.fn((guid: string) => `/api/entity/${guid}`)
const mockGetTypedefUrl = jest.fn((typedef: string) => `/api/typedef/${typedef}`)
const mockEntitiesApiUrl = jest.fn(() => '/api/entities')

jest.mock('../../../api/apiUrlLinks/entityFormApiUrl', () => ({
	geAttributeUrl: (...args: any[]) => mockGeAttributeUrl(...args),
	getEntityUrl: (...args: any[]) => mockGetEntityUrl(...args),
	getTypedefUrl: (...args: any[]) => mockGetTypedefUrl(...args)
}))

jest.mock('../../../api/apiUrlLinks/entitiesApiUrl', () => ({
	entitiesApiUrl: (...args: any[]) => mockEntitiesApiUrl(...args)
}))

describe('entityFormApiMethod', () => {
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
		mockGeAttributeUrl.mockImplementation(() => '/api/entity/attributes')
		mockGetEntityUrl.mockImplementation((guid: string) => `/api/entity/${guid}`)
		mockGetTypedefUrl.mockImplementation((typedef: string) => `/api/typedef/${typedef}`)
		mockEntitiesApiUrl.mockImplementation(() => '/api/entities')
	})

	describe('getAttributes', () => {
		it('should call _get with correct URL and params', async () => {
			const params = { type: 'DataSet' }
			const result = await getAttributes(params)

			expect(mockGeAttributeUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/entity/attributes', {
				method: 'GET',
				params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('createEntity', () => {
		it('should call _get with POST method and data', async () => {
			const params = { name: 'NewEntity', type: 'DataSet' }
			const result = await createEntity(params)

			expect(mockEntitiesApiUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/entities', {
				method: 'POST',
				params: {},
				data: params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getEntity', () => {
		it('should call _get with correct URL, method and params', async () => {
			const guid = 'test-guid-123'
			const method = 'GET'
			const params = { includeDeleted: false }
			const result = await getEntity(guid, method, params)

			expect(mockGetEntityUrl).toHaveBeenCalledWith(guid)
			expect(mockGet).toHaveBeenCalledWith('/api/entity/test-guid-123', {
				method,
				params: { ...params, ignoreRelationships: true }
			})
			expect(result).toEqual(mockResponse)
		})

		it('should handle different HTTP methods', async () => {
			const guid = 'test-guid-123'
			const method = 'PUT'
			const params = { name: 'UpdatedEntity' }
			await getEntity(guid, method, params)

			expect(mockGet).toHaveBeenCalledWith(
				expect.any(String),
				expect.objectContaining({ method: 'PUT' })
			)
		})
	})

	describe('getTypedef', () => {
		it('should call _get with correct URL and params', async () => {
			const typedef = 'DataSet'
			const params = { includeDeleted: false }
			const result = await getTypedef(typedef, params)

			expect(mockGetTypedefUrl).toHaveBeenCalledWith(typedef)
			expect(mockGet).toHaveBeenCalledWith('/api/typedef/DataSet', {
				method: 'GET',
				params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('Error Handling', () => {
		it('should propagate errors from getAttributes', async () => {
			const error = new Error('Attributes Error')
			mockGet.mockRejectedValue(error)

			await expect(getAttributes({})).rejects.toThrow('Attributes Error')
		})

		it('should propagate errors from createEntity', async () => {
			const error = new Error('Create Error')
			mockGet.mockRejectedValue(error)

			await expect(createEntity({})).rejects.toThrow('Create Error')
		})

		it('should propagate errors from getEntity', async () => {
			const error = new Error('Entity Error')
			mockGet.mockRejectedValue(error)

			await expect(getEntity('guid', 'GET', {})).rejects.toThrow('Entity Error')
		})

		it('should propagate errors from getTypedef', async () => {
			const error = new Error('Typedef Error')
			mockGet.mockRejectedValue(error)

			await expect(getTypedef('typedef', {})).rejects.toThrow('Typedef Error')
		})
	})
})
