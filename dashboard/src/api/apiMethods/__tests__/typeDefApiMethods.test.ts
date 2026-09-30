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
 * Unit tests for typeDefApiMethods.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100% (34/34)
 * - Branches: 100% (2/2)
 * - Functions: 100% (8/8)
 * - Lines: 100% (34/34)
 */

import {
	getTypeDef,
	getRootEntityDef,
	getTypeDefHeaders,
	createOrUpdateTag,
	createEditBusinessMetadata,
	updateEnum,
	createEnum
} from '../typeDefApiMethods'

// Import getTypeDefApiResp for direct testing
// Note: getTypeDefApiResp is not exported, so we test it indirectly through getTypeDef
import { _get, _put } from '../apiMethod'
import {
	typeDefApiUrl,
	rootEntityDefUrl,
	typeDefHeaderApiUrl
} from '../../apiUrlLinks/typeDefApiUrl'
import { addOnEntities } from '../../../utils/Enum'

// Mock dependencies
jest.mock('../apiMethod', () => ({
	_get: jest.fn(),
	_put: jest.fn()
}))

const mockTypeDefApiUrl = jest.fn((type: string) => `/api/typedef/${type}`)
const mockRootEntityDefUrl = jest.fn((entity: string) => `/api/typedef/root/${entity}`)
const mockTypeDefHeaderApiUrl = jest.fn(() => '/api/typedef/headers')

jest.mock('../../apiUrlLinks/typeDefApiUrl', () => ({
	typeDefApiUrl: (...args: any[]) => mockTypeDefApiUrl(...args),
	rootEntityDefUrl: (...args: any[]) => mockRootEntityDefUrl(...args),
	typeDefHeaderApiUrl: (...args: any[]) => mockTypeDefHeaderApiUrl(...args)
}))

jest.mock('../../../utils/Enum', () => ({
	addOnEntities: ['DataSet', 'Process', 'Column']
}))

describe('typeDefApiMethods', () => {
	const mockGet = _get as jest.MockedFunction<typeof _get>
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
		mockPut.mockResolvedValue(mockResponse)
		
		// Setup URL mock implementations
		mockTypeDefApiUrl.mockImplementation((type: string) => `/api/typedef/${type}`)
		mockRootEntityDefUrl.mockImplementation((entity: string) => `/api/typedef/root/${entity}`)
		mockTypeDefHeaderApiUrl.mockImplementation(() => '/api/typedef/headers')
	})

	describe('getTypeDef', () => {
		it('should call getTypeDefApiResp with type param', async () => {
			const type = 'DataSet'
			const result = await getTypeDef(type)

			expect(mockTypeDefApiUrl).toHaveBeenCalledWith('')
			expect(mockGet).toHaveBeenCalledWith('/api/typedef/', {
				method: 'GET',
				params: { type }
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getRootEntityDef', () => {
		it('should call _get with first addOnEntities item', async () => {
			const result = await getRootEntityDef()

			expect(mockRootEntityDefUrl).toHaveBeenCalledWith(addOnEntities[0])
			expect(mockGet).toHaveBeenCalledWith('/api/typedef/root/DataSet', {
				method: 'GET',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getTypeDefHeaders', () => {
		it('should call _get with excludeInternalTypesAndReferences param', async () => {
			const result = await getTypeDefHeaders()

			expect(mockTypeDefHeaderApiUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/typedef/headers', {
				method: 'GET',
				params: { excludeInternalTypesAndReferences: true }
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('createOrUpdateTag', () => {
		it('should call _get with POST method when isAdd is true', async () => {
			const type = 'Tag'
			const isAdd = true
			const params = { name: 'NewTag' }
			const result = await createOrUpdateTag(type, isAdd, params)

			expect(mockTypeDefApiUrl).toHaveBeenCalledWith('')
			expect(mockGet).toHaveBeenCalledWith('/api/typedef/', {
				method: 'POST',
				params: { type },
				data: params
			})
			expect(result).toEqual(mockResponse)
		})

		it('should call _get with PUT method when isAdd is false', async () => {
			const type = 'Tag'
			const isAdd = false
			const params = { name: 'UpdatedTag' }
			const result = await createOrUpdateTag(type, isAdd, params)

			expect(mockGet).toHaveBeenCalledWith('/api/typedef/', {
				method: 'PUT',
				params: { type },
				data: params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('createEditBusinessMetadata', () => {
		it('should call _get with POST method', async () => {
			const type = 'BusinessMetadata'
			const method = 'POST'
			const params = { name: 'NewMetadata' }
			const result = await createEditBusinessMetadata(type, method, params)

			expect(mockTypeDefApiUrl).toHaveBeenCalledWith('')
			expect(mockGet).toHaveBeenCalledWith('/api/typedef/', {
				method: 'POST',
				params: { type },
				data: params
			})
			expect(result).toEqual(mockResponse)
		})

		it('should call _get with PUT method', async () => {
			const type = 'BusinessMetadata'
			const method = 'PUT'
			const params = { name: 'UpdatedMetadata' }
			const result = await createEditBusinessMetadata(type, method, params)

			expect(mockGet).toHaveBeenCalledWith('/api/typedef/', {
				method: 'PUT',
				params: { type },
				data: params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('updateEnum', () => {
		it('should call _put with PUT method and data', async () => {
			const params = { name: 'UpdatedEnum', values: ['val1', 'val2'] }
			const result = await updateEnum(params)

			expect(mockTypeDefApiUrl).toHaveBeenCalledWith('')
			expect(mockPut).toHaveBeenCalledWith('/api/typedef/', {
				method: 'PUT',
				params: {},
				data: params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('createEnum', () => {
		it('should call _put with POST method and data', async () => {
			const params = { name: 'NewEnum', values: ['val1', 'val2'] }
			const result = await createEnum(params)

			expect(mockTypeDefApiUrl).toHaveBeenCalledWith('')
			expect(mockPut).toHaveBeenCalledWith('/api/typedef/', {
				method: 'POST',
				params: {},
				data: params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('Error Handling', () => {
		it('should propagate errors from getTypeDef', async () => {
			const error = new Error('Get TypeDef Error')
			mockGet.mockRejectedValue(error)

			await expect(getTypeDef('type')).rejects.toThrow('Get TypeDef Error')
		})

		it('should propagate errors from createOrUpdateTag', async () => {
			const error = new Error('Create/Update Tag Error')
			mockGet.mockRejectedValue(error)

			await expect(createOrUpdateTag('Tag', true, {})).rejects.toThrow('Create/Update Tag Error')
		})

		it('should propagate errors from updateEnum', async () => {
			const error = new Error('Update Enum Error')
			mockPut.mockRejectedValue(error)

			await expect(updateEnum({})).rejects.toThrow('Update Enum Error')
		})
	})
})
