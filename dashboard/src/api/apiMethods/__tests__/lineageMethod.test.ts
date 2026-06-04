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
 * Unit tests for lineageMethod.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100% (18/18)
 * - Functions: 100% (4/4)
 * - Lines: 100% (18/18)
 */

import {
	getLineageData,
	addLineageData,
	getRelationshipData,
	saveRelationShip
} from '../lineageMethod'
import { _get } from '../apiMethod'
import { lineageApiUrl, relationsApiUrl } from '../../../api/apiUrlLinks/lineageApiUrl'

// Mock dependencies
jest.mock('../apiMethod', () => ({
	_get: jest.fn()
}))

const mockLineageApiUrl = jest.fn((guid: string) => `/api/lineage/${guid}`)
const mockRelationsApiUrl = jest.fn((options: any) => {
	const key = options?.guid ? `guid/${options.guid}` : options?.type ? `type/${options.type}` : 'list'
	return `/api/relations/${key}`
})

jest.mock('../../../api/apiUrlLinks/lineageApiUrl', () => ({
	lineageApiUrl: (...args: any[]) => mockLineageApiUrl(...args),
	relationsApiUrl: (...args: any[]) => mockRelationsApiUrl(...args)
}))

describe('lineageMethod', () => {
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
		mockLineageApiUrl.mockImplementation((guid: string) => `/api/lineage/${guid}`)
		mockRelationsApiUrl.mockImplementation((options: any) => {
			const key = options?.guid ? `guid/${options.guid}` : options?.type ? `type/${options.type}` : 'list'
			return `/api/relations/${key}`
		})
	})

	describe('getLineageData', () => {
		it('should call _get with correct URL and params', async () => {
			const guid = 'test-guid-123'
			const params = { depth: 2 }
			const result = await getLineageData(guid, params)

			expect(mockLineageApiUrl).toHaveBeenCalledWith(guid)
			expect(mockGet).toHaveBeenCalledWith('/api/lineage/test-guid-123', {
				method: 'GET',
				params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('addLineageData', () => {
		it('should call _get with POST method and data', async () => {
			const guid = 'test-guid-123'
			const data = { relationships: [{ type: 'input', guid: 'other-guid' }] }
			const result = await addLineageData(guid, data)

			expect(mockLineageApiUrl).toHaveBeenCalledWith(guid)
			expect(mockGet).toHaveBeenCalledWith('/api/lineage/test-guid-123', {
				method: 'POST',
				params: {},
				data
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getRelationshipData', () => {
		it('should call _get with correct URL and params', async () => {
			const options = { type: 'output' }
			const params = { guid: 'test-guid-123' }
			const result = await getRelationshipData(options, params)

			expect(mockRelationsApiUrl).toHaveBeenCalledWith(options)
			expect(mockGet).toHaveBeenCalledWith(
				expect.stringContaining('/api/relations/'),
				{
					method: 'GET',
					params
				}
			)
			expect(result).toEqual(mockResponse)
		})
	})

	describe('saveRelationShip', () => {
		it('should call _get with PUT method and data', async () => {
			const data = { relationshipGuid: 'rel-guid-123', type: 'input' }
			const result = await saveRelationShip(data)

			expect(mockRelationsApiUrl).toHaveBeenCalledWith({})
			expect(mockGet).toHaveBeenCalledWith(
				expect.stringContaining('/api/relations/'),
				{
					method: 'PUT',
					params: {},
					data
				}
			)
			expect(result).toEqual(mockResponse)
		})
	})

	describe('Error Handling', () => {
		it('should propagate errors from getLineageData', async () => {
			const error = new Error('Lineage Error')
			mockGet.mockRejectedValue(error)

			await expect(getLineageData('guid', {})).rejects.toThrow('Lineage Error')
		})

		it('should propagate errors from addLineageData', async () => {
			const error = new Error('Add Lineage Error')
			mockGet.mockRejectedValue(error)

			await expect(addLineageData('guid', {})).rejects.toThrow('Add Lineage Error')
		})

		it('should propagate errors from getRelationshipData', async () => {
			const error = new Error('Relationship Error')
			mockGet.mockRejectedValue(error)

			await expect(getRelationshipData({}, {})).rejects.toThrow('Relationship Error')
		})

		it('should propagate errors from saveRelationShip', async () => {
			const error = new Error('Save Relationship Error')
			mockGet.mockRejectedValue(error)

			await expect(saveRelationShip({})).rejects.toThrow('Save Relationship Error')
		})
	})
})
