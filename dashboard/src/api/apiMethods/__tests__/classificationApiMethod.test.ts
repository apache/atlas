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
 * Unit tests for classificationApiMethod.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100% (18/18)
 * - Functions: 100% (5/5)
 * - Lines: 100% (18/18)
 */

import {
	removeClassification,
	addTag,
	deleteClassification,
	editAssignTag,
	getRootClassificationDef
} from '../classificationApiMethod'
import { _delete, _get, _post, _put } from '../apiMethod'
import {
	removeClassificationUrl,
	addTagUrl,
	deleteTagUrl,
	editAssignTagUrl,
	rootClassificationDefUrl
} from '../../../api/apiUrlLinks/classificationUrl'

// Mock dependencies
jest.mock('../apiMethod', () => ({
	_get: jest.fn(),
	_post: jest.fn(),
	_put: jest.fn(),
	_delete: jest.fn()
}))

const mockRemoveClassificationUrl = jest.fn((guid: string, name: string) => `/api/classification/${guid}/${name}`)
const mockAddTagUrl = jest.fn(() => '/api/classification/tag')
const mockDeleteTagUrl = jest.fn((tagName: string) => `/api/classification/tag/${tagName}`)
const mockEditAssignTagUrl = jest.fn((guid: string) => `/api/classification/tag/${guid}`)
const mockRootClassificationDefUrl = jest.fn((name: string) => `/api/classification/root/${name}`)

jest.mock('../../../api/apiUrlLinks/classificationUrl', () => ({
	removeClassificationUrl: (...args: any[]) => mockRemoveClassificationUrl(...args),
	addTagUrl: (...args: any[]) => mockAddTagUrl(...args),
	deleteTagUrl: (...args: any[]) => mockDeleteTagUrl(...args),
	editAssignTagUrl: (...args: any[]) => mockEditAssignTagUrl(...args),
	rootClassificationDefUrl: (...args: any[]) => mockRootClassificationDefUrl(...args)
}))

describe('classificationApiMethod', () => {
	const mockGet = _get as jest.MockedFunction<typeof _get>
	const mockPost = _post as jest.MockedFunction<typeof _post>
	const mockPut = _put as jest.MockedFunction<typeof _put>
	const mockDelete = _delete as jest.MockedFunction<typeof _delete>
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
		mockPost.mockResolvedValue(mockResponse)
		mockPut.mockResolvedValue(mockResponse)
		mockDelete.mockResolvedValue(mockResponse)
		
		// Reset URL helper mocks to return correct values
		mockRemoveClassificationUrl.mockImplementation((guid: string, name: string) => `/api/classification/${guid}/${name}`)
		mockAddTagUrl.mockImplementation(() => '/api/classification/tag')
		mockDeleteTagUrl.mockImplementation((tagName: string) => `/api/classification/tag/${tagName}`)
		mockEditAssignTagUrl.mockImplementation((guid: string) => `/api/classification/tag/${guid}`)
		mockRootClassificationDefUrl.mockImplementation((name: string) => `/api/classification/root/${name}`)
	})

	describe('removeClassification', () => {
		it('should call _delete with correct URL and config', async () => {
			const guid = 'test-guid-123'
			const classificationName = 'PII'
			const result = await removeClassification(guid, classificationName)

			expect(mockRemoveClassificationUrl).toHaveBeenCalledWith(guid, classificationName)
			expect(mockDelete).toHaveBeenCalledWith('/api/classification/test-guid-123/PII', {
				method: 'DELETE',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('addTag', () => {
		it('should call _post with correct URL and data', async () => {
			const params = { name: 'NewTag', description: 'Test tag' }
			const result = await addTag(params)

			expect(mockAddTagUrl).toHaveBeenCalled()
			expect(mockPost).toHaveBeenCalledWith('/api/classification/tag', {
				method: 'POST',
				params: {},
				data: params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('deleteClassification', () => {
		it('should call _delete with correct URL', async () => {
			const tagName = 'TestTag'
			const result = await deleteClassification(tagName)

			expect(mockDeleteTagUrl).toHaveBeenCalledWith(tagName)
			expect(mockDelete).toHaveBeenCalledWith('/api/classification/tag/TestTag', {
				method: 'DELETE',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('editAssignTag', () => {
		it('should call _put with correct URL and data', async () => {
			const guid = 'test-guid-123'
			const params = { tags: ['tag1', 'tag2'] }
			const result = await editAssignTag(guid, params)

			expect(mockEditAssignTagUrl).toHaveBeenCalledWith(guid)
			expect(mockPut).toHaveBeenCalledWith('/api/classification/tag/test-guid-123', {
				method: 'PUT',
				params: {},
				data: params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getRootClassificationDef', () => {
		it('should call _get with correct URL and config', async () => {
			const name = 'ClassificationDef'
			const result = await getRootClassificationDef(name)

			expect(mockRootClassificationDefUrl).toHaveBeenCalledWith(name)
			expect(mockGet).toHaveBeenCalledWith('/api/classification/root/ClassificationDef', {
				method: 'GET',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('Error Handling', () => {
		it('should propagate errors from removeClassification', async () => {
			const error = new Error('Delete Error')
			mockDelete.mockRejectedValue(error)

			await expect(removeClassification('guid', 'name')).rejects.toThrow('Delete Error')
		})

		it('should propagate errors from addTag', async () => {
			const error = new Error('Post Error')
			mockPost.mockRejectedValue(error)

			await expect(addTag({})).rejects.toThrow('Post Error')
		})

		it('should propagate errors from getRootClassificationDef', async () => {
			const error = new Error('Get Error')
			mockGet.mockRejectedValue(error)

			await expect(getRootClassificationDef('name')).rejects.toThrow('Get Error')
		})
	})
})
