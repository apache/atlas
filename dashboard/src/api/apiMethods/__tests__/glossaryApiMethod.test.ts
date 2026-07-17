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
 * Unit tests for glossaryApiMethod.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100% (55/55)
 * - Functions: 100% (15/15)
 * - Lines: 100% (55/55)
 */

import {
	removeTerm,
	getGlossary,
	getGlossaryImportTmpl,
	getGlossaryImport,
	getGlossaryType,
	createGlossary,
	editGlossary,
	deleteGlossaryorTerm,
	createTermorCategory,
	editTermorCatgeory,
	assignTermstoEntites,
	assignTermstoCategory,
	assignGlossaryType,
	removeTermorCategory,
	deleteGlossaryorType
} from '../glossaryApiMethod'
import { _delete, _get, _post, _put } from '../apiMethod'
// Import URL helpers - they will be mocked
import {
	removeTermUrl,
	glossaryUrl,
	glossaryImportTempUrl,
	glossaryImportUrl,
	glossaryTypeUrl,
	createTermorCategoryUrl,
	editGlossaryUrl,
	editTermorCategoryUrl,
	deleteGlossaryorTermUrl,
	assignTermtoEntitiesUrl,
	assignTermtoCategoryUrl,
	assignGlossaryTypeUrl,
	removeTermorCatgeoryUrl
} from '../../../api/apiUrlLinks/glossaryUrl'

// Mock dependencies
jest.mock('../apiMethod', () => ({
	_get: jest.fn(),
	_post: jest.fn(),
	_put: jest.fn(),
	_delete: jest.fn()
}))

const mockRemoveTermUrl = jest.fn((termId: string) => `/api/glossary/term/${termId}`)
const mockGlossaryUrl = jest.fn(() => '/api/glossary')
const mockGlossaryImportTempUrl = jest.fn(() => '/api/glossary/import/template')
const mockGlossaryImportUrl = jest.fn(() => '/api/glossary/import')
const mockGlossaryTypeUrl = jest.fn((type: string, guid: string) => `/api/glossary/type/${type}/${guid}`)
const mockCreateTermorCategoryUrl = jest.fn((type: string) => `/api/glossary/${type}`)
const mockEditGlossaryUrl = jest.fn((guid: string) => `/api/glossary/${guid}`)
const mockEditTermorCategoryUrl = jest.fn((type: string, guid: string) => `/api/glossary/${type}/${guid}`)
const mockDeleteGlossaryorTermUrl = jest.fn((guid: string) => `/api/glossary/${guid}`)
const mockAssignTermtoEntitiesUrl = jest.fn((termId: string) => `/api/glossary/term/${termId}/entities`)
const mockAssignTermtoCategoryUrl = jest.fn((categoryId: string) => `/api/glossary/category/${categoryId}`)
const mockAssignGlossaryTypeUrl = jest.fn((guid: string) => `/api/glossary/type/${guid}`)
const mockRemoveTermorCatgeoryUrl = jest.fn((guid: string, type: string) => `/api/glossary/${type}/${guid}`)

jest.mock('../../../api/apiUrlLinks/glossaryUrl', () => ({
	removeTermUrl: (...args: any[]) => mockRemoveTermUrl(...args),
	glossaryUrl: (...args: any[]) => mockGlossaryUrl(...args),
	glossaryImportTempUrl: (...args: any[]) => mockGlossaryImportTempUrl(...args),
	glossaryImportUrl: (...args: any[]) => mockGlossaryImportUrl(...args),
	glossaryTypeUrl: (...args: any[]) => mockGlossaryTypeUrl(...args),
	createTermorCategoryUrl: (...args: any[]) => mockCreateTermorCategoryUrl(...args),
	editGlossaryUrl: (...args: any[]) => mockEditGlossaryUrl(...args),
	editTermorCategoryUrl: (...args: any[]) => mockEditTermorCategoryUrl(...args),
	deleteGlossaryorTermUrl: (...args: any[]) => mockDeleteGlossaryorTermUrl(...args),
	assignTermtoEntitiesUrl: (...args: any[]) => mockAssignTermtoEntitiesUrl(...args),
	assignTermtoCategoryUrl: (...args: any[]) => mockAssignTermtoCategoryUrl(...args),
	assignGlossaryTypeUrl: (...args: any[]) => mockAssignGlossaryTypeUrl(...args),
	removeTermorCatgeoryUrl: (...args: any[]) => mockRemoveTermorCatgeoryUrl(...args)
}))

describe('glossaryApiMethod', () => {
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
		mockRemoveTermUrl.mockImplementation((termId: string) => `/api/glossary/term/${termId}`)
		mockGlossaryUrl.mockImplementation(() => '/api/glossary')
		mockGlossaryImportTempUrl.mockImplementation(() => '/api/glossary/import/template')
		mockGlossaryImportUrl.mockImplementation(() => '/api/glossary/import')
		mockGlossaryTypeUrl.mockImplementation((type: string, guid: string) => `/api/glossary/type/${type}/${guid}`)
		mockCreateTermorCategoryUrl.mockImplementation((type: string) => `/api/glossary/${type}`)
		mockEditGlossaryUrl.mockImplementation((guid: string) => `/api/glossary/${guid}`)
		mockEditTermorCategoryUrl.mockImplementation((type: string, guid: string) => `/api/glossary/${type}/${guid}`)
		mockDeleteGlossaryorTermUrl.mockImplementation((guid: string) => `/api/glossary/${guid}`)
		mockAssignTermtoEntitiesUrl.mockImplementation((termId: string) => `/api/glossary/term/${termId}/entities`)
		mockAssignTermtoCategoryUrl.mockImplementation((categoryId: string) => `/api/glossary/category/${categoryId}`)
		mockAssignGlossaryTypeUrl.mockImplementation((guid: string) => `/api/glossary/type/${guid}`)
		mockRemoveTermorCatgeoryUrl.mockImplementation((guid: string, type: string) => `/api/glossary/${type}/${guid}`)
	})

	describe('removeTerm', () => {
		it('should call _put with correct URL and data array', async () => {
			const termId = 'term-123'
			const data = { guid: 'entity-guid', relationshipGuid: 'rel-guid' }
			const result = await removeTerm(termId, data)

			expect(mockRemoveTermUrl).toHaveBeenCalledWith(termId)
			expect(mockPut).toHaveBeenCalledWith('/api/glossary/term/term-123', {
				method: 'PUT',
				params: {},
				data: [data]
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getGlossary', () => {
		it('should call _get with correct URL', async () => {
			const result = await getGlossary()

			expect(mockGlossaryUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/glossary', {
				method: 'GET',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getGlossaryImportTmpl', () => {
		it('should call _get with correct URL and params', async () => {
			const params = { type: 'glossary' }
			const result = await getGlossaryImportTmpl(params)

			expect(mockGlossaryImportTempUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/glossary/import/template', {
				method: 'GET',
				params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getGlossaryImport', () => {
		it('should call _get with POST method, data and uploadProgress', async () => {
			const params = { file: 'test.csv' }
			const uploadProgress = { onUploadProgress: jest.fn() }
			const result = await getGlossaryImport(params, uploadProgress)

			expect(mockGlossaryImportUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/glossary/import', {
				method: 'POST',
				params: {},
				data: params,
				...uploadProgress
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getGlossaryType', () => {
		it('should call _get with correct URL', async () => {
			const glossaryType = 'Term'
			const guid = 'guid-123'
			const result = await getGlossaryType(glossaryType, guid)

			expect(mockGlossaryTypeUrl).toHaveBeenCalledWith(glossaryType, guid)
			expect(mockGet).toHaveBeenCalledWith('/api/glossary/type/Term/guid-123', {
				method: 'GET',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('createGlossary', () => {
		it('should call _post with correct URL and data', async () => {
			const params = { name: 'New Glossary', description: 'Test' }
			const result = await createGlossary(params)

			expect(mockGlossaryUrl).toHaveBeenCalled()
			expect(mockPost).toHaveBeenCalledWith('/api/glossary', {
				method: 'POST',
				params: {},
				data: params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('createTermorCategory', () => {
		it('should call _post with correct URL and data', async () => {
			const type = 'Term'
			const params = { name: 'New Term', glossaryGuid: 'glossary-123' }
			const result = await createTermorCategory(type, params)

			expect(mockCreateTermorCategoryUrl).toHaveBeenCalledWith(type)
			expect(mockPost).toHaveBeenCalledWith('/api/glossary/Term', {
				method: 'POST',
				params: {},
				data: params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('editGlossary', () => {
		it('should call _put with correct URL and data', async () => {
			const guid = 'glossary-123'
			const params = { name: 'Updated Glossary' }
			const result = await editGlossary(guid, params)

			expect(mockEditGlossaryUrl).toHaveBeenCalledWith(guid)
			expect(mockPut).toHaveBeenCalledWith('/api/glossary/glossary-123', {
				method: 'PUT',
				params: {},
				data: params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('editTermorCatgeory', () => {
		it('should call _put with correct URL and data', async () => {
			const type = 'Term'
			const guid = 'term-123'
			const params = { name: 'Updated Term' }
			const result = await editTermorCatgeory(type, guid, params)

			expect(mockEditTermorCategoryUrl).toHaveBeenCalledWith(type, guid)
			expect(mockPut).toHaveBeenCalledWith('/api/glossary/Term/term-123', {
				method: 'PUT',
				params: {},
				data: params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('deleteGlossaryorTerm', () => {
		it('should call _delete with correct URL', async () => {
			const guid = 'glossary-123'
			const result = await deleteGlossaryorTerm(guid)

			expect(mockDeleteGlossaryorTermUrl).toHaveBeenCalledWith(guid)
			expect(mockDelete).toHaveBeenCalledWith('/api/glossary/glossary-123', {
				method: 'DELETE',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('deleteGlossaryorType', () => {
		it('should call _delete with correct URL', async () => {
			const guid = 'type-123'
			const result = await deleteGlossaryorType(guid)

			expect(mockAssignGlossaryTypeUrl).toHaveBeenCalledWith(guid)
			expect(mockDelete).toHaveBeenCalledWith('/api/glossary/type/type-123', {
				method: 'DELETE',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('assignTermstoEntites', () => {
		it('should call _put with POST method and data', async () => {
			const termId = 'term-123'
			const data = { guid: 'entity-guid', relationshipGuid: 'rel-guid' }
			const result = await assignTermstoEntites(termId, data)

			expect(mockAssignTermtoEntitiesUrl).toHaveBeenCalledWith(termId)
			expect(mockPut).toHaveBeenCalledWith('/api/glossary/term/term-123/entities', {
				method: 'POST',
				params: {},
				data
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('assignTermstoCategory', () => {
		it('should call _put with PUT method and data', async () => {
			const categoryId = 'category-123'
			const data = { termGuids: ['term-1', 'term-2'] }
			const result = await assignTermstoCategory(categoryId, data)

			expect(mockAssignTermtoCategoryUrl).toHaveBeenCalledWith(categoryId)
			expect(mockPut).toHaveBeenCalledWith('/api/glossary/category/category-123', {
				method: 'PUT',
				params: {},
				data
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('assignGlossaryType', () => {
		it('should call _put with PUT method and data', async () => {
			const glossaryTypeGuid = 'type-123'
			const data = { entityGuids: ['entity-1', 'entity-2'] }
			const result = await assignGlossaryType(glossaryTypeGuid, data)

			expect(mockAssignGlossaryTypeUrl).toHaveBeenCalledWith(glossaryTypeGuid)
			expect(mockPut).toHaveBeenCalledWith('/api/glossary/type/type-123', {
				method: 'PUT',
				params: {},
				data
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('removeTermorCategory', () => {
		it('should call _delete with PUT method and data', async () => {
			const guid = 'term-123'
			const glossaryType = 'Term'
			const params = { relationshipGuid: 'rel-guid' }
			const result = await removeTermorCategory(guid, glossaryType, params)

			expect(mockRemoveTermorCatgeoryUrl).toHaveBeenCalledWith(guid, glossaryType)
			expect(mockDelete).toHaveBeenCalledWith('/api/glossary/Term/term-123', {
				method: 'PUT',
				params: {},
				data: params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('Error Handling', () => {
		it('should propagate errors from getGlossary', async () => {
			const error = new Error('Get Glossary Error')
			mockGet.mockRejectedValue(error)

			await expect(getGlossary()).rejects.toThrow('Get Glossary Error')
		})

		it('should propagate errors from createGlossary', async () => {
			const error = new Error('Create Glossary Error')
			mockPost.mockRejectedValue(error)

			await expect(createGlossary({})).rejects.toThrow('Create Glossary Error')
		})
	})
})
