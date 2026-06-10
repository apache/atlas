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
 * Unit tests for detailpageApiMethod.ts
 * 
 * Coverage Target:
 * - Statements: ≥80% (target: 7+/8)
 * - Functions: ≥80% (target: 7/7 = 100%)
 * - Lines: ≥80% (target: 7+/8)
 */

import {
	getDetailPageData,
	getEntityWithRelationships,
	getDetailPageAuditData,
	getDetailPageRauditData,
	getAuditData,
	getLabels,
	getEntityBusinessMetadata,
	getDetailPageRelationship
} from '../detailpageApiMethod'
import { _get, _post } from '../apiMethod'
import {
	detailpageApiUrl,
	detailPageAuditApiUrl,
	detailPageRauditApiUrl,
	auditApiurl,
	detailPageLabelApiUrl,
	detailPageBusinessMetadataApiUrl,
	detailPageRelationshipApiUrl
} from '../../apiUrlLinks/detailpageUrl'

// Mock dependencies
jest.mock('../apiMethod', () => ({
	_get: jest.fn(),
	_post: jest.fn()
}))

const mockDetailpageApiUrl = jest.fn((guid: string, header?: string) => `/api/detail/${guid}${header ? `/${header}` : ''}`)
const mockDetailPageAuditApiUrl = jest.fn((guid: string) => `/api/detail/${guid}/audit`)
const mockDetailPageRauditApiUrl = jest.fn(() => '/api/detail/raudit')
const mockAuditApiurl = jest.fn(() => '/api/audit')
const mockDetailPageLabelApiUrl = jest.fn((guid: string) => `/api/detail/${guid}/labels`)
const mockDetailPageBusinessMetadataApiUrl = jest.fn((guid: string) => `/api/detail/${guid}/business-metadata`)
const mockDetailPageRelationshipApiUrl = jest.fn((guid: string) => `/api/detail/${guid}/relationships`)

jest.mock('../../apiUrlLinks/detailpageUrl', () => ({
	detailpageApiUrl: (...args: any[]) => mockDetailpageApiUrl(...args),
	detailPageAuditApiUrl: (...args: any[]) => mockDetailPageAuditApiUrl(...args),
	detailPageRauditApiUrl: (...args: any[]) => mockDetailPageRauditApiUrl(...args),
	auditApiurl: (...args: any[]) => mockAuditApiurl(...args),
	detailPageLabelApiUrl: (...args: any[]) => mockDetailPageLabelApiUrl(...args),
	detailPageBusinessMetadataApiUrl: (...args: any[]) => mockDetailPageBusinessMetadataApiUrl(...args),
	detailPageRelationshipApiUrl: (...args: any[]) => mockDetailPageRelationshipApiUrl(...args)
}))

describe('detailpageApiMethod', () => {
	const mockGet = _get as jest.MockedFunction<typeof _get>
	const mockPost = _post as jest.MockedFunction<typeof _post>
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
		
		// Setup URL mock implementations
		mockDetailpageApiUrl.mockImplementation((guid: string, header?: string) => `/api/detail/${guid}${header ? `/${header}` : ''}`)
		mockDetailPageAuditApiUrl.mockImplementation((guid: string) => `/api/detail/${guid}/audit`)
		mockDetailPageRauditApiUrl.mockImplementation(() => '/api/detail/raudit')
		mockAuditApiurl.mockImplementation(() => '/api/audit')
		mockDetailPageLabelApiUrl.mockImplementation((guid: string) => `/api/detail/${guid}/labels`)
		mockDetailPageBusinessMetadataApiUrl.mockImplementation((guid: string) => `/api/detail/${guid}/business-metadata`)
		mockDetailPageRelationshipApiUrl.mockImplementation((guid: string) => `/api/detail/${guid}/relationships`)
	})

	describe('getDetailPageData', () => {
		it('should call _get with correct URL and config', async () => {
			const guid = 'test-guid-123'
			const params = { includeDeleted: false }
			const result = await getDetailPageData(guid, params)

			expect(mockDetailpageApiUrl).toHaveBeenCalledWith(guid, undefined)
			expect(mockGet).toHaveBeenCalledWith('/api/detail/test-guid-123', {
				method: 'GET',
				params: { ...params, ignoreRelationships: true }
			})
			expect(result).toEqual(mockResponse)
		})

		it('should include header parameter when provided', async () => {
			const guid = 'test-guid-123'
			const params = {}
			const header = 'custom-header'
			await getDetailPageData(guid, params, header)

			expect(mockDetailpageApiUrl).toHaveBeenCalledWith(guid, header)
			expect(mockGet).toHaveBeenCalledWith('/api/detail/test-guid-123/custom-header', {
				method: 'GET',
				params: { ...params, ignoreRelationships: true }
			})
		})
	})

	describe('getEntityWithRelationships', () => {
		it('should call _get with ignoreRelationships false', async () => {
			const guid = 'test-guid-123'
			const result = await getEntityWithRelationships(guid)

			expect(mockDetailpageApiUrl).toHaveBeenCalledWith(guid)
			expect(mockGet).toHaveBeenCalledWith('/api/detail/test-guid-123', {
				method: 'GET',
				params: { ignoreRelationships: false }
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getDetailPageAuditData', () => {
		it('should call _get with audit URL', async () => {
			const guid = 'test-guid-123'
			const params = { limit: 10 }
			const result = await getDetailPageAuditData(guid, params)

			expect(mockDetailPageAuditApiUrl).toHaveBeenCalledWith(guid)
			expect(mockGet).toHaveBeenCalledWith('/api/detail/test-guid-123/audit', {
				method: 'GET',
				params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getDetailPageRauditData', () => {
		it('should call _get with raudit URL', async () => {
			const params = { guid: '123' }
			const result = await getDetailPageRauditData(params)

			expect(mockDetailPageRauditApiUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/detail/raudit', {
				method: 'GET',
				params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getAuditData', () => {
		it('should call _get with POST method and data', async () => {
			const params = { action: 'create', entity: 'test' }
			const result = await getAuditData(params)

			expect(mockAuditApiurl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/audit', {
				method: 'POST',
				params: {},
				data: params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getLabels', () => {
		it('should call _post with POST method and formData', async () => {
			const guid = 'test-guid-123'
			const formData = ['tag1', 'tag2'] as string[]
			const result = await getLabels(guid, formData)

			expect(mockDetailPageLabelApiUrl).toHaveBeenCalledWith(guid)
			expect(mockPost).toHaveBeenCalledWith('/api/detail/test-guid-123/labels', {
				method: 'POST',
				params: {},
				data: formData
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getEntityBusinessMetadata', () => {
		it('should call _get with POST method and isOverwrite param', async () => {
			const guid = 'test-guid-123'
			const formData = { metadata: { key: 'value' } }
			const result = await getEntityBusinessMetadata(guid, formData)

			expect(mockDetailPageBusinessMetadataApiUrl).toHaveBeenCalledWith(guid)
			expect(mockGet).toHaveBeenCalledWith('/api/detail/test-guid-123/business-metadata', {
				method: 'POST',
				params: { isOverwrite: true },
				data: formData
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getDetailPageRelationship', () => {
		it('should call _get with relationship URL', async () => {
			const guid = 'test-guid-123'
			const result = await getDetailPageRelationship(guid)

			expect(mockDetailPageRelationshipApiUrl).toHaveBeenCalledWith(guid)
			expect(mockGet).toHaveBeenCalledWith('/api/detail/test-guid-123/relationships', {
				method: 'GET',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('Error Handling', () => {
		it('should propagate errors from _get', async () => {
			const error = new Error('API Error')
			mockGet.mockRejectedValue(error)

			await expect(getDetailPageData('guid', {})).rejects.toThrow('API Error')
		})
	})
})
