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
 * Unit tests for detailpageUrl.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100%
 * - Branches: 100%
 * - Functions: 100%
 * - Lines: 100%
 */

// Mock dependencies before imports
jest.mock('../commonApiUrl', () => ({
	getBaseApiUrl: jest.fn((url: string) => {
		if (url === 'urlV2') return '/mock-base-url/api/atlas/v2'
		return '/mock-base-url/api/atlas'
	})
}))

jest.mock('../entitiesApiUrl', () => ({
	entitiesApiUrl: jest.fn(() => '/mock-base-url/api/atlas/v2/entity')
}))

import {
	detailpageApiUrl,
	detailPageAuditApiUrl,
	detailPageRauditApiUrl,
	auditApiurl,
	detailPageLabelApiUrl,
	detailPageBusinessMetadataApiUrl,
	detailPageRelationshipApiUrl
} from '../detailpageUrl'
import { getBaseApiUrl } from '../commonApiUrl'
import { entitiesApiUrl } from '../entitiesApiUrl'

const mockGetBaseApiUrl = getBaseApiUrl as jest.MockedFunction<typeof getBaseApiUrl>
const mockEntitiesApiUrl = entitiesApiUrl as jest.MockedFunction<typeof entitiesApiUrl>

describe('detailpageUrl', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetBaseApiUrl.mockImplementation((url: string) => {
			if (url === 'urlV2') return '/mock-base-url/api/atlas/v2'
			return '/mock-base-url/api/atlas'
		})
		mockEntitiesApiUrl.mockReturnValue('/mock-base-url/api/atlas/v2/entity')
	})

	describe('detailpageApiUrl', () => {
		it('should return URL with header when header is provided', () => {
			const guid = 'test-guid-123'
			const header = 'test-header'
			const result = detailpageApiUrl(guid, header)

			expect(mockEntitiesApiUrl).toHaveBeenCalled()
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid/test-guid-123/header')
		})

		it('should return URL without header when header is undefined', () => {
			const guid = 'test-guid-123'
			const result = detailpageApiUrl(guid)

			expect(mockEntitiesApiUrl).toHaveBeenCalled()
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid/test-guid-123')
		})

		it('should return URL without header when header is not provided', () => {
			const guid = 'test-guid-456'
			const result = detailpageApiUrl(guid, undefined)

			expect(mockEntitiesApiUrl).toHaveBeenCalled()
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid/test-guid-456')
		})

		it('should handle different guid values', () => {
			const result1 = detailpageApiUrl('guid-1', 'header-1')
			expect(result1).toBe('/mock-base-url/api/atlas/v2/entity/guid/guid-1/header')

			const result2 = detailpageApiUrl('guid-2')
			expect(result2).toBe('/mock-base-url/api/atlas/v2/entity/guid/guid-2')
		})

		it('should handle empty guid', () => {
			const result = detailpageApiUrl('', 'header')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid//header')
		})
	})

	describe('detailPageAuditApiUrl', () => {
		it('should return correct URL for audit API', () => {
			const guid = 'test-guid-123'
			const result = detailPageAuditApiUrl(guid)

			expect(mockEntitiesApiUrl).toHaveBeenCalled()
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/test-guid-123/audit')
		})

		it('should handle different guid values', () => {
			const result = detailPageAuditApiUrl('guid-456')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid-456/audit')
		})

		it('should handle empty guid', () => {
			const result = detailPageAuditApiUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity//audit')
		})
	})

	describe('detailPageRauditApiUrl', () => {
		it('should return correct URL for raudit API', () => {
			const result = detailPageRauditApiUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('url')
			expect(result).toBe('/mock-base-url/api/atlas/admin/expimp/audit')
		})

		it('should always return the same URL', () => {
			const result1 = detailPageRauditApiUrl()
			const result2 = detailPageRauditApiUrl()
			expect(result1).toBe(result2)
		})
	})

	describe('auditApiurl', () => {
		it('should return correct URL for audit API', () => {
			const result = auditApiurl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('url')
			expect(result).toBe('/mock-base-url/api/atlas/admin/audits')
		})

		it('should always return the same URL', () => {
			const result1 = auditApiurl()
			const result2 = auditApiurl()
			expect(result1).toBe(result2)
		})
	})

	describe('detailPageLabelApiUrl', () => {
		it('should return correct URL for label API', () => {
			const guid = 'test-guid-123'
			const result = detailPageLabelApiUrl(guid)

			expect(mockEntitiesApiUrl).toHaveBeenCalled()
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid/test-guid-123/labels')
		})

		it('should handle different guid values', () => {
			const result = detailPageLabelApiUrl('guid-789')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid/guid-789/labels')
		})

		it('should handle empty guid', () => {
			const result = detailPageLabelApiUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid//labels')
		})
	})

	describe('detailPageBusinessMetadataApiUrl', () => {
		it('should return correct URL for business metadata API', () => {
			const guid = 'test-guid-123'
			const result = detailPageBusinessMetadataApiUrl(guid)

			expect(mockEntitiesApiUrl).toHaveBeenCalled()
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid/test-guid-123/businessmetadata')
		})

		it('should handle different guid values', () => {
			const result = detailPageBusinessMetadataApiUrl('guid-999')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid/guid-999/businessmetadata')
		})

		it('should handle empty guid', () => {
			const result = detailPageBusinessMetadataApiUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid//businessmetadata')
		})
	})

	describe('detailPageRelationshipApiUrl', () => {
		it('should return correct URL for relationship API', () => {
			const guid = 'test-guid-123'
			const result = detailPageRelationshipApiUrl(guid)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/relationship/guid/test-guid-123')
		})

		it('should handle different guid values', () => {
			const result = detailPageRelationshipApiUrl('rel-guid-456')
			expect(result).toBe('/mock-base-url/api/atlas/v2/relationship/guid/rel-guid-456')
		})

		it('should handle empty guid', () => {
			const result = detailPageRelationshipApiUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/relationship/guid/')
		})
	})
})
