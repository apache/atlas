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
 * Unit tests for entityFormApiUrl.ts
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

import {
	geAttributeUrl,
	getEntityUrl,
	getTypedefUrl
} from '../entityFormApiUrl'
import { getBaseApiUrl } from '../commonApiUrl'

const mockGetBaseApiUrl = getBaseApiUrl as jest.MockedFunction<typeof getBaseApiUrl>

describe('entityFormApiUrl', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetBaseApiUrl.mockImplementation((url: string) => {
			if (url === 'urlV2') return '/mock-base-url/api/atlas/v2'
			return '/mock-base-url/api/atlas'
		})
	})

	describe('geAttributeUrl', () => {
		it('should return correct URL for attribute search', () => {
			const result = geAttributeUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/search/attribute')
		})

		it('should always return the same URL', () => {
			const result1 = geAttributeUrl()
			const result2 = geAttributeUrl()
			expect(result1).toBe(result2)
			expect(result1).toBe('/mock-base-url/api/atlas/v2/search/attribute')
		})
	})

	describe('getEntityUrl', () => {
		it('should return correct URL for entity by guid', () => {
			const guid = 'test-guid-123'
			const result = getEntityUrl(guid)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid/test-guid-123')
		})

		it('should handle different guid values', () => {
			const result1 = getEntityUrl('guid-456')
			expect(result1).toBe('/mock-base-url/api/atlas/v2/entity/guid/guid-456')

			const result2 = getEntityUrl('another-guid-789')
			expect(result2).toBe('/mock-base-url/api/atlas/v2/entity/guid/another-guid-789')
		})

		it('should handle empty guid', () => {
			const result = getEntityUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid/')
		})
	})

	describe('getTypedefUrl', () => {
		it('should return correct URL for typedef by name', () => {
			const typeDef = 'TestTypeDef'
			const result = getTypedefUrl(typeDef)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/TestTypeDef')
		})

		it('should handle different typedef names', () => {
			const result1 = getTypedefUrl('DataSet')
			expect(result1).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/DataSet')

			const result2 = getTypedefUrl('Process')
			expect(result2).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/Process')
		})

		it('should handle empty typedef name', () => {
			const result = getTypedefUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/')
		})

		it('should handle names with special characters', () => {
			const result = getTypedefUrl('Test-Type_123')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/Test-Type_123')
		})
	})
})
