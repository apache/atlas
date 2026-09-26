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
 * Unit tests for entitiesApiUrl.ts
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
	entitiesApiUrl,
	businessMetadataImportTempUrl,
	businessMetadataImportUrl,
	getEntityTypeUrl
} from '../entitiesApiUrl'
import { getBaseApiUrl } from '../commonApiUrl'

const mockGetBaseApiUrl = getBaseApiUrl as jest.MockedFunction<typeof getBaseApiUrl>

describe('entitiesApiUrl', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetBaseApiUrl.mockImplementation((url: string) => {
			if (url === 'urlV2') return '/mock-base-url/api/atlas/v2'
			return '/mock-base-url/api/atlas'
		})
	})

	describe('entitiesApiUrl', () => {
		it('should return correct entities URL', () => {
			const result = entitiesApiUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity')
		})

		it('should always return the same URL', () => {
			const result1 = entitiesApiUrl()
			const result2 = entitiesApiUrl()
			expect(result1).toBe(result2)
			expect(result1).toBe('/mock-base-url/api/atlas/v2/entity')
		})
	})

	describe('businessMetadataImportTempUrl', () => {
		it('should return correct URL for business metadata import template', () => {
			const result = businessMetadataImportTempUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/businessmetadata/import/template')
		})

		it('should always return the same URL', () => {
			const result1 = businessMetadataImportTempUrl()
			const result2 = businessMetadataImportTempUrl()
			expect(result1).toBe(result2)
		})
	})

	describe('businessMetadataImportUrl', () => {
		it('should return correct URL for business metadata import', () => {
			const result = businessMetadataImportUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/businessmetadata/import')
		})

		it('should always return the same URL', () => {
			const result1 = businessMetadataImportUrl()
			const result2 = businessMetadataImportUrl()
			expect(result1).toBe(result2)
		})
	})

	describe('getEntityTypeUrl', () => {
		it('should return correct URL for entity type', () => {
			const name = 'DataSet'
			const result = getEntityTypeUrl(name)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/DataSet')
		})

		it('should handle different entity type names', () => {
			const result1 = getEntityTypeUrl('Process')
			expect(result1).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/Process')

			const result2 = getEntityTypeUrl('Table')
			expect(result2).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/Table')
		})

		it('should handle empty name', () => {
			const result = getEntityTypeUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/')
		})

		it('should handle names with special characters', () => {
			const result = getEntityTypeUrl('Test-Type_123')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/Test-Type_123')
		})
	})
})
