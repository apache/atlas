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
 * Unit tests for glossaryUrl.ts
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
	removeTermUrl,
	glossaryUrl,
	glossaryImportTempUrl,
	glossaryImportUrl,
	glossaryTypeUrl,
	editGlossaryUrl,
	deleteGlossaryorTermUrl,
	createTermorCategoryUrl,
	editTermorCategoryUrl,
	assignTermtoEntitiesUrl,
	assignTermtoCategoryUrl,
	assignGlossaryTypeUrl,
	removeTermorCatgeoryUrl
} from '../glossaryUrl'
import { getBaseApiUrl } from '../commonApiUrl'

const mockGetBaseApiUrl = getBaseApiUrl as jest.MockedFunction<typeof getBaseApiUrl>

describe('glossaryUrl', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetBaseApiUrl.mockImplementation((url: string) => {
			if (url === 'urlV2') return '/mock-base-url/api/atlas/v2'
			return '/mock-base-url/api/atlas'
		})
	})

	describe('glossaryUrl', () => {
		it('should return correct glossary URL', () => {
			const result = glossaryUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary')
		})

		it('should always return the same URL', () => {
			const result1 = glossaryUrl()
			const result2 = glossaryUrl()
			expect(result1).toBe(result2)
		})
	})

	describe('removeTermUrl', () => {
		it('should return correct URL for removing term', () => {
			const currentVal = 'term-guid-123'
			const result = removeTermUrl(currentVal)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/terms/term-guid-123/assignedEntities')
		})

		it('should handle different term values', () => {
			const result = removeTermUrl('term-456')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/terms/term-456/assignedEntities')
		})

		it('should handle empty value', () => {
			const result = removeTermUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/terms//assignedEntities')
		})
	})

	describe('glossaryImportTempUrl', () => {
		it('should return correct URL for glossary import template', () => {
			const result = glossaryImportTempUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/import/template')
		})

		it('should always return the same URL', () => {
			const result1 = glossaryImportTempUrl()
			const result2 = glossaryImportTempUrl()
			expect(result1).toBe(result2)
		})
	})

	describe('glossaryImportUrl', () => {
		it('should return correct URL for glossary import', () => {
			const result = glossaryImportUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/import')
		})

		it('should always return the same URL', () => {
			const result1 = glossaryImportUrl()
			const result2 = glossaryImportUrl()
			expect(result1).toBe(result2)
		})
	})

	describe('glossaryTypeUrl', () => {
		it('should return correct URL for glossary type', () => {
			const glossaryType = 'term'
			const guid = 'guid-123'
			const result = glossaryTypeUrl(glossaryType, guid)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/term/guid-123')
		})

		it('should handle category type', () => {
			const result = glossaryTypeUrl('category', 'cat-guid-456')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/category/cat-guid-456')
		})

		it('should handle empty values', () => {
			const result = glossaryTypeUrl('', '')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary//')
		})
	})

	describe('editGlossaryUrl', () => {
		it('should return correct URL for editing glossary', () => {
			const guid = 'guid-123'
			const result = editGlossaryUrl(guid)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/guid-123')
		})

		it('should handle different guid values', () => {
			const result = editGlossaryUrl('guid-789')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/guid-789')
		})

		it('should handle empty guid', () => {
			const result = editGlossaryUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/')
		})
	})

	describe('deleteGlossaryorTermUrl', () => {
		it('should return correct URL for deleting glossary or term', () => {
			const guid = 'guid-123'
			const result = deleteGlossaryorTermUrl(guid)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/guid-123')
		})

		it('should handle different guid values', () => {
			const result = deleteGlossaryorTermUrl('guid-456')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/guid-456')
		})

		it('should handle empty guid', () => {
			const result = deleteGlossaryorTermUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/')
		})
	})

	describe('createTermorCategoryUrl', () => {
		it('should return correct URL for creating term', () => {
			const type = 'term'
			const result = createTermorCategoryUrl(type)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/term')
		})

		it('should return correct URL for creating category', () => {
			const result = createTermorCategoryUrl('category')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/category')
		})

		it('should handle empty type', () => {
			const result = createTermorCategoryUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/')
		})
	})

	describe('editTermorCategoryUrl', () => {
		it('should return correct URL for editing term', () => {
			const type = 'term'
			const guid = 'guid-123'
			const result = editTermorCategoryUrl(type, guid)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/term/guid-123')
		})

		it('should return correct URL for editing category', () => {
			const result = editTermorCategoryUrl('category', 'cat-guid-456')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/category/cat-guid-456')
		})

		it('should handle empty values', () => {
			const result = editTermorCategoryUrl('', '')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary//')
		})
	})

	describe('assignTermtoEntitiesUrl', () => {
		it('should return correct URL for assigning term to entities', () => {
			const guid = 'term-guid-123'
			const result = assignTermtoEntitiesUrl(guid)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/terms/term-guid-123/assignedEntities')
		})

		it('should handle different guid values', () => {
			const result = assignTermtoEntitiesUrl('term-guid-456')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/terms/term-guid-456/assignedEntities')
		})

		it('should handle empty guid', () => {
			const result = assignTermtoEntitiesUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/terms//assignedEntities')
		})
	})

	describe('assignTermtoCategoryUrl', () => {
		it('should return correct URL for assigning term to category', () => {
			const guid = 'category-guid-123'
			const result = assignTermtoCategoryUrl(guid)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/category/category-guid-123')
		})

		it('should handle different guid values', () => {
			const result = assignTermtoCategoryUrl('cat-guid-456')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/category/cat-guid-456')
		})

		it('should handle empty guid', () => {
			const result = assignTermtoCategoryUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/category/')
		})
	})

	describe('assignGlossaryTypeUrl', () => {
		it('should return correct URL for assigning glossary type', () => {
			const guid = 'term-guid-123'
			const result = assignGlossaryTypeUrl(guid)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/term/term-guid-123')
		})

		it('should handle different guid values', () => {
			const result = assignGlossaryTypeUrl('term-guid-789')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/term/term-guid-789')
		})

		it('should handle empty guid', () => {
			const result = assignGlossaryTypeUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/term/')
		})
	})

	describe('removeTermorCatgeoryUrl', () => {
		it('should return correct URL for removing term', () => {
			const guid = 'term-guid-123'
			const glossaryType = 'term'
			const result = removeTermorCatgeoryUrl(guid, glossaryType)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/term/term-guid-123')
		})

		it('should return correct URL for removing category', () => {
			const result = removeTermorCatgeoryUrl('cat-guid-456', 'category')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary/category/cat-guid-456')
		})

		it('should handle empty values', () => {
			const result = removeTermorCatgeoryUrl('', '')
			expect(result).toBe('/mock-base-url/api/atlas/v2/glossary//')
		})
	})
})
