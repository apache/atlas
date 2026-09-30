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
 * Unit tests for classificationUrl.ts
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
	removeClassificationUrl,
	addTagUrl,
	deleteTagUrl,
	editAssignTagUrl,
	rootClassificationDefUrl
} from '../classificationUrl'
import { getBaseApiUrl } from '../commonApiUrl'

const mockGetBaseApiUrl = getBaseApiUrl as jest.MockedFunction<typeof getBaseApiUrl>

describe('classificationUrl', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetBaseApiUrl.mockImplementation((url: string) => {
			if (url === 'urlV2') return '/mock-base-url/api/atlas/v2'
			return '/mock-base-url/api/atlas'
		})
	})

	describe('removeClassificationUrl', () => {
		it('should return correct URL for removing classification', () => {
			const obj = 'test-guid-123'
			const currentVal = 'PII'
			const result = removeClassificationUrl(obj, currentVal)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid/test-guid-123/classification/PII')
		})

		it('should handle different guid and classification values', () => {
			const result = removeClassificationUrl('guid-456', 'Sensitive')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid/guid-456/classification/Sensitive')
		})

		it('should handle empty strings', () => {
			const result = removeClassificationUrl('', '')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid//classification/')
		})
	})

	describe('addTagUrl', () => {
		it('should return correct URL for adding tag', () => {
			const result = addTagUrl()
			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/bulk/classification')
		})

		it('should always return the same URL', () => {
			const result1 = addTagUrl()
			const result2 = addTagUrl()
			expect(result1).toBe(result2)
			expect(result1).toBe('/mock-base-url/api/atlas/v2/entity/bulk/classification')
		})
	})

	describe('deleteTagUrl', () => {
		it('should return correct URL for deleting tag', () => {
			const tagName = 'TestTag'
			const result = deleteTagUrl(tagName)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/TestTag')
		})

		it('should handle different tag names', () => {
			const result = deleteTagUrl('PII')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/PII')
		})

		it('should handle empty tag name', () => {
			const result = deleteTagUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/')
		})
	})

	describe('editAssignTagUrl', () => {
		it('should return correct URL for editing/assigning tag', () => {
			const guid = 'test-guid-789'
			const result = editAssignTagUrl(guid)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid/test-guid-789/classifications')
		})

		it('should handle different guid values', () => {
			const result = editAssignTagUrl('guid-999')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid/guid-999/classifications')
		})

		it('should handle empty guid', () => {
			const result = editAssignTagUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/entity/guid//classifications')
		})
	})

	describe('rootClassificationDefUrl', () => {
		it('should return correct URL for root classification definition', () => {
			const name = 'TestClassification'
			const result = rootClassificationDefUrl(name)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/classificationdef/name/TestClassification')
		})

		it('should handle different classification names', () => {
			const result = rootClassificationDefUrl('PII')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/classificationdef/name/PII')
		})

		it('should handle empty name', () => {
			const result = rootClassificationDefUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/classificationdef/name/')
		})
	})
})
