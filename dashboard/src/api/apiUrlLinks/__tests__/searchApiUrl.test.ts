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
 * Unit tests for searchApiUrl.ts
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

import { searchApiUrl } from '../searchApiUrl'
import { getBaseApiUrl } from '../commonApiUrl'

const mockGetBaseApiUrl = getBaseApiUrl as jest.MockedFunction<typeof getBaseApiUrl>

describe('searchApiUrl', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetBaseApiUrl.mockImplementation((url: string) => {
			if (url === 'urlV2') return '/mock-base-url/api/atlas/v2'
			return '/mock-base-url/api/atlas'
		})
	})

	describe('searchApiUrl', () => {
		it('should return base search URL when searchType is not provided', () => {
			const result = searchApiUrl('')

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/search')
		})

		it('should return base search URL when searchType is falsy', () => {
			const result1 = searchApiUrl(null as any)
			expect(result1).toBe('/mock-base-url/api/atlas/v2/search')

			const result2 = searchApiUrl(undefined as any)
			expect(result2).toBe('/mock-base-url/api/atlas/v2/search')
		})

		it('should return URL with searchType when searchType is provided', () => {
			const searchType = 'basic'
			const result = searchApiUrl(searchType)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/search/basic')
		})

		it('should handle different searchType values', () => {
			const result1 = searchApiUrl('advanced')
			expect(result1).toBe('/mock-base-url/api/atlas/v2/search/advanced')

			const result2 = searchApiUrl('dsl')
			expect(result2).toBe('/mock-base-url/api/atlas/v2/search/dsl')

			const result3 = searchApiUrl('fulltext')
			expect(result3).toBe('/mock-base-url/api/atlas/v2/search/fulltext')
		})

		it('should handle searchType with special characters', () => {
			const result = searchApiUrl('search-type_123')
			expect(result).toBe('/mock-base-url/api/atlas/v2/search/search-type_123')
		})
	})
})
