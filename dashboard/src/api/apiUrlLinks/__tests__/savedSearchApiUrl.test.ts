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
 * Unit tests for savedSearchApiUrl.ts
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

import { getSavedSearchUrl } from '../savedSearchApiUrl'
import { getBaseApiUrl } from '../commonApiUrl'

const mockGetBaseApiUrl = getBaseApiUrl as jest.MockedFunction<typeof getBaseApiUrl>

describe('savedSearchApiUrl', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetBaseApiUrl.mockImplementation((url: string) => {
			if (url === 'urlV2') return '/mock-base-url/api/atlas/v2'
			return '/mock-base-url/api/atlas'
		})
	})

	describe('getSavedSearchUrl', () => {
		it('should return correct saved search URL', () => {
			const result = getSavedSearchUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/search/saved')
		})

		it('should always return the same URL', () => {
			const result1 = getSavedSearchUrl()
			const result2 = getSavedSearchUrl()
			expect(result1).toBe(result2)
			expect(result1).toBe('/mock-base-url/api/atlas/v2/search/saved')
		})
	})
})
