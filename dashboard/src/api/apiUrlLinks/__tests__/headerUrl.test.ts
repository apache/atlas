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
 * Unit tests for headerUrl.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100%
 * - Branches: 100%
 * - Functions: 100%
 * - Lines: 100%
 */

// Mock dependencies before imports
jest.mock('@utils/Utils', () => ({
	getBaseUrl: jest.fn((url: string) => '/mock-base-url')
}))

jest.mock('../commonApiUrl', () => ({
	apiBaseurl: '/atlas',
	getBaseApiUrl: jest.fn((url: string) => {
		if (url === 'url') return '/mock-base-url/api/atlas'
		return '/mock-base-url/api/atlas/v2'
	})
}))

import { apiDocUrl, versionUrl } from '../headerUrl'
import { getBaseUrl } from '@utils/Utils'
import { getBaseApiUrl, apiBaseurl } from '../commonApiUrl'

const mockGetBaseUrl = getBaseUrl as jest.MockedFunction<typeof getBaseUrl>
const mockGetBaseApiUrl = getBaseApiUrl as jest.MockedFunction<typeof getBaseApiUrl>

describe('headerUrl', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetBaseUrl.mockReturnValue('/mock-base-url')
		mockGetBaseApiUrl.mockImplementation((url: string) => {
			if (url === 'url') return '/mock-base-url/api/atlas'
			return '/mock-base-url/api/atlas/v2'
		})
	})

	describe('apiDocUrl', () => {
		it('should return correct API documentation URL', () => {
			const result = apiDocUrl()

			expect(mockGetBaseUrl).toHaveBeenCalledWith(apiBaseurl)
			expect(result).toBe('/mock-base-url/apidocs/index.html')
		})

		it('should always return the same URL', () => {
			const result1 = apiDocUrl()
			const result2 = apiDocUrl()
			expect(result1).toBe(result2)
			expect(result1).toBe('/mock-base-url/apidocs/index.html')
		})
	})

	describe('versionUrl', () => {
		it('should return correct version URL', () => {
			const result = versionUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('url')
			expect(result).toBe('/mock-base-url/api/atlas/admin/version')
		})

		it('should always return the same URL', () => {
			const result1 = versionUrl()
			const result2 = versionUrl()
			expect(result1).toBe(result2)
			expect(result1).toBe('/mock-base-url/api/atlas/admin/version')
		})
	})
})
