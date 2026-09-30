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
 * Unit tests for commonApiUrl.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100%
 * - Branches: 100%
 * - Functions: 100%
 * - Lines: 100%
 */

jest.mock('@utils/Utils', () => ({
	getBaseUrl: jest.fn((url: string) => '/mock-base-url')
}))

const { getBaseUrl: mockGetBaseUrl } = jest.requireMock('@utils/Utils') as {
	getBaseUrl: jest.Mock
}

// Mock window.location.pathname
Object.defineProperty(window, 'location', {
	value: {
		pathname: '/atlas'
	},
	writable: true
})

import { getBaseApiUrl, getDefApiUrl, typedefsUrl } from '../commonApiUrl'

describe('commonApiUrl', () => {
	beforeEach(() => {
		// Don't clear mocks here because getBaseUrl is called at module load time
		// jest.clearAllMocks() would clear those calls
		mockGetBaseUrl.mockReturnValue('/mock-base-url')
	})

	describe('getBaseApiUrl', () => {
		it('should return baseUrl when url is "url"', () => {
			// getBaseUrl is called at module load time, so we can't reliably test the call
			// Instead, we verify the function returns the correct value
			const result = getBaseApiUrl('url')
			expect(result).toBe('/mock-base-url/api/atlas')
		})

		it('should return baseUrlV2 when url is "urlV2"', () => {
			// getBaseUrl is called at module load time, so we can't reliably test the call
			// Instead, we verify the function returns the correct value
			const result = getBaseApiUrl('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2')
		})

		it('should return undefined when url is neither "url" nor "urlV2"', () => {
			const result = getBaseApiUrl('invalid')
			expect(result).toBeUndefined()
		})

		it('should handle empty string', () => {
			const result = getBaseApiUrl('')
			expect(result).toBeUndefined()
		})
	})

	describe('typedefsUrl', () => {
		it('should return typedefs URL object with defs and def properties', () => {
			// getBaseUrl is called at module load time, so we can't reliably test the call
			// Instead, we verify the function returns the correct value
			const result = typedefsUrl()
			expect(result).toEqual({
				defs: '/mock-base-url/api/atlas/v2/types/typedefs',
				def: '/mock-base-url/api/atlas/v2/types/typedef'
			})
		})

		it('should return correct URLs for defs and def', () => {
			const result = typedefsUrl()
			expect(result.defs).toContain('/types/typedefs')
			expect(result.def).toContain('/types/typedef')
		})
	})

	describe('getDefApiUrl', () => {
		it('should return defs URL when name is empty string', () => {
			const result = getDefApiUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedefs')
		})

		it('should return def URL with name when name is provided', () => {
			const name = 'TestType'
			const result = getDefApiUrl(name)
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/TestType')
		})

		it('should handle name with special characters', () => {
			const name = 'Test-Type_123'
			const result = getDefApiUrl(name)
			expect(result).toBe('/mock-base-url/api/atlas/v2/types/typedef/name/Test-Type_123')
		})

		it('should handle falsy name values', () => {
			const result1 = getDefApiUrl(null as any)
			expect(result1).toBe('/mock-base-url/api/atlas/v2/types/typedefs')

			const result2 = getDefApiUrl(undefined as any)
			expect(result2).toBe('/mock-base-url/api/atlas/v2/types/typedefs')
		})
	})
})
