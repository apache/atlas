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
 * Unit tests for sessionApiUrl.ts
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
		if (url === 'url') return '/mock-base-url/api/atlas'
		return '/mock-base-url/api/atlas/v2'
	})
}))

import { getSessionApiUrl } from '../sessionApiUrl'
import { getBaseApiUrl } from '../commonApiUrl'

const mockGetBaseApiUrl = getBaseApiUrl as jest.MockedFunction<typeof getBaseApiUrl>

describe('sessionApiUrl', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetBaseApiUrl.mockImplementation((url: string) => {
			if (url === 'url') return '/mock-base-url/api/atlas'
			return '/mock-base-url/api/atlas/v2'
		})
	})

	describe('getSessionApiUrl', () => {
		it('should return correct session API URL', () => {
			const result = getSessionApiUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('url')
			expect(result).toBe('/mock-base-url/api/atlas/admin/session')
		})

		it('should always return the same URL', () => {
			const result1 = getSessionApiUrl()
			const result2 = getSessionApiUrl()
			expect(result1).toBe(result2)
			expect(result1).toBe('/mock-base-url/api/atlas/admin/session')
		})
	})
})
