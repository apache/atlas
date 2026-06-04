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
 * Unit tests for downloadApiUrl.ts
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
	downloadSearchResultsCSVUrl,
	getDownloadsListUrl,
	downloadSearchResultsFileUrl
} from '../downloadApiUrl'
import { getBaseApiUrl } from '../commonApiUrl'

const mockGetBaseApiUrl = getBaseApiUrl as jest.MockedFunction<typeof getBaseApiUrl>

describe('downloadApiUrl', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetBaseApiUrl.mockImplementation((url: string) => {
			if (url === 'urlV2') return '/mock-base-url/api/atlas/v2'
			return '/mock-base-url/api/atlas'
		})
	})

	describe('downloadSearchResultsCSVUrl', () => {
		it('should return correct URL when searchType is provided', () => {
			const searchType = 'basic'
			const result = downloadSearchResultsCSVUrl(searchType)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/search/basic/download/create_file')
		})

		it('should return correct URL when searchType is null', () => {
			const result = downloadSearchResultsCSVUrl(null)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/search/null/download/create_file')
		})

		it('should handle different searchType values', () => {
			const result1 = downloadSearchResultsCSVUrl('advanced')
			expect(result1).toBe('/mock-base-url/api/atlas/v2/search/advanced/download/create_file')

			const result2 = downloadSearchResultsCSVUrl('dsl')
			expect(result2).toBe('/mock-base-url/api/atlas/v2/search/dsl/download/create_file')
		})

		it('should handle empty string searchType', () => {
			const result = downloadSearchResultsCSVUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/search//download/create_file')
		})
	})

	describe('getDownloadsListUrl', () => {
		it('should return correct URL for downloads list', () => {
			const result = getDownloadsListUrl()

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/search/download/status')
		})

		it('should always return the same URL', () => {
			const result1 = getDownloadsListUrl()
			const result2 = getDownloadsListUrl()
			expect(result1).toBe(result2)
			expect(result1).toBe('/mock-base-url/api/atlas/v2/search/download/status')
		})
	})

	describe('downloadSearchResultsFileUrl', () => {
		it('should return correct URL for downloading file', () => {
			const fileName = 'results.csv'
			const result = downloadSearchResultsFileUrl(fileName)

			expect(mockGetBaseApiUrl).toHaveBeenCalledWith('urlV2')
			expect(result).toBe('/mock-base-url/api/atlas/v2/search/download/results.csv')
		})

		it('should handle different file names', () => {
			const result1 = downloadSearchResultsFileUrl('export.xlsx')
			expect(result1).toBe('/mock-base-url/api/atlas/v2/search/download/export.xlsx')

			const result2 = downloadSearchResultsFileUrl('data.json')
			expect(result2).toBe('/mock-base-url/api/atlas/v2/search/download/data.json')
		})

		it('should handle empty file name', () => {
			const result = downloadSearchResultsFileUrl('')
			expect(result).toBe('/mock-base-url/api/atlas/v2/search/download/')
		})

		it('should handle file names with special characters', () => {
			const result = downloadSearchResultsFileUrl('file-name_123.csv')
			expect(result).toBe('/mock-base-url/api/atlas/v2/search/download/file-name_123.csv')
		})
	})
})
