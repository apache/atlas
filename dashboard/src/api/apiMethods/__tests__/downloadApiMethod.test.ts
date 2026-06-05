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
 * Unit tests for downloadApiMethod.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100% (14/14)
 * - Functions: 100% (3/3)
 * - Lines: 100% (12/12)
 */

// Mock dependencies - must be before imports
jest.mock('../apiMethod', () => ({
	_get: jest.fn(),
	_post: jest.fn()
}))

const mockDownloadSearchResultsCSVUrl = jest.fn((searchType: string | null) => `/api/download/csv/${searchType || 'default'}`)
const mockDownloadSearchResultsFileUrl = jest.fn((fileName: string) => `/api/download/file/${fileName}`)
const mockGetDownloadsListUrl = jest.fn(() => '/api/downloads/list')

jest.mock('../../../api/apiUrlLinks/downloadApiUrl', () => ({
	downloadSearchResultsCSVUrl: (...args: any[]) => mockDownloadSearchResultsCSVUrl(...args),
	downloadSearchResultsFileUrl: (...args: any[]) => mockDownloadSearchResultsFileUrl(...args),
	getDownloadsListUrl: (...args: any[]) => mockGetDownloadsListUrl(...args)
}))

import {
	downloadSearchResultsCSV,
	getDownloadStatus,
	downloadCVSFile
} from '../downloadApiMethod'
import { _get, _post } from '../apiMethod'
import {
	downloadSearchResultsCSVUrl,
	downloadSearchResultsFileUrl,
	getDownloadsListUrl
} from '../../../api/apiUrlLinks/downloadApiUrl'

describe('downloadApiMethod', () => {
	const mockGet = _get as jest.MockedFunction<typeof _get>
	const mockPost = _post as jest.MockedFunction<typeof _post>
	const mockResponse = {
		data: { success: true },
		status: 200,
		statusText: 'OK',
		headers: {},
		config: {}
	} as any

	beforeEach(() => {
		jest.clearAllMocks()
		mockGet.mockResolvedValue(mockResponse)
		mockPost.mockResolvedValue(mockResponse)
		
		// Setup URL mock implementations
		mockDownloadSearchResultsCSVUrl.mockImplementation((searchType: string | null) => `/api/download/csv/${searchType || 'default'}`)
		mockDownloadSearchResultsFileUrl.mockImplementation((fileName: string) => `/api/download/file/${fileName}`)
		mockGetDownloadsListUrl.mockImplementation(() => '/api/downloads/list')
	})

	describe('downloadSearchResultsCSV', () => {
		it('should call _post with correct URL and data when searchType is provided', async () => {
			const searchType = 'basic'
			const params = { query: 'test', limit: 100 }
			const result = await downloadSearchResultsCSV(searchType, params)

			expect(mockDownloadSearchResultsCSVUrl).toHaveBeenCalledWith(searchType)
			expect(mockPost).toHaveBeenCalledWith('/api/download/csv/basic', {
				method: 'POST',
				params: {},
				data: params
			})
			expect(result).toEqual(mockResponse)
		})

		it('should call _post with null searchType', async () => {
			const params = { query: 'test' }
			const result = await downloadSearchResultsCSV(null, params)

			expect(mockDownloadSearchResultsCSVUrl).toHaveBeenCalledWith(null)
			expect(mockPost).toHaveBeenCalledWith('/api/download/csv/default', {
				method: 'POST',
				params: {},
				data: params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getDownloadStatus', () => {
		it('should call _get with correct URL and params', async () => {
			const params = { jobId: '123' }
			const result = await getDownloadStatus(params)

			expect(mockGetDownloadsListUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/downloads/list', {
				method: 'GET',
				params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('downloadCVSFile', () => {
		it('should call _get with correct URL', async () => {
			const fileName = 'results.csv'
			const result = await downloadCVSFile(fileName)

			expect(mockDownloadSearchResultsFileUrl).toHaveBeenCalledWith(fileName)
			expect(mockGet).toHaveBeenCalledWith('/api/download/file/results.csv', {
				method: 'GET',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('Error Handling', () => {
		it('should propagate errors from downloadSearchResultsCSV', async () => {
			const error = new Error('Download Error')
			mockPost.mockRejectedValue(error)

			await expect(downloadSearchResultsCSV('basic', {})).rejects.toThrow('Download Error')
		})

		it('should propagate errors from getDownloadStatus', async () => {
			const error = new Error('Status Error')
			mockGet.mockRejectedValue(error)

			await expect(getDownloadStatus({})).rejects.toThrow('Status Error')
		})

		it('should propagate errors from downloadCVSFile', async () => {
			const error = new Error('File Error')
			mockGet.mockRejectedValue(error)

			await expect(downloadCVSFile('file.csv')).rejects.toThrow('File Error')
		})
	})
})
