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
 * Unit tests for entitiesApiMethods.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100% (14/14)
 * - Functions: 100% (3/3)
 * - Lines: 100% (14/14)
 */

import {
	getBusinessMetadataImportTmpl,
	getBusinessMetadataImport,
	getEntitiesType
} from '../entitiesApiMethods'
import { _get } from '../apiMethod'
import {
	businessMetadataImportTempUrl,
	businessMetadataImportUrl,
	getEntityTypeUrl
} from '../../../api/apiUrlLinks/entitiesApiUrl'

const mockBusinessMetadataImportTempUrl = businessMetadataImportTempUrl as jest.MockedFunction<typeof businessMetadataImportTempUrl>
const mockBusinessMetadataImportUrl = businessMetadataImportUrl as jest.MockedFunction<typeof businessMetadataImportUrl>
const mockGetEntityTypeUrl = getEntityTypeUrl as jest.MockedFunction<typeof getEntityTypeUrl>

// Mock dependencies
jest.mock('../apiMethod', () => ({
	_get: jest.fn()
}))

jest.mock('../../../api/apiUrlLinks/entitiesApiUrl', () => ({
	businessMetadataImportTempUrl: jest.fn(),
	businessMetadataImportUrl: jest.fn(),
	getEntityTypeUrl: jest.fn()
}))

describe('entitiesApiMethods', () => {
	const mockGet = _get as jest.MockedFunction<typeof _get>
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
		
		// Setup URL mock implementations
		mockBusinessMetadataImportTempUrl.mockImplementation(() => '/api/entities/business-metadata/template')
		mockBusinessMetadataImportUrl.mockImplementation(() => '/api/entities/business-metadata/import')
		mockGetEntityTypeUrl.mockImplementation((name: string) => `/api/entities/type/${name}`)
	})

	describe('getBusinessMetadataImportTmpl', () => {
		it('should call _get with correct URL and params', async () => {
			const params = { type: 'DataSet' }
			const result = await getBusinessMetadataImportTmpl(params)

			expect(mockBusinessMetadataImportTempUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/entities/business-metadata/template', {
				method: 'GET',
				params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('getBusinessMetadataImport', () => {
		it('should call _get with correct URL, data and uploadProgress', async () => {
			const params = { file: 'test.csv' }
			const uploadProgress = { onUploadProgress: jest.fn() }
			const result = await getBusinessMetadataImport(params, uploadProgress)

			expect(mockBusinessMetadataImportUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/entities/business-metadata/import', {
				method: 'POST',
				params: {},
				data: params,
				...uploadProgress
			})
			expect(result).toEqual(mockResponse)
		})

		it('should handle uploadProgress with multiple properties', async () => {
			const params = { file: 'test.csv' }
			const uploadProgress = {
				onUploadProgress: jest.fn(),
				onDownloadProgress: jest.fn()
			}
			await getBusinessMetadataImport(params, uploadProgress)

			expect(mockGet).toHaveBeenCalledWith(
				expect.any(String),
				expect.objectContaining(uploadProgress)
			)
		})
	})

	describe('getEntitiesType', () => {
		it('should call _get with correct URL and params', async () => {
			const name = 'DataSet'
			const params = { includeDeleted: false }
			const result = await getEntitiesType(name, params)

			expect(mockGetEntityTypeUrl).toHaveBeenCalledWith(name)
			expect(mockGet).toHaveBeenCalledWith('/api/entities/type/DataSet', {
				method: 'GET',
				params
			})
			expect(result).toEqual(mockResponse)
		})
	})

	describe('Error Handling', () => {
		it('should propagate errors from getBusinessMetadataImportTmpl', async () => {
			const error = new Error('Template Error')
			mockGet.mockRejectedValue(error)

			await expect(getBusinessMetadataImportTmpl({})).rejects.toThrow('Template Error')
		})

		it('should propagate errors from getBusinessMetadataImport', async () => {
			const error = new Error('Import Error')
			mockGet.mockRejectedValue(error)

			await expect(getBusinessMetadataImport({}, {})).rejects.toThrow('Import Error')
		})

		it('should propagate errors from getEntitiesType', async () => {
			const error = new Error('Type Error')
			mockGet.mockRejectedValue(error)

			await expect(getEntitiesType('name', {})).rejects.toThrow('Type Error')
		})
	})
})
