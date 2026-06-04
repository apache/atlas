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
 * Unit tests for headerApiMethods.ts
 * 
 * Coverage Target: 100%
 * - Statements: 100% (6/6)
 * - Functions: 100% (1/1)
 * - Lines: 100% (6/6)
 */

import { getVersion } from '../headerApiMethods'
import { _get } from '../apiMethod'
import { versionUrl } from '../../../api/apiUrlLinks/headerUrl'

// Mock dependencies
jest.mock('../apiMethod', () => ({
	_get: jest.fn()
}))

const mockVersionUrl = jest.fn(() => '/api/version')

jest.mock('../../../api/apiUrlLinks/headerUrl', () => ({
	versionUrl: (...args: any[]) => mockVersionUrl(...args)
}))

describe('headerApiMethods', () => {
	const mockGet = _get as jest.MockedFunction<typeof _get>
	const mockResponse = {
		data: { version: '1.0.0' },
		status: 200,
		statusText: 'OK',
		headers: {},
		config: {}
	} as any

	beforeEach(() => {
		jest.clearAllMocks()
		mockGet.mockResolvedValue(mockResponse)
		mockVersionUrl.mockImplementation(() => '/api/version')
	})

	describe('getVersion', () => {
		it('should call _get with correct URL and config', async () => {
			const result = await getVersion()

			expect(mockVersionUrl).toHaveBeenCalled()
			expect(mockGet).toHaveBeenCalledWith('/api/version', {
				method: 'GET',
				params: {}
			})
			expect(result).toEqual(mockResponse)
		})

		it('should return version data', async () => {
			const result = await getVersion()

			expect(result.data.version).toBe('1.0.0')
		})
	})

	describe('Error Handling', () => {
		it('should propagate errors from getVersion', async () => {
			const error = new Error('Version Error')
			mockGet.mockRejectedValue(error)

			await expect(getVersion()).rejects.toThrow('Version Error')
		})
	})
})
