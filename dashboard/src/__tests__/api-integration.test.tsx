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
 * Integration tests for API methods
 */

// Mock fetchApi before importing API methods (mock hoisting)
const mockFetchApi = jest.fn();
jest.mock('../api/apiMethods/fetchApi', () => ({
	fetchApi: (...args: any[]) => mockFetchApi(...args)
}));

// Mock axios and isAxiosError
const mockAxios = jest.fn();
const mockIsAxiosError = jest.fn((error: any) => error?.isAxiosError === true);
jest.mock('axios', () => ({
	__esModule: true,
	default: jest.fn((...args: any[]) => mockAxios(...args)),
	isAxiosError: (...args: any[]) => mockIsAxiosError(...args)
}));

// Mock global.fetch
global.fetch = jest.fn() as jest.Mock;

import { getBasicSearchResult } from '../api/apiMethods/searchApiMethod';
import { getDetailPageData } from '../api/apiMethods/detailpageApiMethod';
import { getGlossary } from '../api/apiMethods/glossaryApiMethod';
import { AxiosResponse } from 'axios';

describe('API Integration Tests', () => {
	const mockAxiosResponse: AxiosResponse = {
		data: {},
		status: 200,
		statusText: 'OK',
		headers: {},
		config: {} as any
	};

	beforeEach(() => {
		jest.clearAllMocks();
		mockFetchApi.mockClear();
		mockAxios.mockClear();
		mockIsAxiosError.mockClear();
		(global.fetch as jest.Mock).mockClear();
	});

	describe('Search API', () => {
		it(
			'should handle successful search API call',
			async () => {
				const mockResponse = {
					data: {
						entities: [{ guid: '1', typeName: 'DataSet' }],
						approximateCount: 1
					},
					status: 200,
					statusText: 'OK',
					headers: {},
					config: {} as any
				};

				mockFetchApi.mockResolvedValueOnce(mockResponse);

				const result = await getBasicSearchResult({ data: { query: 'test' } }, 'basic');
				expect(result.data).toEqual(mockResponse.data);
				expect(mockFetchApi).toHaveBeenCalled();
			},
			10000
		);

		it(
			'should handle search API error',
			async () => {
				const error = new Error('API Error');
				mockFetchApi.mockRejectedValueOnce(error);

				await expect(getBasicSearchResult({ data: { query: 'test' } }, 'basic')).rejects.toThrow('API Error');
			},
			10000
		);
	});

	describe('Detail Page API', () => {
		it(
			'should handle successful detail page API call',
			async () => {
				const mockResponse: AxiosResponse = {
					data: {
						entity: { guid: 'test-guid', typeName: 'DataSet' },
						referredEntities: {}
					},
					status: 200,
					statusText: 'OK',
					headers: {},
					config: {} as any
				};

				mockFetchApi.mockResolvedValueOnce(mockResponse);

				const result = await getDetailPageData('test-guid', {});
				expect(result.data).toEqual(mockResponse.data);
				expect(mockFetchApi).toHaveBeenCalled();
			},
			10000
		);

		it(
			'should handle detail page API error',
			async () => {
				const error = new Error('API Error');
				mockFetchApi.mockRejectedValueOnce(error);

				await expect(getDetailPageData('test-guid', {})).rejects.toThrow('API Error');
			},
			10000
		);
	});

	describe('Glossary API', () => {
		it(
			'should handle successful glossary API call',
			async () => {
				const mockResponse: AxiosResponse = {
					data: [
						{
							guid: 'glossary-1',
							name: 'Business Glossary',
							terms: []
						}
					],
					status: 200,
					statusText: 'OK',
					headers: {},
					config: {} as any
				};

				mockFetchApi.mockResolvedValueOnce(mockResponse);

				const result = await getGlossary();
				expect(result.data).toEqual(mockResponse.data);
				expect(mockFetchApi).toHaveBeenCalled();
			},
			10000
		);

		it(
			'should handle glossary API error',
			async () => {
				const error = new Error('API Error');
				mockFetchApi.mockRejectedValueOnce(error);

				await expect(getGlossary()).rejects.toThrow('API Error');
			},
			10000
		);
	});

	describe('Error Handling', () => {
		it(
			'should handle network errors',
			async () => {
				const error = new Error('Network error');
				mockFetchApi.mockRejectedValueOnce(error);

				await expect(getBasicSearchResult({ data: { query: 'test' } }, 'basic')).rejects.toThrow('Network error');
			},
			10000
		);

		it(
			'should handle HTTP error responses',
			async () => {
				const axiosError = {
					isAxiosError: true,
					response: {
						status: 500,
						statusText: 'Internal Server Error',
						data: { error: 'Server Error' },
						headers: {},
						config: {} as any
					},
					config: {} as any,
					name: 'AxiosError',
					message: 'Request failed'
				};
				mockFetchApi.mockRejectedValueOnce(axiosError);

				// API should handle error appropriately
				await expect(getBasicSearchResult({ data: { query: 'test' } }, 'basic')).rejects.toEqual(axiosError);
			},
			10000
		);
	});
});
