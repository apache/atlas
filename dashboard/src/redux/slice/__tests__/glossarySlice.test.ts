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
 * Unit tests for glossarySlice
 */

import { configureStore } from '@reduxjs/toolkit';
import { fetchGlossaryData, glossaryReducer } from '../glossarySlice';

// Mock API method
jest.mock('../../../api/apiMethods/glossaryApiMethod', () => ({
	getGlossary: jest.fn()
}));

describe('glossarySlice', () => {
	const initialState = {
		loading: true,
		glossaryData: null,
		error: null
	};

	beforeEach(() => {
		jest.clearAllMocks();
	});

	it('should return initial state', () => {
		const state = glossaryReducer(undefined, { type: 'unknown' });
		expect(state.loading).toBe(true);
		expect(state.glossaryData).toBeNull();
		expect(state.error).toBeNull();
	});

	it('should handle fetchGlossaryData.pending', () => {
		const action = { type: fetchGlossaryData.pending.type };
		const state = glossaryReducer(initialState, action);

		expect(state.loading).toBe(true);
	});

	it('should handle fetchGlossaryData.fulfilled', () => {
		const mockData = [
			{
				guid: 'glossary-1',
				name: 'Business Glossary',
				terms: []
			}
		];

		const action = {
			type: fetchGlossaryData.fulfilled.type,
			payload: mockData
		};
		const state = glossaryReducer(initialState, action);

		expect(state.loading).toBe(false);
		expect(state.glossaryData).toEqual(mockData);
		expect(state.error).toBeNull();
	});

	it('should handle fetchGlossaryData.rejected', () => {
		const error = { message: 'Error fetching glossary data' };
		const action = {
			type: fetchGlossaryData.rejected.type,
			payload: error
		};
		const state = glossaryReducer(initialState, action);

		expect(state.loading).toBe(false);
		expect(state.error).toEqual(error);
	});

	it('should fetch glossary data successfully', async () => {
		const { getGlossary } = require('../../../api/apiMethods/glossaryApiMethod');
		const mockData = [
			{
				guid: 'glossary-1',
				name: 'Business Glossary',
				terms: []
			}
		];
		getGlossary.mockResolvedValue({ data: mockData });

		const store = configureStore({
			reducer: {
				glossary: glossaryReducer
			}
		});

		await store.dispatch(fetchGlossaryData());

		const state = store.getState().glossary;
		expect(state.loading).toBe(false);
		expect(state.glossaryData).toEqual(mockData);
	});

	it('should handle fetch error', async () => {
		const { getGlossary } = require('../../../api/apiMethods/glossaryApiMethod');
		const error = new Error('API Error');
		getGlossary.mockRejectedValue(error);

		const store = configureStore({
			reducer: {
				glossary: glossaryReducer
			}
		});

		await store.dispatch(fetchGlossaryData());

		const state = store.getState().glossary;
		expect(state.loading).toBe(false);
		expect(state.error).toBeTruthy();
	});
});

