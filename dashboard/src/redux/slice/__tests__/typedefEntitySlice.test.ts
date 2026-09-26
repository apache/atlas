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
 * Unit tests for typedefEntitySlice
 */

import { configureStore } from '@reduxjs/toolkit';
import { fetchEntityData, entityReducer } from '../typeDefSlices/typedefEntitySlice';

// Mock API method
jest.mock('../../../api/apiMethods/typeDefApiMethods', () => ({
	getTypeDef: jest.fn()
}));

describe('typedefEntitySlice', () => {
	const initialState = {
		loading: false,
		entityData: null,
		error: null
	};

	beforeEach(() => {
		jest.clearAllMocks();
	});

	it('should return initial state', () => {
		const state = entityReducer(undefined, { type: 'unknown' });
		expect(state.loading).toBe(false);
		expect(state.entityData).toBeNull();
		expect(state.error).toBeNull();
	});

	it('should handle fetchEntityData.pending', () => {
		const action = { type: fetchEntityData.pending.type };
		const state = entityReducer(initialState, action);

		expect(state.loading).toBe(true);
		expect(state.error).toBeNull();
	});

	it('should handle fetchEntityData.fulfilled', () => {
		const mockData = {
			entityDefs: [
				{
					name: 'DataSet',
					attributeDefs: [
						{ name: 'name', typeName: 'string' }
					]
				}
			]
		};

		const action = {
			type: fetchEntityData.fulfilled.type,
			payload: mockData
		};
		const state = entityReducer(initialState, action);

		expect(state.loading).toBe(false);
		expect(state.entityData).toEqual(mockData);
	});

	it('should handle fetchEntityData.rejected', () => {
		const error = { message: 'Error fetching entity data' };
		const action = {
			type: fetchEntityData.rejected.type,
			payload: error
		};
		const state = entityReducer(initialState, action);

		expect(state.loading).toBe(false);
		expect(state.error).toEqual(error);
	});

	it('should fetch entity data successfully', async () => {
		const { getTypeDef } = require('../../../api/apiMethods/typeDefApiMethods');
		const mockData = {
			entityDefs: [
				{
					name: 'DataSet',
					attributeDefs: [
						{ name: 'name', typeName: 'string' }
					]
				}
			]
		};
		getTypeDef.mockResolvedValue({ data: mockData });

		const store = configureStore({
			reducer: {
				entity: entityReducer
			}
		});

		await store.dispatch(fetchEntityData());

		const state = store.getState().entity;
		expect(state.loading).toBe(false);
		expect(state.entityData).toEqual(mockData);
	});

	it('should handle fetch error', async () => {
		const { getTypeDef } = require('../../../api/apiMethods/typeDefApiMethods');
		const error = new Error('API Error');
		getTypeDef.mockRejectedValue(error);

		const store = configureStore({
			reducer: {
				entity: entityReducer
			}
		});

		await store.dispatch(fetchEntityData());

		const state = store.getState().entity;
		expect(state.loading).toBe(false);
		expect(state.error).toBeTruthy();
	});
});

