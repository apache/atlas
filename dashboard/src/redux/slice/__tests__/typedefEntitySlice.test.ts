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

