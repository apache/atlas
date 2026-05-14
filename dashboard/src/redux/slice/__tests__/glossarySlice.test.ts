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

