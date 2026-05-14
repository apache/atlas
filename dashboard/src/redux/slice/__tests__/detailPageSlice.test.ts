/**
 * Unit tests for detailPageSlice
 */

import { configureStore } from '@reduxjs/toolkit';
import { fetchDetailPageData, detailPageReducer } from '../detailPageSlice';

// Mock API method
jest.mock('../../../api/apiMethods/detailpageApiMethod', () => ({
	getDetailPageData: jest.fn()
}));

jest.mock('@utils/Helper', () => ({
	cloneDeep: (obj: any) => JSON.parse(JSON.stringify(obj)),
	toArrayifObject: (val: any) => (val && typeof val === 'object' && !Array.isArray(val) ? [val] : val),
	uniq: (arr: any[]) => Array.from(new Set(arr)),
	invert: (obj: Record<string, string>) => {
		const result: Record<string, string> = {};
		Object.entries(obj || {}).forEach(([key, value]) => {
			result[String(value)] = key;
		});
		return result;
	}
}));

describe('detailPageSlice', () => {
	const initialState = {
		loading: false,
		detailPageData: null,
		error: null
	};

	beforeEach(() => {
		jest.clearAllMocks();
	});

	it('should return initial state', () => {
		const state = detailPageReducer(undefined, { type: 'unknown' });
		expect(state).toEqual(initialState);
	});

	it('should handle fetchDetailPageData.pending', () => {
		const action = { type: fetchDetailPageData.pending.type };
		const state = detailPageReducer(initialState, action);

		expect(state.loading).toBe(true);
		expect(state.error).toBeNull();
	});

	it('should handle fetchDetailPageData.fulfilled', () => {
		const mockData = {
			entity: { guid: 'test-guid', typeName: 'DataSet' },
			referredEntities: {}
		};

		const action = {
			type: fetchDetailPageData.fulfilled.type,
			payload: mockData
		};
		const state = detailPageReducer(initialState, action);

		expect(state.loading).toBe(false);
		expect(state.detailPageData).toEqual(mockData);
		expect(state.error).toBeNull();
	});

	it('should handle fetchDetailPageData.rejected', () => {
		const error = { message: 'Error fetching detail page data' };
		const action = {
			type: fetchDetailPageData.rejected.type,
			payload: error
		};
		const state = detailPageReducer(initialState, action);

		expect(state.loading).toBe(false);
		expect(state.error).toEqual(error);
	});

	it('should fetch detail page data successfully', async () => {
		const { getDetailPageData } = require('../../../api/apiMethods/detailpageApiMethod');
		const mockData = {
			entity: { guid: 'test-guid', typeName: 'DataSet' },
			referredEntities: {}
		};
		getDetailPageData.mockResolvedValue({ data: mockData });

		const store = configureStore({
			reducer: {
				detailPage: detailPageReducer
			}
		});

		await store.dispatch(fetchDetailPageData('test-guid'));

		const state = store.getState().detailPage;
		expect(state.loading).toBe(false);
		expect(state.detailPageData).toEqual(mockData);
	});

	it('should handle fetch error', async () => {
		const { getDetailPageData } = require('../../../api/apiMethods/detailpageApiMethod');
		const error = new Error('API Error');
		getDetailPageData.mockRejectedValue(error);

		const store = configureStore({
			reducer: {
				detailPage: detailPageReducer
			}
		});

		await store.dispatch(fetchDetailPageData('test-guid'));

		const state = store.getState().detailPage;
		expect(state.loading).toBe(false);
		expect(state.error).toBeTruthy();
	});
});

