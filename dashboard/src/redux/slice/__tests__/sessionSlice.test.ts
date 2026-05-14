/**
 * Unit tests for sessionSlice
 */

import { configureStore } from '@reduxjs/toolkit';
import { fetchSessionData, sessionReducer } from '../sessionSlice';

// Mock API methods
jest.mock('../../../api/apiMethods/fetchApi', () => ({
	fetchApi: jest.fn()
}));

jest.mock('../../../api/apiUrlLinks/sessionApiUrl', () => ({
	getSessionApiUrl: jest.fn(() => '/api/session')
}));

jest.mock('../../../utils/Global', () => ({
	globalSession: jest.fn()
}));

describe('sessionSlice', () => {
	const initialState = {
		sessionObj: {
			loading: false,
			data: null,
			error: null
		}
	};

	beforeEach(() => {
		jest.clearAllMocks();
	});

	it('should return initial state', () => {
		const state = sessionReducer(undefined, { type: 'unknown' });
		expect(state.sessionObj.loading).toBe(false);
		expect(state.sessionObj.data).toBeNull();
		expect(state.sessionObj.error).toBeNull();
	});

	it('should handle fetchSessionData.pending', () => {
		const action = { type: fetchSessionData.pending.type };
		const state = sessionReducer(initialState, action);

		expect(state.sessionObj.loading).toBe(true);
		expect(state.sessionObj.data).toBeNull();
		expect(state.sessionObj.error).toBeNull();
	});

	it('should handle fetchSessionData.fulfilled', () => {
		const mockData = {
			'atlas.entity.create.allowed': true,
			'atlas.ui.editable.entity.types': { DataSet: true }
		};

		const action = {
			type: fetchSessionData.fulfilled.type,
			payload: mockData
		};
		const state = sessionReducer(initialState, action);

		expect(state.sessionObj.loading).toBe(false);
		expect(state.sessionObj.data).toEqual(mockData);
		expect(state.sessionObj.error).toBeNull();
	});

	it('should handle fetchSessionData.rejected', () => {
		const error = 'Error fetching session data';
		const action = {
			type: fetchSessionData.rejected.type,
			payload: error
		};
		const state = sessionReducer(initialState, action);

		expect(state.sessionObj.loading).toBe(false);
		expect(state.sessionObj.data).toBeNull();
		expect(state.sessionObj.error).toBe(error);
	});

	it('should fetch session data successfully', async () => {
		const { fetchApi } = require('../../../api/apiMethods/fetchApi');
		const mockData = {
			'atlas.entity.create.allowed': true,
			'atlas.ui.editable.entity.types': { DataSet: true }
		};
		fetchApi.mockResolvedValue({ data: mockData });

		const store = configureStore({
			reducer: {
				session: sessionReducer
			}
		});

		await store.dispatch(fetchSessionData());

		const state = store.getState().session;
		expect(state.sessionObj.loading).toBe(false);
		expect(state.sessionObj.data).toEqual(mockData);
	});

	it('should handle fetch error', async () => {
		const { fetchApi } = require('../../../api/apiMethods/fetchApi');
		const error = 'API Error';
		fetchApi.mockRejectedValue(error);

		const store = configureStore({
			reducer: {
				session: sessionReducer
			}
		});

		await store.dispatch(fetchSessionData());

		const state = store.getState().session;
		expect(state.sessionObj.loading).toBe(false);
		expect(state.sessionObj.error).toBeTruthy();
	});

	it('should call globalSession when data is fetched', async () => {
		const { fetchApi } = require('../../../api/apiMethods/fetchApi');
		const { globalSession } = require('../../../utils/Global');
		const mockData = { key: 'value' };
		fetchApi.mockResolvedValue({ data: mockData });

		const store = configureStore({
			reducer: {
				session: sessionReducer
			}
		});

		await store.dispatch(fetchSessionData());

		expect(globalSession).toHaveBeenCalledWith(mockData);
	});
});

