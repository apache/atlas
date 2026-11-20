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

import React from 'react';
import { render } from '@testing-library/react';
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';
import { useSidebarTypedefBootstrap } from '../useSidebarTypedefBootstrap';
import * as enumSlice from '@redux/slice/enumSlice';
import * as metricsSlice from '@redux/slice/metricsSlice';

jest.mock('@redux/slice/enumSlice', () => ({
	fetchEnumData: jest.fn(() => ({ type: 'enum/fetch' })),
}));

jest.mock('@redux/slice/rootClassificationSlice', () => ({
	fetchRootClassification: jest.fn(() => ({ type: 'rootClassification/fetch' })),
}));

jest.mock('@redux/slice/typeDefSlices/typeDefHeaderSlice', () => ({
	fetchTypeHeaderData: jest.fn(() => ({ type: 'typeHeader/fetch' })),
}));

jest.mock('@redux/slice/allEntityTypesSlice', () => ({
	fetchRootEntity: jest.fn(() => ({ type: 'allEntityTypes/fetch' })),
}));

jest.mock('@redux/slice/metricsSlice', () => ({
	fetchMetricEntity: jest.fn(() => ({ type: 'metrics/fetch' })),
}));

jest.mock('@redux/slice/sessionSlice', () => ({
	fetchVersionData: jest.fn(() => ({ type: 'session/fetchVersion' })),
}));

const Harness = () => {
	useSidebarTypedefBootstrap();
	return null;
};

const buildStore = (preloadedState: Record<string, unknown>) =>
	configureStore({
		reducer: {
			typeHeader: (state = { typeHeaderData: null }) => state,
			allEntityTypes: (state = { allEntityTypesData: null }) => state,
			rootClassification: (
				state = { rootClassificationTypeData: { rootClassificationData: null } },
			) => state,
			enum: (state = { enumObj: { data: null } }) => state,
			metrics: (state = { metricsData: null }) => state,
			session: (state = { versionData: { data: null } }) => state,
		},
		preloadedState,
	});

describe('useSidebarTypedefBootstrap', () => {
	beforeEach(() => {
		jest.clearAllMocks();
	});

	it('dispatches fetches when slice values are null', () => {
		const store = buildStore({});
		render(
			<Provider store={store}>
				<Harness />
			</Provider>,
		);

		expect(enumSlice.fetchEnumData).toHaveBeenCalled();
		expect(metricsSlice.fetchMetricEntity).toHaveBeenCalled();
	});

	it('does not refetch metrics when metricsData is an empty object', () => {
		const store = buildStore({
			metrics: { metricsData: {} },
		});
		render(
			<Provider store={store}>
				<Harness />
			</Provider>,
		);

		expect(metricsSlice.fetchMetricEntity).not.toHaveBeenCalled();
	});
});
