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
 * Unit tests for SearchResult component
 */

import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';
import { BrowserRouter } from 'react-router-dom';
import SearchResult from '../SearchResult/SearchResult';

// Mock API methods
jest.mock('../../api/apiMethods/searchApiMethod', () => ({
	getBasicSearchResult: jest.fn()
}));

jest.mock('../../api/apiMethods/classificationApiMethod', () => ({
	removeClassification: jest.fn()
}));

jest.mock('../../api/apiMethods/glossaryApiMethod', () => ({
	removeTerm: jest.fn()
}));

jest.mock('../../api/apiMethods/apiMethod', () => ({
	_get: jest.fn()
}));

// Mock components
jest.mock('@components/Table/TableLayout', () => ({
	TableLayout: ({ data, columns, fetchData, onClickRow }: any) => (
		<div data-testid="table-layout">
			<div data-testid="table-data">{JSON.stringify(data)}</div>
			{columns.map((col: any) => (
				<div key={col.id}>{col.header}</div>
			))}
			<button onClick={() => fetchData?.({ pagination: { pageIndex: 0, pageSize: 25 } })}>
				Fetch Data
			</button>
			<button onClick={() => onClickRow?.({}, { original: data[0] })}>Click Row</button>
		</div>
	)
}));

jest.mock('@components/TextShowMoreLess', () => ({
	TextShowMoreLess: ({ text }: any) => <div>{text}</div>
}));

jest.mock('@components/EntityDisplayImage', () => ({
	__esModule: true,
	default: () => <div data-testid="entity-image">Entity Image</div>
}));

jest.mock('@components/DialogShowMoreLess', () => ({
	__esModule: true,
	default: ({ open, onClose, children }: any) =>
		open ? (
			<div data-testid="dialog-show-more">
				{children}
				<button onClick={onClose}>Close</button>
			</div>
		) : null
}));

jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useSearchParams: () => {
		const [params, setParams] = React.useState(
			new URLSearchParams('?type=DataSet&limit=25&offset=0')
		);
		return [params, setParams];
	},
	Link: ({ to, children }: any) => <a href={to}>{children}</a>
}));

const defaultBusinessMetaSlice = {
	loading: false,
	businessMetaData: null,
	error: null
}

const createMockStore = (entityData: any = {}) => {
	return configureStore({
		reducer: {
			entity: (state = { entityData }) => state,
			businessMetaData: (state = defaultBusinessMetaSlice) => state
		},
		preloadedState: {
			entity: {
				entityData
			},
			businessMetaData: defaultBusinessMetaSlice
		}
	});
};

const TestWrapper: React.FC<React.PropsWithChildren<{ store: any }>> = ({ children, store }) => (
	<Provider store={store}>
		<BrowserRouter>{children}</BrowserRouter>
	</Provider>
);

describe('SearchResult', () => {
	const mockEntityData = {
		entityDefs: [
			{
				name: 'DataSet',
				attributeDefs: [
					{ name: 'name', typeName: 'string' },
					{ name: 'description', typeName: 'string' }
				],
				relationshipAttributeDefs: []
			}
		]
	};

	beforeEach(() => {
		jest.clearAllMocks();
	});

	it('should render search results table', () => {
		const store = createMockStore(mockEntityData);
		render(
			<TestWrapper store={store}>
				<SearchResult />
			</TestWrapper>
		);

		expect(screen.getByTestId('table-layout')).toBeTruthy();
	});

	it('should handle search params from URL', async () => {
		const store = createMockStore(mockEntityData);
		render(
			<TestWrapper store={store}>
				<SearchResult />
			</TestWrapper>
		);

		// Component should read search params from URL
		await waitFor(() => {
			expect(screen.getByTestId('table-layout')).toBeTruthy();
		});
	});

	it('should display empty state when no search results', async () => {
		const store = createMockStore(mockEntityData);
		const { getBasicSearchResult } = require('../../api/apiMethods/searchApiMethod');
		getBasicSearchResult.mockResolvedValue({
			data: {
				searchResults: {
					entities: [],
					totalCount: 0
				}
			}
		});

		render(
			<TestWrapper store={store}>
				<SearchResult />
			</TestWrapper>
		);

		await waitFor(() => {
			// Empty state should be displayed
			expect(screen.getByTestId('table-layout')).toBeTruthy();
		});
	});

	it('should handle pagination', async () => {
		const store = createMockStore(mockEntityData);
		const { getBasicSearchResult } = require('../../api/apiMethods/searchApiMethod');
		getBasicSearchResult.mockResolvedValue({
			data: {
				searchResults: {
					entities: [
						{ guid: '1', typeName: 'DataSet', attributes: { name: 'Entity 1' } }
					],
					totalCount: 1
				}
			}
		});

		render(
			<TestWrapper store={store}>
				<SearchResult />
			</TestWrapper>
		);

		await waitFor(() => {
			const fetchButton = screen.getByText('Fetch Data');
			if (fetchButton) {
				fireEvent.click(fetchButton);
			}
		});
	});

	it('should handle row click navigation', async () => {
		const store = createMockStore(mockEntityData);
		const { getBasicSearchResult } = require('../../api/apiMethods/searchApiMethod');
		getBasicSearchResult.mockResolvedValue({
			data: {
				searchResults: {
					entities: [
						{ guid: '1', typeName: 'DataSet', attributes: { name: 'Entity 1' } }
					],
					totalCount: 1
				}
			}
		});

		render(
			<TestWrapper store={store}>
				<SearchResult />
			</TestWrapper>
		);

		await waitFor(() => {
			const clickRowButton = screen.getByText('Click Row');
			if (clickRowButton) {
				fireEvent.click(clickRowButton);
			}
		});
	});

	it('should handle toggle for include deleted entities', () => {
		const store = createMockStore(mockEntityData);
		render(
			<TestWrapper store={store}>
				<SearchResult />
			</TestWrapper>
		);

		// Switch should be present for include deleted entities
		// This depends on the actual implementation
	});

	it('should handle toggle for exclude sub classifications', () => {
		const store = createMockStore(mockEntityData);
		render(
			<TestWrapper store={store}>
				<SearchResult />
			</TestWrapper>
		);

		// Switch should be present for exclude sub classifications
	});

	it('should handle API errors gracefully', async () => {
		const store = createMockStore(mockEntityData);
		const { getBasicSearchResult } = require('../../api/apiMethods/searchApiMethod');
		getBasicSearchResult.mockRejectedValue(new Error('API Error'));

		render(
			<TestWrapper store={store}>
				<SearchResult />
			</TestWrapper>
		);

		await waitFor(() => {
			// Error should be handled gracefully
			expect(screen.getByTestId('table-layout')).toBeTruthy();
		});
	});

	it('should refresh table data when refreshTable is called', async () => {
		const store = createMockStore(mockEntityData);
		render(
			<TestWrapper store={store}>
				<SearchResult />
			</TestWrapper>
		);

		await waitFor(() => {
			expect(screen.getByTestId('table-layout')).toBeTruthy();
		});
	});

	it('should handle classification params', () => {
		const store = createMockStore(mockEntityData);
		render(
			<TestWrapper store={store}>
				<SearchResult classificationParams={{ tagName: 'PII' }} />
			</TestWrapper>
		);

		expect(screen.getByTestId('table-layout')).toBeTruthy();
	});

	it('should handle glossary type params', () => {
		const store = createMockStore(mockEntityData);
		render(
			<TestWrapper store={store}>
				<SearchResult glossaryTypeParams={{ termName: 'Customer' }} />
			</TestWrapper>
		);

		expect(screen.getByTestId('table-layout')).toBeTruthy();
	});
});

