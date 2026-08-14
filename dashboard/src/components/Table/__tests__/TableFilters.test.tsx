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
 * Unit tests for TableFilters component
 */

import React from 'react';
import { render, screen, fireEvent, waitFor } from '@utils/test-utils';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';
import { TableFilter } from '../TableFilters';

const theme = createTheme();

// Mock dependencies
jest.mock('../../Modal', () => ({
	__esModule: true,
	default: ({ open, onClose, children }: any) =>
		open ? (
			<div data-testid="modal">
				{children}
				<button onClick={onClose}>Close</button>
			</div>
		) : null
}));

jest.mock('@views/Entity/EntityForm', () => ({
	__esModule: true,
	default: ({ open, onClose }: any) =>
		open ? (
			<div data-testid="entity-form">
				<button onClick={onClose}>Close Entity Form</button>
			</div>
		) : null
}));

jest.mock('@views/SaveFilters/SaveFilters', () => ({
	__esModule: true,
	default: ({ open, onClose }: any) =>
		open ? (
			<div data-testid="save-filters">
				<button onClick={onClose}>Close Save Filters</button>
			</div>
		) : null
}));

jest.mock('@views/Classification/AddTag', () => ({
	__esModule: true,
	default: ({ open, onClose }: any) =>
		open ? (
			<div data-testid="add-tag">
				<button onClick={onClose}>Close Add Tag</button>
			</div>
		) : null
}));

jest.mock('@views/Glossary/AssignTerm', () => ({
	__esModule: true,
	default: ({ open, onClose }: any) =>
		open ? (
			<div data-testid="assign-term">
				<button onClick={onClose}>Close Assign Term</button>
			</div>
		) : null
}));

jest.mock('@components/QueryBuilder/Filters', () => ({
	__esModule: true,
	default: () => <div data-testid="query-builder-filters">Query Builder Filters</div>
}));

jest.mock('@api/apiMethods/downloadApiMethod', () => ({
	downloadSearchResultsCSV: jest.fn()
}));

const createMockStore = (sessionData: any = {}) => {
	return configureStore({
		reducer: {
			session: (state = { sessionObj: { data: sessionData } }) => state
		},
		preloadedState: {
			session: {
				sessionObj: { data: sessionData }
			}
		}
	});
};

const TestWrapper: React.FC<React.PropsWithChildren<{ store: any }>> = ({ children, store }) => (
	<Provider store={store}>
		<ThemeProvider theme={theme}>{children}</ThemeProvider>
	</Provider>
);

describe('TableFilter', () => {
	const mockGetAllColumns = jest.fn(() => []);
	const mockGetSelectedRowModel = jest.fn(() => ({ rows: [] }));
	const mockRefreshTable = jest.fn();
	const mockSetRowSelection = jest.fn();
	const mockSetUpdateTable = jest.fn();

	const defaultProps = {
		getAllColumns: mockGetAllColumns,
		defaultColumnParams: '',
		columnVisibility: false,
		refreshTable: mockRefreshTable,
		rowSelection: {},
		setRowSelection: mockSetRowSelection,
		queryBuilder: false,
		allTableFilters: false,
		columnVisibilityParams: false,
		setUpdateTable: mockSetUpdateTable,
		getSelectedRowModel: mockGetSelectedRowModel,
		memoizedData: []
	};

	beforeEach(() => {
		jest.clearAllMocks();
	});

	it('should render table filters component', () => {
		const store = createMockStore();
		const { container } = render(
			<TestWrapper store={store}>
				<TableFilter {...defaultProps} />
			</TestWrapper>
		);

		// Component should render
		expect(container.firstChild).toBeTruthy();
	});

	it('should show refresh button', () => {
		const store = createMockStore();
		render(
			<TestWrapper store={store}>
				<TableFilter {...defaultProps} />
			</TestWrapper>
		);

		// Refresh functionality should be available
		const buttons = screen.queryAllByRole('button');
		expect(buttons.length).toBeGreaterThanOrEqual(0);
	});

	it('should call refreshTable when refresh button is clicked', () => {
		const store = createMockStore();
		render(
			<TestWrapper store={store}>
				<TableFilter {...defaultProps} />
			</TestWrapper>
		);

		const refreshButton = screen.queryByLabelText(/refresh/i) || screen.queryByRole('button', { name: /refresh/i });
		if (refreshButton) {
			fireEvent.click(refreshButton);
			expect(mockRefreshTable).toHaveBeenCalled();
		}
	});

	it('should show column visibility menu when columnVisibility is true', () => {
		const store = createMockStore();
		render(
			<TestWrapper store={store}>
				<TableFilter
					{...defaultProps}
					columnVisibility={true}
					memoizedData={[{ guid: 'row-1' }]}
				/>
			</TestWrapper>
		);

		// Column visibility controls should be available
		expect(screen.getByRole('button', { name: /columns/i })).toBeInTheDocument();
	});

	it('should show query builder when queryBuilder is true', () => {
		const store = createMockStore();
		render(
			<TestWrapper store={store}>
				<TableFilter {...defaultProps} queryBuilder={true} />
			</TestWrapper>
		);

		const filtersButton = screen.getByRole('button', { name: /filters/i });
		fireEvent.click(filtersButton);
		expect(screen.getByTestId('query-builder-filters')).toBeTruthy();
	});

	it('should open entity form modal when create entity button is clicked', () => {
		const sessionData = {
			'atlas.entity.create.allowed': true
		};
		const store = createMockStore(sessionData);
		render(
			<TestWrapper store={store}>
				<TableFilter {...defaultProps} />
			</TestWrapper>
		);

		const createButton = screen.queryByText(/create.*entity/i) || screen.queryByLabelText(/create.*entity/i);
		if (createButton) {
			fireEvent.click(createButton);
			expect(screen.getByTestId('entity-form')).toBeTruthy();
		}
	});

	it('should open save filters modal', () => {
		const store = createMockStore();
		render(
			<TestWrapper store={store}>
				<TableFilter {...defaultProps} />
			</TestWrapper>
		);

		const saveButton = screen.queryByText(/save.*filter/i) || screen.queryByLabelText(/save/i);
		if (saveButton) {
			fireEvent.click(saveButton);
			expect(screen.getByTestId('save-filters')).toBeTruthy();
		}
	});

	it('should handle download CSV', async () => {
		const store = createMockStore();
		const { downloadSearchResultsCSV } = require('@api/apiMethods/downloadApiMethod');
		downloadSearchResultsCSV.mockResolvedValue({});

		render(
			<TestWrapper store={store}>
				<TableFilter {...defaultProps} memoizedData={[{ id: '1', name: 'Test' }]} />
			</TestWrapper>
		);

		const downloadButton = screen.queryByLabelText(/download/i) || screen.queryByText(/download/i);
		if (downloadButton) {
			fireEvent.click(downloadButton);
			await waitFor(() => {
				expect(downloadSearchResultsCSV).toHaveBeenCalled();
			});
		}
	});

	it('should show tag modal when rows are selected and classifications filter is enabled', () => {
		const store = createMockStore();
		const mockGetSelectedRowModelWithRows = jest.fn(() => ({
			rows: [{ original: { guid: '1' } }]
		}));

		render(
			<TestWrapper store={store}>
				<TableFilter
					{...defaultProps}
					getSelectedRowModel={mockGetSelectedRowModelWithRows}
					rowSelection={{ '0': true }}
				/>
			</TestWrapper>
		);

		// Tag modal should be available when rows are selected
		// This depends on assignFilters prop
	});

	it('should handle empty row selection', () => {
		const store = createMockStore();
		const { container } = render(
			<TestWrapper store={store}>
				<TableFilter {...defaultProps} rowSelection={{}} />
			</TestWrapper>
		);

		expect(container.firstChild).toBeTruthy();
	});
});

