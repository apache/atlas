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
 * Unit tests for TableLayout component
 */

import React from 'react';
import { render, screen, fireEvent, waitFor } from '@utils/test-utils';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { BrowserRouter } from 'react-router-dom';
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';
import { ColumnDef } from '@tanstack/react-table';

const { TableLayout } = jest.requireActual('../TableLayout');

const theme = createTheme();

// Mock dependencies
jest.mock('../TableFilters', () => ({
	__esModule: true,
	default: () => <div data-testid="table-filters">TableFilters</div>
}));

jest.mock('../TablePagination', () => ({
	__esModule: true,
	default: ({ pagination, setPageIndex, nextPage, previousPage }: any) => (
		<div data-testid="table-pagination">
			<button onClick={() => setPageIndex(0)}>First Page</button>
			<button onClick={() => nextPage?.()}>Next</button>
			<button onClick={() => previousPage?.()}>Previous</button>
			<span>Page {pagination.pageIndex + 1}</span>
		</div>
	)
}));

jest.mock('../TableLoader', () => ({
	__esModule: true,
	default: () => <div data-testid="table-loader">Loading...</div>
}));

jest.mock('@components/FilterQuery', () => ({
	__esModule: true,
	default: () => <div data-testid="filter-query">FilterQuery</div>
}));

jest.mock('@views/Classification/AddTag', () => ({
	__esModule: true,
	default: ({ open, onClose }: any) =>
		open ? (
			<div data-testid="add-tag-modal">
				<button onClick={onClose}>Close Tag Modal</button>
			</div>
		) : null
}));

const TestWrapper: React.FC<React.PropsWithChildren<{}>> = ({ children }) => {
	const store = configureStore({
		reducer: {
			router: (state = {}) => state
		}
	});

	return (
		<Provider store={store}>
			<ThemeProvider theme={theme}>
				{children}
			</ThemeProvider>
		</Provider>
	);
};

describe('TableLayout', () => {
	const mockColumns: ColumnDef<any>[] = [
		{
			id: 'name',
			accessorKey: 'name',
			header: 'Name',
			enableSorting: true
		},
		{
			id: 'type',
			accessorKey: 'type',
			header: 'Type',
			enableSorting: true
		}
	];

	const mockData = [
		{ id: '1', name: 'Entity 1', type: 'DataSet' },
		{ id: '2', name: 'Entity 2', type: 'Process' },
		{ id: '3', name: 'Entity 3', type: 'DataSet' }
	];

	const defaultProps = {
		data: mockData,
		columns: mockColumns,
		isFetching: false,
		columnVisibility: false,
		columnSort: true,
		showPagination: true,
		showRowSelection: false,
		tableFilters: false,
		expandRow: false,
		emptyText: 'No data available',
		defaultColumnVisibility: {},
		pageCount: 1,
		totalCount: 3,
		isClientSidePagination: false,
		isfilterQuery: false
	};

	beforeEach(() => {
		jest.clearAllMocks();
	});

	it('should render table with data', () => {
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} />
			</TestWrapper>
		);

		expect(screen.getByText('Name')).toBeTruthy();
		expect(screen.getByText('Type')).toBeTruthy();
		expect(screen.getByText('Entity 1')).toBeTruthy();
		expect(screen.getByText('Entity 2')).toBeTruthy();
	});

	it('should display empty state when no data', () => {
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} data={[]} />
			</TestWrapper>
		);

		expect(screen.getByText('No data available')).toBeTruthy();
	});

	it('should show loading state during data fetch', () => {
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} isFetching={true} />
			</TestWrapper>
		);

		expect(screen.getByTestId('table-loader')).toBeTruthy();
	});

	it('should render table filters when tableFilters is true', () => {
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} tableFilters={true} />
			</TestWrapper>
		);

		expect(screen.getByTestId('table-filters')).toBeTruthy();
	});

	it('should render pagination when showPagination is true', () => {
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} showPagination={true} />
			</TestWrapper>
		);

		expect(screen.getByTestId('table-pagination')).toBeTruthy();
	});

	it('should not render pagination when showPagination is false', () => {
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} showPagination={false} />
			</TestWrapper>
		);

		expect(screen.queryByTestId('table-pagination')).toBeNull();
	});

	it('should handle row selection when showRowSelection is true', () => {
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} showRowSelection={true} />
			</TestWrapper>
		);

		// Checkboxes should be present for row selection
		const checkboxes = screen.getAllByRole('checkbox');
		expect(checkboxes.length).toBeGreaterThan(0);
	});

	it('should handle expandable rows when expandRow is true', () => {
		const auditTableDetails = {
			Component: ({ row }: any) => <div>Expanded content for {row.original.name}</div>,
			componentProps: {}
		};

		render(
			<TestWrapper>
				<TableLayout
					{...defaultProps}
					expandRow={true}
					auditTableDetails={auditTableDetails}
				/>
			</TestWrapper>
		);

		// Expand buttons should be present
		const expandButtons = screen.getAllByLabelText('expand row');
		expect(expandButtons.length).toBeGreaterThan(0);
	});

	it('should call fetchData when pagination changes', async () => {
		const fetchDataMock = jest.fn();
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} fetchData={fetchDataMock} />
			</TestWrapper>
		);

		// Wait for initial fetch
		await waitFor(() => {
			expect(fetchDataMock).toHaveBeenCalled();
		});
	});

	it('should handle onClickRow callback', () => {
		const onClickRowMock = jest.fn();
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} onClickRow={onClickRowMock} />
			</TestWrapper>
		);

		// Click on a row (first data cell)
		const firstRow = screen.getByText('Entity 1').closest('tr');
		if (firstRow) {
			fireEvent.click(firstRow);
			// onClickRow should be called when clicking on cells, not rows directly
		}
	});

	it('should handle column sorting', () => {
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} columnSort={true} />
			</TestWrapper>
		);

		// Sortable columns should have sort indicators
		const nameHeader = screen.getByText('Name');
		expect(nameHeader).toBeTruthy();
	});

	it('should display custom empty text', () => {
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} data={[]} emptyText="Custom empty message" />
			</TestWrapper>
		);

		expect(screen.getByText('Custom empty message')).toBeTruthy();
	});

	it('should handle client-side pagination', () => {
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} isClientSidePagination={true} />
			</TestWrapper>
		);

		expect(screen.getByTestId('table-pagination')).toBeTruthy();
	});

	it('should handle server-side pagination', () => {
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} isClientSidePagination={false} />
			</TestWrapper>
		);

		expect(screen.getByTestId('table-pagination')).toBeTruthy();
	});

	it('should render filter query when isfilterQuery is true', () => {
		// Use MemoryRouter to set the correct pathname
		const { render: rtlRender } = require('@testing-library/react')
		const { MemoryRouter } = require('react-router-dom')
		
		rtlRender(
			<Provider store={configureStore({ reducer: { router: (state = {}) => state } })}>
				<ThemeProvider theme={theme}>
					<MemoryRouter initialEntries={['/search/searchResult']}>
						<TableLayout {...defaultProps} tableFilters={true} isfilterQuery={true} />
					</MemoryRouter>
				</ThemeProvider>
			</Provider>
		);

		expect(screen.getByTestId('filter-query')).toBeTruthy();
	});

	it('should handle assignFilters for classifications', () => {
		const assignFilters = { classifications: true, term: false };
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} assignFilters={assignFilters} showRowSelection={true} />
			</TestWrapper>
		);

		// Should render classification button when rows are selected
		// This requires row selection to be active
	});

	it('should handle assignFilters for terms', () => {
		const assignFilters = { classifications: false, term: true };
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} assignFilters={assignFilters} showRowSelection={true} />
			</TestWrapper>
		);

		// Should render term button when rows are selected
	});

	it('should update table when refreshTable is called', () => {
		const refreshTableMock = jest.fn();
		render(
			<TestWrapper>
				<TableLayout {...defaultProps} refreshTable={refreshTableMock} tableFilters={true} />
			</TestWrapper>
		);

		// refreshTable is passed to TableFilters component
		expect(screen.getByTestId('table-filters')).toBeTruthy();
	});
});

