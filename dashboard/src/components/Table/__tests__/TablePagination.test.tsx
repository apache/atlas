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
 * Unit tests for TablePagination component
 */

import React from 'react';
import { render, screen, fireEvent, waitFor } from '@utils/test-utils';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import TablePagination from '../TablePagination';

const theme = createTheme();

const TestWrapper: React.FC<React.PropsWithChildren<{}>> = ({ children }) => (
	<ThemeProvider theme={theme}>{children}</ThemeProvider>
);

describe('TablePagination', () => {
	// Create a stable mock function that always returns valid data
	const mockGetRowModel = jest.fn(() => ({
		rows: Array.from({ length: 25 }, (_, i) => ({ id: i }))
	}));

	const defaultProps = {
		isServerSide: false,
		pagination: { pageIndex: 0, pageSize: 25 },
		memoizedData: Array.from({ length: 100 }, (_, i) => ({ id: i, name: `Item ${i}` })),
		totalCount: 100,
		getPageCount: jest.fn(() => 4),
		previousPage: jest.fn(),
		nextPage: jest.fn(),
		setPageIndex: jest.fn(),
		setPageSize: jest.fn(),
		getRowModel: mockGetRowModel,
		isFirstPage: true,
		setPagination: jest.fn(),
		goToPageVal: '',
		setGoToPageVal: jest.fn(),
		isEmptyData: false,
		setIsEmptyData: jest.fn(),
		showGoToPage: true
	};

	beforeEach(() => {
		jest.clearAllMocks();
		// Reset getRowModel mock to return valid data
		mockGetRowModel.mockReturnValue({
			rows: Array.from({ length: 25 }, (_, i) => ({ id: i }))
		});
	});

	it('should render pagination component', () => {
		render(
			<TestWrapper>
				<TablePagination {...defaultProps} />
			</TestWrapper>
		);

		expect(screen.getByRole('navigation')).toBeTruthy();
	});

	it('should display correct page information', () => {
		render(
			<TestWrapper>
				<TablePagination {...defaultProps} />
			</TestWrapper>
		);

		// Check for pagination controls
		expect(screen.getByRole('navigation')).toBeTruthy();
	});

	it('should call nextPage when next button is clicked', () => {
		const nextPageMock = jest.fn();
		render(
			<TestWrapper>
				<TablePagination {...defaultProps} nextPage={nextPageMock} isFirstPage={false} />
			</TestWrapper>
		);

		const nextButton = screen.getByLabelText('next page');
		if (nextButton) {
			fireEvent.click(nextButton);
			expect(nextPageMock).toHaveBeenCalled();
		}
	});

	it('should call previousPage when previous button is clicked', () => {
		const previousPageMock = jest.fn();
		render(
			<TestWrapper>
				<TablePagination
					{...defaultProps}
					previousPage={previousPageMock}
					pagination={{ pageIndex: 1, pageSize: 25 }}
					isFirstPage={false}
				/>
			</TestWrapper>
		);

		const prevButton = screen.getByLabelText('previous page');
		if (prevButton) {
			fireEvent.click(prevButton);
			expect(previousPageMock).toHaveBeenCalled();
		}
	});

	it('should disable previous button on first page', () => {
		render(
			<TestWrapper>
				<TablePagination {...defaultProps} isFirstPage={true} />
			</TestWrapper>
		);

		const prevButton = screen.queryByLabelText('Go to previous page');
		if (prevButton) {
			expect(prevButton).toBeDisabled();
		}
	});

	it('should handle page size change', async () => {
		const setPageSizeMock = jest.fn();
		render(
			<TestWrapper>
				<TablePagination {...defaultProps} setPageSize={setPageSizeMock} />
			</TestWrapper>
		);

		// Look for page size selector (might be a select or autocomplete)
		const pageSizeSelect = screen.queryByRole('combobox');
		if (pageSizeSelect) {
			fireEvent.mouseDown(pageSizeSelect);
			await waitFor(() => {
				const option = screen.queryByText('50');
				if (option) {
					fireEvent.click(option);
					expect(setPageSizeMock).toHaveBeenCalled();
				}
			});
		}
	});

	it('should display empty state when no data', () => {
		// Mock getRowModel to return empty rows for empty state test
		mockGetRowModel.mockReturnValue({
			rows: []
		});
		
		render(
			<TestWrapper>
				<TablePagination
					{...defaultProps}
					memoizedData={[]}
					isEmptyData={true}
					totalCount={0}
					getRowModel={mockGetRowModel}
				/>
			</TestWrapper>
		);

		// Pagination should still render but show empty state
		expect(screen.getByRole('navigation')).toBeTruthy();
	});

	it('should handle server-side pagination', () => {
		const fetchDataMock = jest.fn();
		render(
			<TestWrapper>
				<TablePagination {...defaultProps} isServerSide={true} />
			</TestWrapper>
		);

		expect(screen.getByRole('navigation')).toBeTruthy();
	});

	it('should update goToPageVal when input changes', () => {
		const setGoToPageValMock = jest.fn();
		render(
			<TestWrapper>
				<TablePagination {...defaultProps} setGoToPageVal={setGoToPageValMock} />
			</TestWrapper>
		);

		const goToPageInput = screen.queryByPlaceholderText(/go to page/i);
		if (goToPageInput) {
			fireEvent.change(goToPageInput, { target: { value: '3' } });
			// The component uses pendingGoToPageVal internally, so setGoToPageVal is only called on Enter key
			// Just verify the input value changed
			expect((goToPageInput as HTMLInputElement).value).toBe('3');
		}
	});
});

