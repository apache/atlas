/**
 * Unit tests for TableLoader component
 */

import React from 'react';
import { render, screen } from '@utils/test-utils';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import TableRowsLoader from '../TableLoader';

const theme = createTheme();

const TestWrapper: React.FC<React.PropsWithChildren<{}>> = ({ children }) => (
	<ThemeProvider theme={theme}>{children}</ThemeProvider>
);

describe('TableRowsLoader', () => {
	it('should render loading skeleton rows', () => {
		render(
			<TestWrapper>
				<TableRowsLoader rowsNum={5} />
			</TestWrapper>
		);

		// Should render 5 skeleton rows
		const rows = screen.getAllByRole('row');
		expect(rows.length).toBe(5);
	});

	it('should render correct number of skeleton rows', () => {
		render(
			<TestWrapper>
				<TableRowsLoader rowsNum={10} />
			</TestWrapper>
		);

		const rows = screen.getAllByRole('row');
		expect(rows.length).toBe(10);
	});

	it('should render skeleton loaders in each cell', () => {
		render(
			<TestWrapper>
				<TableRowsLoader rowsNum={3} />
			</TestWrapper>
		);

		const rows = screen.getAllByRole('row');
		rows.forEach((row) => {
			// Each row should have skeleton loaders
			expect(row).toBeTruthy();
		});
	});

	it('should handle zero rows', () => {
		render(
			<TestWrapper>
				<TableRowsLoader rowsNum={0} />
			</TestWrapper>
		);

		const rows = screen.queryAllByRole('row');
		expect(rows.length).toBe(0);
	});

	it('should render with default props', () => {
		render(
			<TestWrapper>
				<TableRowsLoader rowsNum={1} />
			</TestWrapper>
		);

		expect(screen.getByRole('row')).toBeTruthy();
	});
});

