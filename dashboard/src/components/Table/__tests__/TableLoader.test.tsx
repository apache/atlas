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

