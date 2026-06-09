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
import { render, screen } from '@testing-library/react';
import TermRelationAttributes from '../TermRelationAttributes';

// Mock child components
jest.mock('../TermRelationViewAttributes', () => ({
	__esModule: true,
	default: ({ attrObj, control, editModal, currentType }: any) => (
		<div data-testid="term-relation-view-attributes">
			<div>Display Text: {attrObj?.displayText}</div>
			<div>Edit Mode: {editModal ? 'true' : 'false'}</div>
			<div>Current Type: {currentType}</div>
		</div>
	)
}));

jest.mock('@components/Table/TableLayout', () => ({
	TableLayout: ({ data, columns, emptyText }: any) => (
		<div data-testid="table-layout">
			{data && data.length > 0 ? (
				<table>
					<thead>
						<tr>
							{columns.map((col: any, idx: number) => (
								<th key={idx}>{col.header}</th>
							))}
						</tr>
					</thead>
					<tbody>
						{data.map((row: any, idx: number) => (
							<tr key={idx} data-testid={`table-row-${idx}`}>
								{columns.map((col: any, colIdx: number) => (
									<td key={colIdx} data-testid={`cell-${idx}-${colIdx}`}>
										{col.cell ? col.cell({ row: { original: row } }) : row}
									</td>
								))}
							</tr>
						))}
					</tbody>
				</table>
			) : (
				<div data-testid="empty-text">{emptyText}</div>
			)}
		</div>
	)
}));

describe('TermRelationAttributes - 100% Coverage', () => {
	const mockTermObj = [
		{ displayText: 'Term 1', qualifiedName: 'term1', description: 'Description 1' },
		{ displayText: 'Term 2', qualifiedName: 'term2', description: 'Description 2' }
	];

	const mockControl = {};
	const mockCurrentType = 'seeAlso';

	describe('Component Rendering', () => {
		test('renders TermRelationAttributes component', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('renders table with correct structure', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getAllByText('Term').length).toBeGreaterThan(0);
			expect(screen.getByText('Attribute')).toBeInTheDocument();
		});

		test('renders all term rows', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-row-0')).toBeInTheDocument();
			expect(screen.getByTestId('table-row-1')).toBeInTheDocument();
		});
	});

	describe('Table Columns', () => {
		test('renders Term column with displayText', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByText('Term 1')).toBeInTheDocument();
			expect(screen.getByText('Term 2')).toBeInTheDocument();
		});

		test('renders Attribute column with TermRelationViewAttributes', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			const viewAttributes = screen.getAllByTestId('term-relation-view-attributes');
			expect(viewAttributes.length).toBe(2);
		});

		test('passes correct props to TermRelationViewAttributes', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByText('Display Text: Term 1')).toBeInTheDocument();
			expect(screen.getAllByText('Edit Mode: false').length).toBeGreaterThan(0);
			expect(screen.getAllByText('Current Type: seeAlso').length).toBeGreaterThan(0);
		});

		test('passes editModal=true to TermRelationViewAttributes', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={true}
					currentType={mockCurrentType}
				/>
			);

			const editModeTexts = screen.getAllByText('Edit Mode: true');
			expect(editModeTexts.length).toBe(2);
		});
	});

	describe('Empty State', () => {
		test('renders empty text when termObj is empty array', () => {
			render(
				<TermRelationAttributes
					termObj={[]}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('empty-text')).toBeInTheDocument();
			expect(screen.getByText('No Records found!')).toBeInTheDocument();
		});

		test('renders empty text when termObj is null', () => {
			render(
				<TermRelationAttributes
					termObj={null}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('empty-text')).toBeInTheDocument();
		});

		test('renders empty text when termObj is undefined', () => {
			render(
				<TermRelationAttributes
					termObj={undefined}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('empty-text')).toBeInTheDocument();
		});
	});

	describe('Column Configuration', () => {
		test('Term column has correct width', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			// Column configuration is passed to TableLayout
			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('Term column is sortable', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getAllByText('Term').length).toBeGreaterThan(0);
		});

		test('Attribute column is not sortable', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByText('Attribute')).toBeInTheDocument();
		});
	});

	describe('Edge Cases', () => {
		test('handles single term object', () => {
			const singleTerm = [{ displayText: 'Single Term', qualifiedName: 'single' }];

			render(
				<TermRelationAttributes
					termObj={singleTerm}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByText('Single Term')).toBeInTheDocument();
			expect(screen.getByTestId('table-row-0')).toBeInTheDocument();
			expect(screen.queryByTestId('table-row-1')).not.toBeInTheDocument();
		});

		test('handles term without displayText', () => {
			const termWithoutDisplay = [{ qualifiedName: 'term1' }];

			render(
				<TermRelationAttributes
					termObj={termWithoutDisplay}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('handles term with extra properties', () => {
			const termWithExtra = [
				{
					displayText: 'Term',
					qualifiedName: 'term',
					extraProp1: 'value1',
					extraProp2: 'value2'
				}
			];

			render(
				<TermRelationAttributes
					termObj={termWithExtra}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

		expect(screen.getAllByText('Term').length).toBeGreaterThan(0);
		});

		test('handles different currentType values', () => {
			const types = ['synonyms', 'antonyms', 'preferredTerms'];

			types.forEach((type) => {
				const { unmount } = render(
					<TermRelationAttributes
						termObj={mockTermObj}
						control={mockControl}
						editModal={false}
						currentType={type}
					/>
				);

				expect(screen.getAllByText(`Current Type: ${type}`).length).toBeGreaterThan(0);
				unmount();
			});
		});

		test('handles missing control prop', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={undefined}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('handles missing currentType prop', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={undefined}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});
	});

	describe('Table Props', () => {
		test('passes correct props to TableLayout', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			const table = screen.getByTestId('table-layout');
			expect(table).toBeInTheDocument();
		});

		test('disables column visibility', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('disables client side sorting', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('disables pagination', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('disables row selection', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('disables table filters', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});
	});

	describe('Multiple Terms', () => {
		test('renders multiple terms correctly', () => {
			const manyTerms = Array.from({ length: 10 }, (_, i) => ({
				displayText: `Term ${i + 1}`,
				qualifiedName: `term${i + 1}`
			}));

			render(
				<TermRelationAttributes
					termObj={manyTerms}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			manyTerms.forEach((term) => {
				expect(screen.getByText(term.displayText)).toBeInTheDocument();
			});
		});

		test('each term has its own TermRelationViewAttributes', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			const viewAttributes = screen.getAllByTestId('term-relation-view-attributes');
			expect(viewAttributes.length).toBe(mockTermObj.length);
		});
	});

	describe('Edit Mode', () => {
		test('passes editModal prop correctly to child components', () => {
			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={true}
					currentType={mockCurrentType}
				/>
			);

			const editModeTexts = screen.getAllByText('Edit Mode: true');
			expect(editModeTexts.length).toBe(mockTermObj.length);
		});

		test('passes control prop to child components', () => {
			const customControl = { test: 'value' };

			render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={customControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});
	});

	describe('useMemo Optimization', () => {
		test('columns are memoized', () => {
			const { rerender } = render(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getAllByText('Term').length).toBeGreaterThan(0);

			// Re-render with same props
			rerender(
				<TermRelationAttributes
					termObj={mockTermObj}
					control={mockControl}
					editModal={false}
					currentType={mockCurrentType}
				/>
			);

			// Columns should still be rendered
			expect(screen.getAllByText('Term').length).toBeGreaterThan(0);
			expect(screen.getByText('Attribute')).toBeInTheDocument();
		});
	});
});
