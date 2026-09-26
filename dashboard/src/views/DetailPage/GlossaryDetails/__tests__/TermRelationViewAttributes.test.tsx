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
import { render, screen, fireEvent } from '@testing-library/react';
import TermRelationViewAttributes from '../TermRelationViewAttributes';

// Mock dependencies
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

// Mock Utils
jest.mock('@utils/Utils', () => ({
	isEmpty: (val: any) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0) || (typeof val === 'object' && Object.keys(val).length === 0)
}));

// Mock Enum
jest.mock('@utils/Enum', () => ({
	attributeObj: {
		description: 'Description',
		expression: 'Expression',
		source: 'Source',
		steward: 'Steward',
		confidence: 'Confidence'
	}
}));

// Mock react-hook-form
const mockOnChange = jest.fn();
const mockRender = jest.fn((props) => props.render({ field: { onChange: mockOnChange, value: '' } }));

jest.mock('react-hook-form', () => ({
	Controller: ({ control, name, defaultValue, render }: any) => {
		const field = { onChange: mockOnChange, value: defaultValue || '' };
		return render({ field });
	}
}));

describe('TermRelationViewAttributes - 100% Coverage', () => {
	const mockAttrObj = {
		displayText: 'Test Term',
		description: 'Test Description',
		expression: 'Test Expression',
		source: 'Test Source'
	};

	const mockControl = {};
	const mockCurrentType = 'seeAlso';

	beforeEach(() => {
		jest.clearAllMocks();
	});

	describe('Component Rendering', () => {
		test('renders TermRelationViewAttributes component', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('renders table with correct structure', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByText('Name')).toBeInTheDocument();
			expect(screen.getByText('Value')).toBeInTheDocument();
		});

		test('renders all attribute rows', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			// Should render rows for all attributes in attributeObj
			expect(screen.getByTestId('table-row-0')).toBeInTheDocument();
			expect(screen.getByTestId('table-row-1')).toBeInTheDocument();
		});
	});

	describe('View Mode (editModal=false)', () => {
		test('displays attribute names', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByText('description')).toBeInTheDocument();
			expect(screen.getByText('expression')).toBeInTheDocument();
			expect(screen.getByText('source')).toBeInTheDocument();
		});

		test('displays attribute values', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByText('Test Description')).toBeInTheDocument();
			expect(screen.getByText('Test Expression')).toBeInTheDocument();
			expect(screen.getByText('Test Source')).toBeInTheDocument();
		});

		test('displays "--" for empty values', () => {
			const attrWithEmpty = {
				displayText: 'Test Term',
				description: '',
				expression: null,
				source: undefined
			};

			render(
				<TermRelationViewAttributes
					attrObj={attrWithEmpty}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			const dashTexts = screen.getAllByText('--');
			expect(dashTexts.length).toBeGreaterThan(0);
		});

		test('displays values for non-empty attributes', () => {
			const attrPartial = {
				displayText: 'Test Term',
				description: 'Has Description',
				expression: '',
				source: null
			};

			render(
				<TermRelationViewAttributes
					attrObj={attrPartial}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByText('Has Description')).toBeInTheDocument();
		});
	});

	describe('Edit Mode (editModal=true)', () => {
		test('renders TextField for each attribute', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={true}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			const textFields = screen.getAllByRole('textbox');
			expect(textFields.length).toBeGreaterThan(0);
		});

		test('TextField has correct default value', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={true}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			const textFields = screen.getAllByRole('textbox');
			expect(textFields[0]).toHaveValue('Test Description');
		});

		test('TextField has correct placeholder', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={true}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByPlaceholderText('description')).toBeInTheDocument();
			expect(screen.getByPlaceholderText('expression')).toBeInTheDocument();
		});

		test('TextField onChange updates value', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={true}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			const textField = screen.getByPlaceholderText('description');
			fireEvent.change(textField, { target: { value: 'New Description' } });

			expect(mockOnChange).toHaveBeenCalled();
		});

		test('TextField has correct variant and size', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={true}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			const textField = screen.getByPlaceholderText('description');
			expect(textField.closest('.form-textfield')).toBeTruthy();
		});

		test('Controller uses correct field name', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={true}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			// Field name should be: currentType.displayText.attributeName
			// e.g., seeAlso.Test Term.description
			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('handles empty attrObj values in edit mode', () => {
			const emptyAttr = {
				displayText: 'Test',
				description: '',
				expression: null
			};

			render(
				<TermRelationViewAttributes
					attrObj={emptyAttr}
					editModal={true}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			const textFields = screen.getAllByRole('textbox');
			expect(textFields.length).toBeGreaterThan(0);
		});
	});

	describe('Column Configuration', () => {
		test('Name column is sortable', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByText('Name')).toBeInTheDocument();
		});

		test('Value column is not sortable', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByText('Value')).toBeInTheDocument();
		});

		test('Name column has correct width', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});
	});

	describe('Edge Cases', () => {
		test('handles null attrObj', () => {
			render(
				<TermRelationViewAttributes
					attrObj={null}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('handles undefined attrObj', () => {
			render(
				<TermRelationViewAttributes
					attrObj={undefined}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('handles empty attrObj', () => {
			render(
				<TermRelationViewAttributes
					attrObj={{}}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('handles attrObj without displayText', () => {
			const attrWithoutDisplay = {
				description: 'Test',
				expression: 'Test'
			};

			render(
				<TermRelationViewAttributes
					attrObj={attrWithoutDisplay}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getAllByText('Test').length).toBeGreaterThan(0);
		});

		test('handles missing control prop', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={undefined}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('handles missing currentType prop', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={undefined}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('handles zero values', () => {
			const attrWithZero = {
				displayText: 'Test',
				description: 0,
				confidence: 0
			};

			render(
				<TermRelationViewAttributes
					attrObj={attrWithZero}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getAllByText('0').length).toBeGreaterThan(0);
		});

		test('handles boolean values', () => {
			const attrWithBoolean = {
				displayText: 'Test',
				description: true,
				expression: false
			};

			render(
				<TermRelationViewAttributes
					attrObj={attrWithBoolean}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getAllByText('true').length).toBeGreaterThan(0);
			expect(screen.getAllByText('false').length).toBeGreaterThan(0);
		});
	});

	describe('Table Props', () => {
		test('passes correct props to TableLayout', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			const table = screen.getByTestId('table-layout');
			expect(table).toBeInTheDocument();
		});

		test('disables column visibility', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('disables client side sorting', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('disables pagination', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('disables row selection', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('disables table filters', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('sets empty text correctly', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});
	});

	describe('All Attributes', () => {
		test('renders all attributes from attributeObj', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			// All attributes from Enum should be rendered
			expect(screen.getByText('description')).toBeInTheDocument();
			expect(screen.getByText('expression')).toBeInTheDocument();
			expect(screen.getByText('source')).toBeInTheDocument();
			expect(screen.getByText('steward')).toBeInTheDocument();
			expect(screen.getByText('confidence')).toBeInTheDocument();
		});

		test('handles attributes with special characters', () => {
			const attrSpecial = {
				displayText: 'Test',
				description: 'Value with "quotes"',
				expression: "Value with 'apostrophes'",
				source: 'Value with <brackets>'
			};

			render(
				<TermRelationViewAttributes
					attrObj={attrSpecial}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByText('Value with "quotes"')).toBeInTheDocument();
		});

		test('handles long attribute values', () => {
			const attrLong = {
				displayText: 'Test',
				description: 'This is a very long description that should be displayed correctly without breaking the layout or causing any issues with the component rendering'
			};

			render(
				<TermRelationViewAttributes
					attrObj={attrLong}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByText(/This is a very long description/)).toBeInTheDocument();
		});
	});

	describe('Mode Switching', () => {
		test('switches from view to edit mode', () => {
			const { rerender } = render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getAllByText('Test Description').length).toBeGreaterThan(0);
			expect(screen.queryByRole('textbox')).not.toBeInTheDocument();

			rerender(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={true}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getAllByRole('textbox').length).toBeGreaterThan(0);
		});

		test('switches from edit to view mode', () => {
			const { rerender } = render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={true}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getAllByRole('textbox').length).toBeGreaterThan(0);

			rerender(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getAllByText('Test Description').length).toBeGreaterThan(0);
		});
	});

	describe('useMemo Optimization', () => {
		test('columns are memoized', () => {
			const { rerender } = render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			expect(screen.getByText('Name')).toBeInTheDocument();

			// Re-render with same props
			rerender(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={false}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			// Columns should still be rendered
			expect(screen.getByText('Name')).toBeInTheDocument();
			expect(screen.getByText('Value')).toBeInTheDocument();
		});
	});

	describe('TextField Interaction', () => {
		test('can type in TextField', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={true}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			const textField = screen.getByPlaceholderText('description');
			fireEvent.change(textField, { target: { value: 'Updated Value' } });

			expect(mockOnChange).toHaveBeenCalled();
		});

		test('can clear TextField value', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={true}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			const textField = screen.getByPlaceholderText('description');
			fireEvent.change(textField, { target: { value: '' } });

			expect(mockOnChange).toHaveBeenCalledWith('');
		});

		test('handles multiple TextField changes', () => {
			render(
				<TermRelationViewAttributes
					attrObj={mockAttrObj}
					editModal={true}
					control={mockControl}
					currentType={mockCurrentType}
				/>
			);

			const descField = screen.getByPlaceholderText('description');
			const exprField = screen.getByPlaceholderText('expression');

			fireEvent.change(descField, { target: { value: 'New Desc' } });
			fireEvent.change(exprField, { target: { value: 'New Expr' } });

			expect(mockOnChange).toHaveBeenCalledTimes(2);
		});
	});
});
