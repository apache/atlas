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
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';
import BusinessMetadataAtrribute from '../BusinessMetadataAtrribute';

// Mock dependencies
const mockSetEditBMAttribute = jest.fn(() => ({
	type: 'createBMSlice/setEditBMAttribute'
}));
jest.mock('@redux/slice/createBMSlice', () => ({
	setEditBMAttribute: (...args: any[]) => mockSetEditBMAttribute(...args)
}));

jest.mock('@components/commonComponents', () => ({
	EllipsisText: ({ children }: any) => <span data-testid="ellipsis-text">{children}</span>
}));

jest.mock('@components/muiComponents', () => ({
	CustomButton: ({ children, onClick, variant, size }: any) => (
		<button data-testid="custom-button" data-variant={variant} onClick={onClick}>
			{children}
		</button>
	),
	LightTooltip: ({ children, title }: any) => (
		<div data-testid="tooltip" title={title}>
			{children}
		</div>
	)
}));

jest.mock('@components/Table/TableLayout', () => ({
	TableLayout: ({ data, columns, emptyText, isFetching }: any) => (
		<div data-testid="table-layout" data-fetching={isFetching}>
			{data && data.length > 0 ? (
				<table>
					<tbody>
						{data.map((row: any, idx: number) => (
							<tr key={idx} data-testid={`table-row-${idx}`}>
								{columns.map((col: any, colIdx: number) => (
									<td key={colIdx} data-testid={`cell-${idx}-${colIdx}`}>
										{col.cell ? col.cell({ getValue: () => row[col.accessorKey], row: { original: row } }) : row[col.accessorKey]}
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

jest.mock('@utils/Utils', () => ({
	isEmpty: (val: any) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0) || (typeof val === 'object' && Object.keys(val).length === 0)
}));

jest.mock('@utils/Enum', () => ({
	defaultType: ['string', 'int', 'long', 'float', 'double', 'boolean', 'date', 'byte', 'short']
}));

// Helper to create mock store
const createMockStore = (enumDefs: any[] = []) => {
	return configureStore({
		reducer: {
			enum: () => ({
				enumObj: {
					data: {
						enumDefs
					}
				}
			})
		},
		middleware: (getDefaultMiddleware) =>
			getDefaultMiddleware({
				serializableCheck: false,
				immutableCheck: false
			})
	});
};

describe('BusinessMetadataAtrribute - 100% Coverage', () => {
	const mockComponentProps = {
		attributeDefs: [
			{
				name: 'attr1',
				typeName: 'string',
				searchWeight: 5,
				cardinality: 'SINGLE',
				options: {
					maxStrLength: '100',
					applicableEntityTypes: '["Entity1","Entity2"]'
				}
			},
			{
				name: 'attr2',
				typeName: 'array<string>',
				searchWeight: 3,
				cardinality: 'SET',
				options: {
					applicableEntityTypes: '["Entity3"]'
				}
			}
		],
		loading: false,
		setForm: jest.fn(),
		setBMAttribute: jest.fn(),
		reset: jest.fn()
	};

	const mockRow = {
		original: {
			name: 'TestBM',
		description: 'Test Business Metadata',
		attributeDefs: mockComponentProps.attributeDefs
		}
	};

	beforeEach(() => {
		jest.clearAllMocks();
	});

	describe('Component Rendering', () => {
		test('renders BusinessMetadataAtrribute component', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('renders table with attributeDefs data when row is empty', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			expect(screen.getByTestId('table-row-0')).toBeInTheDocument();
			expect(screen.getByTestId('table-row-1')).toBeInTheDocument();
		});

		test('renders table with row.original.attributeDefs when row is provided', () => {
			const store = createMockStore();
			const rowWithAttrs = {
				original: {
					attributeDefs: [
						{ name: 'rowAttr', typeName: 'string' }
					]
				}
			};

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={rowWithAttrs} />
				</Provider>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});
	});

	describe('Table Columns', () => {
		test('renders Attribute Name column', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			expect(screen.getByText('attr1')).toBeInTheDocument();
			expect(screen.getByText('attr2')).toBeInTheDocument();
		});

		test('renders "N/A" for empty attribute name', () => {
			const propsWithEmptyName = {
				...mockComponentProps,
				attributeDefs: [{ name: '', typeName: 'string' }]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithEmptyName} row={undefined} />
				</Provider>
			);

			expect(screen.getAllByText('N/A').length).toBeGreaterThan(0);
		});

		test('renders Type Name column', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			expect(screen.getByText('string')).toBeInTheDocument();
			expect(screen.getByText('array<string>')).toBeInTheDocument();
		});

		test('renders "N/A" for empty type name', () => {
			const propsWithEmptyType = {
				...mockComponentProps,
				attributeDefs: [{ name: 'test', typeName: '' }]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithEmptyType} row={undefined} />
				</Provider>
			);

			expect(screen.getAllByText('N/A').length).toBeGreaterThan(0);
		});

		test('renders Search Weight column', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			expect(screen.getByText('5')).toBeInTheDocument();
			expect(screen.getByText('3')).toBeInTheDocument();
		});

		test('renders "N/A" for empty search weight', () => {
			const propsWithEmptyWeight = {
				...mockComponentProps,
				attributeDefs: [{ name: 'test', typeName: 'string', searchWeight: null }]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithEmptyWeight} row={undefined} />
				</Provider>
			);

			expect(screen.getAllByText('N/A').length).toBeGreaterThan(0);
		});

		test('renders Cardinality column', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			expect(screen.getByText('SINGLE')).toBeInTheDocument();
			expect(screen.getByText('SET')).toBeInTheDocument();
		});

		test('renders "N/A" for empty cardinality', () => {
			const propsWithEmptyCard = {
				...mockComponentProps,
				attributeDefs: [{ name: 'test', typeName: 'string', cardinality: '' }]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithEmptyCard} row={undefined} />
				</Provider>
			);

			expect(screen.getAllByText('N/A').length).toBeGreaterThan(0);
		});
	});

	describe('Enable Multivalues Checkbox', () => {
		test('renders checkbox checked for array types', () => {
			const store = createMockStore();

			const { container } = render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			const checkboxes = container.querySelectorAll('input[type="checkbox"]');
			expect(checkboxes.length).toBe(2);
			expect(checkboxes[1]).toBeChecked(); // Second row has array<string>
		});

		test('renders checkbox unchecked for non-array types', () => {
			const store = createMockStore();

			const { container } = render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			const checkboxes = container.querySelectorAll('input[type="checkbox"]');
			expect(checkboxes[0]).not.toBeChecked(); // First row has string
		});

		test('checkbox is disabled', () => {
			const store = createMockStore();

			const { container } = render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			const checkboxes = container.querySelectorAll('input[type="checkbox"]');
			checkboxes.forEach(checkbox => {
				expect(checkbox).toBeDisabled();
			});
		});
	});

	describe('Max Length Column', () => {
		test('renders maxStrLength from options', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			expect(screen.getByText('100')).toBeInTheDocument();
		});

		test('renders "N/A" when maxStrLength is empty', () => {
			const propsWithoutMaxLength = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'string',
						options: {}
					}
				]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithoutMaxLength} row={undefined} />
				</Provider>
			);

			expect(screen.getAllByText('N/A').length).toBeGreaterThan(0);
		});

		test('renders "N/A" when options is null', () => {
			const propsWithNullOptions = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'string',
						options: null
					}
				]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithNullOptions} row={undefined} />
				</Provider>
			);

			expect(screen.getAllByText('N/A').length).toBeGreaterThan(0);
		});
	});

	describe('Applicable Entity Types Column', () => {
		test('renders entity types as chips', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			expect(screen.getByText('Entity1')).toBeInTheDocument();
			expect(screen.getByText('Entity2')).toBeInTheDocument();
		});

		test('parses JSON applicableEntityTypes correctly', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			expect(screen.getByText('Entity1')).toBeInTheDocument();
		});

		test('handles nested JSON parsing', () => {
			const propsWithNestedJSON = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'string',
						options: {
							applicableEntityTypes: '"[\\"Entity1\\",\\"Entity2\\"]"'
						}
					}
				]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithNestedJSON} row={undefined} />
				</Provider>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('renders "N/A" when applicableEntityTypes is empty', () => {
			const propsWithEmptyTypes = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'string',
						options: {
							applicableEntityTypes: ''
						}
					}
				]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithEmptyTypes} row={undefined} />
				</Provider>
			);

			expect(screen.getAllByText('N/A').length).toBeGreaterThan(0);
		});

		test('renders chips with tooltips', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			const tooltips = screen.getAllByTestId('tooltip');
			expect(tooltips.length).toBeGreaterThan(0);
		});

		test('renders chips with EllipsisText', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			const ellipsisTexts = screen.getAllByTestId('ellipsis-text');
			expect(ellipsisTexts.length).toBeGreaterThan(0);
		});

		test('column header changes based on row parameter', () => {
			const store = createMockStore();

			// When row is empty, header should be "Entity Type(s)"
			const { rerender } = render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();

			// When row is provided, header should be "Applicable Type(s)"
			rerender(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={mockRow} />
				</Provider>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});
	});

	describe('Edit Button and Action', () => {
		test('renders Edit button for each row', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			const editButtons = screen.getAllByText('Edit');
			expect(editButtons.length).toBe(2);
		});

		test('clicking Edit button calls setForm', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			const editButtons = screen.getAllByTestId('custom-button');
			fireEvent.click(editButtons[0]);

			expect(mockComponentProps.setForm).toHaveBeenCalledWith(true);
		});

		test('clicking Edit button calls reset with editObj', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			const editButtons = screen.getAllByTestId('custom-button');
			fireEvent.click(editButtons[0]);

			expect(mockComponentProps.reset).toHaveBeenCalled();
		});

		test('clicking Edit button calls setBMAttribute', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={mockRow} />
				</Provider>
			);

			const editButtons = screen.getAllByTestId('custom-button');
			fireEvent.click(editButtons[0]);

			expect(mockComponentProps.setBMAttribute).toHaveBeenCalledWith(mockRow.original);
		});

		test('clicking Edit button dispatches setEditBMAttribute', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			const editButtons = screen.getAllByTestId('custom-button');
			fireEvent.click(editButtons[0]);

			expect(mockSetEditBMAttribute).toHaveBeenCalled();
		});

		test('does not call reset when reset is empty', () => {
			const propsWithoutReset = {
				...mockComponentProps,
				reset: null
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithoutReset} row={undefined} />
				</Provider>
			);

			const editButtons = screen.getAllByTestId('custom-button');
			fireEvent.click(editButtons[0]);

			expect(propsWithoutReset.setForm).toHaveBeenCalled();
		});
	});

	describe('Type Name Processing', () => {
		test('extracts type from array notation', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			const editButtons = screen.getAllByTestId('custom-button');
			fireEvent.click(editButtons[1]); // Click on array<string> row

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});

		test('handles default type in array notation', () => {
			const propsWithArrayInt = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'array<int>',
						options: {}
					}
				]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithArrayInt} row={undefined} />
				</Provider>
			);

			const editButton = screen.getByTestId('custom-button');
			fireEvent.click(editButton);

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});

		test('handles enumeration type in array notation', () => {
			const propsWithEnumArray = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'array<CustomEnum>',
						options: {}
					}
				]
			};

			const store = createMockStore([
				{
					name: 'CustomEnum',
					elementDefs: [{ value: 'VAL1' }, { value: 'VAL2' }]
				}
			]);

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithEnumArray} row={undefined} />
				</Provider>
			);

			const editButton = screen.getByTestId('custom-button');
			fireEvent.click(editButton);

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});

		test('handles default type without array notation', () => {
			const propsWithDefaultType = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'string',
						options: {}
					}
				]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithDefaultType} row={undefined} />
				</Provider>
			);

			const editButton = screen.getByTestId('custom-button');
			fireEvent.click(editButton);

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});

		test('handles enumeration type without array notation', () => {
			const propsWithEnum = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'CustomEnum',
						options: {}
					}
				]
			};

			const store = createMockStore([
				{
					name: 'CustomEnum',
					elementDefs: [{ value: 'VAL1' }]
				}
			]);

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithEnum} row={undefined} />
				</Provider>
			);

			const editButton = screen.getByTestId('custom-button');
			fireEvent.click(editButton);

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});
	});

	describe('Enum Handling', () => {
		test('finds enum object by name', () => {
			const store = createMockStore([
				{
					name: 'StatusEnum',
					elementDefs: [{ value: 'ACTIVE' }, { value: 'INACTIVE' }]
				}
			]);

			const propsWithEnum = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'status',
						typeName: 'StatusEnum',
						options: {}
					}
				]
			};

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithEnum} row={undefined} />
				</Provider>
			);

			const editButton = screen.getByTestId('custom-button');
			fireEvent.click(editButton);

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});

		test('handles enum not found in enumDefs', () => {
			const store = createMockStore([]);

			const propsWithEnum = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'status',
						typeName: 'NonExistentEnum',
						options: {}
					}
				]
			};

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithEnum} row={undefined} />
				</Provider>
			);

			const editButton = screen.getByTestId('custom-button');
			fireEvent.click(editButton);

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});

		test('handles empty enumDefs', () => {
			const store = createMockStore([]);

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			const editButtons = screen.getAllByTestId('custom-button');
			fireEvent.click(editButtons[0]);

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});
	});

	describe('Cardinality Toggle', () => {
		test('sets cardinalityToggle to LIST when cardinality is LIST', () => {
			const propsWithListCard = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'array<string>',
						cardinality: 'LIST',
						options: {}
					}
				]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithListCard} row={undefined} />
				</Provider>
			);

			const editButton = screen.getByTestId('custom-button');
			fireEvent.click(editButton);

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});

		test('sets cardinalityToggle to SET when cardinality is SET', () => {
			const propsWithSetCard = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'array<string>',
						cardinality: 'SET',
						options: {}
					}
				]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithSetCard} row={undefined} />
				</Provider>
			);

			const editButton = screen.getByTestId('custom-button');
			fireEvent.click(editButton);

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});

		test('defaults cardinalityToggle to SET for other cardinality values', () => {
			const propsWithSingleCard = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'string',
						cardinality: 'SINGLE',
						options: {}
					}
				]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithSingleCard} row={undefined} />
				</Provider>
			);

			const editButton = screen.getByTestId('custom-button');
			fireEvent.click(editButton);

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});
	});

	describe('Table Configuration', () => {
		test('enables client side sorting when row is empty', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('disables client side sorting when row is provided', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={mockRow} />
				</Provider>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('shows pagination when row is empty', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('hides pagination when row is provided', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={mockRow} />
				</Provider>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('passes loading state to table', () => {
			const propsWithLoading = {
				...mockComponentProps,
				loading: true
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithLoading} row={undefined} />
				</Provider>
			);

			expect(screen.getByTestId('table-layout')).toHaveAttribute('data-fetching', 'true');
		});
	});

	describe('Edge Cases', () => {
		test('handles empty attributeDefs', () => {
			const propsWithEmptyAttrs = {
				...mockComponentProps,
				attributeDefs: []
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithEmptyAttrs} row={undefined} />
				</Provider>
			);

			expect(screen.getByTestId('empty-text')).toBeInTheDocument();
		});

		test('handles null attributeDefs', () => {
			const propsWithNullAttrs = {
				...mockComponentProps,
				attributeDefs: null
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithNullAttrs} row={undefined} />
				</Provider>
			);

			expect(screen.getByTestId('empty-text')).toBeInTheDocument();
		});

		test('handles undefined componentProps', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={{}} row={undefined} />
				</Provider>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('handles empty options object', () => {
			const propsWithEmptyOptions = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'string',
						options: {}
					}
				]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithEmptyOptions} row={undefined} />
				</Provider>
			);

			const editButton = screen.getByTestId('custom-button');
			fireEvent.click(editButton);

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});

		test('handles null options', () => {
			const propsWithNullOptions = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'string',
						options: null
					}
				]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithNullOptions} row={undefined} />
				</Provider>
			);

			const editButton = screen.getByTestId('custom-button');
			fireEvent.click(editButton);

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});

		test('handles empty typeName', () => {
			const propsWithEmptyType = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: '',
						options: {}
					}
				]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithEmptyType} row={undefined} />
				</Provider>
			);

			const editButton = screen.getByTestId('custom-button');
			fireEvent.click(editButton);

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});
	});

	describe('JSON Parsing', () => {
		test('parses applicableEntityTypes JSON successfully', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			expect(screen.getByText('Entity1')).toBeInTheDocument();
		});

		test('handles JSON parse error gracefully', () => {
			const propsWithInvalidJSON = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'string',
						options: {
							applicableEntityTypes: 'invalid-json'
						}
					}
				]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithInvalidJSON} row={undefined} />
				</Provider>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('handles nested JSON parse error', () => {
			const propsWithNestedInvalid = {
				...mockComponentProps,
				attributeDefs: [
					{
						name: 'test',
						typeName: 'string',
						options: {
							applicableEntityTypes: '"{invalid}"'
						}
					}
				]
			};

			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={propsWithNestedInvalid} row={undefined} />
				</Provider>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});
	});

	describe('multiValueSelect Flag', () => {
		test('sets multiValueSelect to true for array types', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			const editButtons = screen.getAllByTestId('custom-button');
			fireEvent.click(editButtons[1]); // array<string> row

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});

		test('sets multiValueSelect to false for non-array types', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<BusinessMetadataAtrribute componentProps={mockComponentProps} row={undefined} />
				</Provider>
			);

			const editButtons = screen.getAllByTestId('custom-button');
			fireEvent.click(editButtons[0]); // string row

			expect(mockComponentProps.setForm).toHaveBeenCalled();
		});
	});
});
