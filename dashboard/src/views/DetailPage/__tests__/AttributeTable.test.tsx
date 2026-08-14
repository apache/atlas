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
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';
import AttributeTable from '../AttributeTable';

// Mock dependencies
const mockGetNestedSuperTypeObj = jest.fn();
const mockCustomSortBy = jest.fn();
const mockCloneDeep = jest.fn((obj) => JSON.parse(JSON.stringify(obj)));

jest.mock('@utils/Utils', () => ({
	isEmpty: (val: any) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0) || (typeof val === 'object' && Object.keys(val).length === 0),
	isNull: (val: any) => val === null,
	isObject: (val: any) => typeof val === 'object' && val !== null && !Array.isArray(val),
	isString: (val: any) => typeof val === 'string',
	getNestedSuperTypeObj: (...args: any[]) => mockGetNestedSuperTypeObj(...args),
	customSortBy: (...args: any[]) => mockCustomSortBy(...args)
}));

jest.mock('@utils/Helper', () => ({
	cloneDeep: (...args: any[]) => mockCloneDeep(...args)
}));

// Helper to create mock store
const createMockStore = (classificationData: any = {}) => {
	return configureStore({
		reducer: {
			classification: () => ({ classificationData })
		},
		middleware: (getDefaultMiddleware) =>
			getDefaultMiddleware({
				serializableCheck: false,
				immutableCheck: false
			})
	});
};

describe('AttributeTable - 100% Coverage', () => {
	const mockClassificationDefs = [
		{
			name: 'TestClassification',
			attributeDefs: [
				{ name: 'attr1', typeName: 'string' },
				{ name: 'attr2', typeName: 'int' }
			]
		}
	];

	beforeEach(() => {
		jest.clearAllMocks();
		mockCloneDeep.mockImplementation((obj) => JSON.parse(JSON.stringify(obj)));
	});

	describe('Component Rendering', () => {
		test('renders AttributeTable component with data', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {
					attr1: 'value1',
					attr2: 42
				}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([
				{ name: 'attr1', typeName: 'string' },
				{ name: 'attr2', typeName: 'int' }
			]);

			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('Name')).toBeInTheDocument();
			expect(screen.getByText('value')).toBeInTheDocument();
		});

		test('renders table structure correctly', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {
					attr1: 'test'
				}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([
				{ name: 'attr1', typeName: 'string' }
			]);

			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByRole('table')).toBeInTheDocument();
		});
	});

	describe('Data Processing', () => {
		test('clones classification data', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([]);
			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(mockCloneDeep).toHaveBeenCalled();
		});

		test('finds classification object by typeName', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([]);
			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(mockGetNestedSuperTypeObj).toHaveBeenCalled();
		});

		test('calls getNestedSuperTypeObj with correct parameters', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([]);
			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(mockGetNestedSuperTypeObj).toHaveBeenCalledWith({
				data: mockClassificationDefs[0],
				collection: mockClassificationDefs,
				attrMerge: true
			});
		});

		test('sorts attributes by sortKey', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {}
			};

			const mockAttrs = [
				{ name: 'zebra', typeName: 'string' },
				{ name: 'apple', typeName: 'string' }
			];

			mockGetNestedSuperTypeObj.mockReturnValue(mockAttrs);
			mockCustomSortBy.mockImplementation((arr) => [...arr].sort((a, b) => a.sortKey.localeCompare(b.sortKey)));

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(mockCustomSortBy).toHaveBeenCalled();
		});

		test('adds sortKey to attribute objects', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {}
			};

			const mockAttrs = [
				{ name: 'TestAttr', typeName: 'string' }
			];

			mockGetNestedSuperTypeObj.mockReturnValue(mockAttrs);
			mockCustomSortBy.mockImplementation((arr) => {
				expect(arr[0]).toHaveProperty('sortKey');
				expect(arr[0].sortKey).toBe('testattr');
				return arr;
			});

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);
		});

		test('handles attribute name that is not a string', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {}
			};

			const mockAttrs = [
				{ name: null, typeName: 'string' }
			];

			mockGetNestedSuperTypeObj.mockReturnValue(mockAttrs);
			mockCustomSortBy.mockImplementation((arr) => {
				expect(arr[0].sortKey).toBe('-');
				return arr;
			});

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);
		});
	});

	describe('getValues Function', () => {
		test('returns attribute value when present', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {
					attr1: 'test value'
				}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([
				{ name: 'attr1', typeName: 'string' }
			]);

			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('test value')).toBeInTheDocument();
		});

		test('returns "-" for null values', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {
					attr1: null
				}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([
				{ name: 'attr1', typeName: 'string' }
			]);

			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('-')).toBeInTheDocument();
		});

		test('converts boolean true to "true" string', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {
					attr1: true
				}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([
				{ name: 'attr1', typeName: 'boolean' }
			]);

			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('true')).toBeInTheDocument();
		});

		test('converts boolean false to "false" string', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {
					attr1: false
				}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([
				{ name: 'attr1', typeName: 'boolean' }
			]);

			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('false')).toBeInTheDocument();
		});

		test('stringifies object values', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {
					attr1: { key: 'value', nested: { data: 'test' } }
				}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([
				{ name: 'attr1', typeName: 'object' }
			]);

			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('{"key":"value","nested":{"data":"test"}}')).toBeInTheDocument();
		});

		test('handles missing attribute in attributes object', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([
				{ name: 'missingAttr', typeName: 'string' }
			]);

			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('-')).toBeInTheDocument();
		});
	});

	describe('Empty States', () => {
		test('renders "NA" when sortedObj is empty', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([]);
			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('NA')).toBeInTheDocument();
		});

		test('renders "NA" when getNestedSuperTypeObj returns null', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {}
			};

			mockGetNestedSuperTypeObj.mockReturnValue(null);
			mockCustomSortBy.mockImplementation((arr) => arr || []);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('NA')).toBeInTheDocument();
		});

		test('renders "NA" when typeName is empty', () => {
			const mockValues = {
				typeName: '',
				attributes: {}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([]);
			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('NA')).toBeInTheDocument();
		});

		test('renders "NA" when classification not found', () => {
			const mockValues = {
				typeName: 'NonExistentClassification',
				attributes: {}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([]);
			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('NA')).toBeInTheDocument();
		});
	});

	describe('Edge Cases', () => {
		test('handles null values prop', () => {
			mockGetNestedSuperTypeObj.mockReturnValue([]);
			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={null} />
				</Provider>
			);

			expect(screen.getByText('NA')).toBeInTheDocument();
		});

		test('handles undefined values prop', () => {
			mockGetNestedSuperTypeObj.mockReturnValue([]);
			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={undefined} />
				</Provider>
			);

			expect(screen.getByText('NA')).toBeInTheDocument();
		});

		test('handles empty classificationDefs', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([]);
			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: [] });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('NA')).toBeInTheDocument();
		});

		test('handles null classificationData', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([]);
			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore(null);

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('NA')).toBeInTheDocument();
		});
	});

	describe('Multiple Attributes', () => {
		test('renders multiple attribute rows', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {
					attr1: 'value1',
					attr2: 'value2',
					attr3: 'value3'
				}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([
				{ name: 'attr1', typeName: 'string' },
				{ name: 'attr2', typeName: 'string' },
				{ name: 'attr3', typeName: 'string' }
			]);

			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('attr1')).toBeInTheDocument();
			expect(screen.getByText('attr2')).toBeInTheDocument();
			expect(screen.getByText('attr3')).toBeInTheDocument();
			expect(screen.getByText('value1')).toBeInTheDocument();
			expect(screen.getByText('value2')).toBeInTheDocument();
			expect(screen.getByText('value3')).toBeInTheDocument();
		});

		test('renders mixed value types correctly', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: {
					stringAttr: 'text',
					boolAttr: true,
					nullAttr: null,
					objAttr: { key: 'val' }
				}
			};

			mockGetNestedSuperTypeObj.mockReturnValue([
				{ name: 'stringAttr', typeName: 'string' },
				{ name: 'boolAttr', typeName: 'boolean' },
				{ name: 'nullAttr', typeName: 'string' },
				{ name: 'objAttr', typeName: 'object' }
			]);

			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(screen.getByText('text')).toBeInTheDocument();
			expect(screen.getByText('true')).toBeInTheDocument();
			expect(screen.getByText('-')).toBeInTheDocument();
			expect(screen.getByText('{"key":"val"}')).toBeInTheDocument();
		});
	});

	describe('Table Structure', () => {
		test('renders table headers correctly', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: { attr1: 'value' }
			};

			mockGetNestedSuperTypeObj.mockReturnValue([
				{ name: 'attr1', typeName: 'string' }
			]);

			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			const headers = screen.getAllByRole('columnheader');
			expect(headers).toHaveLength(2);
		});

		test('renders dividers between columns', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: { attr1: 'value' }
			};

			mockGetNestedSuperTypeObj.mockReturnValue([
				{ name: 'attr1', typeName: 'string' }
			]);

			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			const { container } = render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			const dividers = container.querySelectorAll('.MuiDivider-root');
			expect(dividers.length).toBeGreaterThan(0);
		});

		test('applies correct CSS classes', () => {
			const mockValues = {
				typeName: 'TestClassification',
				attributes: { attr1: 'value' }
			};

			mockGetNestedSuperTypeObj.mockReturnValue([
				{ name: 'attr1', typeName: 'string' }
			]);

			mockCustomSortBy.mockImplementation((arr) => arr);

			const store = createMockStore({ classificationDefs: mockClassificationDefs });

			const { container } = render(
				<Provider store={store}>
					<AttributeTable values={mockValues} />
				</Provider>
			);

			expect(container.querySelector('.classification-table-container')).toBeInTheDocument();
			expect(container.querySelector('.classification-table-divider')).toBeInTheDocument();
		});
	});
});
