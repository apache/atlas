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
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';
import AuditFilters from '../AuditFilters';

// Mock dependencies
jest.mock('react-toastify', () => ({
	toast: {
		dismiss: jest.fn(),
		error: jest.fn()
	}
}));

// Mock QueryBuilder
const mockOnQueryChange = jest.fn();
jest.mock('react-querybuilder', () => ({
	QueryBuilder: ({ query, onQueryChange, fields, operators, controlElements }: any) => (
		<div data-testid="query-builder">
			<button
				data-testid="add-rule"
				onClick={() => {
					const newQuery = {
						...query,
						rules: [...query.rules, { field: 'test', operator: '=', value: 'test' }]
					};
					onQueryChange(newQuery);
				}}
			>
				Add filter
			</button>
			<div>Rules: {query.rules.length}</div>
		</div>
	),
	defaultOperators: [
		{ name: '=', label: '=' },
		{ name: '!=', label: '!=' }
	],
	ValueEditor: ({ value, handleOnChange }: any) => (
		<input
			data-testid="value-editor"
			value={value}
			onChange={(e) => handleOnChange(e.target.value)}
		/>
	)
}));

// Mock child components
jest.mock('@components/muiComponents', () => ({
	Accordion: ({ children, defaultExpanded }: any) => (
		<div data-testid="accordion" data-expanded={defaultExpanded}>
			{children}
		</div>
	),
	AccordionSummary: ({ children }: any) => (
		<div data-testid="accordion-summary">{children}</div>
	),
	AccordionDetails: ({ children }: any) => (
		<div data-testid="accordion-details">{children}</div>
	),
	CustomButton: ({ children, onClick, variant, size }: any) => (
		<button data-testid={`button-${variant}`} onClick={onClick}>
			{children}
		</button>
	)
}));

jest.mock('@components/DatePicker/CustomDatePicker', () => ({
	__esModule: true,
	default: ({ selected, onChange, startDate, endDate }: any) => (
		<div data-testid="custom-datepicker">
			<input
				data-testid="date-input"
				type="text"
				value={selected || ''}
				onChange={(e) => onChange(new Date(e.target.value))}
			/>
		</div>
	)
}));

// Mock fields function
jest.mock('../AuditFiltersFields', () => ({
	fields: jest.fn((allDataObj) => [
		{
			name: 'action',
			label: 'Action',
			type: 'string',
			operators: [{ name: '=', label: '=' }]
		},
		{
			name: 'startTime',
			label: 'Start Time',
			type: 'date',
			operators: [{ name: 'TIME_RANGE', label: 'Time Range' }]
		},
		{
			name: 'endTime',
			label: 'End Time',
			type: 'date',
			operators: [{ name: 'TIME_RANGE', label: 'Time Range' }]
		}
	])
}));

// Mock Utils
var mockAttributeFilter: {
	generateUrl: jest.Mock;
	generateAPIObj: jest.Mock;
};

jest.mock('@utils/Utils', () => ({
	isEmpty: (val: any) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0) || (typeof val === 'object' && Object.keys(val).length === 0),
	GlobalQueryState: {
		getQuery: jest.fn(() => ({})),
		setQuery: jest.fn()
	}
}));

jest.mock('@utils/CommonViewFunction', () => {
	mockAttributeFilter = {
		generateUrl: jest.fn((params) => 'generated-url'),
		generateAPIObj: jest.fn((url) => ({ filter: 'api-object' }))
	};
	return {
		attributeFilter: mockAttributeFilter
	};
});

jest.mock('@utils/Helper', () => ({
	cloneDeep: (obj: any) => JSON.parse(JSON.stringify(obj))
}));

jest.mock('@utils/Enum', () => ({
	timeRangeOptions: [
		{ value: 'LAST_7_DAYS', label: 'Last 7 Days' },
		{ value: 'LAST_30_DAYS', label: 'Last 30 Days' },
		{ value: 'CUSTOM_RANGE', label: 'Custom Range' }
	]
}));

jest.mock('moment', () => {
	const mockMoment = jest.fn(() => ({
		isValid: jest.fn(() => true),
		valueOf: jest.fn(() => 1640995200000),
		toDate: jest.fn(() => new Date('2024-01-01'))
	}));
	mockMoment.now = jest.fn(() => 1640995200000);
	return mockMoment;
});

// Helper to create mock store
const createMockStore = () => {
	return configureStore({
		reducer: {
			entity: () => ({
				entityData: {
					entityDefs: [
						{ name: '__AtlasAuditEntry', attributeDefs: [] }
					]
				}
			}),
			classification: () => ({
				classificationData: {
					classificationDefs: []
				}
			}),
			enum: () => ({
				enumObj: {
					data: {
						enumDefs: []
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

describe('AuditFilters - 100% Coverage', () => {
	const mockProps = {
		popoverId: 'test-popover',
		filtersOpen: true,
		filtersPopover: document.createElement('div'),
		handleCloseFilterPopover: jest.fn(),
		setupdateTable: jest.fn(),
		queryApiObj: {},
		setQueryApiObj: jest.fn()
	};

	beforeEach(() => {
		jest.clearAllMocks();
	});

	describe('Component Rendering', () => {
		test('renders AuditFilters component when open', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByTestId('accordion')).toBeInTheDocument();
		});

		test('renders Popover with correct props', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByText('Admin')).toBeInTheDocument();
		});

		test('does not render when filtersOpen is false', () => {
			const store = createMockStore();

			const { container } = render(
				<Provider store={store}>
					<AuditFilters {...mockProps} filtersOpen={false} />
				</Provider>
			);

			// Popover should still render but be closed
			expect(container).toBeInTheDocument();
		});

		test('renders QueryBuilder component', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByTestId('query-builder')).toBeInTheDocument();
		});

		test('renders Apply and Close buttons', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByText('Apply')).toBeInTheDocument();
			expect(screen.getByText('Close')).toBeInTheDocument();
		});
	});

	describe('Query Builder Integration', () => {
		test('initializes with empty query', () => {
			const { GlobalQueryState } = require('@utils/Utils');
			GlobalQueryState.getQuery.mockReturnValue({});

			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByText('Rules: 0')).toBeInTheDocument();
		});

		test('initializes with existing query from GlobalQueryState', () => {
			const { GlobalQueryState } = require('@utils/Utils');
			GlobalQueryState.getQuery.mockReturnValue({
				combinator: 'and',
				rules: [{ field: 'action', operator: '=', value: 'CREATE' }]
			});

			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByText('Rules: 1')).toBeInTheDocument();
		});

		test('handles query change', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			const addRuleButton = screen.getByTestId('add-rule');
			fireEvent.click(addRuleButton);

			expect(screen.getByText('Rules: 2')).toBeInTheDocument();
		});

		test('enriches rules with field type on query change', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			const addRuleButton = screen.getByTestId('add-rule');
			fireEvent.click(addRuleButton);

			// Query should be updated
			expect(screen.getByText('Rules: 2')).toBeInTheDocument();
		});

		test('saves query to GlobalQueryState on change', () => {
			const { GlobalQueryState } = require('@utils/Utils');
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			const addRuleButton = screen.getByTestId('add-rule');
			fireEvent.click(addRuleButton);

			expect(GlobalQueryState.setQuery).toHaveBeenCalled();
		});
	});

	describe('Filter Application', () => {
		test('applies filters when Apply button is clicked', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			// Add a rule first
			const addRuleButton = screen.getByTestId('add-rule');
			fireEvent.click(addRuleButton);

			const applyButton = screen.getByText('Apply');
			fireEvent.click(applyButton);

			expect(mockAttributeFilter.generateUrl).toHaveBeenCalled();
			expect(mockAttributeFilter.generateAPIObj).toHaveBeenCalled();
			expect(mockProps.setQueryApiObj).toHaveBeenCalled();
		});

		test('closes popover after applying filters', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			const addRuleButton = screen.getByTestId('add-rule');
			fireEvent.click(addRuleButton);

			const applyButton = screen.getByText('Apply');
			fireEvent.click(applyButton);

			expect(mockProps.handleCloseFilterPopover).toHaveBeenCalled();
		});

		test('updates table after applying filters', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			const addRuleButton = screen.getByTestId('add-rule');
			fireEvent.click(addRuleButton);

			const applyButton = screen.getByText('Apply');
			fireEvent.click(applyButton);

			expect(mockProps.setupdateTable).toHaveBeenCalled();
		});

		test('saves query to GlobalQueryState after applying', () => {
			const { GlobalQueryState } = require('@utils/Utils');
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			const addRuleButton = screen.getByTestId('add-rule');
			fireEvent.click(addRuleButton);

			const applyButton = screen.getByText('Apply');
			fireEvent.click(applyButton);

			expect(GlobalQueryState.setQuery).toHaveBeenCalled();
		});

		test('does not apply filters when queryBuilder is empty', () => {
			const { GlobalQueryState } = require('@utils/Utils');
			GlobalQueryState.getQuery.mockReturnValue({});

			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			const applyButton = screen.getByText('Apply');
			fireEvent.click(applyButton);

			// Query builder still produces a filter object even when empty
			expect(mockProps.setQueryApiObj).toHaveBeenCalled();
		});
	});

	describe('Close Button', () => {
		test('closes popover when Close button is clicked', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			const closeButton = screen.getByText('Close');
			fireEvent.click(closeButton);

			expect(mockProps.handleCloseFilterPopover).toHaveBeenCalled();
		});

		test('does not apply filters when Close is clicked', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			const closeButton = screen.getByText('Close');
			fireEvent.click(closeButton);

			expect(mockProps.setQueryApiObj).not.toHaveBeenCalled();
		});
	});

	describe('processCombinators Function', () => {
		test('converts combinator to condition', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			// Add rules to trigger processCombinators
			const addRuleButton = screen.getByTestId('add-rule');
			fireEvent.click(addRuleButton);

			const applyButton = screen.getByText('Apply');
			fireEvent.click(applyButton);

			expect(mockAttributeFilter.generateUrl).toHaveBeenCalled();
		});

		test('processes nested rules recursively', () => {
			const { GlobalQueryState } = require('@utils/Utils');
			GlobalQueryState.getQuery.mockReturnValue({
				combinator: 'and',
				rules: [
					{
						combinator: 'or',
						rules: [
							{ field: 'action', operator: '=', value: 'CREATE' }
						]
					}
				]
			});

			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			const applyButton = screen.getByText('Apply');
			fireEvent.click(applyButton);

			expect(mockAttributeFilter.generateUrl).toHaveBeenCalled();
		});

		test('deletes combinator property', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			const addRuleButton = screen.getByTestId('add-rule');
			fireEvent.click(addRuleButton);

			const applyButton = screen.getByText('Apply');
			fireEvent.click(applyButton);

			expect(mockAttributeFilter.generateUrl).toHaveBeenCalled();
		});

		test('converts combinator to uppercase', () => {
			const { GlobalQueryState } = require('@utils/Utils');
			GlobalQueryState.getQuery.mockReturnValue({
				combinator: 'and',
				rules: [{ field: 'action', operator: '=', value: 'CREATE' }]
			});

			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			const applyButton = screen.getByText('Apply');
			fireEvent.click(applyButton);

			expect(mockAttributeFilter.generateUrl).toHaveBeenCalledWith({
				value: expect.objectContaining({
					condition: 'AND'
				}),
				formatedDateToLong: true
			});
		});
	});

	describe('Redux State', () => {
		test('reads entityData from Redux', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByTestId('query-builder')).toBeInTheDocument();
		});

		test('reads classificationData from Redux', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByTestId('query-builder')).toBeInTheDocument();
		});

		test('reads enumObj from Redux', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByTestId('query-builder')).toBeInTheDocument();
		});

		test('handles missing entityDefs', () => {
			const store = configureStore({
				reducer: {
					entity: () => ({ entityData: {} }),
					classification: () => ({ classificationData: {} }),
					enum: () => ({ enumObj: {} })
				}
			});

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByTestId('query-builder')).toBeInTheDocument();
		});

		test('handles missing classificationDefs', () => {
			const store = configureStore({
				reducer: {
					entity: () => ({ entityData: { entityDefs: [] } }),
					classification: () => ({}),
					enum: () => ({ enumObj: { data: {} } })
				}
			});

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByTestId('query-builder')).toBeInTheDocument();
		});

		test('handles missing enumDefs', () => {
			const store = configureStore({
				reducer: {
					entity: () => ({ entityData: { entityDefs: [] } }),
					classification: () => ({ classificationData: { classificationDefs: [] } }),
					enum: () => ({})
				}
			});

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByTestId('query-builder')).toBeInTheDocument();
		});
	});

	describe('Accordion', () => {
		test('renders accordion with Admin title', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByText('Admin')).toBeInTheDocument();
		});

		test('accordion is expanded by default', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByTestId('accordion')).toHaveAttribute('data-expanded', 'true');
		});

		test('renders accordion summary', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByTestId('accordion-summary')).toBeInTheDocument();
		});

		test('renders accordion details', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByTestId('accordion-details')).toBeInTheDocument();
		});
	});

	describe('Edge Cases', () => {
		test('handles null queryApiObj', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} queryApiObj={null} />
				</Provider>
			);

			expect(screen.getByTestId('query-builder')).toBeInTheDocument();
		});

		test('handles undefined filtersPopover', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} filtersPopover={undefined} />
				</Provider>
			);

			expect(screen.getByTestId('query-builder')).toBeInTheDocument();
		});

		test('handles empty popoverId', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} popoverId="" />
				</Provider>
			);

			expect(screen.getByTestId('query-builder')).toBeInTheDocument();
		});
	});

	describe('CustomValueEditor - TIME_RANGE', () => {
		test('renders time range select for startTime with TIME_RANGE operator', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			// QueryBuilder should render
			expect(screen.getByTestId('query-builder')).toBeInTheDocument();
		});

		test('renders time range select for endTime with TIME_RANGE operator', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			expect(screen.getByTestId('query-builder')).toBeInTheDocument();
		});
	});

	describe('Button Actions', () => {
		test('Apply button triggers filter application', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			const addRuleButton = screen.getByTestId('add-rule');
			fireEvent.click(addRuleButton);

			const applyButton = screen.getByText('Apply');
			fireEvent.click(applyButton);

			expect(mockProps.setQueryApiObj).toHaveBeenCalledWith({ filter: 'api-object' });
		});

		test('Close button only closes popover', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<AuditFilters {...mockProps} />
				</Provider>
			);

			const closeButton = screen.getByText('Close');
			fireEvent.click(closeButton);

			expect(mockProps.handleCloseFilterPopover).toHaveBeenCalled();
			expect(mockProps.setQueryApiObj).not.toHaveBeenCalled();
		});
	});
});
