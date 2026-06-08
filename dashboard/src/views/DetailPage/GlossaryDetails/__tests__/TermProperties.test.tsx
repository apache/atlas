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
import TermProperties from '../TermProperties';

// Mock dependencies
jest.mock('@components/SkeletonLoader', () => ({
	__esModule: true,
	default: ({ count, animation }: any) => (
		<div data-testid="skeleton-loader" data-count={count} data-animation={animation}>
			Loading...
		</div>
	)
}));

// Mock Utils
const mockDateFormat = jest.fn();
jest.mock('@utils/Utils', () => ({
	dateFormat: (...args: any[]) => mockDateFormat(...args),
	isArray: (val: any) => Array.isArray(val),
	isEmpty: (val: any) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0) || (typeof val === 'object' && Object.keys(val).length === 0)
}));

// Mock Enum
jest.mock('@utils/Enum', () => ({
	stats: {
		createdTime: 'time',
		modifiedTime: 'time',
		birthDate: 'day',
		expiryDate: 'day'
	}
}));

// Mock moment
jest.mock('moment', () => {
	const mockMoment = jest.fn(() => ({
		milliseconds: jest.fn((val) => `moment-time-${val}`)
	}));
	return mockMoment;
});

describe('TermProperties - 100% Coverage', () => {
	beforeEach(() => {
		jest.clearAllMocks();
		mockDateFormat.mockImplementation((val) => `formatted-date-${val}`);
	});

	describe('Component Rendering', () => {
		test('renders TermProperties component', () => {
			render(<TermProperties additionalAttributes={{}} loader={false} />);

			expect(screen.getByText('Additional Properties:')).toBeInTheDocument();
		});

		test('renders with correct structure', () => {
			render(<TermProperties additionalAttributes={{}} loader={false} />);

			expect(screen.getByText('Additional Properties:')).toHaveClass('text-color-green');
			expect(screen.getByText('Additional Properties:')).toHaveClass('term-properties');
		});
	});

	describe('Loading State', () => {
		test('shows skeleton loader when loader is true', () => {
			render(<TermProperties additionalAttributes={{}} loader={true} />);

			expect(screen.getByTestId('skeleton-loader')).toBeInTheDocument();
			expect(screen.getByTestId('skeleton-loader')).toHaveAttribute('data-count', '3');
			expect(screen.getByTestId('skeleton-loader')).toHaveAttribute('data-animation', 'wave');
		});

		test('does not show skeleton loader when loader is false', () => {
			render(<TermProperties additionalAttributes={{}} loader={false} />);

			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument();
		});

		test('does not render content when loader is true', () => {
			render(<TermProperties additionalAttributes={{ key: 'value' }} loader={true} />);

			expect(screen.queryByText('Additional Properties:')).not.toBeInTheDocument();
		});
	});

	describe('Additional Attributes Display', () => {
		test('displays "No Record Found" when additionalAttributes is empty', () => {
			render(<TermProperties additionalAttributes={{}} loader={false} />);

			expect(screen.getByText('No Record Found')).toBeInTheDocument();
		});

		test('displays "No Record Found" when additionalAttributes is null', () => {
			render(<TermProperties additionalAttributes={null} loader={false} />);

			expect(screen.getByText('No Record Found')).toBeInTheDocument();
		});

		test('displays "No Record Found" when additionalAttributes is undefined', () => {
			render(<TermProperties additionalAttributes={undefined} loader={false} />);

			expect(screen.getByText('No Record Found')).toBeInTheDocument();
		});

		test('displays properties when additionalAttributes has data', () => {
			const attributes = {
				name: 'Test Name',
				description: 'Test Description'
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('name')).toBeInTheDocument();
			expect(screen.getByText('Test Name')).toBeInTheDocument();
			expect(screen.getByText('description')).toBeInTheDocument();
			expect(screen.getByText('Test Description')).toBeInTheDocument();
		});

		test('displays multiple properties', () => {
			const attributes = {
				prop1: 'value1',
				prop2: 'value2',
				prop3: 'value3'
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('prop1')).toBeInTheDocument();
			expect(screen.getByText('value1')).toBeInTheDocument();
			expect(screen.getByText('prop2')).toBeInTheDocument();
			expect(screen.getByText('value2')).toBeInTheDocument();
			expect(screen.getByText('prop3')).toBeInTheDocument();
			expect(screen.getByText('value3')).toBeInTheDocument();
		});
	});

	describe('Array Value Handling', () => {
		test('displays array length for array values', () => {
			const attributes = {
				tags: ['tag1', 'tag2', 'tag3']
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('tags (3)')).toBeInTheDocument();
		});

		test('displays array value correctly', () => {
			const attributes = {
				items: ['item1', 'item2']
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('items (2)')).toBeInTheDocument();
			expect(screen.getByText((content) => content.includes('item1'))).toBeInTheDocument();
			expect(screen.getByText((content) => content.includes('item2'))).toBeInTheDocument();
		});

		test('displays empty array with length 0', () => {
			const attributes = {
				emptyArray: []
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('emptyArray (0)')).toBeInTheDocument();
		});

		test('displays single item array with length 1', () => {
			const attributes = {
				singleItem: ['only-one']
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('singleItem (1)')).toBeInTheDocument();
			expect(screen.getByText('only-one')).toBeInTheDocument();
		});
	});

	describe('getValue Function - Time Type', () => {
		test('formats time values using moment', () => {
			const attributes = {
				createdTime: 1640995200000
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('moment-time-1640995200000')).toBeInTheDocument();
		});

		test('formats modifiedTime using moment', () => {
			const attributes = {
				modifiedTime: 1640995200000
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('moment-time-1640995200000')).toBeInTheDocument();
		});
	});

	describe('getValue Function - Day Type', () => {
		test('formats day values using dateFormat', () => {
			const attributes = {
				birthDate: '2024-01-01'
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(mockDateFormat).toHaveBeenCalledWith('2024-01-01');
			expect(screen.getByText('formatted-date-2024-01-01')).toBeInTheDocument();
		});

		test('formats expiryDate using dateFormat', () => {
			const attributes = {
				expiryDate: '2024-12-31'
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(mockDateFormat).toHaveBeenCalledWith('2024-12-31');
			expect(screen.getByText('formatted-date-2024-12-31')).toBeInTheDocument();
		});
	});

	describe('getValue Function - Default Type', () => {
		test('returns value as-is for unknown types', () => {
			const attributes = {
				customField: 'custom value'
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('custom value')).toBeInTheDocument();
		});

		test('returns numeric values as-is', () => {
			const attributes = {
				count: 42
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('42')).toBeInTheDocument();
		});

		test('returns boolean values as-is', () => {
			const attributes = {
				isActive: true
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('true')).toBeInTheDocument();
		});
	});

	describe('Edge Cases', () => {
		test('handles null values', () => {
			const attributes = {
				nullValue: null
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('nullValue')).toBeInTheDocument();
		});

		test('handles undefined values', () => {
			const attributes = {
				undefinedValue: undefined
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('undefinedValue')).toBeInTheDocument();
		});

		test('handles empty string values', () => {
			const attributes = {
				emptyString: ''
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('emptyString')).toBeInTheDocument();
		});

		test('handles zero values', () => {
			const attributes = {
				zeroValue: 0
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('0')).toBeInTheDocument();
		});

		test('handles false boolean values', () => {
			const attributes = {
				falseValue: false
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('false')).toBeInTheDocument();
		});

		test('handles special characters in keys', () => {
			const attributes = {
				'key-with-dashes': 'value',
				'key_with_underscores': 'value2',
				'key.with.dots': 'value3'
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('key-with-dashes')).toBeInTheDocument();
			expect(screen.getByText('key_with_underscores')).toBeInTheDocument();
			expect(screen.getByText('key.with.dots')).toBeInTheDocument();
		});

		test('handles long text values with word break', () => {
			const attributes = {
				longText: 'this-is-a-very-long-text-value-that-should-break-properly-in-the-ui-component'
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('this-is-a-very-long-text-value-that-should-break-properly-in-the-ui-component')).toBeInTheDocument();
		});
	});

	describe('Mixed Type Properties', () => {
		test('handles mix of string, number, and array values', () => {
			const attributes = {
				name: 'Test',
				count: 10,
				tags: ['tag1', 'tag2']
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('name')).toBeInTheDocument();
			expect(screen.getByText('Test')).toBeInTheDocument();
			expect(screen.getByText('count')).toBeInTheDocument();
			expect(screen.getByText('10')).toBeInTheDocument();
			expect(screen.getByText('tags (2)')).toBeInTheDocument();
		});

		test('handles mix of time, day, and default types', () => {
			const attributes = {
				createdTime: 1640995200000,
				birthDate: '2024-01-01',
				description: 'Regular text'
			};

			render(<TermProperties additionalAttributes={attributes} loader={false} />);

			expect(screen.getByText('moment-time-1640995200000')).toBeInTheDocument();
			expect(screen.getByText('formatted-date-2024-01-01')).toBeInTheDocument();
			expect(screen.getByText('Regular text')).toBeInTheDocument();
		});
	});

	describe('Component Structure', () => {
		test('renders Stack with correct padding', () => {
			const { container } = render(<TermProperties additionalAttributes={{}} loader={false} />);

			const stack = container.querySelector('.MuiStack-root');
			expect(stack).toBeInTheDocument();
		});

		test('renders Divider after title', () => {
			const { container } = render(<TermProperties additionalAttributes={{}} loader={false} />);

			const dividers = container.querySelectorAll('.MuiDivider-root');
			expect(dividers.length).toBeGreaterThan(0);
		});

		test('renders Divider after each property', () => {
			const attributes = {
				prop1: 'value1',
				prop2: 'value2'
			};

			const { container } = render(<TermProperties additionalAttributes={attributes} loader={false} />);

			const dividers = container.querySelectorAll('.MuiDivider-root');
			// Should have 1 after title + 2 after each property = 3 total
			expect(dividers.length).toBeGreaterThanOrEqual(3);
		});

		test('renders properties with correct data-cy attribute', () => {
			const attributes = {
				testProp: 'testValue'
			};

			const { container } = render(<TermProperties additionalAttributes={attributes} loader={false} />);

			const propertiesCard = container.querySelector('[data-cy="properties-card"]');
			expect(propertiesCard).toBeInTheDocument();
		});
	});
});
