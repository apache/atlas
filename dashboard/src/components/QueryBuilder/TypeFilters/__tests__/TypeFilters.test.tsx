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
 * Unit tests for TypeFilters component
 * 
 * Coverage Target: 100%
 */

import React from 'react'
import { render, screen } from '@testing-library/react'
import TypeFilters from '../TypeFilters'

// Mock react-router-dom - hoist mock function
const mockUseLocation = jest.fn(() => ({ search: '?type=DataSet' }))
jest.mock('react-router-dom', () => ({
	useLocation: () => mockUseLocation()
}))

jest.mock('react-querybuilder', () => ({
	__esModule: true,
	default: ({ fields, query, onQueryChange }: any) => (
		<div data-testid="query-builder">
			<div data-testid="fields-count">{fields?.length || 0}</div>
			<button onClick={() => onQueryChange({ combinator: 'and', rules: [] })}>
				Change Query
			</button>
		</div>
	),
	defaultValidator: jest.fn()
}))

jest.mock('@components/muiComponents', () => ({
	Accordion: ({ children, defaultExpanded }: any) => (
		<div data-testid="accordion" data-expanded={defaultExpanded}>
			{children}
		</div>
	),
	AccordionSummary: ({ children }: any) => <div>{children}</div>,
	AccordionDetails: ({ children }: any) => <div>{children}</div>,
	Typography: ({ children, className, fontSize, fontWeight }: any) => (
		<span className={className} style={{ fontSize, fontWeight }}>
			{children}
		</span>
	)
}))

jest.mock('@mui/icons-material/AddOutlined', () => ({
	__esModule: true,
	default: () => <span data-testid="add-icon">Add</span>
}))

jest.mock('../TypeCustomValueEditor', () => ({
	TypeCustomValueEditor: () => <div data-testid="custom-value-editor" />
}))

describe('TypeFilters', () => {
	const mockFieldsObj = [
		{
			label: 'Group 1',
			options: [
				{ name: 'field1', label: 'Field 1' },
				{ name: 'field2', label: 'Field 2' }
			]
		}
	]

	const mockTypeQuery = { combinator: 'and', rules: [] }
	const mockSetTypeQuery = jest.fn()

	beforeEach(() => {
		jest.clearAllMocks()
		// Reset location mock to default
		mockUseLocation.mockReturnValue({ search: '?type=DataSet' })
	})

	it('renders accordion with type parameter', () => {
		render(
			<TypeFilters
				fieldsObj={mockFieldsObj}
				typeQuery={mockTypeQuery}
				setTypeQuery={mockSetTypeQuery}
			/>
		)

		expect(screen.getByText(/Type:.*DataSet/)).toBeTruthy()
	})

	it('renders QueryBuilder with fields', () => {
		render(
			<TypeFilters
				fieldsObj={mockFieldsObj}
				typeQuery={mockTypeQuery}
				setTypeQuery={mockSetTypeQuery}
			/>
		)

		expect(screen.getByTestId('query-builder')).toBeTruthy()
		expect(screen.getByTestId('fields-count').textContent).toBe('1')
	})

	it('calls setTypeQuery when query changes', () => {
		render(
			<TypeFilters
				fieldsObj={mockFieldsObj}
				typeQuery={mockTypeQuery}
				setTypeQuery={mockSetTypeQuery}
			/>
		)

		const changeButton = screen.getByText('Change Query')
		changeButton.click()

		expect(mockSetTypeQuery).toHaveBeenCalledWith({
			combinator: 'and',
			rules: []
		})
	})

	it('renders with defaultExpanded accordion', () => {
		const { container } = render(
			<TypeFilters
				fieldsObj={mockFieldsObj}
				typeQuery={mockTypeQuery}
				setTypeQuery={mockSetTypeQuery}
			/>
		)

		const accordion = container.querySelector('[data-testid="accordion"]')
		expect(accordion?.getAttribute('data-expanded')).toBe('true')
	})

	it('handles empty fieldsObj', () => {
		render(
			<TypeFilters
				fieldsObj={[]}
				typeQuery={mockTypeQuery}
				setTypeQuery={mockSetTypeQuery}
			/>
		)

		expect(screen.getByTestId('query-builder')).toBeTruthy()
		expect(screen.getByTestId('fields-count').textContent).toBe('0')
	})

	it('handles missing type parameter', () => {
		mockUseLocation.mockReturnValueOnce({
			search: ''
		})

		render(
			<TypeFilters
				fieldsObj={mockFieldsObj}
				typeQuery={mockTypeQuery}
				setTypeQuery={mockSetTypeQuery}
			/>
		)

		// When typeParams is null, it renders as "Type: " (empty string), not "Type: null"
		expect(screen.getByText(/Type:/)).toBeTruthy()
	})

	it('renders with custom type parameter', () => {
		mockUseLocation.mockReturnValueOnce({
			search: '?type=CustomType'
		})

		render(
			<TypeFilters
				fieldsObj={mockFieldsObj}
				typeQuery={mockTypeQuery}
				setTypeQuery={mockSetTypeQuery}
			/>
		)

		expect(screen.getByText(/Type:.*CustomType/)).toBeTruthy()
	})
})
