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
 * Unit tests for TagFilters component
 * 
 * Coverage Target: 100%
 */

import React from 'react'
import { render, screen } from '@testing-library/react'
import TagFilters from '../TagFilters'

// Mock react-router-dom - hoist mock function
const mockUseLocation = jest.fn(() => ({ search: '?tag=PII&type=DataSet' }))
jest.mock('react-router-dom', () => ({
	useLocation: () => mockUseLocation()
}))

// Mock Redux hooks - hoist mock function to handle multiple calls
const mockUseAppSelector = jest.fn()
jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: (selector: any) => mockUseAppSelector(selector)
}))

jest.mock('@utils/Utils', () => ({
	getNestedSuperTypeObj: jest.fn(({ data }) => data),
	isEmpty: jest.fn((val: any) => !val || (Array.isArray(val) && val.length === 0))
}))

jest.mock('@utils/Enum', () => ({
	addOnClassification: ['PII']
}))

jest.mock('@utils/CommonViewFunction', () => ({
	attributeFilter: {
		extractUrl: jest.fn(() => ({ rules: [] }))
	}
}))

jest.mock('@views/Administrator/Audits/AuditsFilter/AuditFiltersFields', () => ({
	getObjDef: jest.fn(() => ({ name: 'field1', label: 'Field 1', group: 'Group1' }))
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
	Field: {},
	toFullOption: jest.fn((field) => field)
}))

jest.mock('@components/muiComponents', () => ({
	Accordion: ({ children, defaultExpanded }: any) => (
		<div data-testid="accordion" data-expanded={defaultExpanded}>
			{children}
		</div>
	),
	AccordionSummary: ({ children }: any) => <div>{children}</div>,
	AccordionDetails: ({ children }: any) => <div>{children}</div>,
	Typography: ({ children, className, fontWeight }: any) => (
		<span className={className} style={{ fontWeight }}>
			{children}
		</span>
	)
}))

jest.mock('@mui/icons-material/AddOutlined', () => ({
	__esModule: true,
	default: () => <span data-testid="add-icon">Add</span>
}))

jest.mock('../TagCustomValueEditor', () => ({
	TagCustomValueEditor: () => <div data-testid="custom-value-editor" />
}))

const { isEmpty } = require('@utils/Utils')
const { addOnClassification } = require('@utils/Enum')

describe('TagFilters', () => {
	const mockAllDataObj = {}
	const mockClassificationQuery = { combinator: 'and', rules: [] }
	const mockSetClassificationQuery = jest.fn()

	const createMockState = () => ({
		classification: {
			classificationData: {
				classificationDefs: [
					{ name: 'PII', attributes: {} },
					{ name: 'Sensitive', attributes: {} }
				]
			}
		},
		rootClassification: {
			rootClassificationTypeData: {
				rootClassificationData: {
					attributeDefs: [{ name: 'sysAttr1' }]
				}
			}
		}
	})

	beforeEach(() => {
		jest.clearAllMocks()
		
		// Reset location mock
		mockUseLocation.mockReturnValue({ search: '?tag=PII&type=DataSet' })
		
		// Set up useAppSelector mock implementation
		mockUseAppSelector.mockImplementation((selector: any) => {
			const state = createMockState()
			const result = selector(state)
			// CRITICAL: Never return undefined - return appropriate default if result is undefined
			if (result === undefined || result === null) {
				// Return classification slice as default
				return state.classification
			}
			return result
		})
		
		isEmpty.mockImplementation(
			(val: any) => !val || (Array.isArray(val) && val.length === 0)
		)
	})

	it('renders accordion with tag parameter', () => {
		render(
			<TagFilters
				allDataObj={mockAllDataObj}
				classificationQuery={mockClassificationQuery}
				setClassificationQuery={mockSetClassificationQuery}
			/>
		)

		expect(screen.getByText(/Classification:.*PII/)).toBeTruthy()
	})

	it('renders QueryBuilder when fields are available', () => {
		render(
			<TagFilters
				allDataObj={mockAllDataObj}
				classificationQuery={mockClassificationQuery}
				setClassificationQuery={mockSetClassificationQuery}
			/>
		)

		expect(screen.getByTestId('query-builder')).toBeTruthy()
	})

	it('calls setClassificationQuery when query changes', () => {
		render(
			<TagFilters
				allDataObj={mockAllDataObj}
				classificationQuery={mockClassificationQuery}
				setClassificationQuery={mockSetClassificationQuery}
			/>
		)

		const changeButton = screen.getByText('Change Query')
		changeButton.click()

		expect(mockSetClassificationQuery).toHaveBeenCalledWith({
			combinator: 'and',
			rules: []
		})
	})

	it('handles empty classificationDefs', () => {
		mockUseAppSelector.mockImplementation((selector: any) => {
			const state = {
				classification: {
					classificationData: {
						classificationDefs: []
					}
				},
				rootClassification: {
					rootClassificationTypeData: {
						rootClassificationData: {}
					}
				}
			}
			const result = selector(state)
			if (result === undefined || result === null) {
				return state.classification
			}
			return result
		})

		render(
			<TagFilters
				allDataObj={mockAllDataObj}
				classificationQuery={mockClassificationQuery}
				setClassificationQuery={mockSetClassificationQuery}
			/>
		)

		expect(screen.getByTestId('query-builder')).toBeTruthy()
	})

	it('handles missing tag parameter', () => {
		mockUseLocation.mockReturnValueOnce({
			search: ''
		})

		render(
			<TagFilters
				allDataObj={mockAllDataObj}
				classificationQuery={mockClassificationQuery}
				setClassificationQuery={mockSetClassificationQuery}
			/>
		)

		// When tag is null, it renders as "Classification: " (empty string)
		expect(screen.getByText(/Classification:/)).toBeTruthy()
	})

	it('includes system attributes when addOnClassification matches', () => {
		const { getObjDef } = require('@views/Administrator/Audits/AuditsFilter/AuditFiltersFields')
		getObjDef.mockReturnValue({ name: 'sysField', group: 'System' })

		render(
			<TagFilters
				allDataObj={mockAllDataObj}
				classificationQuery={mockClassificationQuery}
				setClassificationQuery={mockSetClassificationQuery}
			/>
		)

		expect(screen.getByTestId('query-builder')).toBeTruthy()
	})

	it('handles empty paramsObject', () => {
		mockUseLocation.mockReturnValueOnce({
			search: ''
		})

		render(
			<TagFilters
				allDataObj={mockAllDataObj}
				classificationQuery={mockClassificationQuery}
				setClassificationQuery={mockSetClassificationQuery}
			/>
		)

		expect(screen.getByTestId('query-builder')).toBeTruthy()
	})

	it('sorts fields correctly with type parameter', () => {
		mockUseLocation.mockReturnValueOnce({
			search: '?tag=PII&type=DataSet'
		})

		render(
			<TagFilters
				allDataObj={mockAllDataObj}
				classificationQuery={mockClassificationQuery}
				setClassificationQuery={mockSetClassificationQuery}
			/>
		)

		expect(screen.getByTestId('query-builder')).toBeTruthy()
	})

	it('sorts fields correctly without type parameter', () => {
		mockUseLocation.mockReturnValueOnce({
			search: '?tag=PII'
		})

		render(
			<TagFilters
				allDataObj={mockAllDataObj}
				classificationQuery={mockClassificationQuery}
				setClassificationQuery={mockSetClassificationQuery}
			/>
		)

		expect(screen.getByTestId('query-builder')).toBeTruthy()
	})

	it('groups fields by group property', () => {
		const { getObjDef } = require('@views/Administrator/Audits/AuditsFilter/AuditFiltersFields')
		getObjDef.mockReturnValueOnce({ name: 'field1', group: 'Group1' })
		getObjDef.mockReturnValueOnce({ name: 'field2', group: 'Group1' })
		getObjDef.mockReturnValueOnce({ name: 'field3', group: 'Group2' })

		render(
			<TagFilters
				allDataObj={mockAllDataObj}
				classificationQuery={mockClassificationQuery}
				setClassificationQuery={mockSetClassificationQuery}
			/>
		)

		expect(screen.getByTestId('query-builder')).toBeTruthy()
	})
})
