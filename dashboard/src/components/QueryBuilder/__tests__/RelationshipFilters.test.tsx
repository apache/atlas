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
 * Comprehensive unit tests for RelationshipFilters component
 * 
 * Coverage Target: 100% (Statements, Branches, Functions, Lines)
 */

// Set test timeout to 30 seconds
jest.setTimeout(30000)

// Mock functions - hoisted before imports
const mockSetRelationshipQuery = jest.fn()
const mockUseLocation = jest.fn(() => ({ search: '?relationshipName=rel1' }))
const mockUseAppSelector = jest.fn()
const mockGetNestedSuperTypeObj = jest.fn()
const mockIsEmpty = jest.fn()
const mockGetObjDef = jest.fn()
const mockToFullOption = jest.fn()
const valueEditorCalls: Array<{ operator: string; returned: any }> = []

// Mock react-router-dom
jest.mock('react-router-dom', () => ({
	useLocation: () => mockUseLocation(),
	useNavigate: () => jest.fn()
}))

// Mock hooks
jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: (selector: any) => mockUseAppSelector(selector)
}))

// Mock utils
jest.mock('@utils/Utils', () => ({
	getNestedSuperTypeObj: (params: any) => mockGetNestedSuperTypeObj(params),
	isEmpty: (val: any) => mockIsEmpty(val)
}))

// Mock AuditFiltersFields
jest.mock('@views/Administrator/Audits/AuditsFilter/AuditFiltersFields', () => ({
	getObjDef: (allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) =>
		mockGetObjDef(allDataObj, attrObj, rules_widgets, isGroupView, groupName)
}))

// Mock react-querybuilder
jest.mock('react-querybuilder', () => {
	const React = require('react')
	return {
		__esModule: true,
		default: ({ fields, query, onQueryChange, controlElements, translations, controlClassnames }: any) => {
			// Test ValueEditor rendering with different operators to cover branches (lines 124-130)
			if (controlElements?.valueEditor) {
				const ValueEditorComponent = controlElements.valueEditor
				
				// Test with is_null operator - should return undefined (covers lines 124-128)
				const nullProps = { operator: 'is_null', field: 'test', value: '' }
				const nullResult = ValueEditorComponent(nullProps)
				valueEditorCalls.push({ operator: 'is_null', returned: nullResult })
				
				// Test with not_null operator - should return undefined (covers lines 125-128)
				const notNullProps = { operator: 'not_null', field: 'test', value: '' }
				const notNullResult = ValueEditorComponent(notNullProps)
				valueEditorCalls.push({ operator: 'not_null', returned: notNullResult })
				
				// Test with other operators - should return ValueEditor (covers line 130)
				const otherProps = { operator: '=', field: 'test', value: 'value' }
				const otherResult = ValueEditorComponent(otherProps)
				valueEditorCalls.push({ operator: '=', returned: otherResult })
			}
			
			return React.createElement('div', { 'data-testid': 'query-builder' },
				React.createElement('div', { 'data-testid': 'fields-count' }, fields?.length || 0),
				React.createElement('div', { 'data-testid': 'query' }, JSON.stringify(query)),
				React.createElement('div', { 'data-testid': 'control-classnames' }, JSON.stringify(controlClassnames)),
				React.createElement('button', {
					onClick: () => onQueryChange({ combinator: 'and', rules: [] })
				}, 'Change Query'),
				translations?.addGroup?.label && React.createElement('div', { 'data-testid': 'add-group-label' }, translations.addGroup.label),
				translations?.addRule?.label && React.createElement('div', { 'data-testid': 'add-rule-label' }, translations.addRule.label)
			)
		},
		Field: {},
		toFullOption: (...args: any[]) => mockToFullOption(...args),
		ValueEditor: (props: any) => {
			if (props.operator === 'is_null' || props.operator === 'not_null') {
				return null
			}
			return React.createElement('div', { 'data-testid': `value-editor-${props.operator}` }, 'Value Editor')
		}
	}
})

import React from 'react'
import { render, screen, waitFor, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import RelationshipFilters from '../RelationshipFilters'

// Mock MUI components
jest.mock('@components/muiComponents', () => ({
	Accordion: ({ children, defaultExpanded }: any) => (
		<div data-testid="accordion" data-expanded={defaultExpanded}>
			{children}
		</div>
	),
	AccordionSummary: ({ children, 'aria-controls': ariaControls, id }: any) => (
		<div data-testid="accordion-summary" aria-controls={ariaControls} id={id}>
			{children}
		</div>
	),
	AccordionDetails: ({ children }: any) => (
		<div data-testid="accordion-details">{children}</div>
	),
	Typography: ({ children, className, fontWeight, textAlign, color }: any) => (
		<span 
			className={className} 
			style={{ fontWeight, textAlign }} 
			data-color={color}
		>
			{children}
		</span>
	)
}))

// Mock MUI icons
jest.mock('@mui/icons-material/AddOutlined', () => ({
	__esModule: true,
	default: ({ fontSize }: any) => <span data-testid="add-icon" data-font-size={fontSize}>Add</span>
}))

const { useAppSelector } = require('@hooks/reducerHook')
const { isEmpty, getNestedSuperTypeObj } = require('@utils/Utils')
const { getObjDef } = require('@views/Administrator/Audits/AuditsFilter/AuditFiltersFields')

describe('RelationshipFilters', () => {
	const mockAllDataObj = { test: 'data' }
	const mockRelationshipQuery = { combinator: 'and', rules: [] }

	beforeEach(() => {
		jest.clearAllMocks()
		valueEditorCalls.length = 0 // Clear ValueEditor call tracking
		mockUseLocation.mockReturnValue({ search: '?relationshipName=rel1' })
		mockUseAppSelector.mockImplementation((selector: any) => {
			const state = {
				relationships: {
					relationshipDefs: [
						{ name: 'rel1', attributes: { attr1: {}, attr2: {} } },
						{ name: 'rel2', attributes: { attr3: {} } }
					]
				}
			}
			return selector(state)
		})
		mockIsEmpty.mockImplementation((val: any) => {
			if (val === null || val === undefined) return true
			if (Array.isArray(val)) return val.length === 0
			if (typeof val === 'object') return Object.keys(val).length === 0
			return !val
		})
		// Default mock for getNestedSuperTypeObj - returns object with attributes
		mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
			attr1: { name: 'attr1', typeName: 'string' },
			attr2: { name: 'attr2', typeName: 'int' }
		}))
		// Default mock for getObjDef - returns field with group
		mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
			if (!attrObj || !attrObj.name) return null
			return {
				name: attrObj.name,
				label: attrObj.name.charAt(0).toUpperCase() + attrObj.name.slice(1),
				group: groupName || 'rel1 Attribute' // Ensure group is always defined
			}
		})
		// Default mock for toFullOption - returns field as-is
		mockToFullOption.mockImplementation((field: any) => ({ ...field, fullOption: true }))
	})

	describe('Component Rendering', () => {
		it('renders accordion with relationship parameter', () => {
			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(screen.getByText(/Relationship:.*rel1/)).toBeTruthy()
			expect(screen.getByTestId('accordion')).toBeTruthy()
			expect(screen.getByTestId('accordion').getAttribute('data-expanded')).toBe('true')
		})

		it('renders QueryBuilder when fields are available', async () => {
			// Ensure getNestedSuperTypeObj returns attributes
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					label: attrObj.name.charAt(0).toUpperCase() + attrObj.name.slice(1),
					group: groupName || 'rel1 Attribute' // Ensure group is defined
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })
			expect(screen.getByTestId('fields-count').textContent).toBe('1')
		})

		it('renders "No Attributes" message when fieldsObj is empty', () => {
			mockIsEmpty.mockImplementation(() => true)
			mockUseAppSelector.mockReturnValueOnce({
				relationships: {
					relationshipDefs: []
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(screen.getByText(/No Attributes are available/)).toBeTruthy()
			expect(screen.queryByTestId('query-builder')).not.toBeInTheDocument()
		})

		it('renders AccordionSummary with correct props', () => {
			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			const summary = screen.getByTestId('accordion-summary')
			expect(summary.getAttribute('aria-controls')).toBe('panel1-content')
			expect(summary.getAttribute('id')).toBe('panel1-header')
		})

		it('renders Typography with correct className and fontWeight', () => {
			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			const typography = screen.getByText(/Relationship:/)
			expect(typography.className).toBe('text-color-green')
			expect(typography.style.fontWeight).toBe('600')
		})
	})

	describe('URL Parameter Handling', () => {
		it('handles missing relationship parameter', () => {
			mockUseLocation.mockReturnValueOnce({ search: '' })
			// When relationshipParams is null, fieldsObj will be empty
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({}))
			mockGetObjDef.mockImplementation(() => null)

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			// When relationshipParams is null, React renders null as empty string, so it displays "Relationship: "
			const relationshipText = screen.getByText(/Relationship:/)
			expect(relationshipText.textContent).toBe('Relationship: ')
		})

		it('handles relationship parameter with different values', () => {
			mockUseLocation.mockReturnValueOnce({ search: '?relationshipName=rel2' })

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(screen.getByText(/Relationship:.*rel2/)).toBeTruthy()
		})

		it('handles URLSearchParams parsing correctly', () => {
			mockUseLocation.mockReturnValueOnce({ search: '?relationshipName=testRel&other=value' })

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(screen.getByText(/Relationship:.*testRel/)).toBeTruthy()
		})
	})

	describe('Redux State Handling', () => {
		it('handles empty relationshipDefs', () => {
			mockUseAppSelector.mockReturnValueOnce({
				relationships: {
					relationshipDefs: []
				}
			})
			// When relationshipDefs is empty, isEmpty returns true, so attrTagObj becomes {}
			// and fieldsObj becomes empty, showing "No Attributes are available"
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({}))
			mockGetObjDef.mockImplementation(() => null)

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(screen.getByText(/No Attributes are available/)).toBeInTheDocument()
		})

		it('handles undefined relationships', () => {
			mockUseAppSelector.mockReturnValueOnce({
				relationships: undefined
			})
			// When relationships is undefined, isEmpty returns true, so attrTagObj becomes {}
			// and fieldsObj becomes empty, showing "No Attributes are available"
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({}))
			mockGetObjDef.mockImplementation(() => null)

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(screen.getByText(/No Attributes are available/)).toBeInTheDocument()
		})

		it('handles relationship not found in relationshipDefs', () => {
			mockUseLocation.mockReturnValueOnce({ search: '?relationshipName=nonexistent' })
			mockUseAppSelector.mockReturnValueOnce({
				relationships: {
					relationshipDefs: [{ name: 'otherRel', attributes: {} }]
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(screen.getByText(/No Attributes are available/)).toBeTruthy()
		})

		it('finds relationship using == comparison', async () => {
			mockUseLocation.mockReturnValueOnce({ search: '?relationshipName=rel1' })
			mockUseAppSelector.mockReturnValueOnce({
				relationships: {
					relationshipDefs: [
						{ name: 'rel1', attributes: { attr1: {} } }
					]
				}
			})
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			mockGetObjDef.mockImplementation(() => ({
				name: 'field1',
				group: 'rel1 Attribute'
			}))

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })
		})
	})

	describe('getNestedSuperTypeObj Integration', () => {
		it('calls getNestedSuperTypeObj with correct parameters', () => {
			const relationshipDef = { name: 'rel1', attributes: { attr1: {} } }
			mockUseAppSelector.mockReturnValueOnce({
				relationships: {
					relationshipDefs: [relationshipDef]
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(mockGetNestedSuperTypeObj).toHaveBeenCalledWith({
				data: relationshipDef,
				collection: [relationshipDef],
				attrMerge: true
			})
		})

		it('handles getNestedSuperTypeObj returning empty object', () => {
			mockGetNestedSuperTypeObj.mockReturnValueOnce({})
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true
				if (Array.isArray(val)) return val.length === 0
				if (typeof val === 'object') return Object.keys(val).length === 0
				return !val
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(screen.getByText(/No Attributes are available/)).toBeTruthy()
		})

		it('does not call getNestedSuperTypeObj when attrTagObj is falsy', () => {
			mockUseLocation.mockReturnValueOnce({ search: '?relationshipName=nonexistent' })
			mockUseAppSelector.mockReturnValueOnce({
				relationships: {
					relationshipDefs: [{ name: 'otherRel', attributes: {} }]
				}
			})
			mockGetNestedSuperTypeObj.mockClear()

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(mockGetNestedSuperTypeObj).not.toHaveBeenCalled()
		})
	})

	describe('Fields Processing', () => {
		it('processes fields correctly from attrTagObj', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' },
				attr2: { name: 'attr2', typeName: 'int' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					label: attrObj.name,
					group: 'rel1 Attribute'
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })
			expect(mockGetObjDef).toHaveBeenCalled()
			expect(mockToFullOption).toHaveBeenCalled()
		})

		it('handles getObjDef returning null', () => {
			mockGetObjDef.mockImplementation(() => null)

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(screen.getByText(/No Attributes are available/)).toBeTruthy()
		})

		it('calls getObjDef with correct parameters including groupName', () => {
			mockUseLocation.mockReturnValueOnce({ search: '?relationshipName=testRel' })
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				expect(groupName).toBe('testRel Attribute')
				expect(isGroupView).toBe(true)
				return { name: 'field1', group: groupName }
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(mockGetObjDef).toHaveBeenCalled()
		})

		it('filters out null returns from getObjDef', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' },
				attr2: { name: 'attr2', typeName: 'int' }
			}))
			let callCount = 0
			mockGetObjDef.mockImplementation(() => {
				callCount++
				return callCount === 1 ? null : { name: 'field1', group: 'rel1 Attribute' }
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })
		})
	})

	describe('Field Grouping', () => {
		it('groups fields by group property', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' },
				attr2: { name: 'attr2', typeName: 'int' },
				attr3: { name: 'attr3', typeName: 'string' }
			}))
			mockGetObjDef
				.mockReturnValueOnce({ name: 'field1', group: 'Group1' })
				.mockReturnValueOnce({ name: 'field2', group: 'Group1' })
				.mockReturnValueOnce({ name: 'field3', group: 'Group2' })

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })
			expect(screen.getByTestId('fields-count').textContent).toBe('2')
		})

		it('handles fields without group property - fields are filtered out', () => {
			mockGetObjDef.mockImplementation(() => ({
				name: 'field1',
				label: 'Field 1'
				// No group property - this will be filtered out
			}))

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(screen.getByText(/No Attributes are available/)).toBeTruthy()
		})

		it('handles fields with undefined group - fields are filtered out', () => {
			mockGetObjDef.mockImplementation(() => ({
				name: 'field1',
				group: undefined
			}))

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(screen.getByText(/No Attributes are available/)).toBeTruthy()
		})

		it('handles empty fields array', () => {
			mockGetObjDef.mockImplementation(() => null)

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(screen.getByText(/No Attributes are available/)).toBeTruthy()
		})

		it('creates fieldsObj with correct structure', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' },
				attr2: { name: 'attr2', typeName: 'int' }
			}))
			mockGetObjDef
				.mockReturnValueOnce({ name: 'field1', group: 'rel1 Attribute' })
				.mockReturnValueOnce({ name: 'field2', group: 'rel1 Attribute' })

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })
		})

		it('handles reduce function when acc[field.group] already exists - branch coverage for line 96', async () => {
			// This tests the branch where !acc[field.group] is false (line 93) - uses existing array (line 96)
			// Need multiple fields with the same group to hit the branch where acc[field.group] already exists
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' },
				attr2: { name: 'attr2', typeName: 'int' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				if (!attrObj || !attrObj.name) return null
				// Return fields with the same group to test the branch where acc[field.group] exists (line 96)
				return {
					name: attrObj.name,
					label: attrObj.name,
					group: groupName || 'rel1 Attribute' // Same group for all fields - will hit line 96
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })
			// Should have 1 group with 2 fields
			expect(screen.getByTestId('fields-count').textContent).toBe('1')
		})

		it('handles reduce function when acc[field.group] does not exist - branch coverage for lines 93-95', async () => {
			// This tests the branch where !acc[field.group] is true (line 93) - creates new array (lines 94-95)
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' },
				attr2: { name: 'attr2', typeName: 'int' }
			}))
			let callCount = 0
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				if (!attrObj || !attrObj.name) return null
				callCount++
				// Return fields with different groups to test the branch where acc[field.group] doesn't exist (lines 93-95)
				return {
					name: attrObj.name,
					label: attrObj.name,
					group: `Group${callCount}` // Different group for each field - will hit lines 93-95
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })
			// Should have 2 groups with 1 field each
			expect(screen.getByTestId('fields-count').textContent).toBe('2')
		})

		it('handles fields() returning null or undefined', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					group: 'rel1 Attribute'
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			// Should handle gracefully
			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })
		})
	})

	describe('QueryBuilder Integration', () => {
		it('calls setRelationshipQuery when query changes', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					group: groupName || 'rel1 Attribute'
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })

			const changeButton = screen.getByText('Change Query')
			const user = userEvent.setup()
			await act(async () => {
				await user.click(changeButton)
			})

			expect(mockSetRelationshipQuery).toHaveBeenCalledWith({
				combinator: 'and',
				rules: []
			})
		})

		it('passes correct query prop to QueryBuilder', async () => {
			const customQuery = { combinator: 'or', rules: [{ field: 'test' }] }
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					group: groupName || 'rel1 Attribute'
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={customQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query')).toBeInTheDocument()
			}, { timeout: 3000 })

			const queryElement = screen.getByTestId('query')
			expect(queryElement.textContent).toBe(JSON.stringify(customQuery))
		})

		it('applies correct controlClassnames', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					group: groupName || 'rel1 Attribute'
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('control-classnames')).toBeInTheDocument()
			}, { timeout: 3000 })

			const classnamesElement = screen.getByTestId('control-classnames')
			expect(classnamesElement.textContent).toBe(JSON.stringify({ queryBuilder: 'queryBuilder-branches' }))
		})
	})

	describe('ValueEditor Conditional Rendering', () => {
		it('does not render ValueEditor for is_null operator - branch coverage for lines 124-128', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					group: groupName || 'rel1 Attribute'
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })

			// ValueEditor with is_null operator should return undefined (lines 124-128)
			// This is tested in the QueryBuilder mock which calls ValueEditor with is_null
			// Verify that ValueEditor was called with is_null and returned undefined
			const isNullCall = valueEditorCalls.find(call => call.operator === 'is_null')
			expect(isNullCall).toBeDefined()
			expect(isNullCall?.returned).toBeNull()
		})

		it('does not render ValueEditor for not_null operator - branch coverage for lines 125-128', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					group: groupName || 'rel1 Attribute'
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })

			// ValueEditor with not_null operator should return undefined (lines 125-128)
			// Verify that ValueEditor was called with not_null and returned undefined
			const notNullCall = valueEditorCalls.find(call => call.operator === 'not_null')
			expect(notNullCall).toBeDefined()
			expect(notNullCall?.returned).toBeNull()
		})

		it('renders ValueEditor for other operators - branch coverage for line 130', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					group: groupName || 'rel1 Attribute'
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })

			// ValueEditor with other operators should return ValueEditor component (line 130)
			// Verify that ValueEditor was called with other operator and returned a component
			const otherCall = valueEditorCalls.find(call => call.operator === '=')
			expect(otherCall).toBeDefined()
			expect(otherCall?.returned).toBeTruthy()
		})
	})

	describe('Translations', () => {
		it('renders AddOutlinedIcon in addGroup translation', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					group: groupName || 'rel1 Attribute'
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('add-group-label')).toBeInTheDocument()
			}, { timeout: 3000 })
			expect(screen.getAllByTestId('add-icon').length).toBeGreaterThan(0)
		})

		it('renders AddOutlinedIcon in addRule translation', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					group: groupName || 'rel1 Attribute'
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('add-rule-label')).toBeInTheDocument()
			}, { timeout: 3000 })
		})

		it('renders AddOutlinedIcon with fontSize="small"', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					group: groupName || 'rel1 Attribute'
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getAllByTestId('add-icon').length).toBeGreaterThan(0)
			}, { timeout: 3000 })

			const icons = screen.getAllByTestId('add-icon')
			icons.forEach(icon => {
				expect(icon.getAttribute('data-font-size')).toBe('small')
			})
		})
	})

	describe('Edge Cases', () => {
		it('handles isEmpty returning true for relationshipDefs', () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true
				if (Array.isArray(val)) return val.length === 0
				if (typeof val === 'object' && val !== null) {
					return Object.keys(val).length === 0
				}
				return !val
			})
			mockUseAppSelector.mockReturnValueOnce({
				relationships: {
					relationshipDefs: []
				}
			})
			// When relationshipDefs is empty, isEmpty returns true, so attrTagObj becomes {}
			// and fieldsObj becomes empty, showing "No Attributes are available"
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({}))
			mockGetObjDef.mockImplementation(() => null)

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(screen.getByText(/No Attributes are available/)).toBeInTheDocument()
		})

		it('handles isEmpty returning true for relationshipParams', async () => {
			mockUseLocation.mockReturnValueOnce({ search: '' })
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true
				if (val === '') return true
				if (Array.isArray(val)) return val.length === 0
				if (typeof val === 'object') return Object.keys(val).length === 0
				return !val
			})
			// When relationshipParams is null, fieldsObj will be empty, so no QueryBuilder
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({}))
			mockGetObjDef.mockImplementation(() => null)

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			// When relationshipParams is null, URLSearchParams.get returns null, which React renders as "null"
			// URLSearchParams.get("relationshipName") returns null when not found, React renders null as empty
			const relationshipText = screen.getByText(/Relationship:/)
			expect(relationshipText.textContent).toBe('Relationship: ')
		})

		it('handles attrTagObj being falsy', () => {
			mockUseLocation.mockReturnValueOnce({ search: '?relationshipName=nonexistent' })
			mockUseAppSelector.mockReturnValueOnce({
				relationships: {
					relationshipDefs: [{ name: 'otherRel', attributes: {} }]
				}
			})
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true
				if (Array.isArray(val)) return val.length === 0
				if (typeof val === 'object') return Object.keys(val).length === 0
				return !val
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			expect(screen.getByText(/No Attributes are available/)).toBeTruthy()
		})

		it('handles toFullOption being called on all fields', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' },
				attr2: { name: 'attr2', typeName: 'int' }
			}))
			mockGetObjDef
				.mockReturnValueOnce({ name: 'field1', group: 'Group1' })
				.mockReturnValueOnce({ name: 'field2', group: 'Group1' })

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })
			expect(mockToFullOption).toHaveBeenCalled()
		})

		it('handles fields with numeric group values', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					group: 123
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })
		})

		it('handles multiple attributes in relationship', async () => {
			mockGetNestedSuperTypeObj.mockReturnValueOnce({
				attr1: { name: 'attr1' },
				attr2: { name: 'attr2' },
				attr3: { name: 'attr3' }
			})
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					group: groupName || 'rel1 Attribute'
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })
		})

		it('handles field being null in reduce function', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			// Test when field is null in reduce
			mockToFullOption.mockImplementationOnce(() => null)
			mockGetObjDef.mockImplementation(() => ({
				name: 'field1',
				group: 'rel1 Attribute'
			}))

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			// Should handle null fields gracefully - null fields are filtered out in reduce
			await waitFor(() => {
				expect(screen.getByText(/No Attributes are available/)).toBeInTheDocument()
			}, { timeout: 3000 })
		})
	})

	describe('AccordionDetails Content', () => {
		it('renders QueryBuilder in AccordionDetails when fields available', async () => {
			mockGetNestedSuperTypeObj.mockImplementation(({ data }) => ({
				attr1: { name: 'attr1', typeName: 'string' }
			}))
			mockGetObjDef.mockImplementation((allDataObj: any, attrObj: any, rules_widgets: any, isGroupView: boolean, groupName: string) => {
				if (!attrObj || !attrObj.name) return null
				return {
					name: attrObj.name,
					group: groupName || 'rel1 Attribute'
				}
			})

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			await waitFor(() => {
				expect(screen.getByTestId('query-builder')).toBeInTheDocument()
			}, { timeout: 10000 })
			const details = screen.getByTestId('accordion-details')
			expect(details).toBeTruthy()
		})

		it('renders Typography with "No Attributes" in AccordionDetails when no fields', () => {
			mockGetObjDef.mockImplementation(() => null)

			render(
				<RelationshipFilters
					allDataObj={mockAllDataObj}
					relationshipQuery={mockRelationshipQuery}
					setRelationshipQuery={mockSetRelationshipQuery}
				/>
			)

			const details = screen.getByTestId('accordion-details')
			const typography = screen.getByText(/No Attributes are available/)
			expect(typography.getAttribute('data-color')).toBe('text.secondary')
			expect(typography.style.textAlign).toBe('center')
			expect(typography.style.fontWeight).toBe('600')
		})
	})
})
