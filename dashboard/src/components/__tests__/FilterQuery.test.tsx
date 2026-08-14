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
 * Unit tests for FilterQuery component
 */

import React from 'react'
import { render, screen, fireEvent } from '@testing-library/react'
import FilterQuery from '../FilterQuery'

const navigateMock = jest.fn()

jest.mock('react-router-dom', () => ({
	useLocation: () => ({ search: '?type=DataSet&tag=PII&query=abc' }),
	useNavigate: () => navigateMock
}))

jest.mock('@mui/material', () => ({
	Chip: ({ label, onDelete, ...props }: any) => (
		<div
			data-type={props['data-type']}
			data-id={props['data-id']}
			onClick={onDelete}
		>
			{label}
		</div>
	),
	Stack: ({ children }: any) => <div>{children}</div>,
	Typography: ({ children }: any) => <span>{children}</span>
}))

jest.mock('@utils/CommonViewFunction', () => ({
	attributeFilter: {
		extractUrl: jest.fn()
	}
}))

jest.mock('@utils/Enum', () => ({
	queryBuilderDateRangeUIValueToAPI: { Today: 'TODAY' },
	systemAttributes: { createTime: 'Created' },
	filterQueryValue: { status: { ACTIVE: 'Active' } },
	getDisplayOperator: (operator: string) => operator
}))

jest.mock('moment-timezone', () => {
	const mockMoment: any = jest.fn(() => ({
		format: () => '',
		valueOf: () => 0
	}))
	mockMoment.version = '2.29.4'
	mockMoment.tz = Object.assign(
		jest.fn(() => ({
			format: () => '',
			zoneAbbr: () => 'UTC'
		})),
		{ guess: () => 'UTC' }
	)
	return mockMoment
})

jest.mock('moment', () => {
	const mockMoment: any = jest.fn((input?: unknown) => ({
		format: () => (typeof input === 'number' ? 'formatted' : ''),
		valueOf: () => 0
	}))
	mockMoment.tz = Object.assign(
		jest.fn(() => ({
			format: () => '',
			zoneAbbr: () => 'UTC'
		})),
		{ guess: () => 'UTC' }
	)
	return { __esModule: true, default: mockMoment }
})

const { attributeFilter } = jest.requireMock('@utils/CommonViewFunction')

describe('FilterQuery', () => {
	beforeEach(() => {
		jest.clearAllMocks()
	})

	it('returns null when no filters are provided', () => {
		const { container } = render(<FilterQuery value={{}} />)
		expect(container.firstChild).toBeNull()
	})

	it('renders type and entity filters with date mapping', () => {
		attributeFilter.extractUrl.mockReturnValue({
			condition: 'AND',
			rules: [
				{
					id: 'createTime',
					type: 'date',
					operator: '=',
					value: 'Today'
				}
			]
		})

		render(
			<FilterQuery
				value={{
					type: 'DataSet',
					entityFilters: { rules: [{ id: 'createTime' }] }
				}}
			/>
		)

		expect(screen.getByText('Type:')).toBeTruthy()
		expect(screen.getByText('DataSet')).toBeTruthy()
		expect(screen.getByText('Created')).toBeTruthy()
		expect(screen.getByText('TODAY')).toBeTruthy()
	})

	it('renders date value without mapping', () => {
		attributeFilter.extractUrl.mockReturnValue({
			condition: 'AND',
			rules: [
				{
					id: 'createTime',
					type: 'date',
					operator: '=',
					value: '2020-01-01'
				}
			]
		})

		render(
			<FilterQuery
				value={{
					type: 'DataSet',
					entityFilters: { rules: [{ id: 'createTime' }] }
				}}
			/>
		)

		expect(screen.getByText('2020-01-01 (UTC)')).toBeTruthy()
	})

	it('renders tag and relationship filters', () => {
		attributeFilter.extractUrl.mockReturnValueOnce({
			condition: 'AND',
			rules: [{ id: 'status', operator: '=', value: 'ACTIVE' }]
		})
		attributeFilter.extractUrl.mockReturnValueOnce({
			condition: 'OR',
			rules: [{ id: 'status', operator: '=', value: 'ACTIVE' }]
		})

		render(
			<FilterQuery
				value={{
					tag: 'PII',
					tagFilters: { rules: [{ id: 'status' }] },
					relationshipName: 'rel',
					relationshipFilters: { rules: [{ id: 'status' }] }
				}}
			/>
		)

		expect(screen.getByText('Classification:')).toBeTruthy()
		expect(screen.getAllByText('Active').length).toBeGreaterThan(0)
		expect(screen.getByText('Relationship:')).toBeTruthy()
	})

	it('renders term, query and flags', () => {
		render(
			<FilterQuery
				value={{
					term: 'Term1',
					query: ' hello ',
					excludeST: 'true',
					excludeSC: 'true',
					includeDE: 'true'
				}}
			/>
		)

		expect(screen.getByText('Term:')).toBeTruthy()
		expect(screen.getByText('Term1')).toBeTruthy()
		expect(screen.getByText('Query:')).toBeTruthy()
		expect(screen.getByText('hello')).toBeTruthy()
		expect(screen.getByText('Exclude sub-types:')).toBeTruthy()
		expect(screen.getByText('Exclude sub-classifications:')).toBeTruthy()
		expect(screen.getByText('Show historical entities:')).toBeTruthy()
	})

	it('navigates to search when clearing the last filter', () => {
		const localNavigate = jest.fn()
		jest.spyOn(require('react-router-dom'), 'useNavigate').mockReturnValueOnce(localNavigate)
		jest.spyOn(require('react-router-dom'), 'useLocation').mockReturnValueOnce({
			search: '?type=DataSet'
		})

		render(<FilterQuery value={{ type: 'DataSet' }} />)

		fireEvent.click(screen.getByText('DataSet'))
		expect(localNavigate).toHaveBeenCalledWith({ pathname: '/search' })
	})


	it('navigates to searchResult when other filters remain', () => {
		const localNavigate = jest.fn()
		jest.spyOn(require('react-router-dom'), 'useNavigate').mockReturnValueOnce(localNavigate)
		jest.spyOn(require('react-router-dom'), 'useLocation').mockReturnValueOnce({
			search: '?type=DataSet&tag=PII&query=abc'
		})

		render(<FilterQuery value={{ type: 'DataSet', tag: 'PII' }} />)

		fireEvent.click(screen.getByText('DataSet'))
		expect(localNavigate).toHaveBeenCalledWith({
			pathname: '/search/searchResult',
			search: expect.stringContaining('query=abc')
		})
	})

	it('clears term related params when term chip is removed', () => {
		const localNavigate = jest.fn()
		jest.spyOn(require('react-router-dom'), 'useNavigate').mockReturnValueOnce(localNavigate)
		jest.spyOn(require('react-router-dom'), 'useLocation').mockReturnValueOnce({
			search: '?term=Term1&gtype=term&viewType=term&guid=123'
		})

		render(<FilterQuery value={{ term: 'Term1' }} />)
		fireEvent.click(screen.getByText('Term1'))
		expect(localNavigate).toHaveBeenCalledWith({ pathname: '/search' })
	})
})
