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
 * Unit tests for TreeNodeIcons component
 */

import React from 'react'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import TreeNodeIcons from '../TreeNodeIcons'

const navigateMock = jest.fn()
const removeSavedSearchMock = jest.fn()
const editSavedSearchMock = jest.fn()

jest.mock('react-router-dom', () => ({
	useNavigate: () => navigateMock
}))

jest.mock('../Modal', () => ({
	__esModule: true,
	default: ({ open, title, button2Label, button2Handler, children }: any) => (
		open ? (
			<div>
				<div>{title}</div>
				{children}
				<button onClick={button2Handler}>{button2Label}</button>
			</div>
		) : null
	)
}))

jest.mock('@mui/material', () => ({
	IconButton: ({ children, onClick }: any) => (
		<button onClick={onClick}>{children}</button>
	),
	Menu: ({ open, children }: any) => (open ? <div>{children}</div> : null),
	MenuItem: ({ children, onClick }: any) => (
		<div onClick={onClick}>{children}</div>
	),
	ListItemIcon: ({ children }: any) => <div>{children}</div>,
	TextField: ({ value, onChange }: any) => (
		<input value={value} onChange={onChange} />
	),
	Typography: ({ children }: any) => <span>{children}</span>,
	Stack: ({ children }: any) => <div>{children}</div>
}))

jest.mock('../../hooks/reducerHook', () => ({
	useAppSelector: () => ({
		savedSearchData: [{ name: 'Search1', guid: 'g1' }]
	})
}))

jest.mock('../../api/apiMethods/savedSearchApiMethod', () => ({
	removeSavedSearch: (...args: any[]) => removeSavedSearchMock(...args),
	editSavedSearch: (...args: any[]) => editSavedSearchMock(...args)
}))

jest.mock('../../utils/Utils', () => ({
	isEmpty: (val: any) =>
		val == null || (Array.isArray(val) ? val.length === 0 : val === '') ,
	serverError: jest.fn()
}))

jest.mock('@views/Classification/DeleteTag', () => ({
	__esModule: true,
	default: () => <div data-testid="delete-tag" />
}))

jest.mock('@views/Classification/ClassificationForm', () => ({
	__esModule: true,
	default: () => <div data-testid="classification-form" />
}))

jest.mock('@views/Glossary/AddUpdateTermForm', () => ({
	__esModule: true,
	default: () => <div data-testid="term-form" />
}))

jest.mock('@views/Glossary/AddUpdateGlossaryForm', () => ({
	__esModule: true,
	default: () => <div data-testid="glossary-form" />
}))

jest.mock('@views/Glossary/DeleteGlossary', () => ({
	__esModule: true,
	default: () => <div data-testid="delete-glossary" />
}))

jest.mock('@views/Glossary/AddUpdateCategoryForm', () => ({
	__esModule: true,
	default: () => <div data-testid="category-form" />
}))

jest.mock('../../utils/Enum', () => ({
	addOnClassification: []
}))

describe('TreeNodeIcons', () => {
	beforeEach(() => {
		jest.clearAllMocks()
	})

	it('handles CustomFilters rename flow', async () => {
		
		const updatedData = jest.fn()
		render(
			<TreeNodeIcons
				node={{ id: 'Search1', types: 'child', parent: 'Filter' }}
				treeName="CustomFilters"
				updatedData={updatedData}
				isEmptyServicetype={false}
			/>
		)

		fireEvent.click(screen.getByTestId('MoreHorizOutlinedIcon'))
		fireEvent.click(screen.getByText('Rename'))
		fireEvent.click(screen.getByText('Update'))

		await waitFor(() => {
			expect(editSavedSearchMock).toHaveBeenCalled()
			expect(updatedData).toHaveBeenCalled()
		})
	})

	it('handles CustomFilters delete flow', async () => {
		removeSavedSearchMock.mockResolvedValue({})
		const updatedData = jest.fn()
		render(
			<TreeNodeIcons
				node={{ id: 'Search1', types: 'child', parent: 'Filter' }}
				treeName="CustomFilters"
				updatedData={updatedData}
				isEmptyServicetype={false}
			/>
		)

		fireEvent.click(screen.getByTestId('MoreHorizOutlinedIcon'))
		fireEvent.click(screen.getByText('Delete'))
		fireEvent.click(screen.getByText('Ok'))

		await waitFor(() => {
			expect(removeSavedSearchMock).toHaveBeenCalledWith('g1')
			expect(navigateMock).toHaveBeenCalledWith(
				{ pathname: '/search' },
				{ replace: true }
			)
		})
	})

	it('shows classification menu actions', () => {
		render(
			<TreeNodeIcons
				node={{ id: 'Class1', types: 'child', children: [] }}
				treeName="Classifications"
				updatedData={jest.fn()}
				isEmptyServicetype={false}
			/>
		)

		fireEvent.click(screen.getByTestId('MoreHorizOutlinedIcon'))
		fireEvent.click(screen.getByText('Create Sub-classification'))
		expect(screen.getByTestId('classification-form')).toBeTruthy()
	})

	it('shows glossary actions for parent when empty service type', () => {
		render(
			<TreeNodeIcons
				node={{ id: 'Gloss1', types: 'parent', children: [] }}
				treeName="Glossary"
				updatedData={jest.fn()}
				isEmptyServicetype={true}
			/>
		)

		fireEvent.click(screen.getByTestId('MoreHorizOutlinedIcon'))
		fireEvent.click(screen.getByText('Create Term'))
		expect(screen.getByTestId('term-form')).toBeTruthy()
	})

	it('shows glossary category action for child when not empty', () => {
		render(
			<TreeNodeIcons
				node={{ id: 'Term1', types: 'child', children: [], cGuid: 'c1' }}
				treeName="Glossary"
				updatedData={jest.fn()}
				isEmptyServicetype={false}
			/>
		)

		fireEvent.click(screen.getByTestId('MoreHorizOutlinedIcon'))
		fireEvent.click(screen.getByText('Create Sub-Category'))
		expect(screen.getByTestId('category-form')).toBeTruthy()
	})

	it('opens delete tag modal for classifications', () => {
		render(
			<TreeNodeIcons
				node={{ id: 'Class1', types: 'child', children: [] }}
				treeName="Classifications"
				updatedData={jest.fn()}
				isEmptyServicetype={false}
			/>
		)

		fireEvent.click(screen.getByTestId('MoreHorizOutlinedIcon'))
		fireEvent.click(screen.getByText('Delete'))
		expect(screen.getByTestId('delete-tag')).toBeTruthy()
	})

	it('opens delete glossary modal for glossary parent', () => {
		render(
			<TreeNodeIcons
				node={{ id: 'Gloss1', types: 'parent', children: [] }}
				treeName="Glossary"
				updatedData={jest.fn()}
				isEmptyServicetype={true}
			/>
		)

		fireEvent.click(screen.getByTestId('MoreHorizOutlinedIcon'))
		fireEvent.click(screen.getByText('Delete Glossary'))
		expect(screen.getByTestId('delete-glossary')).toBeTruthy()
	})

	it('opens glossary edit modal for parent', () => {
		render(
			<TreeNodeIcons
				node={{ id: 'Gloss1', types: 'parent', children: [] }}
				treeName="Glossary"
				updatedData={jest.fn()}
				isEmptyServicetype={true}
			/>
		)

		fireEvent.click(screen.getByTestId('MoreHorizOutlinedIcon'))
		fireEvent.click(screen.getByText('View/Edit Glossary'))
		expect(screen.getByTestId('glossary-form')).toBeTruthy()
	})

	it('navigates to search for classification', () => {
		render(
			<TreeNodeIcons
				node={{ id: 'Class1', types: 'child', children: [] }}
				treeName="Classifications"
				updatedData={jest.fn()}
				isEmptyServicetype={false}
			/>
		)

		fireEvent.click(screen.getByTestId('MoreHorizOutlinedIcon'))
		fireEvent.click(screen.getByText('Search'))
		expect(navigateMock).toHaveBeenCalledWith({
			pathname: '/search/searchResult',
			search: expect.stringContaining('tag=Class1')
		})
	})

	it('navigates to glossary child view/edit', () => {
		render(
			<TreeNodeIcons
				node={{
					id: 'Term1',
					parent: 'Gloss',
					types: 'child',
					cGuid: 'c1'
				}}
				treeName="Glossary"
				updatedData={jest.fn()}
				isEmptyServicetype={true}
			/>
		)

		fireEvent.click(screen.getByTestId('MoreHorizOutlinedIcon'))
		fireEvent.click(screen.getByText('View/Edit Term'))
		expect(navigateMock).toHaveBeenCalledWith({
			pathname: '/glossary/c1',
			search: expect.stringContaining('term=Term1%40Gloss') // URL encoded @ symbol
		})
	})

})
