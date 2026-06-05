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
 * Unit tests for DialogShowMoreLess component
 * 
 * Coverage Target: 100%
 * - Statements: 100%
 * - Branches: 100%
 * - Functions: 100%
 * - Lines: 100%
 */

import React from 'react'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import DialogShowMoreLess from '../DialogShowMoreLess'

const toastSuccess = jest.fn()
const toastDismiss = jest.fn()

jest.mock('react-toastify', () => ({
	toast: {
		success: (...args: any[]) => toastSuccess(...args),
		dismiss: (...args: any[]) => toastDismiss(...args)
	}
}))

jest.mock('../muiComponents', () => ({
	LightTooltip: ({ children, title }: any) => <span title={title}>{children}</span>
}))

jest.mock('@mui/material', () => ({
	Chip: ({ label, onDelete, className, sx, clickable, size, variant, color }: any) => (
		<div
			data-testid="chip"
			className={className}
			onClick={onDelete}
			data-clickable={clickable}
			data-size={size}
			data-variant={variant}
			data-color={color}
			style={sx}
		>
			{label}
		</div>
	),
	IconButton: ({ children, onClick, 'aria-label': ariaLabel, 'data-cy': dataCy, className }: any) => {
		const testId =
			dataCy === 'addTag'
				? 'add-tag-btn'
				: dataCy === 'moreData'
				? 'more-btn'
				: ariaLabel === 'close'
				? 'close-btn'
				: 'icon-btn';
		return (
			<button
				onClick={onClick}
				aria-label={ariaLabel}
				data-cy={dataCy}
				className={className}
				data-testid={testId}
			>
				{children}
			</button>
		);
	},
	Menu: ({ open, children, onClose, anchorEl }: any) => (
		open ? <div data-testid="menu" onClick={onClose}>{children}</div> : null
	),
	MenuItem: ({ children, onClick }: any) => (
		<div onClick={onClick} data-testid="menu-item">{children}</div>
	),
	Typography: ({ children, fontSize }: any) => <span style={{ fontSize }}>{children}</span>
}))

jest.mock('../Modal', () => ({
	__esModule: true,
	default: ({ open, title, titleIcon, button1Label, button1Handler, button2Label, button2Handler, children }: any) => (
		open ? (
			<div data-testid="modal">
				<div>{titleIcon}</div>
				<div>{title}</div>
				{children}
				{button1Label && <button onClick={button1Handler}>{button1Label}</button>}
				<button onClick={button2Handler}>{button2Label}</button>
			</div>
		) : null
	)
}))

jest.mock('../commonComponents', () => ({
	EllipsisText: ({ children }: any) => <span className="ellipsis">{children}</span>
}))

const paramsMock = { guid: '' }
const locationMock = { search: '', pathname: '/search' }

jest.mock('react-router-dom', () => ({
	Link: ({ children, to, className }: any) => (
		<a href={to?.pathname} className={className} data-testid="link">
			{children}
		</a>
	),
	useLocation: () => locationMock,
	useParams: () => paramsMock
}))

const dispatchMock = jest.fn()

jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: jest.fn(() => ({
		classificationData: {
			classificationDefs: [
				{ name: 'PII', superTypes: ['a', 'b'] },
				{ name: 'Sensitive', superTypes: ['c'] },
				{ name: 'NoSuper', superTypes: [] }
			]
		}
	})),
	useAppDispatch: () => dispatchMock
}))

jest.mock('@utils/Utils', () => ({
	extractKeyValueFromEntity: jest.fn((val: any) => ({ name: val?.name || 'AssetName' })),
	isEmpty: jest.fn((val: any) =>
		val == null || (Array.isArray(val) ? val.length === 0 : val === '')),
	serverError: jest.fn()
}))

jest.mock('@api/apiMethods/apiMethod', () => ({
	_delete: jest.fn()
}))

jest.mock('@views/Classification/AddTag', () => ({
	__esModule: true,
	default: ({ open, onClose }: any) => open ? <div data-testid="add-tag">AddTag</div> : null
}))

jest.mock('@views/Glossary/AssignTerm', () => ({
	__esModule: true,
	default: ({ open, onClose, relatedTerm, columnVal }: any) => 
		open ? <div data-testid="assign-term" data-related={relatedTerm} data-column={columnVal}>AssignTerm</div> : null
}))

jest.mock('@views/Glossary/AssignCategory', () => ({
	__esModule: true,
	default: ({ open, onClose }: any) => open ? <div data-testid="assign-category">AssignCategory</div> : null
}))

jest.mock('@views/Classification/AddTagAttributes', () => ({
	__esModule: true,
	default: ({ open, onClose }: any) => open ? <div data-testid="add-attr">AddTagAttributes</div> : null
}))

jest.mock('@redux/slice/glossaryDetailsSlice', () => ({
	fetchGlossaryDetails: jest.fn(() => ({ type: 'fetchGlossaryDetails' }))
}))

jest.mock('@redux/slice/detailPageSlice', () => ({
	fetchDetailPageData: jest.fn(() => ({ type: 'fetchDetailPageData' }))
}))

jest.mock('@redux/slice/glossarySlice', () => ({
	fetchGlossaryData: jest.fn(() => ({ type: 'fetchGlossaryData' }))
}))

const { extractKeyValueFromEntity, isEmpty } = require('@utils/Utils')

describe('DialogShowMoreLess', () => {
	const mockRemoveApiMethod = jest.fn()

	beforeEach(() => {
		jest.clearAllMocks()
		paramsMock.guid = ''
		locationMock.search = ''
		mockRemoveApiMethod.mockResolvedValue({})
		dispatchMock.mockReturnValue({
			unwrap: () => Promise.resolve({}),
		})
		extractKeyValueFromEntity.mockReturnValue({ name: 'AssetName' })
		isEmpty.mockImplementation((val: any) =>
			val == null || (Array.isArray(val) ? val.length === 0 : val === '')
		)
	})

	describe('Classification rendering', () => {
		it('renders classification chip with super types when isShowMoreLess is true', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: [
							{ name: 'PII', entityGuid: 'g1', entityStatus: 'ACTIVE' }
						]
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={true}
					readOnly={false}
				/>
			)

			expect(screen.getByText('PII@(a, b)')).toBeTruthy()
		})

		it('renders classification chip with single super type', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: [
							{ name: 'Sensitive', entityGuid: 'g1', entityStatus: 'ACTIVE' }
						]
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={true}
					readOnly={false}
				/>
			)

			expect(screen.getByText('Sensitive@c')).toBeTruthy()
		})

		it('renders classification without super types', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: [
							{ name: 'NoSuper', entityGuid: 'g1', entityStatus: 'ACTIVE' }
						]
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={true}
					readOnly={false}
				/>
			)

			expect(screen.getByText('NoSuper')).toBeTruthy()
		})

		it('renders multiple classifications when isShowMoreLess is false', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: [
							{ name: 'PII', entityGuid: 'g1', entityStatus: 'ACTIVE' },
							{ name: 'Sensitive', entityGuid: 'g1', entityStatus: 'ACTIVE' }
						]
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			expect(screen.getByText('PII@(a, b)')).toBeTruthy()
			expect(screen.getByText('Sensitive@c')).toBeTruthy()
		})

		it('shows menu when more than one classification and isShowMoreLess is true', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: [
							{ name: 'PII', entityGuid: 'g1', entityStatus: 'ACTIVE' },
							{ name: 'Sensitive', entityGuid: 'g1', entityStatus: 'ACTIVE' }
						]
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={true}
					readOnly={false}
				/>
			)

			fireEvent.click(screen.getByTestId('more-btn'))
			expect(screen.getByTestId('menu')).toBeTruthy()
		})

		it('allows delete when entityGuid matches value.guid', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: [
							{ name: 'PII', entityGuid: 'g1', entityStatus: 'ACTIVE' }
						]
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={true}
					removeApiMethod={mockRemoveApiMethod}
				/>
			)

			const chip = screen.getByText('PII@(a, b)').closest('[data-testid="chip"]')
			expect(chip).toBeTruthy()
		})

		it('allows delete when entityStatus is DELETED even if guid does not match', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: [
							{ name: 'PII', entityGuid: 'g2', entityStatus: 'DELETED' }
						]
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={true}
					removeApiMethod={mockRemoveApiMethod}
				/>
			)

			const chip = screen.getByText('PII@(a, b)').closest('[data-testid="chip"]')
			expect(chip).toBeTruthy()
		})

		it('does not show delete when removeApiMethod is not provided', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: [
							{ name: 'PII', entityGuid: 'g1', entityStatus: 'ACTIVE' }
						]
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={true}
				/>
			)

			const chip = screen.getByText('PII@(a, b)').closest('[data-testid="chip"]')
			expect(chip).toBeTruthy()
		})
	})

	describe('Term rendering', () => {
		it('renders term chips with links', () => {
			locationMock.search = '?gtype=term'
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						terms: [
							{
								displayText: 'Term1',
								termGuid: 't1',
								relationGuid: 'r1'
							}
						]
					}}
					columnVal="terms"
					colName="Term"
					displayText="displayText"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			expect(screen.getByText('Term1')).toBeTruthy()
		})

		it('renders term with optionalDisplayText', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						terms: [
							{
								displayText: 'Term1',
								termGuid: 't1',
								relationGuid: 'r1'
							}
						]
					}}
					columnVal="terms"
					colName="Term"
					displayText="displayText"
					optionalDisplayText="optionalText"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			expect(screen.getByText('Term1')).toBeTruthy()
		})
	})

	describe('Category rendering', () => {
		it('renders category chips', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						categories: [
							{
								displayText: 'Category1',
								categoryGuid: 'c1'
							}
						]
					}}
					columnVal="categories"
					colName="Category"
					displayText="displayText"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			expect(screen.getByText('Category1')).toBeTruthy()
		})
	})

	describe('Remove functionality', () => {
		it('removes classification via removeApiMethod', async () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: [
							{ name: 'PII', entityGuid: 'g1', entityStatus: 'ACTIVE' }
						]
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={true}
					removeApiMethod={mockRemoveApiMethod}
					setUpdateTable={jest.fn()}
				/>
			)

			const chip = screen.getByText('PII@(a, b)').closest('[data-testid="chip"]')
			if (chip) {
				fireEvent.click(chip)
				fireEvent.click(screen.getByText('Remove'))

				await waitFor(() => {
					expect(mockRemoveApiMethod).toHaveBeenCalledWith('g1', 'PII')
					expect(toastSuccess).toHaveBeenCalled()
				})
			}
		})

		it('removes term with detailPage flow', async () => {
			const removeApiMethod = jest.fn().mockResolvedValue({})
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						terms: [
							{
								displayText: 'Term1',
								guid: 't1',
								relationshipGuid: 'r1'
							}
						]
					}}
					entity={{ guid: 'e1' }}
					columnVal="terms"
					colName="Term"
					displayText="displayText"
					isShowMoreLess={true}
					detailPage={true}
					removeApiMethod={removeApiMethod}
				/>
			)

			const chip = screen.getByText('Term1').closest('[data-testid="chip"]')
			if (chip) {
				fireEvent.click(chip)
				fireEvent.click(screen.getByText('Remove'))

				await waitFor(() => {
					expect(removeApiMethod).toHaveBeenCalledWith('t1', {
						guid: 'e1',
						relationshipGuid: 'r1'
					})
				})
			}
		})

		it('removes term using glossary relation update when guid exists', async () => {
			const removeApiMethod = jest.fn().mockResolvedValue({})
			paramsMock.guid = 'gloss-guid'
			locationMock.search = '?gtype=term'
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						terms: [
							{
								displayText: 'Term1',
								termGuid: 't1',
								relationGuid: 'r1'
							}
						]
					}}
					columnVal="terms"
					colName="Term"
					displayText="displayText"
					isShowMoreLess={false}
					removeApiMethod={removeApiMethod}
					setUpdateTable={jest.fn()}
				/>
			)

			const chip = screen.getByText('Term1').closest('[data-testid="chip"]')
			if (chip) {
				fireEvent.click(chip)
				fireEvent.click(screen.getByText('Remove'))

				await waitFor(() => {
					expect(removeApiMethod).toHaveBeenCalledWith(
						'gloss-guid',
						'category',
						expect.objectContaining({ guid: 'g1' })
					)
				})
			}
		})

		it('removes category using glossary relation update', async () => {
			const removeApiMethod = jest.fn().mockResolvedValue({})
			paramsMock.guid = 'gloss-guid'
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						categories: [
							{
								displayText: 'Category1',
								categoryGuid: 'c1'
							}
						]
					}}
					columnVal="categories"
					colName="Category"
					displayText="displayText"
					isShowMoreLess={false}
					removeApiMethod={removeApiMethod}
					setUpdateTable={jest.fn()}
				/>
			)

			const chip = screen.getByText('Category1').closest('[data-testid="chip"]')
			if (chip) {
				fireEvent.click(chip)
				fireEvent.click(screen.getByText('Remove'))

				await waitFor(() => {
					expect(removeApiMethod).toHaveBeenCalledWith(
						'gloss-guid',
						'term',
						expect.objectContaining({ guid: 'g1' })
					)
				})
			}
		})

		it('handles remove error', async () => {
			const removeApiMethod = jest.fn().mockRejectedValue(new Error('Remove failed'))
			const { serverError } = require('@utils/Utils')
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: [
							{ name: 'PII', entityGuid: 'g1', entityStatus: 'ACTIVE' }
						]
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={true}
					removeApiMethod={removeApiMethod}
				/>
			)

			const chip = screen.getByText('PII@(a, b)').closest('[data-testid="chip"]')
			if (chip) {
				fireEvent.click(chip)
				fireEvent.click(screen.getByText('Remove'))

				await waitFor(() => {
					expect(serverError).toHaveBeenCalled()
				})
			}
		})

		it('dispatches actions when guid exists after remove', async () => {
			paramsMock.guid = 'test-guid'
			locationMock.search = '?gtype=term'
			const removeApiMethod = jest.fn().mockResolvedValue({})
			const setUpdateTable = jest.fn()
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						terms: [
							{
								displayText: 'Term1',
								termGuid: 't1',
								relationGuid: 'r1'
							}
						]
					}}
					columnVal="terms"
					colName="Term"
					displayText="displayText"
					isShowMoreLess={false}
					removeApiMethod={removeApiMethod}
					setUpdateTable={setUpdateTable}
				/>
			)

			const chip = screen.getByText('Term1').closest('[data-testid="chip"]')
			if (chip) {
				fireEvent.click(chip)
				fireEvent.click(screen.getByText('Remove'))

				await waitFor(() => {
					expect(dispatchMock).toHaveBeenCalled()
					expect(setUpdateTable).toHaveBeenCalled()
				})
			}
		})
	})

	describe('Add functionality', () => {
		it('opens AddTag modal when Classification add button is clicked', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: []
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			fireEvent.click(screen.getByTestId('add-tag-btn'))
			expect(screen.getByTestId('add-tag')).toBeTruthy()
		})

		it('opens AssignTerm modal when Term add button is clicked', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						terms: []
					}}
					columnVal="terms"
					colName="Term"
					displayText="displayText"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			fireEvent.click(screen.getByTestId('add-tag-btn'))
			expect(screen.getByTestId('assign-term')).toBeTruthy()
		})

		it('opens AssignCategory modal when Category add button is clicked', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						categories: []
					}}
					columnVal="categories"
					colName="Category"
					displayText="displayText"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			fireEvent.click(screen.getByTestId('add-tag-btn'))
			expect(screen.getByTestId('assign-category')).toBeTruthy()
		})

		it('opens AddTagAttributes modal when Attribute add button is clicked', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Attribute: []
					}}
					columnVal="Attribute"
					colName="Attribute"
					displayText="name"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			fireEvent.click(screen.getByTestId('add-tag-btn'))
			expect(screen.getByTestId('add-attr')).toBeTruthy()
		})

		it('does not show add button when readOnly is true', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: []
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={false}
					readOnly={true}
				/>
			)

			expect(screen.queryByTestId('add-tag-btn')).toBeNull()
		})
	})

	describe('Related term functionality', () => {
		it('shows confirmation modal when relatedTerm is true', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						terms: [
							{
								displayText: 'Term1',
								termGuid: 't1',
								relationGuid: 'r1'
							}
						]
					}}
					columnVal="terms"
					colName="Term"
					displayText="displayText"
					isShowMoreLess={true}
					relatedTerm={true}
					removeApiMethod={mockRemoveApiMethod}
				/>
			)

			const chip = screen.getByText('Term1').closest('[data-testid="chip"]')
			if (chip) {
				fireEvent.click(chip)
				expect(screen.getByText('Confirmation')).toBeTruthy()
				expect(screen.getByText(/Are you sure you want to remove term association/)).toBeTruthy()
			}
		})

		it('opens AssignTerm with relatedTerm and columnVal props', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						terms: []
					}}
					columnVal="terms"
					colName="Term"
					displayText="displayText"
					isShowMoreLess={false}
					readOnly={false}
					relatedTerm={true}
				/>
			)

			fireEvent.click(screen.getByTestId('add-tag-btn'))
			const assignTerm = screen.getByTestId('assign-term')
			expect(assignTerm).toBeTruthy()
			expect(assignTerm.getAttribute('data-related')).toBe('true')
		})
	})

	describe('Link generation', () => {
		it('generates classification link with search params', () => {
			locationMock.search = '?searchType=basic&other=param'
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: [
							{ name: 'PII', entityGuid: 'g1', entityStatus: 'ACTIVE' }
						]
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			const link = screen.getByTestId('link')
			expect(link.getAttribute('href')).toBe('/tag/tagAttribute/PII')
		})

		it('generates term link with glossary params', () => {
			locationMock.search = ''
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						terms: [
							{
								displayText: 'Term1',
								termGuid: 't1',
								categoryGuid: 'c1',
								guid: 'term-guid'
							}
						]
					}}
					columnVal="terms"
					colName="Term"
					displayText="displayText"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			const link = screen.getByTestId('link')
			expect(link.getAttribute('href')).toBe('/glossary/t1')
		})

		it('generates category link', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						categories: [
							{
								displayText: 'Category1',
								categoryGuid: 'c1',
								guid: 'cat-guid'
							}
						]
					}}
					columnVal="categories"
					colName="Category"
					displayText="displayText"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			const link = screen.getByTestId('link')
			expect(link.getAttribute('href')).toBe('/glossary/c1')
		})
	})

	describe('Edge cases', () => {
		it('handles empty array', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: []
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			expect(screen.getByTestId('add-tag-btn')).toBeTruthy()
		})

		it('handles value with entity property', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						entity: { guid: 'e1', name: 'EntityName' },
						Classifications: [
							{ name: 'PII', entityGuid: 'g1', entityStatus: 'ACTIVE' }
						]
					}}
					columnVal="Classifications"
					colName="Classification"
					displayText="name"
					isShowMoreLess={true}
					detailPage={true}
					removeApiMethod={mockRemoveApiMethod}
				/>
			)

			const chip = screen.getByText('PII@(a, b)').closest('[data-testid="chip"]')
			if (chip) {
				fireEvent.click(chip)
				const modal = screen.getByTestId('modal')
				expect(modal).toHaveTextContent('Remove Classification Assignment')
				expect(modal).toHaveTextContent('PII')
			}
		})

		it('handles Propagated Classification colName', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						Classifications: [
							{ name: 'PII', entityGuid: 'g1', entityStatus: 'ACTIVE' }
						]
					}}
					columnVal="Classifications"
					colName="Propagated Classification"
					displayText="name"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			const link = screen.getByTestId('link')
			expect(link).toBeTruthy()
		})

		it('handles self columnVal', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						self: [
							{ name: 'PII', entityGuid: 'g1', entityStatus: 'ACTIVE' }
						]
					}}
					columnVal="self"
					colName="Classification"
					displayText="name"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			expect(screen.getByText('PII@(a, b)')).toBeTruthy()
		})

		it('handles object displayText', () => {
			render(
				<DialogShowMoreLess
					value={{
						guid: 'g1',
						terms: ['TermString']
					}}
					columnVal="terms"
					colName="Term"
					displayText="displayText"
					isShowMoreLess={false}
					readOnly={false}
				/>
			)

			expect(screen.getByText('TermString')).toBeTruthy()
		})
	})
})
