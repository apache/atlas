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

import React from 'react'
import { render, screen, fireEvent, waitFor } from '@utils/test-utils'
import { MemoryRouter } from 'react-router-dom'
import CreateDropdown from '../CreateDropdown'

const mockNavigate = jest.fn()

jest.mock('react-router-dom', () => ({
	...jest.requireActual<typeof import('react-router-dom')>('react-router-dom'),
	useNavigate: () => mockNavigate,
}))

jest.mock('@views/Entity/EntityForm', () => ({
	__esModule: true,
	default: ({ open, onClose }: { open: boolean; onClose: () => void }) =>
		open ? (
			<div data-testid="entity-form">
				<button type="button" onClick={onClose}>
					Close entity
				</button>
			</div>
		) : null
}))

jest.mock('@views/Classification/ClassificationForm', () => ({
	__esModule: true,
	default: ({ open, onClose }: { open: boolean; onClose: () => void }) =>
		open ? (
			<div data-testid="classification-form">
				<button type="button" onClick={onClose}>
					Close classification
				</button>
			</div>
		) : null
}))

jest.mock('@views/Glossary/AddUpdateGlossaryForm', () => ({
	__esModule: true,
	default: ({ open, onClose }: { open: boolean; onClose: () => void }) =>
		open ? (
			<div data-testid="glossary-form">
				<button type="button" onClick={onClose}>
					Close glossary
				</button>
			</div>
		) : null
}))

describe('CreateDropdown (header Create menu)', () => {
	beforeEach(() => {
		mockNavigate.mockClear()
	})

	it('opens menu and launches Entity / Classification / Glossary modals', async () => {
		render(
			<MemoryRouter>
				<CreateDropdown />
			</MemoryRouter>,
			{ withRouter: false }
		)

		fireEvent.click(screen.getByRole('button', { name: /create/i }))

		await waitFor(() => {
			expect(screen.getByRole('menu')).toBeInTheDocument()
		})

		fireEvent.click(screen.getByText('Entity'))
		expect(await screen.findByTestId('entity-form')).toBeInTheDocument()

		fireEvent.click(screen.getByText('Close entity'))
		await waitFor(() => {
			expect(screen.queryByTestId('entity-form')).not.toBeInTheDocument()
		})

		fireEvent.click(screen.getByRole('button', { name: /create/i }))
		fireEvent.click(screen.getByText('Classification'))
		expect(await screen.findByTestId('classification-form')).toBeInTheDocument()
		fireEvent.click(screen.getByText('Close classification'))

		fireEvent.click(screen.getByRole('button', { name: /create/i }))
		fireEvent.click(screen.getByText('Glossary'))
		expect(await screen.findByTestId('glossary-form')).toBeInTheDocument()
		fireEvent.click(screen.getByText('Close glossary'))
	})

	it('navigates to Administrator for Business Metadata and Enum', async () => {
		render(
			<MemoryRouter>
				<CreateDropdown />
			</MemoryRouter>,
			{ withRouter: false }
		)

		fireEvent.click(screen.getByRole('button', { name: /create/i }))
		fireEvent.click(screen.getByText('Business Metadata'))

		expect(mockNavigate).toHaveBeenCalledWith({
			pathname: '/administrator',
			search: 'tabActive=businessMetadata&create=true'
		})

		fireEvent.click(screen.getByRole('button', { name: /create/i }))
		fireEvent.click(screen.getByText('Enum'))

		expect(mockNavigate).toHaveBeenCalledWith({
			pathname: '/administrator',
			search: 'tabActive=enum'
		})
	})
})
