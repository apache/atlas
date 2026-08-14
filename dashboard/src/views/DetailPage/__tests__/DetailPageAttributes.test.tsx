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
import { fireEvent, render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'

const mockParams: {
	guid?: string
	tagName?: string
	bmguid?: string
} = { guid: 'eg', tagName: '', bmguid: undefined }

let mockGtype: string | null = 'term'

jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useParams: () => mockParams,
	useSearchParams: () =>
		[
			{
				get: (k: string) => (k === 'gtype' ? mockGtype : null),
			},
			jest.fn(),
		] as unknown as ReturnType<
			typeof import('react-router-dom')['useSearchParams']
		>,
}))

jest.mock('@utils/Utils', () => {
	const actual = jest.requireActual('@utils/Utils')
	return {
		...actual,
		sanitizeHtmlContent: (s: string) => `san(${s})`,
	}
})

const mockUseAppSelector = jest.fn()
jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: (fn: (s: unknown) => unknown) => mockUseAppSelector(fn),
}))

jest.mock('@components/ShowMore/ShowMoreText', () => ({
	__esModule: true,
	default: ({
		value,
		isHtml,
	}: {
		value: string
		isHtml?: boolean
	}) => (
		<span data-testid={`smt-${isHtml ? 'html' : 'plain'}`}>{value}</span>
	),
}))

jest.mock('@components/ShowMore/ShowMoreView', () => ({
	__esModule: true,
	default: ({ title, id }: { title: string; id: string }) => (
		<div data-testid={`smv-${id}`}>{title}</div>
	),
}))

jest.mock('@components/SkeletonLoader', () => ({
	__esModule: true,
	default: () => <div data-testid="skeleton" />,
}))

jest.mock('@components/muiComponents', () => ({
	CustomButton: ({
		children,
		onClick,
		...rest
	}: React.ComponentProps<'button'> & { 'data-cy'?: string }) => (
		<button type="button" onClick={onClick} {...rest}>
			{children}
		</button>
	),
	LightTooltip: ({
		children,
		title,
	}: {
		children: React.ReactNode
		title: string
	}) => (
		<span data-title={title}>{children}</span>
	),
}))

jest.mock('@views/Classification/ClassificationForm', () => ({
	__esModule: true,
	default: ({ open, onClose }: { open: boolean; onClose: () => void }) =>
		open ? (
			<button type="button" data-testid="classification-form" onClick={onClose}>
				close-cf
			</button>
		) : null,
}))

jest.mock('@views/Classification/AddTag', () => ({
	__esModule: true,
	default: ({
		open,
		onClose,
	}: {
		open: boolean
		onClose: () => void
	}) =>
		open ? (
			<button type="button" data-testid="add-tag" onClick={onClose}>
				close-tag
			</button>
		) : null,
}))

jest.mock('@views/Glossary/AddUpdateTermForm', () => ({
	__esModule: true,
	default: ({ open, onClose }: { open: boolean; onClose: () => void }) =>
		open ? (
			<button type="button" data-testid="edit-term" onClick={onClose}>
				close-term
			</button>
		) : null,
}))

jest.mock('@views/Glossary/AddUpdateCategoryForm', () => ({
	__esModule: true,
	default: ({ open, onClose }: { open: boolean; onClose: () => void }) =>
		open ? (
			<button type="button" data-testid="edit-category" onClick={onClose}>
				close-category
			</button>
		) : null,
}))

jest.mock('@views/Classification/AddTagAttributes', () => ({
	__esModule: true,
	default: ({ open, onClose }: { open: boolean; onClose: () => void }) =>
		open ? (
			<button type="button" data-testid="add-attrs" onClick={onClose}>
				close-attrs
			</button>
		) : null,
}))

jest.mock('@views/Glossary/AssignCategory', () => ({
	__esModule: true,
	default: ({
		open,
		onClose,
		data,
	}: {
		open: boolean
		onClose: () => void
		data: unknown
	}) =>
		open ? (
			<button
				type="button"
				data-testid="assign-category"
				data-received={JSON.stringify(data)}
				onClick={onClose}
			>
				close-assign-cat
			</button>
		) : null,
}))

jest.mock('@views/Glossary/AssignTerm', () => ({
	__esModule: true,
	default: ({
		open,
		onClose,
		data,
	}: {
		open: boolean
		onClose: () => void
		data: unknown
	}) =>
		open ? (
			<button
				type="button"
				data-testid="assign-term"
				data-received={JSON.stringify(data)}
				onClick={onClose}
			>
				close-assign-term
			</button>
		) : null,
}))

jest.mock('@api/apiMethods/classificationApiMethod', () => ({
	removeClassification: jest.fn(),
}))

jest.mock('@api/apiMethods/glossaryApiMethod', () => ({
	removeTermorCategory: jest.fn(),
}))

const mockToast = { dismiss: jest.fn(), info: jest.fn() }
jest.mock('react-toastify', () => ({
	toast: mockToast,
}))

import DetailPageAttribute from '../DetailPageAttributes'

const baseData = {
	name: 'Atlas Entity One',
	classifications: [{ typeName: 'c1' }],
	terms: [{ displayText: 't1' }],
	categories: [{ displayText: 'cat1' }],
	superTypes: [{ z: 1 }],
	subTypes: [{ z: 2 }],
	attributeDefs: [{ name: 'n1' }],
}

const defaultProps = {
	data: baseData,
	description: 'plain-desc',
	shortDescription: 'short-one',
	subTypes: ['sub'],
	superTypes: ['sup'],
	loading: false,
	attributeDefs: [{}],
}

describe('DetailPageAttribute', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockParams.guid = 'eg'
		mockParams.tagName = ''
		mockParams.bmguid = undefined
		mockGtype = 'term'
		mockUseAppSelector.mockImplementation((fn: (s: unknown) => unknown) =>
			fn({
				glossary: {
					glossaryData: [{ terms: [{ id: 't1' }] }],
				},
			}),
		)
	})

	const renderComp = (override: Record<string, unknown> = {}) =>
		render(
			<MemoryRouter>
				<DetailPageAttribute {...defaultProps} {...override} />
			</MemoryRouter>,
		)

	it('renders entity title and short + long description', () => {
		renderComp()
		const heading = screen.getByRole('heading', { level: 1 })
		expect(heading).toHaveTextContent('Atlas Entity One')
		expect(screen.getByText('Short Description')).toBeInTheDocument()
		expect(
			screen.getByTestId('smt-plain').textContent?.includes('short-one'),
		).toBe(true)
		expect(screen.getByText(/Long Description/)).toBeInTheDocument()
		expect(screen.getByTestId('smt-html').textContent).toContain('san(')
	})

	it('omits short description block when undefined', () => {
		renderComp({ shortDescription: undefined })
		expect(screen.queryByText('Short Description')).toBeNull()
		expect(screen.getByText(/Description/)).toBeInTheDocument()
	})

	it('shows N/A when short description empty', () => {
		renderComp({ shortDescription: '' })
		expect(screen.getByText('N/A')).toBeInTheDocument()
	})

	const tooltipIconClick = (label: string): void => {
		const wrap = document.querySelector(`[data-title="${label}"]`) as HTMLElement | null
		if (!wrap) {
			throw new Error(`missing tooltip: ${label}`)
		}
		const target =
			(wrap.querySelector('.MuiIconButton-root') as HTMLElement | null) ??
			(wrap.querySelector('button') as HTMLElement | null)
		if (!target) {
			throw new Error(`no icon in tooltip: ${label}`)
		}
		fireEvent.click(target)
	}

	it('plain toggle swaps long description variant', () => {
		renderComp()
		fireEvent.click(screen.getByRole('button', { name: 'Plain' }))
		expect(screen.getAllByTestId('smt-plain').length).toBeGreaterThan(0)
		fireEvent.click(screen.getByRole('button', { name: 'Formatted' }))
		expect(screen.getByTestId('smt-html')).toBeInTheDocument()
	})

	it('edit classification opens tag modal when tagName set', () => {
		mockParams.tagName = 'mytag'
		renderComp()
		const addTagBtn = document.querySelector('[data-cy="addTag"]') as HTMLElement
		fireEvent.click(addTagBtn)
		expect(screen.getByTestId('classification-form')).toBeInTheDocument()
		fireEvent.click(screen.getByText('close-cf'))
	})

	it('edit opens term form when gtype term and guid set', () => {
		mockGtype = 'term'
		mockParams.guid = 'g-term'
		renderComp()
		const addTagBtn = document.querySelector('[data-cy="addTag"]') as HTMLElement
		fireEvent.click(addTagBtn)
		expect(screen.getByTestId('edit-term')).toBeInTheDocument()
		fireEvent.click(screen.getByText('close-term'))
	})

	it('edit opens category form when gtype category', () => {
		mockGtype = 'category'
		mockParams.guid = 'g-cat'
		renderComp()
		const addTagBtn = document.querySelector('[data-cy="addTag"]') as HTMLElement
		fireEvent.click(addTagBtn)
		expect(screen.getByTestId('edit-category')).toBeInTheDocument()
		fireEvent.click(screen.getByText('close-category'))
	})

	it('hides edit button when bmguid present', () => {
		mockParams.bmguid = 'bm-1'
		renderComp()
		expect(document.querySelector('[data-cy="addTag"]')).toBeNull()
	})

	it('shows classifications for term gtype when loaded', () => {
		mockGtype = 'term'
		renderComp()
		expect(screen.getByTestId('smv-Classifications')).toBeInTheDocument()
		tooltipIconClick('Add Classifications')
		expect(screen.getByTestId('add-tag')).toBeInTheDocument()
		fireEvent.click(screen.getByText('close-tag'))
	})

	it('shows skeleton for classifications while loading', () => {
		mockGtype = 'term'
		renderComp({ loading: true })
		const loaders = screen.getAllByTestId('skeleton')
		expect(loaders.length).toBeGreaterThan(0)
	})

	it('category page shows terms and assign term flow', () => {
		mockGtype = 'category'
		mockParams.guid = 'gc'
		renderComp()
		expect(screen.getByTestId('smv-Terms')).toBeInTheDocument()
		tooltipIconClick('Add Term')
		expect(screen.getByTestId('assign-term')).toBeInTheDocument()
		fireEvent.click(screen.getByText('close-assign-term'))
	})

	it('toast when no glossary terms available for assign term', () => {
		mockGtype = 'category'
		mockParams.guid = 'gc'
		mockUseAppSelector.mockImplementation((fn: (s: unknown) => unknown) =>
			fn({
				glossary: {
					glossaryData: [{ terms: [] }],
				},
			}),
		)
		renderComp()
		tooltipIconClick('Add Term')
		expect(mockToast.info).toHaveBeenCalledWith('There are no available terms')
		expect(screen.queryByTestId('assign-term')).toBeNull()
	})

	it('term page shows categories and assign category', () => {
		mockGtype = 'term'
		mockParams.guid = 'gt'
		renderComp()
		expect(screen.getByTestId('smv-Category')).toBeInTheDocument()
		tooltipIconClick('Add Categories')
		expect(screen.getByTestId('assign-category')).toBeInTheDocument()
		fireEvent.click(screen.getByText('close-assign-cat'))
	})

	it('superTypes section loading and loaded', () => {
		const { rerender } = render(
			<MemoryRouter>
				<DetailPageAttribute {...defaultProps} superTypes={['x']} loading />
			</MemoryRouter>,
		)
		expect(screen.getAllByTestId('skeleton').length).toBeGreaterThan(0)
		rerender(
			<MemoryRouter>
				<DetailPageAttribute {...defaultProps} superTypes={['x']} loading={false} />
			</MemoryRouter>,
		)
		expect(screen.getByText('Direct super-classifications')).toBeInTheDocument()
		expect(screen.getByTestId('smv-Super Classifications')).toBeInTheDocument()
	})

	it('subTypes section loading and loaded', () => {
		const { rerender } = render(
			<MemoryRouter>
				<DetailPageAttribute {...defaultProps} subTypes={['s']} loading />
			</MemoryRouter>,
		)
		expect(screen.getAllByTestId('skeleton').length).toBeGreaterThan(0)
		rerender(
			<MemoryRouter>
				<DetailPageAttribute {...defaultProps} subTypes={['s']} loading={false} />
			</MemoryRouter>,
		)
		expect(screen.getByText('Direct sub-classifications')).toBeInTheDocument()
		expect(screen.getByTestId('smv-Sub Classifications')).toBeInTheDocument()
	})

	it('attribute defs section and add attributes modal', () => {
		renderComp()
		expect(screen.getByText(/Attributes/)).toBeInTheDocument()
		expect(screen.getByTestId('smv-Atrributes')).toBeInTheDocument()
		tooltipIconClick('Add Attributes')
		expect(screen.getByTestId('add-attrs')).toBeInTheDocument()
		fireEvent.click(screen.getByText('close-attrs'))
	})

	it('attribute defs loading shows skeleton', () => {
		renderComp({ loading: true, attributeDefs: [{}] })
		expect(screen.getAllByTestId('skeleton').length).toBeGreaterThan(0)
	})

	it('getDescriptionForDisplay via object shape description', () => {
		renderComp({ description: { k: 'inner' } })
		expect(screen.getByTestId('smt-html').textContent).toContain('san(inner)')
	})

	it('description object without string values uses empty string path', () => {
		renderComp({ description: { a: 1, b: 2 } })
		expect(screen.getByTestId('smt-html').textContent).toContain('san()')
	})

	it('getDescriptionForDisplay returns empty for numeric description', () => {
		renderComp({ description: 42 })
		expect(screen.getByTestId('smt-html').textContent).toContain('san()')
	})

	it('getDescriptionForDisplay returns empty for array description', () => {
		renderComp({ description: ['x'] })
		expect(screen.getByTestId('smt-html').textContent).toContain('san()')
	})

	it('getDescriptionForDisplay returns empty for undefined description', () => {
		renderComp({ description: undefined })
		expect(screen.getByTestId('smt-html').textContent).toContain('san()')
	})

	it('category Terms section shows skeleton while loading', () => {
		mockGtype = 'category'
		mockParams.guid = 'gc'
		renderComp({ loading: true })
		expect(screen.getAllByTestId('skeleton').length).toBeGreaterThan(0)
		expect(screen.queryByTestId('smv-Terms')).toBeNull()
	})

	it('term Categories section hidden when guid is empty and not loading', () => {
		mockGtype = 'term'
		mockParams.guid = ''
		renderComp({ loading: false })
		expect(screen.queryByTestId('smv-Category')).toBeNull()
	})

	it('treats hasAnyGlossaryTerms as false when glossaryData is not an array', () => {
		mockGtype = 'category'
		mockParams.guid = 'gc'
		mockUseAppSelector.mockImplementation((fn: (s: unknown) => unknown) =>
			fn({
				glossary: {
					glossaryData: { terms: [{ id: 't1' }] },
				},
			}),
		)
		renderComp()
		tooltipIconClick('Add Term')
		expect(mockToast.info).toHaveBeenCalledWith('There are no available terms')
	})

	it('omits Attributes block when attributeDefs is undefined', () => {
		renderComp({ attributeDefs: undefined })
		expect(screen.queryByText(/Attributes:/)).toBeNull()
		expect(screen.queryByTestId('smv-Atrributes')).toBeNull()
	})

	it('uses empty lists when data is null for optional ShowMore paths', () => {
		mockGtype = 'term'
		mockParams.guid = 'g1'
		renderComp({
			data: null,
			superTypes: [],
			subTypes: [],
		})
		expect(screen.getByTestId('smv-Classifications')).toBeInTheDocument()
	})

	it('category Terms list tolerates null entity data via optional chaining', () => {
		mockGtype = 'category'
		mockParams.guid = 'gc'
		renderComp({
			data: null,
			superTypes: [],
			subTypes: [],
		})
		expect(screen.getByTestId('smv-Terms')).toBeInTheDocument()
	})

	it('term Categories list tolerates null entity data via optional chaining', () => {
		mockGtype = 'term'
		mockParams.guid = 'gt'
		renderComp({
			data: null,
			superTypes: [],
			subTypes: [],
		})
		expect(screen.getByTestId('smv-Category')).toBeInTheDocument()
	})

	it('Sub Classifications ShowMore tolerates null entity data', () => {
		renderComp({
			data: null,
			superTypes: [],
			subTypes: ['s'],
			loading: false,
		})
		expect(screen.getByTestId('smv-Sub Classifications')).toBeInTheDocument()
	})

	it('Attributes ShowMore tolerates null entity data', () => {
		renderComp({
			data: null,
			superTypes: [],
			subTypes: [],
			attributeDefs: [{}],
			loading: false,
		})
		expect(screen.getByTestId('smv-Atrributes')).toBeInTheDocument()
	})

	it('uses empty superTypes list when data omits superTypes key', () => {
		renderComp({
			data: {
				name: 'OnlyName',
				classifications: [],
				terms: [],
				categories: [],
				subTypes: [{ z: 1 }],
				attributeDefs: [],
			},
			superTypes: ['trigger'],
			subTypes: [],
			loading: false,
		})
		expect(screen.getByTestId('smv-Super Classifications')).toBeInTheDocument()
	})

	it('hasAnyGlossaryTerms is false when glossary entry terms is not an array', () => {
		mockGtype = 'category'
		mockParams.guid = 'gc'
		mockUseAppSelector.mockImplementation((fn: (s: unknown) => unknown) =>
			fn({
				glossary: {
					glossaryData: [{ terms: 'not-array' }],
				},
			}),
		)
		renderComp()
		tooltipIconClick('Add Term')
		expect(mockToast.info).toHaveBeenCalledWith('There are no available terms')
	})

	it('hasAnyGlossaryTerms skips null glossary rows then finds terms', () => {
		mockGtype = 'category'
		mockParams.guid = 'gc'
		mockUseAppSelector.mockImplementation((fn: (s: unknown) => unknown) =>
			fn({
				glossary: {
					glossaryData: [null, { terms: [{ id: 't1' }] }],
				},
			}),
		)
		renderComp()
		tooltipIconClick('Add Term')
		expect(screen.getByTestId('assign-term')).toBeInTheDocument()
		fireEvent.click(screen.getByText('close-assign-term'))
	})

	it('AssignTerm receives empty object when entity data is null', () => {
		mockGtype = 'category'
		mockParams.guid = 'gc'
		mockUseAppSelector.mockImplementation((fn: (s: unknown) => unknown) =>
			fn({
				glossary: {
					glossaryData: [{ terms: [{ id: 't1' }] }],
				},
			}),
		)
		renderComp({
			data: null,
			superTypes: [],
			subTypes: [],
		})
		tooltipIconClick('Add Term')
		expect(screen.getByTestId('assign-term').getAttribute('data-received')).toBe('{}')
		fireEvent.click(screen.getByText('close-assign-term'))
	})

	it('AssignCategory receives empty object when entity data is null', () => {
		mockGtype = 'term'
		mockParams.guid = 'gt'
		renderComp({
			data: null,
			superTypes: [],
			subTypes: [],
		})
		tooltipIconClick('Add Categories')
		expect(screen.getByTestId('assign-category').getAttribute('data-received')).toBe(
			'{}',
		)
		fireEvent.click(screen.getByText('close-assign-cat'))
	})
})
