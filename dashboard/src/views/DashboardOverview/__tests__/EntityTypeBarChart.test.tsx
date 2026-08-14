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
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import userEvent from '@testing-library/user-event'

jest.mock('@utils/Helper', () => ({
	numberFormatWithComma: (n: number | string) => String(n),
}))

const mockNavigateToSearch = jest.fn()
const mockNavigateToServiceTypeEntitySearch = jest.fn()
jest.mock('@utils/dashboardSearchUtils', () => ({
	navigateToSearch: (...a: unknown[]) => mockNavigateToSearch(...a),
	navigateToServiceTypeEntitySearch: (...a: unknown[]) =>
		mockNavigateToServiceTypeEntitySearch(...a),
}))

let barClickPayload: unknown = null
let tooltipScenario:
	| 'full'
	| 'inactive'
	| 'emptyPayload'
	| 'noInnerPayload'
	| 'none'
	| 'activeNoPayload'
	| 'nullFirstPayload'
	| 'payloadNull' = 'full'

jest.mock('recharts', () => ({
	ResponsiveContainer: ({ children }: { children?: React.ReactNode }) => (
		<div data-testid="rc">{children}</div>
	),
	BarChart: ({ children }: { children?: React.ReactNode }) => (
		<div data-testid="bar-chart">{children}</div>
	),
	CartesianGrid: () => <div data-testid="grid" />,
	XAxis: ({ tickFormatter }: { tickFormatter?: (v: number) => string }) => (
		<div data-testid="x-axis">
			{tickFormatter ? tickFormatter(1000) : ''}
		</div>
	),
	YAxis: ({ tick }: { tick?: React.ComponentType<Record<string, unknown>> }) => {
		const Tick = tick
		if (!Tick) return null
		return (
			<div data-testid="y-axis">
				<Tick x={10} y={20} payload={{ value: 'hive' }} />
				<Tick x={10} y={40} payload={{}} />
				<Tick
					x={10}
					y={60}
					payload={{ value: 'kafka', name: 'kafka' }}
				/>
				<Tick x={10} y={100} payload={{ name: 'nameonly' }} />
				<Tick
					x={10}
					y={120}
					payload={'fromstr' as unknown as { value?: string; name?: string }}
				/>
				<Tick
					x={10}
					y={140}
					payload={{
						value: null as unknown as string,
						name: 'nullCoalesce',
					}}
				/>
				<Tick x={10} y={160} />
			</div>
		)
	},
	Tooltip: ({ content }: { content?: (p: unknown) => React.ReactNode }) => {
		const row = {
			name: 'hive',
			active: 3,
			deleted: 2,
			count: 5,
		}
		let props: unknown
		if (tooltipScenario === 'inactive') props = { active: false }
		else if (tooltipScenario === 'activeNoPayload') props = { active: true }
		else if (tooltipScenario === 'emptyPayload') props = { active: true, payload: [] }
		else if (tooltipScenario === 'noInnerPayload') {
			props = { active: true, payload: [{}] }
		} else if (tooltipScenario === 'nullFirstPayload') {
			props = { active: true, payload: [null] }
		} else if (tooltipScenario === 'payloadNull') {
			props = { active: true, payload: null }
		} else if (tooltipScenario === 'none') {
			props = null
		} else {
			props = { active: true, payload: [{ payload: row }] }
		}
		const node =
			typeof content === 'function' ? content(props) : null
		return <div data-testid="tooltip-mock">{node}</div>
	},
	Bar: ({
		dataKey,
		onClick,
		children,
	}: {
		dataKey?: string
		onClick?: (e: unknown) => void
		children?: React.ReactNode
	}) => (
		<button
			type="button"
			data-testid={`bar-${dataKey}`}
			onClick={() =>
				onClick?.(
					barClickPayload ?? {
						payload: {
							name: 'hive',
							active: 1,
							deleted: 1,
							count: 2,
							underlyingTypeNames: ['hive_table'],
						},
					},
				)
			}
		>
			{children}
		</button>
	),
	Cell: () => <span data-testid="cell" />,
	LabelList: ({
		formatter,
	}: {
		formatter?: (v: number) => string
	}) => (
		<span data-testid="label-list" data-fmt={formatter ? formatter(7) : ''} />
	),
}))

import EntityTypeBarChart from '../EntityTypeBarChart'

const navigateMock = jest.fn()

jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useNavigate: () => navigateMock,
}))

describe('EntityTypeBarChart', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		barClickPayload = null
		tooltipScenario = 'full'
	})

	const chartEntity = {
		entityActive: { hive_table: 4 },
		entityDeleted: { hive_table: 1 },
	}

	it('returns null while loading', () => {
		const { container } = render(
			<MemoryRouter>
				<EntityTypeBarChart entity={chartEntity} isLoading />
			</MemoryRouter>,
		)
		expect(container.firstChild).toBeNull()
	})

	it('shows empty state when no service type data', () => {
		render(
			<MemoryRouter>
				<EntityTypeBarChart entity={{}} typeHeaderData={[]} />
			</MemoryRouter>,
		)
		expect(
			screen.getByText('No service type data available'),
		).toBeInTheDocument()
	})

	it('View All navigates to all entities search', async () => {
		const user = userEvent.setup()
		render(
			<MemoryRouter>
				<EntityTypeBarChart entity={chartEntity} />
			</MemoryRouter>,
		)
		await user.click(screen.getByRole('button', { name: /view all entities/i }))
		expect(mockNavigateToSearch).toHaveBeenCalled()
	})

	it('active bar click navigates with includeDeleted false', async () => {
		const user = userEvent.setup()
		render(
			<MemoryRouter>
				<EntityTypeBarChart entity={chartEntity} />
			</MemoryRouter>,
		)
		await user.click(screen.getByTestId('bar-active'))
		expect(mockNavigateToServiceTypeEntitySearch).toHaveBeenCalledWith(
			navigateMock,
			['hive_table'],
			false,
		)
	})

	it('deleted bar click navigates with includeDeleted true', async () => {
		const user = userEvent.setup()
		render(
			<MemoryRouter>
				<EntityTypeBarChart entity={chartEntity} />
			</MemoryRouter>,
		)
		await user.click(screen.getByTestId('bar-deleted'))
		expect(mockNavigateToServiceTypeEntitySearch).toHaveBeenCalledWith(
			navigateMock,
			['hive_table'],
			true,
		)
	})

	it('uses row.name when underlyingTypeNames empty on bar click', async () => {
		barClickPayload = {
			payload: {
				name: 'solo',
				active: 1,
				deleted: 0,
				count: 1,
				underlyingTypeNames: [],
			},
		}
		const user = userEvent.setup()
		render(
			<MemoryRouter>
				<EntityTypeBarChart entity={chartEntity} />
			</MemoryRouter>,
		)
		await user.click(screen.getByTestId('bar-active'))
		expect(mockNavigateToServiceTypeEntitySearch).toHaveBeenCalledWith(
			navigateMock,
			['solo'],
			false,
		)
	})

	it('uses row.name when underlyingTypeNames is undefined', async () => {
		barClickPayload = {
			payload: {
				name: 'alone',
				active: 1,
				deleted: 0,
				count: 1,
			},
		}
		const user = userEvent.setup()
		render(
			<MemoryRouter>
				<EntityTypeBarChart entity={chartEntity} />
			</MemoryRouter>,
		)
		await user.click(screen.getByTestId('bar-active'))
		expect(mockNavigateToServiceTypeEntitySearch).toHaveBeenCalledWith(
			navigateMock,
			['alone'],
			false,
		)
	})

	it('ignores bar click when payload missing', async () => {
		barClickPayload = null
		const user = userEvent.setup()
		render(
			<MemoryRouter>
				<EntityTypeBarChart entity={chartEntity} />
			</MemoryRouter>,
		)
		mockNavigateToServiceTypeEntitySearch.mockClear()
		barClickPayload = 'bad' as unknown
		await user.click(screen.getByTestId('bar-active'))
		expect(mockNavigateToServiceTypeEntitySearch).not.toHaveBeenCalled()
	})

	it('YAxis tick click for unknown label does not navigate', () => {
		render(
			<MemoryRouter>
				<EntityTypeBarChart entity={chartEntity} />
			</MemoryRouter>,
		)
		const ticks = document.querySelectorAll('g[role="button"]')
		mockNavigateToServiceTypeEntitySearch.mockClear()
		if (ticks.length >= 3) {
			fireEvent.click(ticks[2])
		}
		expect(mockNavigateToServiceTypeEntitySearch).not.toHaveBeenCalled()
	})

	it('YAxis tick with empty label skips navigation on click', () => {
		render(
			<MemoryRouter>
				<EntityTypeBarChart entity={chartEntity} />
			</MemoryRouter>,
		)
		const y = screen.getByTestId('y-axis')
		const groups = y.querySelectorAll('g')
		mockNavigateToServiceTypeEntitySearch.mockClear()
		fireEvent.click(groups[1])
		expect(mockNavigateToServiceTypeEntitySearch).not.toHaveBeenCalled()
	})

	it('YAxis tick keyboard Enter triggers label navigation', () => {
		render(
			<MemoryRouter>
				<EntityTypeBarChart entity={chartEntity} />
			</MemoryRouter>,
		)
		const tickButtons = document.querySelectorAll('g[role="button"]')
		expect(tickButtons.length).toBeGreaterThan(0)
		mockNavigateToServiceTypeEntitySearch.mockClear()
		fireEvent.keyDown(tickButtons[0], { key: 'Enter', preventDefault: jest.fn() })
		expect(mockNavigateToServiceTypeEntitySearch).toHaveBeenCalled()
	})

	it('YAxis tick Space key triggers label navigation', () => {
		render(
			<MemoryRouter>
				<EntityTypeBarChart entity={chartEntity} />
			</MemoryRouter>,
		)
		const tickButtons = document.querySelectorAll('g[role="button"]')
		mockNavigateToServiceTypeEntitySearch.mockClear()
		fireEvent.keyDown(tickButtons[0], { key: ' ', preventDefault: jest.fn() })
		expect(mockNavigateToServiceTypeEntitySearch).toHaveBeenCalled()
	})

	it('YAxis tick click navigates for label', () => {
		render(
			<MemoryRouter>
				<EntityTypeBarChart entity={chartEntity} />
			</MemoryRouter>,
		)
		const tickButtons = document.querySelectorAll('g[role="button"]')
		mockNavigateToServiceTypeEntitySearch.mockClear()
		fireEvent.click(tickButtons[0])
		expect(mockNavigateToServiceTypeEntitySearch).toHaveBeenCalled()
		expect(screen.getByTestId('label-list')).toHaveAttribute('data-fmt', '7')
	})

	it('renders with typeHeaderData path', () => {
		render(
			<MemoryRouter>
				<EntityTypeBarChart
					entity={chartEntity}
					typeHeaderData={[
						{ name: 'hive_table', category: 'ENTITY', serviceType: 'hive' },
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Service Type Distribution')).toBeInTheDocument()
	})

	it('active bar prefers underlyingTypeNames when multiple', async () => {
		barClickPayload = {
			payload: {
				name: 'hive',
				active: 1,
				deleted: 0,
				count: 1,
				underlyingTypeNames: ['a', 'b'],
			},
		}
		const user = userEvent.setup()
		render(
			<MemoryRouter>
				<EntityTypeBarChart entity={chartEntity} />
			</MemoryRouter>,
		)
		await user.click(screen.getByTestId('bar-active'))
		expect(mockNavigateToServiceTypeEntitySearch).toHaveBeenCalledWith(
			navigateMock,
			['a', 'b'],
			false,
		)
	})

	it('tooltip scenarios cover branches', () => {
		tooltipScenario = 'full'
		const { rerender, getByTestId } = render(
			<MemoryRouter>
				<EntityTypeBarChart key="tt-full" entity={chartEntity} />
			</MemoryRouter>,
		)
		expect(getByTestId('tooltip-mock').textContent).toContain('hive')

		tooltipScenario = 'activeNoPayload'
		rerender(
			<MemoryRouter>
				<EntityTypeBarChart key="tt-nopayloadkey" entity={chartEntity} />
			</MemoryRouter>,
		)
		expect(getByTestId('tooltip-mock')).toBeEmptyDOMElement()

		tooltipScenario = 'inactive'
		rerender(
			<MemoryRouter>
				<EntityTypeBarChart key="tt-inactive" entity={chartEntity} />
			</MemoryRouter>,
		)
		expect(getByTestId('tooltip-mock')).toBeEmptyDOMElement()

		tooltipScenario = 'emptyPayload'
		rerender(
			<MemoryRouter>
				<EntityTypeBarChart key="tt-empty" entity={chartEntity} />
			</MemoryRouter>,
		)
		expect(getByTestId('tooltip-mock')).toBeEmptyDOMElement()

		tooltipScenario = 'noInnerPayload'
		rerender(
			<MemoryRouter>
				<EntityTypeBarChart key="tt-noinner" entity={chartEntity} />
			</MemoryRouter>,
		)
		expect(getByTestId('tooltip-mock')).toBeEmptyDOMElement()

		tooltipScenario = 'nullFirstPayload'
		rerender(
			<MemoryRouter>
				<EntityTypeBarChart key="tt-nullfirst" entity={chartEntity} />
			</MemoryRouter>,
		)
		expect(getByTestId('tooltip-mock')).toBeEmptyDOMElement()

		tooltipScenario = 'payloadNull'
		rerender(
			<MemoryRouter>
				<EntityTypeBarChart key="tt-payloadnull" entity={chartEntity} />
			</MemoryRouter>,
		)
		expect(getByTestId('tooltip-mock')).toBeEmptyDOMElement()

		tooltipScenario = 'none'
		rerender(
			<MemoryRouter>
				<EntityTypeBarChart key="tt-none" entity={chartEntity} />
			</MemoryRouter>,
		)
		expect(getByTestId('tooltip-mock')).toBeEmptyDOMElement()
		tooltipScenario = 'full'
	})
})
