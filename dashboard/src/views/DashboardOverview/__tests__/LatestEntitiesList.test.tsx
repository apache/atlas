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
import { render, screen, fireEvent, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'

jest.mock('@utils/dashboardSearchUtils', () => ({
	navigateToLatestEntitiesSearch: jest.fn(),
}))

jest.mock('moment', () => {
	const actual = jest.requireActual('moment')
	const invalidMarkerMs = Date.parse('1985-06-15T00:00:00.000Z')
	const shim = function momentShim(this: unknown, ...args: unknown[]) {
		const first = args[0]
		if (first === invalidMarkerMs) {
			return { isValid: () => false }
		}
		return actual.apply(this, args as [unknown])
	}
	return Object.assign(shim, actual)
})

import { navigateToLatestEntitiesSearch } from '@utils/dashboardSearchUtils'
import LatestEntitiesList from '../LatestEntitiesList'

const navigateMock = jest.fn()

jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useNavigate: () => navigateMock,
}))

describe('LatestEntitiesList', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		jest.useFakeTimers()
		jest.setSystemTime(new Date('2026-05-10T12:00:00.000Z'))
	})

	afterEach(() => {
		jest.useRealTimers()
	})

	it('returns null while loading', () => {
		const { container } = render(
			<MemoryRouter>
				<LatestEntitiesList entities={[]} isLoading />
			</MemoryRouter>,
		)
		expect(container.firstChild).toBeNull()
	})

	it('shows error message', () => {
		render(
			<MemoryRouter>
				<LatestEntitiesList entities={[]} error="Load failed" />
			</MemoryRouter>,
		)
		expect(screen.getByText('Load failed')).toBeInTheDocument()
	})

	it('returns null while loading even when error and entities are set', () => {
		const { container } = render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{ guid: 'g1', name: 'A', typeName: 'T', attributes: {} },
					]}
					isLoading
					error="Load failed"
				/>
			</MemoryRouter>,
		)
		expect(container.firstChild).toBeNull()
		expect(screen.queryByText('Load failed')).not.toBeInTheDocument()
		expect(screen.queryByText('A')).not.toBeInTheDocument()
	})

	it('shows error instead of entity list when error is set with entities', () => {
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{ guid: 'g1', name: 'Row', typeName: 'T', attributes: {} },
					]}
					error="Refresh failed"
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Refresh failed')).toBeInTheDocument()
		expect(screen.queryByText('Row')).not.toBeInTheDocument()
		expect(screen.queryByText('No recent entities')).not.toBeInTheDocument()
	})

	it('shows empty when entities missing or empty', () => {
		const { rerender } = render(
			<MemoryRouter>
				<LatestEntitiesList entities={[]} />
			</MemoryRouter>,
		)
		expect(screen.getByText('No recent entities')).toBeInTheDocument()
		rerender(
			<MemoryRouter>
				<LatestEntitiesList entities={undefined as unknown as []} />
			</MemoryRouter>,
		)
		expect(screen.getByText('No recent entities')).toBeInTheDocument()
	})

	it('View All navigates via dashboardSearchUtils', () => {
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{ guid: 'g1', name: 'A', typeName: 'T', attributes: { __timestamp: 1000 } },
					]}
				/>
			</MemoryRouter>,
		)
		fireEvent.click(screen.getByRole('button', { name: /view all entities/i }))
		expect(navigateToLatestEntitiesSearch).toHaveBeenCalledWith(navigateMock)
	})

	it('renders link when guid present and slices to seven', () => {
		const many = Array.from({ length: 9 }, (_, i) => ({
			guid: `id-${i}`,
			name: `Entity-${i}`,
			typeName: 'hive_table',
			attributes: { __timestamp: 1_700_000_000_000 + i },
		}))
		render(
			<MemoryRouter>
				<LatestEntitiesList entities={many} />
			</MemoryRouter>,
		)
		expect(screen.getByRole('link', { name: 'Entity-0' })).toHaveAttribute(
			'href',
			'/detailPage/id-0',
		)
		expect(screen.queryByText('Entity-8')).not.toBeInTheDocument()
	})

	it('renders span without link when guid missing', () => {
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[{ name: 'Orphan', typeName: 'hive_table' }]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Orphan')).toBeInTheDocument()
		expect(screen.queryByRole('link', { name: 'Orphan' })).toBeNull()
	})

	it('uses top-level __timestamp when attributes missing', () => {
		jest.setSystemTime(new Date('2026-05-10T12:00:10.000Z'))
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'x',
							name: 'X',
							typeName: 't',
							attributes: {},
							__timestamp: 1_000,
						} as never,
					]}
				/>
			</MemoryRouter>,
		)
		const row = screen.getByText('X').closest('li')
		expect(row).toBeTruthy()
		expect(
			within(row as HTMLElement).getByText(/^Created /),
		).toBeInTheDocument()
	})

	it('shows Created today for unusable timestamp', () => {
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'g',
							name: 'BadTs',
							typeName: 't',
							attributes: { __timestamp: '' },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Created today')).toBeInTheDocument()
	})

	it('relative: just now, seconds, minutes, hours, fromNow future', () => {
		const base = new Date('2026-05-10T12:00:00.000Z').getTime()
		jest.setSystemTime(new Date(base))
		const { rerender } = render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'a',
							name: 'A',
							typeName: 't',
							attributes: { __timestamp: base - 500 },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Created just now')).toBeInTheDocument()

		jest.setSystemTime(new Date(base))
		rerender(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'a',
							name: 'A',
							typeName: 't',
							attributes: { __timestamp: base - 30_000 },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Created 30 seconds ago')).toBeInTheDocument()

		jest.setSystemTime(new Date(base))
		rerender(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'a',
							name: 'A',
							typeName: 't',
							attributes: { __timestamp: base - 90_000 },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Created 1 minute ago')).toBeInTheDocument()

		jest.setSystemTime(new Date(base))
		rerender(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'a',
							name: 'A',
							typeName: 't',
							attributes: { __timestamp: base - 120_000 },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Created 2 minutes ago')).toBeInTheDocument()

		jest.setSystemTime(new Date(base))
		rerender(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'a',
							name: 'A',
							typeName: 't',
							attributes: { __timestamp: base - 3_600_000 },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Created 1 hour ago')).toBeInTheDocument()

		jest.setSystemTime(new Date(base))
		rerender(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'a',
							name: 'A',
							typeName: 't',
							attributes: { __timestamp: base - 7_200_000 },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Created 2 hours ago')).toBeInTheDocument()

		jest.setSystemTime(new Date(base))
		rerender(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'a',
							name: 'A',
							typeName: 't',
							attributes: { __timestamp: base + 86_400_000 },
						},
					]}
				/>
			</MemoryRouter>,
		)
		const li = screen.getByText('A').closest('li') as HTMLElement
		expect(within(li).getByText(/Created in /)).toBeInTheDocument()
	})

	it('normalizeEntityTimestampMs: $numberLong and longValue wrappers', () => {
		jest.setSystemTime(new Date('2026-05-10T15:00:00.000Z'))
		const ts = 1_700_000_000_000
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: '1',
							name: 'L1',
							typeName: 't',
							attributes: { __timestamp: { $numberLong: String(ts) } },
						},
						{
							guid: '2',
							name: 'L2',
							typeName: 't',
							attributes: { __timestamp: { longValue: ts } },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('L1')).toBeInTheDocument()
		expect(screen.getByText('L2')).toBeInTheDocument()
	})

	it('timestamp string numeric seconds and ms and ISO string', () => {
		jest.setSystemTime(new Date('2026-05-10T12:00:00.000Z'))
		const sec = 1_700_000_000
		const ms = sec * 1000
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 's',
							name: 'Sec',
							typeName: 't',
							attributes: { __timestamp: String(sec) },
						},
						{
							guid: 'm',
							name: 'Ms',
							typeName: 't',
							attributes: { __timestamp: String(ms) },
						},
						{
							guid: 'iso',
							name: 'Iso',
							typeName: 't',
							attributes: { __timestamp: '2020-01-01T00:00:01.000Z' },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Sec')).toBeInTheDocument()
	})

	it('timestamp rejects zero epoch and invalid date', () => {
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'z',
							name: 'Zero',
							typeName: 't',
							attributes: { __timestamp: 0 },
						},
						{
							guid: 'i',
							name: 'Inv',
							typeName: 't',
							attributes: { __timestamp: new Date(NaN) },
						},
					]}
				/>
			</MemoryRouter>,
		)
		const labels = screen.getAllByText('Created today')
		expect(labels.length).toBeGreaterThanOrEqual(2)
	})

	it('formatCreatedRelativeFromMs uses fallback when delta is not finite', () => {
		jest.spyOn(Date, 'now').mockReturnValue(Number.NaN as unknown as number)
		jest.setSystemTime(new Date('2026-05-10T12:00:00.000Z'))
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'a',
							name: 'N',
							typeName: 't',
							attributes: { __timestamp: Date.now() },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Created today')).toBeInTheDocument()
		jest.spyOn(Date, 'now').mockRestore()
	})

	it('normalizeEntityTimestampMs rejects invalid Date and epoch Date', () => {
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'd1',
							name: 'InvD',
							typeName: 't',
							attributes: { __timestamp: new Date(Number.NaN) },
						},
						{
							guid: 'd2',
							name: 'Epoch',
							typeName: 't',
							attributes: { __timestamp: new Date(0) },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getAllByText('Created today').length).toBeGreaterThanOrEqual(2)
	})

	it('timestamp string with no valid numeric or date parse returns Created today', () => {
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'g',
							name: 'Garb',
							typeName: 't',
							attributes: { __timestamp: 'not-a-parseable-date-xyz' },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Created today')).toBeInTheDocument()
	})

	it('timestamp number with invalid moment after scale returns Created today', () => {
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'g',
							name: 'NumBad',
							typeName: 't',
							attributes: { __timestamp: 9e15 },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Created today')).toBeInTheDocument()
	})

	it('unwrapLongLike returns raw for array payload', () => {
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'a',
							name: 'Arr',
							typeName: 't',
							attributes: { __timestamp: [1, 2] as unknown as number },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Created today')).toBeInTheDocument()
	})

	it('uses twenty-five hour bucket for fromNow wording', () => {
		const baseMs = new Date('2026-05-10T12:00:00.000Z').getTime()
		jest.setSystemTime(new Date(baseMs))
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'a',
							name: 'Old',
							typeName: 't',
							attributes: { __timestamp: baseMs - 30 * 3_600_000 },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(
			within(screen.getByText('Old').closest('li') as HTMLElement).getByText(
				/^Created /,
			),
		).toBeInTheDocument()
	})

	it('single second ago plural branch', () => {
		const baseMs = new Date('2026-05-10T12:00:00.000Z').getTime()
		jest.setSystemTime(new Date(baseMs))
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'a',
							name: 'OneSec',
							typeName: 't',
							attributes: { __timestamp: baseMs - 1_000 },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Created 1 second ago')).toBeInTheDocument()
	})

	it('uses Created today when Date.now is non-finite for relative time', () => {
		const spy = jest.spyOn(Date, 'now').mockReturnValue(Number.NaN)
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'g1',
							name: 'A',
							typeName: 'T',
							attributes: { __timestamp: 1_700_000_000_000 },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(
			within(screen.getByRole('listitem')).getByText(/^Created /),
		).toHaveTextContent('Created today')
		spy.mockRestore()
	})

	it('Date __timestamp uses getTime when moment accepts', () => {
		jest.setSystemTime(new Date('2026-05-10T12:00:00.000Z'))
		const historic = new Date('2020-01-01T00:00:00.000Z')
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'g1',
							name: 'Historic',
							typeName: 'T',
							attributes: { __timestamp: historic },
						},
					]}
				/>
			</MemoryRouter>,
		)
		const row = screen.getByText('Historic').closest('li') as HTMLElement
		expect(
			within(row).getByText(/^Created /).textContent,
		).not.toMatch(/^Created today$/)
	})

	it('Date __timestamp returns null when moment rejects ms validity', () => {
		const markerMs = Date.parse('1985-06-15T00:00:00.000Z')
		render(
			<MemoryRouter>
				<LatestEntitiesList
					entities={[
						{
							guid: 'g1',
							name: 'Stamp',
							typeName: 'T',
							attributes: { __timestamp: new Date(markerMs) },
						},
					]}
				/>
			</MemoryRouter>,
		)
		expect(screen.getByText('Created today')).toBeInTheDocument()
	})
})
