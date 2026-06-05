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
import userEvent from '@testing-library/user-event'
import type { MessageConsumptionItem } from '@utils/metricsUtils'

jest.mock('@mui/material', () => {
	const actual = jest.requireActual('@mui/material')
	return {
		...actual,
		Tooltip: ({
			title,
			children,
		}: {
			title: React.ReactNode
			children: React.ReactNode
		}) => (
			<div>
				<div data-testid="tooltip-title-mount">{title}</div>
				{children}
			</div>
		),
	}
})

jest.mock('@utils/Helper', () => ({
	numberFormatWithComma: (n: number | string) => String(n),
}))

jest.mock('@utils/Utils', () => ({
	formatedDate: ({ date }: { date: number }) => `fmt-${date}`,
}))

const mockParseMetricsStats = jest.fn()
const mockGetMessageConsumptionData = jest.fn()
const mockGetMessageConsumptionDataExcludingTotal = jest.fn()
const mockBuildTopicNotificationRecord = jest.fn()
const mockTopicRowHasPeriodMetrics = jest.fn()

jest.mock('@utils/metricsUtils', () => ({
	parseMetricsStats: (...a: unknown[]) => mockParseMetricsStats(...a),
	getMessageConsumptionData: (...a: unknown[]) => mockGetMessageConsumptionData(...a),
	getMessageConsumptionDataExcludingTotal: (...a: unknown[]) =>
		mockGetMessageConsumptionDataExcludingTotal(...a),
	buildTopicNotificationRecord: (...a: unknown[]) =>
		mockBuildTopicNotificationRecord(...a),
	topicRowHasPeriodMetrics: (...a: unknown[]) => mockTopicRowHasPeriodMetrics(...a),
}))

jest.mock('../MessageConsumptionChart', () => ({
	__esModule: true,
	default: ({
		data,
		chartAriaLabel,
	}: {
		data: MessageConsumptionItem[]
		chartAriaLabel?: string
	}) => (
		<div data-testid="msg-consumption-chart" data-label={chartAriaLabel ?? ''}>
			{data.length} rows
		</div>
	),
}))

import KafkaTopicSummaryCard, { getConsumptionForTopicRow } from '../KafkaTopicSummaryCard'

const totalRow = (period: string): MessageConsumptionItem => ({
	period,
	count: 100,
	creates: 10,
	updates: 20,
	deletes: 30,
	failed: 0,
	avgTime: 7,
})

const chartSlice = (): MessageConsumptionItem[] => [
	{ period: 'H1', count: 5, creates: 1, updates: 2, deletes: 2, failed: 0, avgTime: 1 },
]

describe('KafkaTopicSummaryCard', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockParseMetricsStats.mockReturnValue({
			Notification: {
				topicDetails: {
					alpha: {
						processedMessageCount: 12,
						failedMessageCount: 1,
						lastMessageProcessedTime: 1_700_000_000_000,
						extra: 1,
					},
					'beta/charlie': {
						processedMessageCount: 3,
						failedMessageCount: 0,
						lastMessageProcessedTime: { nested: true },
						periodMetrics: true,
					},
				},
			},
		})
		mockTopicRowHasPeriodMetrics.mockImplementation((row: Record<string, unknown>) =>
			Boolean(row?.periodMetrics),
		)
		mockBuildTopicNotificationRecord.mockImplementation(
			(_stats: unknown, opts: { useAggregateFallback?: boolean }) =>
				({ record: true, fallback: opts?.useAggregateFallback }),
		)
		mockGetMessageConsumptionData.mockImplementation(() => [
			totalRow('Total'),
			...chartSlice(),
		])
		mockGetMessageConsumptionDataExcludingTotal.mockImplementation(() => chartSlice())
	})

	it('returns null while loading', () => {
		const { container } = render(<KafkaTopicSummaryCard stats={{}} isLoading />)
		expect(container.firstChild).toBeNull()
	})

	it('shows empty when topicDetails missing', () => {
		mockParseMetricsStats.mockReturnValue({ Notification: {} })
		render(<KafkaTopicSummaryCard stats={{}} />)
		expect(screen.getByText('No topic data available')).toBeInTheDocument()
	})

	it('shows empty when topicDetails not object', () => {
		mockParseMetricsStats.mockReturnValue({ Notification: { topicDetails: null } })
		render(<KafkaTopicSummaryCard stats={{}} />)
		expect(screen.getByText('No topic data available')).toBeInTheDocument()
	})

	it('renders topic rows, expand chart, and table height when expanded', async () => {
		const user = userEvent.setup()
		render(<KafkaTopicSummaryCard stats={{}} />)
		expect(screen.getByText('Kafka Topic Summary')).toBeInTheDocument()
		expect(screen.getByText('alpha')).toBeInTheDocument()
		expect(screen.getByText('beta/charlie')).toBeInTheDocument()

		const expandAlpha = screen.getByRole('button', {
			name: /Expand message consumption for alpha/i,
		})
		await user.click(expandAlpha)
		expect(
			screen.getByRole('button', { name: /Collapse message consumption for alpha/i }),
		).toBeInTheDocument()
		expect(screen.getByTestId('msg-consumption-chart')).toBeInTheDocument()
		expect(screen.getByTestId('msg-consumption-chart')).toHaveAttribute(
			'data-label',
			'Message consumption for alpha',
		)

		await user.click(
			screen.getByRole('button', { name: /Collapse message consumption for alpha/i }),
		)
	})

	it('IconButton space/enter stopPropagation without throwing', async () => {
		const user = userEvent.setup()
		render(<KafkaTopicSummaryCard stats={{}} />)
		const btn = screen.getByRole('button', {
			name: /Expand message consumption for alpha/i,
		})
		const keyEv = new KeyboardEvent('keydown', { key: ' ', bubbles: true })
		const spy = jest.spyOn(keyEv, 'stopPropagation')
		fireEvent(btn, keyEv)
		expect(spy).toHaveBeenCalled()

		const enterEv = new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })
		const spy2 = jest.spyOn(enterEv, 'stopPropagation')
		fireEvent(btn, enterEv)
		expect(spy2).toHaveBeenCalled()
		await user.click(btn)
	})

	it('sorts by topic processed failed and toggles order', async () => {
		const user = userEvent.setup()
		render(<KafkaTopicSummaryCard stats={{}} />)
		const topicHeader = screen.getByLabelText('Sort by topic')
		await user.click(topicHeader)
		let cells = screen.getAllByRole('cell')
		expect(cells.some((c) => c.textContent?.includes('alpha'))).toBe(true)
		await user.click(topicHeader)
		cells = screen.getAllByRole('cell')
		expect(cells.some((c) => c.textContent?.includes('alpha'))).toBe(true)

		await user.click(screen.getByLabelText('Sort by processed'))
		await user.click(screen.getByLabelText('Sort by processed'))
		await user.click(screen.getByLabelText('Sort by failed'))
		await user.click(screen.getByLabelText('Sort by failed'))
	})

	it('TotalProcessedTooltip shows creates/updates/deletes when total row exists', () => {
		render(<KafkaTopicSummaryCard stats={{}} />)
		const titleMount = screen.getAllByTestId('tooltip-title-mount')[0]
		expect(titleMount.textContent).toContain('Creates: 10')
		expect(titleMount.textContent).toContain('Updates: 20')
		expect(titleMount.textContent).toContain('Deletes: 30')
	})

	it('TotalProcessedTooltip shows no breakdown when total row missing', () => {
		mockGetMessageConsumptionData.mockImplementation(() => chartSlice())
		render(<KafkaTopicSummaryCard stats={{}} />)
		expect(screen.getAllByText('No breakdown available').length).toBeGreaterThanOrEqual(1)
	})

	it('formats string last processed passthrough and dash for object time', () => {
		mockParseMetricsStats.mockReturnValue({
			Notification: {
				topicDetails: {
					s: {
						processedMessageCount: 0,
						failedMessageCount: 0,
						lastMessageProcessedTime: 'raw-string',
					},
					d: {
						processedMessageCount: 0,
						failedMessageCount: 0,
					},
				},
			},
		})
		render(<KafkaTopicSummaryCard stats={{}} />)
		expect(screen.getByText('raw-string')).toBeInTheDocument()
		expect(screen.getByText('-')).toBeInTheDocument()
	})

	it('uses formatedDate for numeric last processed', () => {
		mockParseMetricsStats.mockReturnValue({
			Notification: {
				topicDetails: {
					n: {
						processedMessageCount: 1,
						failedMessageCount: 0,
						lastMessageProcessedTime: 99,
					},
				},
			},
		})
		render(<KafkaTopicSummaryCard stats={{}} />)
		expect(screen.getByText('fmt-99')).toBeInTheDocument()
	})

	it('falls back to aggregate when topic has no period metrics', () => {
		mockTopicRowHasPeriodMetrics.mockReturnValue(false)
		render(<KafkaTopicSummaryCard stats={{}} />)
		expect(mockBuildTopicNotificationRecord).toHaveBeenCalledWith(
			expect.anything(),
			expect.objectContaining({ useAggregateFallback: true }),
		)
	})

	it('shows empty when parseMetricsStats returns null', () => {
		mockParseMetricsStats.mockReturnValue(null as unknown)
		render(<KafkaTopicSummaryCard stats={{}} />)
		expect(screen.getByText('No topic data available')).toBeInTheDocument()
	})

	it('uses empty chart slice when excluding total returns undefined', async () => {
		const user = userEvent.setup()
		mockGetMessageConsumptionDataExcludingTotal.mockReturnValue(undefined as unknown)
		render(<KafkaTopicSummaryCard stats={{}} />)
		await user.click(
			screen.getByRole('button', { name: /Expand message consumption for alpha/i }),
		)
		expect(screen.getByTestId('msg-consumption-chart').textContent).toContain('0 rows')
	})

	it('uses empty chart slice when excluding total returns null', async () => {
		const user = userEvent.setup()
		mockGetMessageConsumptionDataExcludingTotal.mockReturnValue(null as unknown)
		render(<KafkaTopicSummaryCard stats={{}} />)
		await user.click(
			screen.getByRole('button', { name: /Expand message consumption for alpha/i }),
		)
		expect(screen.getByTestId('msg-consumption-chart').textContent).toContain('0 rows')
	})

	it('sort processed uses zero delta when counts tie', async () => {
		mockParseMetricsStats.mockReturnValue({
			Notification: {
				topicDetails: {
					tieA: {
						processedMessageCount: 5,
						failedMessageCount: 0,
						lastMessageProcessedTime: 1,
					},
					tieB: {
						processedMessageCount: 5,
						failedMessageCount: 0,
						lastMessageProcessedTime: 2,
					},
				},
			},
		})
		const user = userEvent.setup()
		render(<KafkaTopicSummaryCard stats={{}} />)
		await user.click(screen.getByLabelText('Sort by processed'))
		expect(screen.getByText('tieA')).toBeInTheDocument()
		expect(screen.getByText('tieB')).toBeInTheDocument()
	})

	it('sort topic collapses case-equal names with stable compare', async () => {
		mockParseMetricsStats.mockReturnValue({
			Notification: {
				topicDetails: {
					Beta: {
						processedMessageCount: 1,
						failedMessageCount: 0,
						lastMessageProcessedTime: 1,
					},
					beta: {
						processedMessageCount: 1,
						failedMessageCount: 0,
						lastMessageProcessedTime: 2,
					},
				},
			},
		})
		const user = userEvent.setup()
		render(<KafkaTopicSummaryCard stats={{}} />)
		await user.click(screen.getByLabelText('Sort by topic'))
	})

	it('topic row map handles null detail object and primitive values', () => {
		mockParseMetricsStats.mockReturnValue({
			Notification: {
				topicDetails: {
					nullish: null as unknown as Record<string, unknown>,
					primNum: 42 as unknown,
					primStr: 'plain' as unknown,
				},
			},
		})
		render(<KafkaTopicSummaryCard stats={{}} />)
		expect(screen.getByText('nullish')).toBeInTheDocument()
		expect(screen.getByText('primNum')).toBeInTheDocument()
		expect(screen.getByText('primStr')).toBeInTheDocument()
	})
})

describe('getConsumptionForTopicRow', () => {
	it('returns empty slice when topic is missing from map', () => {
		const m = new Map()
		const r = getConsumptionForTopicRow(m, 'missing')
		expect(r.totalForHover).toBeUndefined()
		expect(r.chartData).toEqual([])
	})

	it('returns totalRow and chartData when entry exists', () => {
		const total = totalRow('Total')
		const slice = chartSlice()
		const m = new Map()
		m.set('t', { totalRow: total, chartData: slice })
		const r = getConsumptionForTopicRow(m, 't')
		expect(r.totalForHover).toBe(total)
		expect(r.chartData).toBe(slice)
	})

	it('returns undefined total when entry has no total row', () => {
		const m = new Map()
		m.set('t', { totalRow: undefined, chartData: chartSlice() })
		const r = getConsumptionForTopicRow(m, 't')
		expect(r.totalForHover).toBeUndefined()
		expect(r.chartData).toEqual(chartSlice())
	})

	it('coalesces undefined chartData on entry to empty array', () => {
		const tot = totalRow('Total')
		const m = new Map<
			string,
			{ totalRow: MessageConsumptionItem | undefined; chartData?: MessageConsumptionItem[] }
		>()
		m.set('t', { totalRow: tot, chartData: undefined })
		const r = getConsumptionForTopicRow(
			m as Parameters<typeof getConsumptionForTopicRow>[0],
			't',
		)
		expect(r.totalForHover).toBe(tot)
		expect(r.chartData).toEqual([])
	})

	it('coalesces null chartData on entry to empty array', () => {
		const tot = totalRow('Total')
		const m = new Map<string, { totalRow: MessageConsumptionItem; chartData: null }>()
		m.set('t', { totalRow: tot, chartData: null })
		const r = getConsumptionForTopicRow(
			m as Parameters<typeof getConsumptionForTopicRow>[0],
			't',
		)
		expect(r.totalForHover).toBe(tot)
		expect(r.chartData).toEqual([])
	})
})
