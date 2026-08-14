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
import { render, screen } from '@testing-library/react'
import type { MessageConsumptionItem } from '@utils/metricsUtils'

jest.mock('@utils/Helper', () => ({
	numberFormatWithComma: (n: number | string) => String(n),
}))

import MessageConsumptionChart from '../MessageConsumptionChart'

let tooltipScenario:
	| 'full'
	| 'inactive'
	| 'emptyLabel'
	| 'payloadOnly'
	| 'noRow'
	| 'numericLabel'
	| 'noLabelKey'
	| 'labelNull'
	| 'noPayloadKey'
	| 'payloadNull'
	| 'nullProps' =
	'full'

jest.mock('recharts', () => ({
	ResponsiveContainer: ({ children }: { children?: React.ReactNode }) => (
		<div data-testid="responsive-container">{children}</div>
	),
	BarChart: ({ children }: { children?: React.ReactNode }) => (
		<div data-testid="bar-chart">{children}</div>
	),
	Bar: ({ dataKey, children }: { dataKey?: string; children?: React.ReactNode }) => (
		<div data-testid={`bar-${dataKey}`}>{children}</div>
	),
	XAxis: () => <div data-testid="x-axis" />,
	YAxis: ({
		tickFormatter,
	}: {
		tickFormatter?: (v: number) => string
	}) => (
		<div
			data-testid="y-axis"
			data-tick-formatted={tickFormatter ? tickFormatter(1_234) : ''}
		/>
	),
	CartesianGrid: () => <div data-testid="grid" />,
	Tooltip: ({ content }: { content?: (p: unknown) => React.ReactNode }) => {
		const sampleRow: MessageConsumptionItem = {
			period: 'Current Hour',
			count: 10,
			creates: 1,
			updates: 2,
			deletes: 3,
			failed: 0,
			avgTime: 5,
		}
		let props: unknown
		if (tooltipScenario === 'inactive') {
			props = { active: false }
		} else if (tooltipScenario === 'emptyLabel') {
			props = { active: true, label: '', payload: [{ payload: sampleRow }] }
		} else if (tooltipScenario === 'payloadOnly') {
			props = {
				active: true,
				label: 'Missing',
				payload: [{ payload: sampleRow }],
			}
		} else if (tooltipScenario === 'numericLabel') {
			props = {
				active: true,
				label: 99,
				payload: [{ payload: { ...sampleRow, period: 'fallback' } }],
			}
		} else if (tooltipScenario === 'noLabelKey') {
			props = { active: true, payload: [{ payload: sampleRow }] }
		} else if (tooltipScenario === 'payloadNull') {
			props = { active: true, label: 'X', payload: null }
		} else if (tooltipScenario === 'noPayloadKey') {
			props = { active: true, label: 'Missing' }
		} else if (tooltipScenario === 'labelNull') {
			props = { active: true, label: null, payload: [{ payload: sampleRow }] }
		} else if (tooltipScenario === 'nullProps') {
			props = null
		} else if (tooltipScenario === 'noRow') {
			props = { active: true, label: 'X', payload: [{}] }
		} else {
			props = {
				active: true,
				label: 'Current Hour',
				payload: [{ payload: sampleRow }],
			}
		}
		const node =
			typeof content === 'function' ? content(props) : null
		return <div data-testid="tooltip-mock">{node}</div>
	},
	Cell: ({ fill }: { fill?: string }) => (
		<span data-testid="cell" data-fill={fill} />
	),
	LabelList: ({
		formatter,
	}: {
		formatter?: (v: number) => string
	}) => (
		<span
			data-testid="label-list"
			data-label-formatted={formatter ? formatter(99) : ''}
		/>
	),
}))

const baseRow = (period: string): MessageConsumptionItem => ({
	period,
	count: 4,
	creates: 1,
	updates: 1,
	deletes: 2,
	failed: 0,
	avgTime: 9,
})

describe('MessageConsumptionChart', () => {
	it('renders empty state when data is empty', () => {
		const { container } = render(<MessageConsumptionChart data={[]} />)
		expect(
			screen.getByText('No message consumption data available'),
		).toBeInTheDocument()
		expect(container.querySelector('[role="region"]')).toBeNull()
	})

	it('renders chart with legend and aria region when chartAriaLabel set', () => {
		const { container } = render(
			<MessageConsumptionChart
				data={[baseRow('Current Hour')]}
				chartAriaLabel="Kafka chart"
			/>,
		)
		const region = screen.getByRole('region', { name: 'Kafka chart' })
		expect(region).toBeInTheDocument()
		expect(container.querySelector('[role="region"]')).toBe(region)
		expect(screen.getByLabelText('Chart legend')).toBeInTheDocument()
		expect(screen.getByTestId('responsive-container')).toBeInTheDocument()
		expect(screen.getByTestId('y-axis')).toHaveAttribute(
			'data-tick-formatted',
			'1234',
		)
		expect(screen.getByTestId('label-list')).toHaveAttribute(
			'data-label-formatted',
			'99',
		)
		expect(screen.getByTestId('bar-chart')).toBeInTheDocument()
		expect(screen.getByTestId('bar-creates')).toBeInTheDocument()
		expect(screen.getByTestId('bar-updates')).toBeInTheDocument()
		expect(screen.getByTestId('bar-deletes')).toBeInTheDocument()
	})

	it('renders chart without region role when chartAriaLabel omitted', () => {
		const { container } = render(
			<MessageConsumptionChart data={[baseRow('Current Hour')]} />,
		)
		expect(container.querySelector('[role="region"]')).toBeNull()
		expect(screen.getByLabelText('Chart legend')).toBeInTheDocument()
		expect(screen.getByTestId('bar-chart')).toBeInTheDocument()
	})

	it('tooltip inactive returns null', () => {
		tooltipScenario = 'inactive'
		const { getByTestId } = render(
			<MessageConsumptionChart data={[baseRow('Current Hour')]} />,
		)
		expect(getByTestId('tooltip-mock')).toBeEmptyDOMElement()
		tooltipScenario = 'full'
	})

	it('tooltip resolves row from label match', () => {
		tooltipScenario = 'full'
		render(<MessageConsumptionChart data={[baseRow('Current Hour')]} />)
		expect(screen.getByText('Current Hour')).toBeInTheDocument()
		expect(screen.getByText(/Creates:/)).toBeInTheDocument()
	})

	it('tooltip uses payload when label missing in data', () => {
		tooltipScenario = 'payloadOnly'
		render(<MessageConsumptionChart data={[baseRow('Current Hour')]} />)
		expect(screen.getByText('Current Hour')).toBeInTheDocument()
		tooltipScenario = 'full'
	})

	it('tooltip uses payload when label is empty string', () => {
		tooltipScenario = 'emptyLabel'
		render(<MessageConsumptionChart data={[baseRow('Current Hour')]} />)
		expect(screen.getByText('Current Hour')).toBeInTheDocument()
		tooltipScenario = 'full'
	})

	it('tooltip matches row when label is numeric', () => {
		tooltipScenario = 'numericLabel'
		render(<MessageConsumptionChart data={[baseRow('99')]} />)
		expect(screen.getByText('99')).toBeInTheDocument()
		tooltipScenario = 'full'
	})

	it('tooltip resolves row from payload when label property omitted', () => {
		tooltipScenario = 'noLabelKey'
		render(<MessageConsumptionChart data={[baseRow('Current Hour')]} />)
		expect(screen.getByText('Current Hour')).toBeInTheDocument()
		tooltipScenario = 'full'
	})

	it('tooltip returns null when payload is null', () => {
		tooltipScenario = 'payloadNull'
		const { getByTestId } = render(
			<MessageConsumptionChart data={[baseRow('Current Hour')]} />,
		)
		expect(getByTestId('tooltip-mock')).toBeEmptyDOMElement()
		tooltipScenario = 'full'
	})

	it('tooltip returns null when payload array missing', () => {
		tooltipScenario = 'noPayloadKey'
		const { getByTestId } = render(
			<MessageConsumptionChart data={[baseRow('Current Hour')]} />,
		)
		expect(getByTestId('tooltip-mock')).toBeEmptyDOMElement()
		tooltipScenario = 'full'
	})

	it('tooltip treats null label like empty and uses payload', () => {
		tooltipScenario = 'labelNull'
		render(<MessageConsumptionChart data={[baseRow('Current Hour')]} />)
		expect(screen.getByText('Current Hour')).toBeInTheDocument()
		tooltipScenario = 'full'
	})

	it('tooltip returns null when chart props are null', () => {
		tooltipScenario = 'nullProps'
		const { getByTestId } = render(
			<MessageConsumptionChart data={[baseRow('Current Hour')]} />,
		)
		expect(getByTestId('tooltip-mock')).toBeEmptyDOMElement()
		tooltipScenario = 'full'
	})

	it('tooltip returns null when no row resolved', () => {
		tooltipScenario = 'noRow'
		const { getByTestId } = render(
			<MessageConsumptionChart data={[baseRow('Current Hour')]} />,
		)
		expect(getByTestId('tooltip-mock')).toBeEmptyDOMElement()
		tooltipScenario = 'full'
	})
})
