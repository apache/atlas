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

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import ClassificationDistributionCard from '../ClassificationDistributionCard';
import {
	CLASSIFICATION_Y_AXIS_LABEL_MAX_LENGTH,
	CLASSIFICATION_Y_AXIS_LABEL_SUFFIX,
} from '../dashboardChartPalette';

jest.mock('@utils/Helper', () => ({
	numberFormatWithComma: (n: number | string) => String(n),
}));

const mockNavigateToSearch = jest.fn();
const mockNavigateToClassificationSearch = jest.fn();
jest.mock('@utils/dashboardSearchUtils', () => ({
	navigateToSearch: (...args: unknown[]) => mockNavigateToSearch(...args),
	navigateToClassificationSearch: (...args: unknown[]) =>
		mockNavigateToClassificationSearch(...args),
}));

const shortName = 'PII';
const longName = 'a'.repeat(CLASSIFICATION_Y_AXIS_LABEL_MAX_LENGTH + 10);
const truncatedLongName = `${'a'.repeat(CLASSIFICATION_Y_AXIS_LABEL_MAX_LENGTH)}${CLASSIFICATION_Y_AXIS_LABEL_SUFFIX}`;

const mockGetClassificationDistribution = jest.fn(() => [
	{ name: shortName, count: 12 },
	{ name: longName, count: 8 },
]);
const mockGetTagEntityAssociationTotal = jest.fn(() => 20);

jest.mock('@utils/metricsUtils', () => ({
	getClassificationDistribution: (...args: unknown[]) =>
		mockGetClassificationDistribution(...args),
	getTagEntityAssociationTotal: (...args: unknown[]) =>
		mockGetTagEntityAssociationTotal(...args),
}));

jest.mock('recharts', () => ({
	ResponsiveContainer: ({ children }: { children?: React.ReactNode }) => (
		<div data-testid="rc">{children}</div>
	),
	BarChart: ({ children }: { children?: React.ReactNode }) => (
		<div data-testid="bar-chart">{children}</div>
	),
	CartesianGrid: () => <div data-testid="grid" />,
	XAxis: ({
		tickFormatter,
		label,
	}: {
		tickFormatter?: (value: number) => string;
		label?: { value?: string };
	}) => (
		<div data-testid="x-axis">
			<span data-testid="x-axis-tick">{tickFormatter?.(1200)}</span>
			<span data-testid="x-axis-label">{label?.value}</span>
		</div>
	),
	YAxis: ({ tick }: { tick?: React.ComponentType<Record<string, unknown>> }) => {
		const Tick = tick;
		if (!Tick) return null;
		return (
			<div data-testid="y-axis">
				<Tick x={10} y={20} payload={{ value: shortName }} />
				<Tick x={10} y={40} payload={{ value: longName }} />
				<Tick x={10} y={60} payload={{ name: 'fromNameField' }} />
				<Tick x={10} y={80} payload="" />
				<Tick x={10} y={100} payload={{}} />
			</div>
		);
	},
	Tooltip: ({
		content,
	}: {
		content?: (props: unknown) => React.ReactNode;
	}) => {
		if (!content) {
			return <div data-testid="tooltip-mock" />;
		}
		const inactive = content({ active: false, payload: [] });
		const active = content({
			active: true,
			payload: [{ payload: { name: shortName, count: 12 } }],
		});
		const missingRow = content({ active: true, payload: [{}] });
		return (
			<div data-testid="tooltip-mock">
				<div data-testid="tooltip-inactive">{inactive}</div>
				<div data-testid="tooltip-active">{active}</div>
				<div data-testid="tooltip-missing-row">{missingRow}</div>
			</div>
		);
	},
	Bar: ({
		children,
		onClick,
	}: {
		children?: React.ReactNode;
		onClick?: (entry: { name?: string }) => void;
	}) => (
		<div data-testid="bar">
			<button
				type="button"
				data-testid="bar-click-valid"
				onClick={() => onClick?.({ name: shortName })}
			>
				Bar
			</button>
			<button
				type="button"
				data-testid="bar-click-empty"
				onClick={() => onClick?.({})}
			>
				Bar Empty
			</button>
			{children}
		</div>
	),
	Cell: () => <div data-testid="cell" />,
	LabelList: ({
		formatter,
	}: {
		formatter?: (value: unknown) => string;
	}) => (
		<div data-testid="label-list">
			{formatter?.(12)}
			{formatter?.('text')}
		</div>
	),
}));

describe('ClassificationDistributionCard', () => {
	beforeEach(() => {
		jest.clearAllMocks();
		mockGetClassificationDistribution.mockReturnValue([
			{ name: shortName, count: 12 },
			{ name: longName, count: 8 },
		]);
		mockGetTagEntityAssociationTotal.mockReturnValue(20);
	});

	it('renders short Y-axis labels without truncation or title tooltip', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		const yAxis = screen.getByTestId('y-axis');
		expect(yAxis).toHaveTextContent(shortName);
		const shortLabelGroup = yAxis.querySelector('g');
		expect(shortLabelGroup?.querySelector('title')).toBeNull();
	});

	it('truncates long Y-axis labels and exposes full name in SVG title tooltip', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		const yAxis = screen.getByTestId('y-axis');
		const tickGroups = yAxis.querySelectorAll('g');
		expect(tickGroups.length).toBeGreaterThan(1);

		const truncatedLabelGroup = tickGroups[1];
		expect(truncatedLabelGroup).toHaveTextContent(truncatedLongName);

		const titleNode = truncatedLabelGroup?.querySelector('title');
		expect(titleNode).not.toBeNull();
		expect(titleNode?.textContent).toBe(longName);

		const visibleTextNodes = truncatedLabelGroup?.querySelectorAll('text');
		expect(visibleTextNodes?.length).toBe(1);
		expect(visibleTextNodes?.[0]?.textContent).toBe(truncatedLongName);
	});

	it('renders total tag-entity associations label with bold formatting and description text', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		const boldLabel = screen.getByText('Tag-entity associations (total):');
		expect(boldLabel).toBeInTheDocument();
		expect(boldLabel.tagName).toBe('STRONG');

		expect(
			screen.getByText(
				'The chart shows the top 5 classifications by number of entities in use.',
			),
		).toBeInTheDocument();
	});

	it('returns null when isLoading is true', () => {
		const { container } = render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} isLoading={true} />
			</MemoryRouter>,
		);
		expect(container.firstChild).toBeNull();
	});

	it('triggers View All navigation when View All link is clicked', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		const viewAllLink = screen.getByRole('button', { name: /view all classifications/i });
		expect(viewAllLink).toBeInTheDocument();
		viewAllLink.click();
		expect(mockNavigateToSearch).toHaveBeenCalledWith(expect.anything(), 'all_classifications');
	});

	it('triggers classification search navigation when clicking Y-axis tick label', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		const yAxis = screen.getByTestId('y-axis');
		const shortLabelGroup = yAxis.querySelector('g');
		expect(shortLabelGroup).not.toBeNull();
		fireEvent.click(shortLabelGroup!);
		expect(mockNavigateToClassificationSearch).toHaveBeenCalledWith(
			expect.anything(),
			shortName,
		);
	});

	it('triggers classification search navigation when bar is clicked', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		fireEvent.click(screen.getByTestId('bar-click-valid'));
		expect(mockNavigateToClassificationSearch).toHaveBeenCalledWith(
			expect.anything(),
			shortName,
		);
	});

	it('does not navigate when bar click payload has no name', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		fireEvent.click(screen.getByTestId('bar-click-empty'));
		expect(mockNavigateToClassificationSearch).not.toHaveBeenCalled();
	});

	it('triggers classification search navigation when Y-axis label receives Enter key', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		const shortLabelGroup = screen.getByTestId('y-axis').querySelector('g');
		expect(shortLabelGroup).not.toBeNull();
		fireEvent.keyDown(shortLabelGroup!, { key: 'Enter' });
		expect(mockNavigateToClassificationSearch).toHaveBeenCalledWith(
			expect.anything(),
			shortName,
		);
	});

	it('triggers classification search navigation when Y-axis label receives Space key', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		const shortLabelGroup = screen.getByTestId('y-axis').querySelector('g');
		expect(shortLabelGroup).not.toBeNull();
		fireEvent.keyDown(shortLabelGroup!, { key: ' ' });
		expect(mockNavigateToClassificationSearch).toHaveBeenCalledWith(
			expect.anything(),
			shortName,
		);
	});

	it('ignores unrelated keyboard events on Y-axis tick label', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		const shortLabelGroup = screen.getByTestId('y-axis').querySelector('g');
		expect(shortLabelGroup).not.toBeNull();
		mockNavigateToClassificationSearch.mockClear();
		fireEvent.keyDown(shortLabelGroup!, { key: 'Tab' });
		expect(mockNavigateToClassificationSearch).not.toHaveBeenCalled();
	});

	it('formats X-axis ticks and renders axis label', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		expect(screen.getByTestId('x-axis-tick')).toHaveTextContent('1200');
		expect(screen.getByTestId('x-axis-label')).toHaveTextContent('Entity Count');
	});

	it('formats bar count labels through LabelList formatter', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		expect(screen.getByTestId('label-list')).toHaveTextContent('12');
	});

	it('renders tooltip content when active and hides when inactive', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		expect(screen.getByTestId('tooltip-active')).toHaveTextContent(shortName);
		expect(screen.getByTestId('tooltip-active')).toHaveTextContent('Entities: 12');
		expect(screen.getByTestId('tooltip-inactive')).toBeEmptyDOMElement();
		expect(screen.getByTestId('tooltip-missing-row')).toBeEmptyDOMElement();
	});

	it('renders Y-axis ticks from payload.name and string payload without navigation handlers', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		const yAxis = screen.getByTestId('y-axis');
		const tickGroups = yAxis.querySelectorAll('g');
		const nameFieldGroup = tickGroups[2];
		expect(nameFieldGroup).toHaveClass(
			'classification-distribution-card__y-axis-tick-pointer',
		);
		fireEvent.click(nameFieldGroup);
		expect(mockNavigateToClassificationSearch).toHaveBeenCalledWith(
			expect.anything(),
			'fromNameField',
		);

		const emptyValueGroup = tickGroups[4];
		expect(emptyValueGroup).toHaveClass(
			'classification-distribution-card__y-axis-tick-default',
		);
		mockNavigateToClassificationSearch.mockClear();
		fireEvent.click(emptyValueGroup);
		expect(mockNavigateToClassificationSearch).not.toHaveBeenCalled();
	});

	it('renders empty state when no classification data is available', () => {
		mockGetClassificationDistribution.mockReturnValue([]);
		mockGetTagEntityAssociationTotal.mockReturnValue(0);

		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		expect(screen.getByText('Tag-entity associations (total):')).toBeInTheDocument();
		expect(screen.getByText('0')).toBeInTheDocument();
		expect(
			screen.getByText('No classification data available'),
		).toBeInTheDocument();
		expect(screen.queryByTestId('bar-chart')).not.toBeInTheDocument();
	});
});
