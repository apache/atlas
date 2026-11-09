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
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
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

let mockBarClickPayload: unknown = undefined;

jest.mock('@utils/metricsUtils', () => ({
	...jest.requireActual('@utils/metricsUtils'),
	getClassificationDistribution: jest.fn(() => [
		{ name: shortName, count: 12 },
		{ name: longName, count: 8 },
	]),
	getTagEntityAssociationTotal: jest.fn(() => 20),
}));

jest.mock('recharts', () => ({
	ResponsiveContainer: ({ children }: { children?: React.ReactNode }) => (
		<div data-testid="rc">{children}</div>
	),
	BarChart: ({ children }: { children?: React.ReactNode }) => (
		<div data-testid="bar-chart">{children}</div>
	),
	CartesianGrid: () => <div data-testid="grid" />,
	XAxis: () => <div data-testid="x-axis" />,
	YAxis: ({ tick }: { tick?: React.ComponentType<Record<string, unknown>> }) => {
		const Tick = tick;
		if (!Tick) return null;
		return (
			<div data-testid="y-axis">
				<Tick x={10} y={20} payload={{ value: shortName }} />
				<Tick x={10} y={40} payload={{ value: longName }} />
			</div>
		);
	},
	Tooltip: () => <div data-testid="tooltip-mock" />,
	Bar: ({
		onClick,
		children,
	}: {
		onClick?: (e: unknown) => void;
		children?: React.ReactNode;
	}) => (
		<button
			type="button"
			data-testid="bar"
			onClick={() =>
				onClick?.(
					mockBarClickPayload !== undefined
						? mockBarClickPayload
						: {
								payload: {
									name: shortName,
								},
						  },
				)
			}
		>
			{children}
		</button>
	),
	Cell: () => <div data-testid="cell" />,
	LabelList: () => <div data-testid="label-list" />,
}));

describe('ClassificationDistributionCard', () => {
	beforeEach(() => {
		jest.clearAllMocks();
		mockBarClickPayload = undefined;
	});

	it('renders short Y-axis labels without truncation or title tooltip', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		expect(screen.getByText(shortName)).toBeInTheDocument();
		const shortLabelGroup = screen.getByText(shortName).closest('g');
		expect(shortLabelGroup?.querySelector('title')).toBeNull();
	});

	it('truncates long Y-axis labels and exposes full name in SVG title tooltip', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		expect(screen.getByText(truncatedLongName)).toBeInTheDocument();

		const truncatedLabelGroup = screen.getByText(truncatedLongName).closest('g');
		const titleNode = truncatedLabelGroup?.querySelector('title');
		expect(titleNode).not.toBeNull();
		expect(titleNode?.textContent).toBe(longName);

		const visibleTextNodes = truncatedLabelGroup?.querySelectorAll('text');
		expect(visibleTextNodes?.length).toBe(1);
		expect(visibleTextNodes?.[0]?.textContent).toBe(truncatedLongName);
	});

	it('navigates to classification search on valid bar click', async () => {
		const user = userEvent.setup();
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		await user.click(screen.getByTestId('bar'));
		expect(mockNavigateToClassificationSearch).toHaveBeenCalledWith(expect.anything(), shortName);
	});

	it('ignores bar click when payload is invalid/missing', async () => {
		const user = userEvent.setup();
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		mockNavigateToClassificationSearch.mockClear();
		mockBarClickPayload = null;
		await user.click(screen.getByTestId('bar'));
		expect(mockNavigateToClassificationSearch).not.toHaveBeenCalled();

		mockBarClickPayload = 'bad' as unknown;
		await user.click(screen.getByTestId('bar'));
		expect(mockNavigateToClassificationSearch).not.toHaveBeenCalled();

		mockBarClickPayload = {};
		await user.click(screen.getByTestId('bar'));
		expect(mockNavigateToClassificationSearch).not.toHaveBeenCalled();
	});

	it('navigates to classification search on YAxis tick click', async () => {
		const user = userEvent.setup();
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		const tickButtons = document.querySelectorAll('g[role="button"]');
		expect(tickButtons.length).toBeGreaterThan(0);

		mockNavigateToClassificationSearch.mockClear();
		await user.click(tickButtons[0]);
		expect(mockNavigateToClassificationSearch).toHaveBeenCalledWith(expect.anything(), shortName);
	});

	it('navigates to classification search on YAxis tick Enter key press', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		const tickButtons = document.querySelectorAll('g[role="button"]');
		mockNavigateToClassificationSearch.mockClear();

		const preventDefault = jest.fn();
		const group = tickButtons[0];
		const event = new KeyboardEvent('keydown', { key: 'Enter', bubbles: true, cancelable: true });
		Object.defineProperty(event, 'preventDefault', { value: preventDefault });
		group.dispatchEvent(event);

		expect(mockNavigateToClassificationSearch).toHaveBeenCalledWith(expect.anything(), shortName);
	});

	it('navigates to classification search on YAxis tick Space key press', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		const tickButtons = document.querySelectorAll('g[role="button"]');
		mockNavigateToClassificationSearch.mockClear();

		const preventDefault = jest.fn();
		const group = tickButtons[0];
		const event = new KeyboardEvent('keydown', { key: ' ', bubbles: true, cancelable: true });
		Object.defineProperty(event, 'preventDefault', { value: preventDefault });
		group.dispatchEvent(event);

		expect(mockNavigateToClassificationSearch).toHaveBeenCalledWith(expect.anything(), shortName);
	});

	it('ignores non-Enter/Space key presses on YAxis tick', () => {
		render(
			<MemoryRouter>
				<ClassificationDistributionCard tag={{}} />
			</MemoryRouter>,
		);

		const tickButtons = document.querySelectorAll('g[role="button"]');
		mockNavigateToClassificationSearch.mockClear();

		const group = tickButtons[0];
		const event = new KeyboardEvent('keydown', { key: 'Tab', bubbles: true, cancelable: true });
		group.dispatchEvent(event);

		expect(mockNavigateToClassificationSearch).not.toHaveBeenCalled();
	});
});

