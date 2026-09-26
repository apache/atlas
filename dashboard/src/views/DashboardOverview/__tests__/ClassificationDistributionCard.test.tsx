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

jest.mock('@utils/metricsUtils', () => ({
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
	Bar: ({ children }: { children?: React.ReactNode }) => (
		<div data-testid="bar">{children}</div>
	),
	Cell: () => <div data-testid="cell" />,
	LabelList: () => <div data-testid="label-list" />,
}));

describe('ClassificationDistributionCard', () => {
	beforeEach(() => {
		jest.clearAllMocks();
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
});
