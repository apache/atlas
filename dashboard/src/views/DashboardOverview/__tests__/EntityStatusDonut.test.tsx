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

import { render, screen, fireEvent } from "@testing-library/react";
import EntityStatusDonut from "../EntityStatusDonut";
import { navigateToSearch } from "@utils/dashboardSearchUtils";
import { numberFormatWithComma } from "@utils/Helper";

jest.mock("react-router-dom", () => ({
	useNavigate: jest.fn(),
}));

jest.mock("@utils/Helper", () => ({
	numberFormatWithComma: jest.fn((val) => `Formatted: ${val}`),
}));

jest.mock("@utils/metricsUtils", () => ({
	getEntityStatusTotals: jest.fn(() => ({ active: 10, shell: 5, deleted: 2 })),
}));

jest.mock("@utils/dashboardSearchUtils", () => ({
	navigateToSearch: jest.fn(),
}));

let mockTooltipFormatter: any;

jest.mock("recharts", () => {
	const OriginalRecharts = jest.requireActual("recharts");
	return {
		...OriginalRecharts,
		ResponsiveContainer: ({ children }: any) => <div>{children}</div>,
		PieChart: ({ children }: any) => <div data-testid="pie-chart">{children}</div>,
		Pie: ({ onClick, data }: any) => (
			<div data-testid="pie">
				{data.map((entry: any) => (
					<button
						key={entry.name}
						data-testid={`pie-slice-${entry.name}`}
						onClick={() => onClick && onClick(entry)}
					>
						{entry.name}
					</button>
				))}
			</div>
		),
		Tooltip: ({ formatter }: any) => {
			mockTooltipFormatter = formatter;
			return <div data-testid="tooltip" />;
		},
		Cell: ({ fill }: any) => <div data-testid="cell" data-fill={fill} />,
	};
});

describe("EntityStatusDonut", () => {
	const mockNavigate = jest.fn();

	beforeEach(() => {
		jest.clearAllMocks();
		(require("react-router-dom").useNavigate as jest.Mock).mockReturnValue(mockNavigate);
	});

	it("renders pie chart and slices", () => {
		render(<EntityStatusDonut entity={{}} />);
		expect(screen.getByTestId("pie-slice-Active")).toBeInTheDocument();
		expect(screen.getByTestId("pie-slice-Shell")).toBeInTheDocument();
		expect(screen.getByTestId("pie-slice-Deleted")).toBeInTheDocument();
	});

	it("navigates on Active pie slice click", () => {
		render(<EntityStatusDonut entity={{}} />);
		fireEvent.click(screen.getByTestId("pie-slice-Active"));

		expect(navigateToSearch).toHaveBeenCalledWith(mockNavigate, "entity_status");
	});

	it("navigates on Deleted pie slice click", () => {
		render(<EntityStatusDonut entity={{}} />);
		fireEvent.click(screen.getByTestId("pie-slice-Deleted"));

		expect(navigateToSearch).toHaveBeenCalledWith(mockNavigate, "entity_status", {
			includeDE: true,
			entityFilters: {
				condition: "AND",
				criterion: [{ attributeName: "__state", operator: "eq", attributeValue: "DELETED" }]
			}
		});
	});

	it("navigates on Shell pie slice click", () => {
		render(<EntityStatusDonut entity={{}} />);
		fireEvent.click(screen.getByTestId("pie-slice-Shell"));

		expect(navigateToSearch).toHaveBeenCalledWith(mockNavigate, "entity_status", {
			entityFilters: {
				condition: "AND",
				criterion: [{ attributeName: "__isIncomplete", operator: "eq", attributeValue: "true" }]
			}
		});
	});

	it("formats tooltip values using numberFormatWithComma", () => {
		render(<EntityStatusDonut entity={{}} />);
		expect(mockTooltipFormatter).toBeDefined();

		const formattedValue = mockTooltipFormatter(12345);
		expect(numberFormatWithComma).toHaveBeenCalledWith(12345);
		expect(formattedValue).toBe("Formatted: 12345");
	});

	it("handles tooltip formatter with null/undefined value", () => {
		render(<EntityStatusDonut entity={{}} />);
		expect(mockTooltipFormatter).toBeDefined();

		mockTooltipFormatter(null);
		expect(numberFormatWithComma).toHaveBeenCalledWith(0);
	});
});
