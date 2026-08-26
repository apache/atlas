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

import React from "react";
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

let mockTooltipFormatter: (val: unknown) => React.ReactNode;

jest.mock("recharts", () => {
	const OriginalRecharts = jest.requireActual("recharts");
	return {
		...OriginalRecharts,
		ResponsiveContainer: ({ children }: { children?: React.ReactNode }) => <div>{children}</div>,
		PieChart: ({ children }: { children?: React.ReactNode }) => <div data-testid="pie-chart">{children}</div>,
		Pie: ({ onClick, data }: { onClick?: (data: unknown) => void; data: Array<{ name: string; value: number }> }) => (
			<div data-testid="pie">
				{data.map((entry) => (
					<button
						key={entry.name}
						type="button"
						data-testid={`pie-slice-${entry.name}`}
						onClick={() => onClick?.(entry)}
					>
						{entry.name}
					</button>
				))}
				<button type="button" data-testid="pie-slice-invalid" onClick={() => onClick?.(null)}>
					Invalid
				</button>
				<button type="button" data-testid="pie-slice-invalid-empty" onClick={() => onClick?.({})}>
					Invalid Empty
				</button>
			</div>
		),
		Tooltip: ({ formatter }: { formatter?: (val: unknown) => React.ReactNode }) => {
			if (formatter) {
				mockTooltipFormatter = formatter;
			}
			return <div data-testid="tooltip" />;
		},
		Cell: ({ fill }: { fill?: string }) => <div data-testid="cell" data-fill={fill} />,
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

	it("returns null when isLoading is true", () => {
		const { container } = render(<EntityStatusDonut entity={{}} isLoading={true} />);
		expect(container.firstChild).toBeNull();
	});

	it("renders empty pie chart when all totals are zero", () => {
		const { getEntityStatusTotals } = require("@utils/metricsUtils");
		getEntityStatusTotals.mockReturnValueOnce({ active: 0, shell: 0, deleted: 0 });
		render(<EntityStatusDonut entity={{}} />);
		
		expect(screen.queryByTestId("pie-slice-Active")).not.toBeInTheDocument();
		expect(screen.queryByTestId("pie-slice-Shell")).not.toBeInTheDocument();
		expect(screen.queryByTestId("pie-slice-Deleted")).not.toBeInTheDocument();
	});

	it("navigates on Active side button click", () => {
		render(<EntityStatusDonut entity={{}} />);
		fireEvent.click(screen.getByRole("button", { name: "View Active entities" }));
		expect(navigateToSearch).toHaveBeenCalledWith(mockNavigate, "entity_status");
	});

	it("navigates on Deleted side button click", () => {
		render(<EntityStatusDonut entity={{}} />);
		fireEvent.click(screen.getByRole("button", { name: "View Deleted entities" }));
		expect(navigateToSearch).toHaveBeenCalledWith(mockNavigate, "entity_status", {
			includeDE: true,
			entityFilters: {
				condition: "AND",
				criterion: [{ attributeName: "__state", operator: "eq", attributeValue: "DELETED" }]
			}
		});
	});

	it("navigates on Shell side button click", () => {
		render(<EntityStatusDonut entity={{}} />);
		fireEvent.click(screen.getByRole("button", { name: "View Shell entities" }));
		expect(navigateToSearch).toHaveBeenCalledWith(mockNavigate, "entity_status", {
			entityFilters: {
				condition: "AND",
				criterion: [{ attributeName: "__isIncomplete", operator: "eq", attributeValue: "true" }]
			}
		});
	});

	it("ignores invalid pie slice click payloads without crashing", () => {
		render(<EntityStatusDonut entity={{}} />);
		
		fireEvent.click(screen.getByTestId("pie-slice-invalid"));
		expect(navigateToSearch).not.toHaveBeenCalled();

		fireEvent.click(screen.getByTestId("pie-slice-invalid-empty"));
		expect(navigateToSearch).not.toHaveBeenCalled();
	});
});
