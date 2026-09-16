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
import { render, screen, fireEvent, act } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import EntityStatsChart from "../EntityStatsChart";

jest.mock("recharts", () => {
	const OriginalRecharts = jest.requireActual("recharts");
	return {
		...OriginalRecharts,
		ResponsiveContainer: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
		AreaChart: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
		Area: () => <div data-testid="area" />,
		CartesianGrid: () => <div />,
		XAxis: () => <div />,
		YAxis: () => <div />,
		Tooltip: () => <div />,
		Legend: ({ content }: { content: () => React.ReactNode }) => {
			const Content = content;
			return <div data-testid="legend-mock">{Content ? <Content /> : null}</div>;
		},
	};
});

describe("EntityStatsChart custom legend", () => {
	const mockOnLegendClick = jest.fn();
	const mockGetColorForKey = jest.fn((key: string) => {
		if (key === "Active") return "blue";
		if (key === "Deleted") return "red";
		if (key === "Shell") return "orange";
		return "black";
	});

	const defaultProps = {
		chartData: [
			{ timestamp: 1600000000000, Active: 10, Deleted: 2, Shell: 1 },
		],
		chartMode: "stacked",
		activeKeys: { Active: true, Deleted: false, Shell: true },
		onLegendClick: mockOnLegendClick,
		getColorForKey: mockGetColorForKey,
	};

	beforeEach(() => {
		jest.clearAllMocks();
	});

	it("renders the custom legend with correct aria-labels and handles click toggles", () => {
		render(<EntityStatsChart {...defaultProps} />);

		const activeLegend = screen.getByTestId("legend-Active");
		const deletedLegend = screen.getByTestId("legend-Deleted");

		expect(activeLegend).toHaveAttribute("aria-label", "Active");
		expect(deletedLegend).toHaveAttribute("aria-label", "Deleted");

		fireEvent.click(activeLegend);
		expect(mockOnLegendClick).toHaveBeenCalledWith("Active");

		fireEvent.click(deletedLegend);
		expect(mockOnLegendClick).toHaveBeenCalledWith("Deleted");
	});

	it("applies active styling when key is active", () => {
		render(<EntityStatsChart {...defaultProps} />);

		const activeLegend = screen.getByTestId("legend-Active");
		const typography = activeLegend.querySelector(".legend-typography");
		const colorBox = activeLegend.querySelector(".legend-color-box");

		expect(typography).toHaveClass("legend-active");
		expect(typography).not.toHaveClass("legend-inactive");
		expect(colorBox).toHaveStyle({ "--legend-color": "blue" });
	});

	it("applies inactive styling when key is inactive", () => {
		render(<EntityStatsChart {...defaultProps} />);

		const deletedLegend = screen.getByTestId("legend-Deleted");
		const typography = deletedLegend.querySelector(".legend-typography");
		const colorBox = deletedLegend.querySelector(".legend-color-box");

		expect(typography).toHaveClass("legend-inactive");
		expect(typography).not.toHaveClass("legend-active");
		expect(colorBox).toHaveStyle({ "--legend-color": "#d3d3d3" });
	});

	it("triggers onLegendClick on keyboard Enter and Space key presses on legend ButtonBase", async () => {
		const user = userEvent.setup();
		render(<EntityStatsChart {...defaultProps} />);

		const activeLegend = screen.getByTestId("legend-Active");
		mockOnLegendClick.mockClear();

		act(() => {
			activeLegend.focus();
		});
		await user.keyboard("{Enter}");
		expect(mockOnLegendClick).toHaveBeenCalledWith("Active");

		mockOnLegendClick.mockClear();
		await user.keyboard(" ");
		expect(mockOnLegendClick).toHaveBeenCalledWith("Active");
	});

	it("does not call onLegendClick when an unhandled key is pressed on legend ButtonBase", async () => {
		const user = userEvent.setup();
		render(<EntityStatsChart {...defaultProps} />);

		const activeLegend = screen.getByTestId("legend-Active");
		mockOnLegendClick.mockClear();

		act(() => {
			activeLegend.focus();
		});
		await user.keyboard("{ArrowDown}");
		expect(mockOnLegendClick).not.toHaveBeenCalled();
	});
});

