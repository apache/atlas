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

import { useMemo, useCallback } from "react";
import moment from "moment";
import { Stack, Typography, Box, ButtonBase } from "@mui/material";
import {
	Area,
	AreaChart,
	CartesianGrid,
	Legend,
	ResponsiveContainer,
	Tooltip,
	XAxis,
	YAxis,
} from "recharts";
import GraphCustomTooltip from "./StatsGraphs/GraphCustomTooltip";

type ChartDataPoint = {
	timestamp: number;
	Active: number;
	Deleted: number;
	Shell: number;
};

type ActiveKeys = {
	Active: boolean;
	Deleted: boolean;
	Shell: boolean;
};

type EntityStatsChartProps = {
	chartData: ChartDataPoint[];
	chartMode: string;
	activeKeys: ActiveKeys;
	onLegendClick: (dataKey: string) => void;
	getColorForKey: (key: string) => string;
};

const EntityStatsChart = ({
	chartData,
	chartMode,
	activeKeys,
	onLegendClick,
	getColorForKey,
}: EntityStatsChartProps) => {
	const legendPayload = useMemo(() => {
		return Object.keys(activeKeys).map((key) => ({
			id: key,
			value: key,
			color: activeKeys[key as keyof ActiveKeys] === true ? getColorForKey(key) : "#d3d3d3",
			inactive: !activeKeys[key as keyof ActiveKeys],
		}));
	}, [activeKeys, getColorForKey]);

	const renderLegend = useCallback(
		() => (
			<Stack direction="row" spacing={2} justifyContent="center" mt={1}>
				{legendPayload.map((entry) => (
					<ButtonBase
						key={entry.id}
						data-testid={`legend-${entry.id}`}
						onClick={() => onLegendClick(String(entry.value))}
						aria-label={String(entry.value)}
						className="legend-button"
					>
						<Box className="legend-color-box" style={{ backgroundColor: entry.color }} />
						<Typography variant="body2" className={`legend-typography ${entry.inactive ? "legend-inactive" : "legend-active"}`}>
							{entry.value}
						</Typography>
					</ButtonBase>
				))}
			</Stack>
		),
		[legendPayload, onLegendClick]
	);

	return (
		<ResponsiveContainer width="100%" height={400}>
			<AreaChart
				data={chartData}
				margin={{ top: 10, right: 60, left: 0, bottom: 0 }}
			>
				<CartesianGrid strokeDasharray="3 3" />
				<XAxis
					dataKey="timestamp"
					tickFormatter={(tick) => moment(tick).format("MM/DD/YYYY")}
				/>
				<YAxis
					domain={
						chartMode === "stream"
							? [
									(dataMin: number) => dataMin - 10,
									(dataMax: number) => dataMax + 10,
								]
							: ["auto", "auto"]
					}
					tickFormatter={(tick) =>
						chartMode === "expanded"
							? `${tick.toFixed(2)}%`
							: tick.toFixed(2)
					}
				/>
				<Tooltip
					content={<GraphCustomTooltip />}
					cursor={{ stroke: "rgba(0, 0, 0, 0.1)", strokeWidth: 2 }}
				/>
				<Legend content={renderLegend} />
				{activeKeys.Active && (
					<Area
						type={chartMode === "stream" ? "basis" : "monotone"}
						dataKey="Active"
						stackId={chartMode === "expanded" ? undefined : "1"}
						stroke="rgb(31, 119, 180)"
						fill="rgb(31, 119, 180)"
					/>
				)}
				{activeKeys.Deleted && (
					<Area
						type={chartMode === "stream" ? "basis" : "monotone"}
						dataKey="Deleted"
						stackId={chartMode === "expanded" ? undefined : "1"}
						stroke="rgb(174, 199, 232)"
						fill="rgb(174, 199, 232)"
					/>
				)}
				{activeKeys.Shell && (
					<Area
						type={chartMode === "stream" ? "basis" : "monotone"}
						dataKey="Shell"
						stackId={chartMode === "expanded" ? undefined : "1"}
						stroke="rgb(255, 127, 14)"
						fill="rgb(255, 127, 14)"
					/>
				)}
			</AreaChart>
		</ResponsiveContainer>
	);
};

export default EntityStatsChart;
