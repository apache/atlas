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

import { memo, useCallback } from "react";
import { Stack, Typography, Box } from "@mui/material";
import {
	BarChart,
	Bar,
	XAxis,
	YAxis,
	CartesianGrid,
	Tooltip,
	ResponsiveContainer,
	Cell,
	LabelList,
} from "recharts";
import { numberFormatWithComma } from "@utils/Helper";
import { type MessageConsumptionItem } from "@utils/metricsUtils";
import {
	CHART_BAR_ACTIVE_BLUE,
	ENTITY_STATUS_DONUT_COLORS,
} from "./dashboardChartPalette";

const CREATES_COLOR = ENTITY_STATUS_DONUT_COLORS.Active;
const UPDATES_COLOR = CHART_BAR_ACTIVE_BLUE;
const DELETES_COLOR = ENTITY_STATUS_DONUT_COLORS.Deleted;

interface MessageConsumptionChartProps {
	/** Rows must already exclude the Total period when used from Kafka Topic Summary. */
	data: MessageConsumptionItem[];
	/** Optional accessible name for the chart region (embedded context). */
	chartAriaLabel?: string;
}

const MessageConsumptionChart = memo(
	({ data, chartAriaLabel }: MessageConsumptionChartProps) => {
		const renderTooltip = useCallback(
			(props: unknown) => {
				const p = props as {
					active?: boolean;
					label?: string | number;
					payload?: Array<{ payload?: MessageConsumptionItem }>;
				};
				if (!p?.active) return null;
				const periodLabel =
					p.label !== undefined && p.label !== null ? String(p.label) : "";
				const rowFromLabel = periodLabel
					? data.find((d) => d.period === periodLabel)
					: undefined;
				const row =
					rowFromLabel ?? p.payload?.[0]?.payload ?? undefined;
				if (!row) return null;
				return (
					<Box
						sx={{
							p: 1.5,
							bgcolor: "background.paper",
							borderRadius: 1,
							boxShadow: 2,
							minWidth: 160,
						}}
					>
						<Typography variant="body2" fontWeight={600} sx={{ mb: 0.5 }}>
							{row.period}
						</Typography>
						<Typography variant="caption" display="block" sx={{ color: CREATES_COLOR }}>
							Creates: {numberFormatWithComma(row.creates)}
						</Typography>
						<Typography variant="caption" display="block" sx={{ color: UPDATES_COLOR }}>
							Updates: {numberFormatWithComma(row.updates)}
						</Typography>
						<Typography variant="caption" display="block" sx={{ color: DELETES_COLOR }}>
							Deletes: {numberFormatWithComma(row.deletes)}
						</Typography>
						<Typography variant="caption" display="block" sx={{ color: "#6c757d", mt: 0.5 }}>
							Messages processed: {numberFormatWithComma(row.count)}
						</Typography>
						<Typography variant="caption" display="block" sx={{ color: "#6c757d" }}>
							Avg time (ms): {numberFormatWithComma(row.avgTime)}
						</Typography>
					</Box>
				);
			},
			[data]
		);

		if (data.length === 0) {
			return (
				<Stack alignItems="center" justifyContent="center" minHeight={200}>
					<Typography variant="body2" color="text.secondary">
						No message consumption data available
					</Typography>
				</Stack>
			);
		}

		return (
			<Box
				role={chartAriaLabel ? "region" : undefined}
				aria-label={chartAriaLabel}
				sx={{ minHeight: 260, height: 260, width: "100%", minWidth: 280 }}
			>
				<Stack
					direction="row"
					spacing={2}
					sx={{ mb: 1, flexWrap: "wrap" }}
					aria-label="Chart legend"
				>
					<Stack direction="row" alignItems="center" spacing={0.75}>
						<Box
							sx={{
								width: 10,
								height: 10,
								borderRadius: "50%",
								backgroundColor: CREATES_COLOR,
							}}
							aria-hidden
						/>
						<Typography
							variant="caption"
							sx={{ color: "#6c757d", fontSize: "0.8125rem" }}
						>
							Creates
						</Typography>
					</Stack>
					<Stack direction="row" alignItems="center" spacing={0.75}>
						<Box
							sx={{
								width: 10,
								height: 10,
								borderRadius: "50%",
								backgroundColor: UPDATES_COLOR,
							}}
							aria-hidden
						/>
						<Typography
							variant="caption"
							sx={{ color: "#6c757d", fontSize: "0.8125rem" }}
						>
							Updates
						</Typography>
					</Stack>
					<Stack direction="row" alignItems="center" spacing={0.75}>
						<Box
							sx={{
								width: 10,
								height: 10,
								borderRadius: "50%",
								backgroundColor: DELETES_COLOR,
							}}
							aria-hidden
						/>
						<Typography
							variant="caption"
							sx={{ color: "#6c757d", fontSize: "0.8125rem" }}
						>
							Deletes
						</Typography>
					</Stack>
				</Stack>
				<ResponsiveContainer width="100%" height="100%">
					<BarChart
						data={data}
						margin={{ top: 36, right: 28, left: 52, bottom: 36 }}
					>
						<CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
						<XAxis
							dataKey="period"
							tick={{ fontSize: 11 }}
							height={36}
							label={{
								value: "Period",
								position: "bottom",
								offset: 12,
								style: { fontSize: 11, fill: "#6c757d" },
							}}
						/>
						<YAxis
							tickFormatter={(v) => numberFormatWithComma(v)}
							width={48}
							label={{
								value: "Count",
								angle: -90,
								position: "left",
								offset: 6,
								style: { fontSize: 11, fill: "#6c757d", textAnchor: "middle" },
							}}
						/>
						<Tooltip content={renderTooltip} cursor={{ fill: "transparent" }} />
						<Bar
							dataKey="creates"
							name="Creates"
							stackId="a"
							fill={CREATES_COLOR}
							radius={[0, 0, 0, 0]}
						>
							{data.map((_, index) => (
								<Cell key={`creates-${index}`} fill={CREATES_COLOR} />
							))}
						</Bar>
						<Bar
							dataKey="updates"
							name="Updates"
							stackId="a"
							fill={UPDATES_COLOR}
							radius={[0, 0, 0, 0]}
						>
							{data.map((_, index) => (
								<Cell key={`updates-${index}`} fill={UPDATES_COLOR} />
							))}
						</Bar>
						<Bar
							dataKey="deletes"
							name="Deletes"
							stackId="a"
							fill={DELETES_COLOR}
							radius={[0, 4, 4, 0]}
						>
							<LabelList
								dataKey="count"
								position="top"
								offset={8}
								formatter={(v: number) => numberFormatWithComma(v)}
								style={{
									fontSize: 11,
									fontWeight: 600,
									fill: "#374151",
								}}
							/>
							{data.map((_, index) => (
								<Cell key={`deletes-${index}`} fill={DELETES_COLOR} />
							))}
						</Bar>
					</BarChart>
				</ResponsiveContainer>
			</Box>
		);
	}
);

MessageConsumptionChart.displayName = "MessageConsumptionChart";

export default MessageConsumptionChart;
