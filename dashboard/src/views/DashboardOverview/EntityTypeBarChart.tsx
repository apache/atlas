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

import React, { memo, useCallback, useMemo } from "react";
import { Paper, Stack, Typography, Link, Box } from "@mui/material";
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
import { useNavigate } from "react-router-dom";
import { numberFormatWithComma } from "@utils/Helper";
import type { EntityTypeDistributionItem } from "@utils/metricsUtils";
import {
	getServiceTypeDistribution,
	type TypeHeaderCatalogRow,
} from "@utils/typeCatalogUtils";
import {
	navigateToSearch,
	navigateToServiceTypeEntitySearch,
} from "@utils/dashboardSearchUtils";
import {
	CHART_BAR_ACTIVE_BLUE,
	ENTITY_STATUS_DONUT_COLORS,
	HORIZONTAL_BAR_CHART_MARGIN,
} from "./dashboardChartPalette";

const ACTIVE_COLOR = CHART_BAR_ACTIVE_BLUE;
const DELETED_COLOR = ENTITY_STATUS_DONUT_COLORS.Deleted;

interface EntityTypeBarChartProps {
	entity: Record<string, unknown> | undefined;
	typeHeaderData?: TypeHeaderCatalogRow[] | null;
	isLoading?: boolean;
}

const payloadFromBarEvent = (item: unknown): EntityTypeDistributionItem | undefined => {
	if (!item || typeof item !== "object") return undefined;
	const rec = item as { payload?: EntityTypeDistributionItem };
	return rec.payload;
};

const EntityTypeBarChart = memo(
	({ entity, typeHeaderData, isLoading }: EntityTypeBarChartProps) => {
		const navigate = useNavigate();
		const data = useMemo(
			() => getServiceTypeDistribution(entity, typeHeaderData, 5),
			[entity, typeHeaderData]
		);

		const navigateForRow = useCallback(
			(row: EntityTypeDistributionItem, includeDeleted: boolean) => {
				const targets =
					row.underlyingTypeNames?.length && row.underlyingTypeNames.length > 0
						? row.underlyingTypeNames
						: [row.name];
				navigateToServiceTypeEntitySearch(navigate, targets, includeDeleted);
			},
			[navigate]
		);

		const handleActiveBarClick = useCallback(
			(barProps: unknown) => {
				const row = payloadFromBarEvent(barProps);
				if (row) navigateForRow(row, false);
			},
			[navigateForRow]
		);

		const handleDeletedBarClick = useCallback(
			(barProps: unknown) => {
				const row = payloadFromBarEvent(barProps);
				if (row) navigateForRow(row, true);
			},
			[navigateForRow]
		);

		const handleViewAll = useCallback(() => {
			navigateToSearch(navigate, "all_entities");
		}, [navigate]);

		const handleLabelClick = useCallback(
			(serviceLabel: string) => {
				const row = data.find((d) => d.name === serviceLabel);
				if (row) navigateForRow(row, false);
			},
			[data, navigateForRow]
		);

		const renderTooltip = useCallback((props: unknown) => {
			const p = props as {
				active?: boolean;
				payload?: Array<{
					payload?: EntityTypeDistributionItem;
				}>;
			};
			if (!p?.active || !p?.payload?.length) return null;
			const row = p.payload[0]?.payload;
			if (!row) return null;
			return (
				<Box sx={{ p: 1.5, bgcolor: "background.paper", borderRadius: 1, boxShadow: 2, minWidth: 140 }}>
					<Typography variant="body2" fontWeight={600} sx={{ mb: 0.5 }}>
						{row.name}
					</Typography>
					<Typography variant="caption" display="block" color="primary">
						Active: {numberFormatWithComma(row.active)}
					</Typography>
					<Typography variant="caption" display="block" sx={{ color: DELETED_COLOR }}>
						Deleted: {numberFormatWithComma(row.deleted)}
					</Typography>
					<Typography variant="caption" display="block" fontWeight={600}>
						Total: {numberFormatWithComma(row.count)}
					</Typography>
				</Box>
			);
		}, []);

		if (isLoading) return null;

		return (
			<Paper
				elevation={1}
				sx={{
					padding: 2,
					borderRadius: 2,
					minHeight: 340,
					minWidth: 0,
					width: "100%",
					height: "100%",
					boxSizing: "border-box",
					transition: "box-shadow 0.3s ease",
					"&:hover": { boxShadow: 4 },
				}}
			>
				<Box sx={{ pb: 2, borderBottom: "1px solid", borderColor: "divider" }}>
					<Stack direction="row" justifyContent="space-between" alignItems="center">
						<Typography sx={{ fontSize: "1rem", fontWeight: 600, color: "#1a1a1a" }}>
							Service Type Distribution
						</Typography>
						<Link
							component="button"
							onClick={handleViewAll}
							sx={{
								fontSize: "0.875rem",
								cursor: "pointer",
								textDecoration: "none",
								color: "primary.main",
							}}
							aria-label="View all entities"
						>
							View All
						</Link>
					</Stack>
				</Box>
				{data.length === 0 ? (
					<Stack alignItems="center" justifyContent="center" height={200}>
						<Typography variant="body2" color="text.secondary">
							No service type data available
						</Typography>
					</Stack>
				) : (
					<Box sx={{ mt: 2, minHeight: 260, height: 260, width: "100%", minWidth: 280 }}>
						<Stack direction="row" spacing={2} sx={{ mb: 1, flexWrap: "wrap" }} aria-label="Chart legend">
							<Stack direction="row" alignItems="center" spacing={0.75}>
								<Box
									sx={{
										width: 10,
										height: 10,
										borderRadius: "50%",
										backgroundColor: ACTIVE_COLOR,
									}}
									aria-hidden
								/>
								<Typography variant="caption" sx={{ color: "#6c757d", fontSize: "0.8125rem" }}>
									Active
								</Typography>
							</Stack>
							<Stack direction="row" alignItems="center" spacing={0.75}>
								<Box
									sx={{
										width: 10,
										height: 10,
										borderRadius: "50%",
										backgroundColor: DELETED_COLOR,
									}}
									aria-hidden
								/>
								<Typography variant="caption" sx={{ color: "#6c757d", fontSize: "0.8125rem" }}>
									Deleted
								</Typography>
							</Stack>
						</Stack>
						<ResponsiveContainer width="100%" height="100%" style={{ cursor: "pointer" }}>
							<BarChart data={data} layout="vertical" margin={{ ...HORIZONTAL_BAR_CHART_MARGIN }}>
								<CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
								<XAxis
									type="number"
									tickFormatter={(v) => numberFormatWithComma(v)}
									height={36}
									label={{
										value: "Entity Count",
										position: "bottom",
										offset: 12,
										style: { fontSize: 11, fill: "#6c757d" },
									}}
								/>
								<YAxis
									type="category"
									dataKey="name"
									width={88}
									label={{
										value: "Service Type",
										angle: -90,
										position: "left",
										offset: 4,
										style: { fontSize: 11, fill: "#6c757d", textAnchor: "middle" },
									}}
									tick={(props: Record<string, unknown>) => {
										const { x = 0, y = 0, payload } = props;
										const p = payload as { value?: string; name?: string } | undefined;
										const value =
											p?.value ??
											p?.name ??
											(typeof payload === "string" ? payload : "");
										return (
											<g
												transform={`translate(${x},${y})`}
												onClick={() => (value ? handleLabelClick(value) : undefined)}
												style={{ cursor: value ? "pointer" : "default" }}
												role={value ? "button" : undefined}
												tabIndex={value ? 0 : undefined}
												onKeyDown={
													value
														? (e: React.KeyboardEvent<SVGGElement>) => {
																if (e.key === "Enter" || e.key === " ") {
																	e.preventDefault();
																	handleLabelClick(value);
																}
															}
														: undefined
												}
											>
												<text x={0} y={0} dy={4} textAnchor="end" fill="#333" fontSize={12}>
													{value}
												</text>
											</g>
										);
									}}
								/>
								<Tooltip content={renderTooltip} cursor={{ fill: "transparent" }} />
								<Bar
									dataKey="active"
									name="Active"
									stackId="a"
									fill={ACTIVE_COLOR}
									radius={[0, 0, 0, 0]}
									isAnimationActive
									animationDuration={800}
									animationEasing="ease-out"
									onClick={handleActiveBarClick}
									cursor="pointer"
									activeBar={{ fill: ACTIVE_COLOR }}
								>
									{data.map((_, index) => (
										<Cell key={`active-${index}`} fill={ACTIVE_COLOR} />
									))}
								</Bar>
								<Bar
									dataKey="deleted"
									name="Deleted"
									stackId="a"
									fill={DELETED_COLOR}
									radius={[0, 4, 4, 0]}
									isAnimationActive
									animationDuration={800}
									animationEasing="ease-out"
									onClick={handleDeletedBarClick}
									cursor="pointer"
									activeBar={{ fill: DELETED_COLOR }}
								>
									<LabelList
										dataKey="count"
										position="right"
										offset={10}
										formatter={(v: number) => numberFormatWithComma(v)}
										style={{
											fontSize: 12,
											fontWeight: 500,
											fill: ACTIVE_COLOR,
										}}
									/>
									{data.map((_, index) => (
										<Cell key={`deleted-${index}`} fill={DELETED_COLOR} />
									))}
								</Bar>
							</BarChart>
						</ResponsiveContainer>
					</Box>
				)}
			</Paper>
		);
	}
);

EntityTypeBarChart.displayName = "EntityTypeBarChart";

export default EntityTypeBarChart;
