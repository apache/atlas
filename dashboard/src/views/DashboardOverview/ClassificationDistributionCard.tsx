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
import { Paper, Stack, Typography, Box, Link } from "@mui/material";
import {
	BarChart,
	Bar,
	XAxis,
	YAxis,
	CartesianGrid,
	Tooltip,
	ResponsiveContainer,
	Cell,
	LabelList
} from "recharts";
import { useNavigate } from "react-router-dom";
import { numberFormatWithComma } from "@utils/Helper";
import {
	getClassificationDistribution,
	getTagEntityAssociationTotal,
} from "@utils/metricsUtils";
import { navigateToSearch, navigateToClassificationSearch } from "@utils/dashboardSearchUtils";
import {
	CHART_BAR_ACTIVE_BLUE,
	CLASSIFICATION_DISTRIBUTION_CHART_MARGIN,
} from "./dashboardChartPalette";

const BAR_COLOR = CHART_BAR_ACTIVE_BLUE;

interface ClassificationDistributionCardProps {
	tag: Record<string, unknown> | undefined;
	isLoading?: boolean;
}

const ClassificationDistributionCard = memo(({ tag, isLoading }: ClassificationDistributionCardProps) => {
	const navigate = useNavigate();
	const data = getClassificationDistribution(tag, 5);
	const associationTotal = useMemo(() => getTagEntityAssociationTotal(tag), [tag]);

	const handleBarClick = useCallback(
		(entry: { name: string }) => {
			navigateToClassificationSearch(navigate, entry.name);
		},
		[navigate]
	);

	const handleViewAll = useCallback(() => {
		navigateToSearch(navigate, "all_classifications");
	}, [navigate]);

	const handleLabelClick = useCallback(
		(tagName: string) => {
			navigateToClassificationSearch(navigate, tagName);
		},
		[navigate]
	);

	const renderTooltip = useCallback((props: unknown) => {
		const p = props as { active?: boolean; payload?: Array<{ payload?: { name: string; count: number } }> };
		if (!p?.active || !p?.payload?.length) return null;
		const row = p.payload[0]?.payload;
		if (!row) return null;
		return (
			<Box sx={{ p: 1.5, bgcolor: "background.paper", borderRadius: 1, boxShadow: 2, minWidth: 140 }}>
				<Typography variant="body2" fontWeight={600} sx={{ mb: 0.5 }}>
					{row.name}
				</Typography>
				<Typography variant="caption" display="block">
					Entities: {numberFormatWithComma(row.count)}
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
				"&:hover": { boxShadow: 4 }
			}}
		>
			<Box sx={{ pb: 2, borderBottom: "1px solid", borderColor: "divider" }}>
				<Stack direction="row" justifyContent="space-between" alignItems="center">
					<Typography sx={{ fontSize: "1rem", fontWeight: 600, color: "#1a1a1a" }}>
						Classification Distribution
					</Typography>
					<Link
						component="button"
						onClick={handleViewAll}
						sx={{ fontSize: "0.875rem", cursor: "pointer", textDecoration: "none", color: "primary.main" }}
						aria-label="View all classifications"
					>
						View All
					</Link>
				</Stack>
			</Box>
			<Typography variant="body2" sx={{ color: "#6c757d", mt: 2, lineHeight: 1.4 }}>
				<strong>Tag–entity associations (total):</strong>{" "}
				{numberFormatWithComma(associationTotal)}
			</Typography>
			<Typography variant="caption" sx={{ color: "#868e96", display: "block", mt: 0.75, lineHeight: 1.4 }}>
				The chart shows the top 5 classifications by number of entities in use.
			</Typography>
			{data.length === 0 ? (
				<Stack alignItems="center" justifyContent="center" height={200}>
					<Typography variant="body2" color="text.secondary">
						No classification data available
					</Typography>
				</Stack>
			) : (
				<Box sx={{ mt: 2, minHeight: 260, height: 260, width: "100%", minWidth: 280 }}>
					<ResponsiveContainer width="100%" height="100%" style={{ cursor: "pointer" }}>
						<BarChart
							data={data}
							layout="vertical"
							margin={{ ...CLASSIFICATION_DISTRIBUTION_CHART_MARGIN }}
						>
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
								width={52}
								label={{
									value: "Classification",
									angle: -90,
									position: "left",
									offset: 2,
									style: { fontSize: 10, fill: "#6c757d", textAnchor: "middle" },
								}}
								tick={(props: Record<string, unknown>) => {
									const { x = 0, y = 0, payload } = props;
									const p = payload as { value?: string; name?: string } | undefined;
									const value = p?.value ?? p?.name ?? (typeof payload === "string" ? payload : "");
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
								dataKey="count"
								name="Entities"
								fill={BAR_COLOR}
								radius={[0, 4, 4, 0]}
								onClick={(entry) => handleBarClick(entry)}
								cursor="pointer"
							>
								<LabelList
									dataKey="count"
									position="right"
									offset={10}
									formatter={(v: number) => numberFormatWithComma(v)}
									style={{
										fontSize: 12,
										fontWeight: 500,
										fill: BAR_COLOR,
									}}
								/>
								{data.map((_, index) => <Cell key={index} fill={BAR_COLOR} />)}
							</Bar>
						</BarChart>
					</ResponsiveContainer>
				</Box>
			)}
		</Paper>
	);
});

ClassificationDistributionCard.displayName = "ClassificationDistributionCard";

export default ClassificationDistributionCard;
