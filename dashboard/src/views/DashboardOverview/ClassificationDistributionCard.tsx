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
	getPayloadFromRechartsEvent,
	type ClassificationDistributionItem,
} from "@utils/metricsUtils";
import { navigateToSearch, navigateToClassificationSearch } from "@utils/dashboardSearchUtils";
import {
	CHART_BAR_ACTIVE_BLUE,
	CLASSIFICATION_DISTRIBUTION_CHART_MARGIN,
	getClassificationYAxisWidth,
	isClassificationYAxisLabelTruncated,
	truncateClassificationYAxisLabel,
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
	const yAxisWidth = useMemo(
		() => getClassificationYAxisWidth(data.map((item) => item.name)),
		[data],
	);

	const handleBarClick = useCallback(
		(barProps: unknown) => {
			const name = getPayloadFromRechartsEvent<ClassificationDistributionItem>(barProps)?.name;
			if (name) {
				navigateToClassificationSearch(navigate, name);
			}
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
			<Paper className="chart-tooltip-box">
				<Typography variant="body2" className="chart-tooltip-title">
					{row.name}
				</Typography>
				<Typography variant="caption" display="block">
					Entities: {numberFormatWithComma(row.count)}
				</Typography>
			</Paper>
		);
	}, []);

	if (isLoading) return null;

	return (
		<Paper elevation={1} className="classification-card-wrapper">
			<Box className="chart-card-header">
				<Stack direction="row" justifyContent="space-between" alignItems="center">
					<Typography className="chart-card-title">
						Classification Distribution
					</Typography>
					<Link
						component="button"
						onClick={handleViewAll}
						className="chart-view-all-link"
						aria-label="View all classifications"
					>
						View All
					</Link>
				</Stack>
			</Box>
			<Typography variant="body2" className="classification-assoc-total">
				<strong>Tag–entity associations (total):</strong>{" "}
				{numberFormatWithComma(associationTotal)}
			</Typography>
			<Typography variant="caption" className="classification-caption">
				The chart shows the top 5 classifications by number of entities in use.
			</Typography>
			{data.length === 0 ? (
				<Stack alignItems="center" justifyContent="center" height={200}>
					<Typography variant="body2" color="text.secondary">
						No classification data available
					</Typography>
				</Stack>
			) : (
				<Box className="classification-chart-container">
					<ResponsiveContainer width="100%" height="100%" className="chart-cursor-pointer">
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
									className: "chart-axis-label",
								}}
							/>
							<YAxis
								type="category"
								dataKey="name"
								width={yAxisWidth}
								tickMargin={4}
								tick={(props: Record<string, unknown>) => {
									const { x = 0, y = 0, payload } = props;
									const p = payload as { value?: string; name?: string } | undefined;
									const value = p?.value ?? p?.name ?? (typeof payload === "string" ? payload : "");
									const displayLabel = truncateClassificationYAxisLabel(value);
									const isTruncated = isClassificationYAxisLabelTruncated(value);
									return (
										<g
											transform={`translate(${x},${y})`}
											onClick={() => (value ? handleLabelClick(value) : undefined)}
											className={value ? "chart-cursor-pointer" : "chart-cursor-default"}
											role={value ? "button" : undefined}
											tabIndex={value ? 0 : undefined}
											aria-label={value || undefined}
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
											{isTruncated ? <title>{value}</title> : null}
											<text x={0} y={0} dy={4} textAnchor="end" fill="#333" fontSize={12}>
												{displayLabel}
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
								onClick={handleBarClick}
								cursor="pointer"
							>
								<LabelList
									dataKey="count"
									position="right"
									offset={10}
									formatter={(v: unknown) => numberFormatWithComma(Number(v))}
									className="chart-label-list"
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
