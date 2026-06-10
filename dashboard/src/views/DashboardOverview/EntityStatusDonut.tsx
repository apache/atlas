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

import { memo, useCallback, useState } from "react";
import { Paper, Stack, Typography, Box } from "@mui/material";
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer, Sector } from "recharts";
import { useNavigate } from "react-router-dom";
import { numberFormatWithComma } from "@utils/Helper";
import { getEntityStatusTotals } from "@utils/metricsUtils";
import { navigateToSearch } from "@utils/dashboardSearchUtils";
import { ENTITY_STATUS_DONUT_COLORS as COLORS } from "./dashboardChartPalette";

interface EntityStatusDonutProps {
	entity: Record<string, unknown> | undefined;
	isLoading?: boolean;
}

const EntityStatusDonut = memo(({ entity, isLoading }: EntityStatusDonutProps) => {
	const [activeIndex, setActiveIndex] = useState<number>(-1);
	const navigate = useNavigate();
	const totals = getEntityStatusTotals(entity);
	const total = totals.active + totals.shell + totals.deleted;

	const chartData = [
		{ name: "Active", value: totals.active, color: COLORS.Active },
		{ name: "Shell", value: totals.shell, color: COLORS.Shell },
		{ name: "Deleted", value: totals.deleted, color: COLORS.Deleted }
	].filter((d) => d.value > 0);

	const getPercent = (val: number) => (total > 0 ? Math.round((val / total) * 100) : 0);

	const handleStatusClick = useCallback(
		(status: "Active" | "Shell" | "Deleted") => {
			if (status === "Active") {
				navigateToSearch(navigate, "entity_status");
			} else if (status === "Deleted") {
				navigateToSearch(navigate, "entity_status", {
					includeDE: true,
					entityFilters: {
						condition: "AND",
						criterion: [{ attributeName: "__state", operator: "eq", attributeValue: "DELETED" }]
					}
				});
			} else if (status === "Shell") {
				navigateToSearch(navigate, "entity_status", {
					entityFilters: {
						condition: "AND",
						criterion: [{ attributeName: "__isIncomplete", operator: "eq", attributeValue: "true" }]
					}
				});
			}
		},
		[navigate]
	);

	if (isLoading) return null;

	const renderActiveShape = (props: unknown) => {
		const p = props as { outerRadius?: number; innerRadius?: number; [k: string]: unknown };
		return (
			<Sector
				{...p}
				outerRadius={(p.outerRadius ?? 60) * 1.08}
				innerRadius={p.innerRadius ?? 40}
			/>
		);
	};

	return (
		<Paper
			elevation={1}
			sx={{
				padding: 2,
				borderRadius: 2,
				minHeight: 200,
				transition: "box-shadow 0.3s ease",
				"&:hover": { boxShadow: 4 }
			}}
		>
			<Box sx={{ pb: 2, borderBottom: "1px solid", borderColor: "divider" }}>
				<Typography sx={{ fontSize: "1rem", fontWeight: 600, color: "#1a1a1a" }}>
					Entity Status Overview
				</Typography>
			</Box>
			<Stack direction="row" spacing={2} alignItems="center" height={160} sx={{ pt: 2 }}>
				<Stack spacing={1.5} flex={1}>
					{(["Active", "Shell", "Deleted"] as const).map((status) => (
						<Box
							key={status}
							component="button"
							type="button"
							onClick={() => handleStatusClick(status)}
							aria-label={`View ${status} entities`}
							sx={{
								display: "flex",
								alignItems: "center",
								gap: 1.5,
								cursor: "pointer",
								background: "none",
								border: "none",
								padding: 0,
								margin: 0,
								font: "inherit",
								textAlign: "left",
								"&:hover": { opacity: 0.85 }
							}}
						>
							<Box
								sx={{
									width: 12,
									height: 12,
									borderRadius: "50%",
									backgroundColor: COLORS[status],
									flexShrink: 0
								}}
							/>
							<Typography component="span" sx={{ fontSize: "0.875rem", color: "#374151" }}>
								{status} {getPercent(totals[status.toLowerCase() as keyof typeof totals])}%
							</Typography>
						</Box>
					))}
				</Stack>
				<ResponsiveContainer width="50%" height="100%" style={{ cursor: "pointer" }}>
					<PieChart>
						<Pie
							data={chartData}
							cx="50%"
							cy="50%"
							innerRadius={40}
							outerRadius={60}
							paddingAngle={2}
							dataKey="value"
							isAnimationActive
							animationDuration={800}
							animationEasing="ease-out"
							activeIndex={activeIndex}
							activeShape={renderActiveShape}
							onMouseEnter={(_, index) => setActiveIndex(index)}
							onMouseLeave={() => setActiveIndex(-1)}
							onClick={(data) => handleStatusClick(data.name as "Active" | "Shell" | "Deleted")}
						>
							{chartData.map((entry, index) => (
								<Cell key={`cell-${index}`} fill={entry.color} stroke="none" />
							))}
						</Pie>
						<Tooltip
							formatter={(value: number) => numberFormatWithComma(value)}
							contentStyle={{ borderRadius: 8 }}
						/>
					</PieChart>
				</ResponsiveContainer>
			</Stack>
		</Paper>
	);
});

EntityStatusDonut.displayName = "EntityStatusDonut";

export default EntityStatusDonut;
