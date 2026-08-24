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
		<Paper elevation={1} className="chart-card">
			<Box className="chart-card-header">
				<Typography className="chart-card-title">
					Entity Status Overview
				</Typography>
			</Box>
			<Stack direction="row" spacing={2} alignItems="center" height={160} className="donut-status-stack">
				<Stack spacing={1.5} flex={1}>
					{(["Active", "Shell", "Deleted"] as const).map((status) => (
						<Box
							key={status}
							component="button"
							type="button"
							onClick={() => handleStatusClick(status)}
							aria-label={`View ${status} entities`}
							className="donut-status-button"
						>
							<Box className="donut-status-box" style={{ backgroundColor: COLORS[status] }} />
							<Typography component="span" className="donut-status-text">
								{status} {getPercent(totals[status.toLowerCase() as keyof typeof totals])}%
							</Typography>
						</Box>
					))}
				</Stack>
				<ResponsiveContainer width="50%" height="100%" className="chart-cursor-pointer">
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
							activeShape={renderActiveShape}
							onClick={(data) => handleStatusClick(data.name as "Active" | "Shell" | "Deleted")}
						>
							{chartData.map((entry, index) => (
								<Cell key={`cell-${index}`} fill={entry.color} stroke="none" />
							))}
						</Pie>
						<Tooltip
							formatter={(value: unknown) => numberFormatWithComma(Number(value || 0))}
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
