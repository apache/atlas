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
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from "recharts";
import { useNavigate } from "react-router-dom";
import { numberFormatWithComma } from "@utils/Helper";
import { getEntityStatusTotals, getPayloadFromRechartsEvent } from "@utils/metricsUtils";
import { navigateToSearch } from "@utils/dashboardSearchUtils";
import { ENTITY_STATUS_DONUT_COLORS as COLORS } from "./dashboardChartPalette";

interface EntityStatusDonutProps {
	entity: Record<string, unknown> | undefined;
	isLoading?: boolean;
}

interface StatusDonutDataItem {
	name: "Active" | "Shell" | "Deleted";
	value: number;
	color: string;
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
							<Box className={`donut-status-box donut-status-${status.toLowerCase()}`} />
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
							onClick={(data: unknown) => {
								const payload = getPayloadFromRechartsEvent<StatusDonutDataItem>(data);
								const name = payload?.name ?? (data as { name?: "Active" | "Shell" | "Deleted" })?.name;
								if (name === "Active" || name === "Shell" || name === "Deleted") {
									handleStatusClick(name);
								}
							}}
						>
							{chartData.map((entry, index) => (
								<Cell key={`cell-${index}`} fill={entry.color} stroke="none" />
							))}
						</Pie>
						<Tooltip
							formatter={(value: unknown) => numberFormatWithComma(Number(value || 0))}
							wrapperClassName="donut-tooltip-wrapper"
						/>
					</PieChart>
				</ResponsiveContainer>
			</Stack>
		</Paper>
	);
});

EntityStatusDonut.displayName = "EntityStatusDonut";

export default EntityStatusDonut;
