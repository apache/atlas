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

import { useEffect, useState, useMemo, useCallback } from "react";
import { Grid, Stack } from "@mui/material";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { fetchTypeHeaderData } from "@redux/slice/typeDefSlices/typeDefHeaderSlice";
import { getLatestEntities } from "@api/apiMethods/searchApiMethod";
import type { TypeHeaderInterface } from "@models/entityTreeType";
import DashboardSkeleton from "./DashboardSkeleton";
import EntityTypeBarChartSkeleton from "./EntityTypeBarChartSkeleton";
import LatestEntitiesSkeleton from "./LatestEntitiesSkeleton";
import OverviewCard from "./OverviewCard";
import EntityStatusDonut from "./EntityStatusDonut";
import ClassificationCoverage from "./ClassificationCoverage";
import EntityTypeBarChart from "./EntityTypeBarChart";
import LatestEntitiesList from "./LatestEntitiesList";
import RecentActivity from "./RecentActivity";
import KafkaTopicSummaryCard from "./KafkaTopicSummaryCard";
import ClassificationDistributionCard from "./ClassificationDistributionCard";

const DashboardOverview = () => {
	const dispatch = useAppDispatch();
	const { metricsData, loading: metricsLoading } = useAppSelector((state: { metrics: { metricsData: unknown; loading: boolean } }) => state.metrics);
	const typeHeaderData = useAppSelector(
		(state: { typeHeader?: { typeHeaderData?: TypeHeaderInterface[] | null } }) =>
			state.typeHeader?.typeHeaderData ?? null
	);
	const dashboardRefreshVersion = useAppSelector((state) => state.dashboardRefresh.version);
	const [latestEntities, setLatestEntities] = useState<unknown[]>([]);
	const [latestLoading, setLatestLoading] = useState(true);
	const [latestError, setLatestError] = useState<string | null>(null);

	const metrics = metricsData as {
		data?: {
			general?: { entityCount?: number; tagCount?: number; stats?: Record<string, unknown> };
			entity?: Record<string, unknown>;
			tag?: Record<string, unknown>;
		};
	} | null;
	const general = metrics?.data?.general;
	const entity = metrics?.data?.entity;
	const tag = metrics?.data?.tag;
	const stats = general?.stats;
	const entityCount = general?.entityCount ?? 0;
	const tagCount = general?.tagCount ?? 0;

	const isLoading = metricsLoading;

	const fetchLatestEntities = useCallback(async () => {
		setLatestLoading(true);
		setLatestError(null);
		try {
			const resp = await getLatestEntities();
			const entities = (resp as { data?: { entities?: unknown[] } })?.data?.entities ?? [];
			setLatestEntities(Array.isArray(entities) ? entities : []);
		} catch (err) {
			setLatestError(err instanceof Error ? err.message : "Failed to load latest entities");
			setLatestEntities([]);
		} finally {
			setLatestLoading(false);
		}
	}, []);

	useEffect(() => {
		fetchLatestEntities();
	}, [dashboardRefreshVersion, fetchLatestEntities]);

	useEffect(() => {
		dispatch(fetchTypeHeaderData());
	}, [dispatch]);

	const latestEntitiesList = useMemo(() => {
		if (!Array.isArray(latestEntities)) return [];
		return latestEntities.slice(0, 7) as { guid?: string; typeName?: string; attributes?: { name?: string; qualifiedName?: string; __timestamp?: number } }[];
	}, [latestEntities]);

	return (
		<Stack
			spacing={3}
			width="100%"
			sx={{
				maxWidth: "100%",
				boxSizing: "border-box",
				backgroundColor: "#f5f7f9",
				padding: 3,
				borderRadius: 2
			}}
		>
			<Grid container spacing={3} sx={{ width: "100%", alignItems: "stretch" }}>
				<Grid item xs={12} md={4}>
					{isLoading ? <DashboardSkeleton /> : <OverviewCard entityCount={entityCount} tagCount={tagCount} />}
				</Grid>
				<Grid item xs={12} md={4}>
					{isLoading ? <DashboardSkeleton /> : <EntityStatusDonut entity={entity} />}
				</Grid>
				<Grid item xs={12} md={4}>
					{isLoading ? (
						<DashboardSkeleton />
					) : (
						<ClassificationCoverage
							classificationTypeDefinitions={tagCount}
							tag={tag}
						/>
					)}
				</Grid>
				<Grid item xs={12} md={8} sx={{ display: "flex", minWidth: 0 }}>
					{isLoading ? (
						<EntityTypeBarChartSkeleton />
					) : (
						<EntityTypeBarChart entity={entity} typeHeaderData={typeHeaderData} />
					)}
				</Grid>
				<Grid item xs={12} md={4} sx={{ display: "flex", minWidth: 0 }}>
					{latestLoading ? (
						<LatestEntitiesSkeleton />
					) : (
						<LatestEntitiesList entities={latestEntitiesList} error={latestError} />
					)}
				</Grid>
				<Grid item xs={12} md={12} sx={{ display: "flex", minWidth: 0 }}>
					<RecentActivity />
				</Grid>
				<Grid item xs={12} md={12} sx={{ display: "flex", minWidth: 0 }}>
					{isLoading ? <EntityTypeBarChartSkeleton /> : <ClassificationDistributionCard tag={tag} isLoading={isLoading} />}
				</Grid>
				<Grid item xs={12} md={12} sx={{ display: "flex", minWidth: 0 }}>
					{isLoading ? <EntityTypeBarChartSkeleton /> : <KafkaTopicSummaryCard stats={stats} isLoading={isLoading} />}
				</Grid>
			</Grid>
		</Stack>
	);
};

export default DashboardOverview;
