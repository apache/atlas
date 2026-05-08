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
import { Paper, Stack, Typography, Link, List, ListItem, Box } from "@mui/material";
import { Link as RouterLink } from "react-router-dom";
import moment from "moment";
import { useNavigate } from "react-router-dom";
import { navigateToLatestEntitiesSearch } from "@utils/dashboardSearchUtils";
import type { LatestEntityRowModel } from "./latestEntitiesList.utils";
import {
	resolveLatestEntityDisplayName,
	resolveLatestEntityGuid,
	resolveLatestEntityTypeName
} from "./latestEntitiesList.utils";

interface EntityItem extends LatestEntityRowModel {
	createTime?: number | Date | string;
	attributes?: LatestEntityRowModel["attributes"] & {
		__timestamp?: number | string | Record<string, unknown>;
		createTime?: number | string;
	};
}

interface LatestEntitiesListProps {
	entities: EntityItem[];
	isLoading?: boolean;
	error?: string | null;
}

const INVALID_TS_LABEL = "Created today";

const unwrapLongLike = (raw: unknown): unknown => {
	if (raw == null || typeof raw !== "object" || Array.isArray(raw)) return raw;
	const o = raw as Record<string, unknown>;
	if (typeof o.$numberLong === "string" || typeof o.$numberLong === "number") {
		return o.$numberLong;
	}
	if (typeof o.longValue === "string" || typeof o.longValue === "number") {
		return o.longValue;
	}
	return raw;
};

/**
 * Milliseconds since epoch, or null only when missing / unusable.
 * Rejects 0 (epoch) to avoid "56 years ago". Accepts sec or ms numbers, ISO strings.
 */
const normalizeEntityTimestampMs = (raw: unknown): number | null => {
	const v = unwrapLongLike(raw);
	if (v == null) return null;
	if (v instanceof Date) {
		const t = v.getTime();
		if (!Number.isFinite(t) || t <= 0) return null;
		return moment(t).isValid() ? t : null;
	}
	if (typeof v === "string") {
		const trimmed = v.trim();
		if (trimmed === "") return null;
		const n = Number(trimmed);
		if (Number.isFinite(n) && n > 0) {
			const ms = n < 1e12 ? n * 1000 : n;
			if (moment(ms).isValid() && ms > 0) return ms;
		}
		const parsed = moment(trimmed);
		if (parsed.isValid()) {
			const ms = parsed.valueOf();
			if (ms > 0) return ms;
		}
		return null;
	}
	if (typeof v === "number") {
		if (!Number.isFinite(v) || v <= 0) return null;
		const ms = v < 1e12 ? v * 1000 : v;
		return moment(ms).isValid() && ms > 0 ? ms : null;
	}
	return null;
};

/** Use only `__timestamp` for relative "Created … ago" (not `createTime`, often 0 in API). */
const getEntityTimestampRawForDisplay = (entity: EntityItem): unknown => {
	const a = entity.attributes;
	const top = entity as EntityItem & { __timestamp?: unknown };
	return a?.__timestamp ?? top.__timestamp;
};

/** Valid timestamp only: seconds / minutes / hours for last 24h, then moment relative. */
const formatCreatedRelativeFromMs = (ms: number): string => {
	const now = Date.now();
	const deltaMs = now - ms;
	if (!Number.isFinite(deltaMs)) return INVALID_TS_LABEL;
	if (deltaMs < 0) {
		return `Created ${moment(ms).fromNow()}`;
	}
	const totalSec = Math.floor(deltaMs / 1000);
	if (totalSec < 1) {
		return "Created just now";
	}
	if (totalSec < 60) {
		return totalSec === 1
			? "Created 1 second ago"
			: `Created ${totalSec} seconds ago`;
	}
	const totalMin = Math.floor(totalSec / 60);
	if (totalMin < 60) {
		return totalMin === 1
			? "Created 1 minute ago"
			: `Created ${totalMin} minutes ago`;
	}
	const totalHr = Math.floor(totalMin / 60);
	if (totalHr < 24) {
		return totalHr === 1
			? "Created 1 hour ago"
			: `Created ${totalHr} hours ago`;
	}
	return `Created ${moment(ms).fromNow()}`;
};

const formatRelativeTime = (raw: unknown): string => {
	const ms = normalizeEntityTimestampMs(raw);
	if (ms == null) return INVALID_TS_LABEL;
	return formatCreatedRelativeFromMs(ms);
};

const LatestEntitiesList = memo(({ entities, isLoading, error }: LatestEntitiesListProps) => {
	const navigate = useNavigate();

	const handleViewAll = useCallback(() => {
		navigateToLatestEntitiesSearch(navigate);
	}, [navigate]);

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
				flex: 1,
				boxSizing: "border-box",
				transition: "box-shadow 0.3s ease",
				"&:hover": { boxShadow: 4 }
			}}
		>
			<Box sx={{ pb: 2, borderBottom: "1px solid", borderColor: "divider" }}>
				<Stack direction="row" justifyContent="space-between" alignItems="center">
					<Typography sx={{ fontSize: "1rem", fontWeight: 600, color: "#1a1a1a" }}>
						Latest Entities Created
					</Typography>
					<Link
						component="button"
						onClick={handleViewAll}
						sx={{
							fontSize: "0.875rem",
							cursor: "pointer",
							textDecoration: "none",
							color: "primary.main"
						}}
						aria-label="View all entities"
					>
						View All
					</Link>
				</Stack>
			</Box>
			{error ? (
				<Stack alignItems="center" justifyContent="center" height={200} sx={{ pt: 2 }}>
					<Typography variant="body2" color="error">
						{error}
					</Typography>
				</Stack>
			) : !entities || entities.length === 0 ? (
				<Stack alignItems="center" justifyContent="center" height={200} sx={{ pt: 2 }}>
					<Typography variant="body2" color="text.secondary">
						No recent entities
					</Typography>
				</Stack>
			) : (
				<List disablePadding sx={{ pt: 2 }}>
					{entities.slice(0, 7).map((entity) => {
						const displayName = resolveLatestEntityDisplayName(entity);
						const entityGuid = resolveLatestEntityGuid(entity);
						const typeName = resolveLatestEntityTypeName(entity);
						const timestamp = getEntityTimestampRawForDisplay(entity);
						const detailHref = entityGuid
							? `/detailPage/${entityGuid}`
							: undefined;

						return (
							<ListItem
								key={entityGuid || displayName}
								disablePadding
								sx={{
									py: 1,
									borderBottom: "1px solid",
									borderColor: "divider",
									"&:last-child": { borderBottom: "none" }
								}}
							>
								<Stack width="100%" direction="row" justifyContent="space-between" alignItems="center">
									<Stack direction="row" spacing={0.5} alignItems="center" flexWrap="wrap" flex={1} minWidth={0} mr={1}>
										{detailHref ? (
											<Link
												component={RouterLink}
												to={detailHref}
												underline="hover"
												color="primary"
												sx={{
													fontSize: "0.875rem",
													overflow: "hidden",
													textOverflow: "ellipsis",
													cursor: "pointer",
													maxWidth: "100%"
												}}
											>
												{displayName}
											</Link>
										) : (
											<Typography
												component="span"
												sx={{
													fontSize: "0.875rem",
													fontWeight: 500,
													color: "text.primary"
												}}
											>
												{displayName}
											</Typography>
										)}
										<Typography
											component="span"
											sx={{
												fontSize: "0.875rem",
												color: "#6c757d",
												flexShrink: 0
											}}
										>
											({typeName})
										</Typography>
									</Stack>
									<Typography sx={{ fontSize: "0.8125rem", color: "#6c757d", flexShrink: 0, ml: 1 }}>
										{formatRelativeTime(timestamp)}
									</Typography>
								</Stack>
							</ListItem>
						);
					})}
				</List>
			)}
		</Paper>
	);
});

LatestEntitiesList.displayName = "LatestEntitiesList";

export default LatestEntitiesList;
