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

import { Fragment, memo, useMemo, useState, useCallback } from "react";
import {
	Paper,
	Stack,
	Typography,
	Table,
	TableBody,
	TableCell,
	TableContainer,
	TableHead,
	TableRow,
	Box,
	IconButton,
	Tooltip,
	Collapse,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import { numberFormatWithComma } from "@utils/Helper";
import { formatedDate } from "@utils/Utils";
import {
	parseMetricsStats,
	getMessageConsumptionData,
	getMessageConsumptionDataExcludingTotal,
	buildTopicNotificationRecord,
	topicRowHasPeriodMetrics,
	type MessageConsumptionItem,
} from "@utils/metricsUtils";
import MessageConsumptionChart from "./MessageConsumptionChart";
import {
	CHART_BAR_ACTIVE_BLUE,
	ENTITY_STATUS_DONUT_COLORS,
} from "./dashboardChartPalette";

const CREATES_COLOR = ENTITY_STATUS_DONUT_COLORS.Active;
const UPDATES_COLOR = CHART_BAR_ACTIVE_BLUE;
const DELETES_COLOR = ENTITY_STATUS_DONUT_COLORS.Deleted;

interface TopicDetail {
	topic: string;
	processed: number;
	failed: number;
	lastProcessed: string | number;
	/** Partition stats — same shape as aggregate Notification (per AtlasMetricsUtil). */
	topicStats: Record<string, unknown>;
}

interface KafkaTopicSummaryCardProps {
	stats: Record<string, unknown> | undefined;
	isLoading?: boolean;
}

const getTopicConsumptionPanelId = (topic: string): string =>
	`kafka-topic-msg-panel-${topic.replace(/[^a-zA-Z0-9_-]/g, "_")}`;

const TotalProcessedTooltip = ({
	total,
}: {
	total: MessageConsumptionItem | undefined;
}) => {
	if (!total) {
		return (
			<Typography variant="caption" component="span">
				No breakdown available
			</Typography>
		);
	}
	return (
		<Box sx={{ p: 1, minWidth: 160, bgcolor: "#fff" }}>
			<Typography
				variant="body2"
				fontWeight={600}
				sx={{ mb: 0.5, color: "#1a1a1a" }}
			>
				{total.period}
			</Typography>
			<Typography variant="caption" display="block" sx={{ color: CREATES_COLOR }}>
				Creates: {numberFormatWithComma(total.creates)}
			</Typography>
			<Typography variant="caption" display="block" sx={{ color: UPDATES_COLOR }}>
				Updates: {numberFormatWithComma(total.updates)}
			</Typography>
			<Typography variant="caption" display="block" sx={{ color: DELETES_COLOR }}>
				Deletes: {numberFormatWithComma(total.deletes)}
			</Typography>
			<Typography variant="caption" display="block" sx={{ color: "#6c757d", mt: 0.5 }}>
				Messages processed: {numberFormatWithComma(total.count)}
			</Typography>
			<Typography variant="caption" display="block" sx={{ color: "#6c757d" }}>
				Avg time (ms): {numberFormatWithComma(total.avgTime)}
			</Typography>
		</Box>
	);
};

const KafkaTopicSummaryCard = memo(({ stats, isLoading }: KafkaTopicSummaryCardProps) => {
	const [sortKey, setSortKey] = useState<"topic" | "processed" | "failed">("topic");
	const [sortOrder, setSortOrder] = useState<"asc" | "desc">("asc");
	/** One expanded topic at a time to limit vertical growth. */
	const [expandedTopic, setExpandedTopic] = useState<string | null>(null);

	const parsed = useMemo(() => parseMetricsStats(stats), [stats]);
	const notification = parsed?.Notification as Record<string, unknown> | undefined;
	const topicDetails = notification?.topicDetails as
		| Record<string, Record<string, unknown>>
		| undefined;

	const rows = useMemo((): TopicDetail[] => {
		if (!topicDetails || typeof topicDetails !== "object") return [];
		return Object.entries(topicDetails).map(([topic, data]) => {
			const lp = data?.lastMessageProcessedTime;
			const lastProcessed: string | number =
				lp != null && typeof lp !== "object" ? (lp as string | number) : "-";
			return {
				topic,
				processed: Number(data?.processedMessageCount ?? 0),
				failed: Number(data?.failedMessageCount ?? 0),
				lastProcessed,
				topicStats: data ?? {},
			};
		});
	}, [topicDetails]);

	const sortedRows = useMemo(() => {
		const list = [...rows];
		const dir = sortOrder === "asc" ? 1 : -1;
		return list.sort((a, b) => {
			if (sortKey === "topic") {
				const av = String(a.topic).toLowerCase();
				const bv = String(b.topic).toLowerCase();
				return (av < bv ? -1 : av > bv ? 1 : 0) * dir;
			}
			const av = a[sortKey];
			const bv = b[sortKey];
			return ((av as number) - (bv as number)) * dir;
		});
	}, [rows, sortKey, sortOrder]);

	const consumptionByTopic = useMemo(() => {
		const m = new Map<
			string,
			{ totalRow: MessageConsumptionItem | undefined; chartData: MessageConsumptionItem[] }
		>();
		for (const row of rows) {
			const record = buildTopicNotificationRecord(row.topicStats, {
				aggregateNotification: notification,
				useAggregateFallback: !topicRowHasPeriodMetrics(row.topicStats),
			});
			const full = getMessageConsumptionData(record);
			m.set(row.topic, {
				totalRow: full.find((d) => d.period === "Total"),
				chartData: getMessageConsumptionDataExcludingTotal(record),
			});
		}
		return m;
	}, [rows, notification]);

	const handleSort = useCallback((key: "topic" | "processed" | "failed") => {
		setSortKey(key);
		setSortOrder((prev) => (prev === "asc" ? "desc" : "asc"));
	}, []);

	const handleToggleExpand = useCallback((topic: string) => {
		setExpandedTopic((prev) => (prev === topic ? null : topic));
	}, []);

	const formatLastProcessed = (val: string | number): string => {
		if (val === "-" || val == null) return "-";
		if (typeof val === "number") return formatedDate({ date: val });
		return String(val);
	};

	const hasExpanded = expandedTopic !== null;

	if (isLoading) return null;

	return (
		<Paper
			elevation={1}
			sx={{
				padding: 2,
				borderRadius: 2,
				minHeight: 280,
				width: "100%",
				boxSizing: "border-box",
				transition: "box-shadow 0.3s ease",
				"&:hover": { boxShadow: 4 },
			}}
		>
			<Box sx={{ pb: 2, borderBottom: "1px solid", borderColor: "divider" }}>
				<Typography sx={{ fontSize: "1rem", fontWeight: 600, color: "#1a1a1a" }}>
					Kafka Topic Summary
				</Typography>
			</Box>
			{rows.length === 0 ? (
				<Stack alignItems="center" justifyContent="center" height={180}>
					<Typography variant="body2" color="text.secondary">
						No topic data available
					</Typography>
				</Stack>
			) : (
				<TableContainer sx={{ maxHeight: hasExpanded ? 560 : 220, mt: 1 }}>
					<Table size="small" stickyHeader>
						<TableHead>
							<TableRow>
								<TableCell
									onClick={() => handleSort("topic")}
									sx={{ cursor: "pointer", fontWeight: 600 }}
									aria-label="Sort by topic"
								>
									Topic{" "}
									{sortKey === "topic"
										? sortOrder === "asc"
											? "▲"
											: "▼"
										: ""}
								</TableCell>
								<TableCell
									align="right"
									onClick={() => handleSort("processed")}
									sx={{ cursor: "pointer", fontWeight: 600 }}
									aria-label="Sort by processed"
								>
									Processed{" "}
									{sortKey === "processed"
										? sortOrder === "asc"
											? "▲"
											: "▼"
										: ""}
								</TableCell>
								<TableCell
									align="right"
									onClick={() => handleSort("failed")}
									sx={{ cursor: "pointer", fontWeight: 600 }}
									aria-label="Sort by failed"
								>
									Failed{" "}
									{sortKey === "failed"
										? sortOrder === "asc"
											? "▲"
											: "▼"
										: ""}
								</TableCell>
								<TableCell align="right" sx={{ fontWeight: 600 }}>
									Last Processed
								</TableCell>
							</TableRow>
						</TableHead>
						<TableBody>
							{sortedRows.map((row) => {
								const isExpanded = expandedTopic === row.topic;
								const panelId = getTopicConsumptionPanelId(row.topic);
								const cons = consumptionByTopic.get(row.topic);
								const totalForHover = cons?.totalRow;
								const chartData = cons?.chartData ?? [];
								return (
									<Fragment key={row.topic}>
										<TableRow hover>
											<TableCell sx={{ fontSize: "0.8125rem" }}>
												<Stack direction="row" alignItems="center" spacing={0.5}>
													<IconButton
														size="small"
														aria-label={
															isExpanded
																? `Collapse message consumption for ${row.topic}`
																: `Expand message consumption for ${row.topic}`
														}
														aria-expanded={isExpanded}
														aria-controls={panelId}
														id={`${panelId}-trigger`}
														onClick={(e) => {
															e.stopPropagation();
															handleToggleExpand(row.topic);
														}}
														onKeyDown={(e) => {
															if (e.key === " " || e.key === "Enter") {
																e.stopPropagation();
															}
														}}
													>
														{isExpanded ? (
															<ExpandLessIcon fontSize="small" />
														) : (
															<ExpandMoreIcon fontSize="small" />
														)}
													</IconButton>
													<Typography
														component="span"
														variant="body2"
														sx={{ fontSize: "0.8125rem" }}
													>
														{row.topic}
													</Typography>
												</Stack>
											</TableCell>
											<TableCell align="right" sx={{ fontSize: "0.8125rem" }}>
												<Tooltip
													title={<TotalProcessedTooltip total={totalForHover} />}
													arrow
													enterDelay={200}
													placement="top"
													componentsProps={{
														tooltip: {
															sx: {
																bgcolor: "#fff",
																color: "#1a1a1a",
																border: "1px solid",
																borderColor: "rgba(0, 0, 0, 0.12)",
																boxShadow: 2,
															},
														},
														arrow: {
															sx: {
																color: "#fff",
															},
														},
													}}
												>
													<Box
														component="span"
														tabIndex={0}
														sx={{
															cursor: "help",
															borderBottom: "1px dotted",
															borderColor: "text.secondary",
														}}
														aria-label="Total processed — hover for creates, updates, deletes"
													>
														{numberFormatWithComma(row.processed)}
													</Box>
												</Tooltip>
											</TableCell>
											<TableCell align="right" sx={{ fontSize: "0.8125rem" }}>
												{numberFormatWithComma(row.failed)}
											</TableCell>
											<TableCell align="right" sx={{ fontSize: "0.8125rem" }}>
												{formatLastProcessed(row.lastProcessed)}
											</TableCell>
										</TableRow>
										{isExpanded ? (
											<TableRow>
												<TableCell
													colSpan={4}
													sx={{ p: 0, borderBottom: "none" }}
												>
													<Collapse in={isExpanded} timeout="auto" unmountOnExit>
														<Box
															id={panelId}
															sx={{
																py: 2,
																px: 2,
																bgcolor: "action.hover",
																borderBottom: "1px solid",
																borderColor: "divider",
															}}
														>
															<Typography
																variant="subtitle2"
																sx={{ mb: 1, fontWeight: 600, color: "#1a1a1a" }}
															>
																Message consumption
															</Typography>
															<MessageConsumptionChart
																data={chartData}
																chartAriaLabel={`Message consumption for ${row.topic}`}
															/>
														</Box>
													</Collapse>
												</TableCell>
											</TableRow>
										) : null}
									</Fragment>
								);
							})}
						</TableBody>
					</Table>
				</TableContainer>
			)}
		</Paper>
	);
});

KafkaTopicSummaryCard.displayName = "KafkaTopicSummaryCard";

export default KafkaTopicSummaryCard;
