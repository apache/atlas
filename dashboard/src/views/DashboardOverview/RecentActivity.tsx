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

import { memo, useCallback, useEffect, useState } from "react";
import { useAppSelector } from "@hooks/reducerHook";
import { Paper, Stack, Typography, Link, Box, Chip, List, ListItem } from "@mui/material";
import { RecentActivityListSkeleton } from "./RecentActivitySkeleton";
import TypeDefAuditDetailModal from "@components/TypeDefAuditDetailModal";
import { Link as RouterLink, useNavigate } from "react-router-dom";
import { getAuditData } from "@api/apiMethods/detailpageApiMethod";
import { category } from "@utils/Enum";
import { extractTypeDefDetailObject } from "@utils/auditTypeDefUtils";
import { dateFormat, isEmpty, jsonParse } from "@utils/Utils";

export interface AuditRecord {
	guid?: string;
	userName?: string;
	operation?: string;
	params?: string;
	result?: string;
	startTime?: number;
	endTime?: number;
}

const AUDIT_TABS = [
	{ id: "typeCreated", label: "Created", operations: ["TYPE_DEF_CREATE"] },
	{ id: "typeUpdated", label: "Updated", operations: ["TYPE_DEF_UPDATE"] },
	{ id: "typeDeleted", label: "Deleted", operations: ["TYPE_DEF_DELETE"] },
	{ id: "purge", label: "Purge", operations: ["PURGE", "AUTO_PURGE"] },
	{ id: "import", label: "Import", operations: ["IMPORT"] },
	{ id: "export", label: "Export", operations: ["EXPORT"] }
] as const;

interface ParsedAuditResult {
	typeLabel: string;
	entityName: string;
	entityGuid?: string;
	actionPhrase: string;
	user: string;
	dateStr: string;
}

const parseAuditRecord = (record: AuditRecord): ParsedAuditResult => {
	const { operation, params, result, userName, endTime } = record;
	const user = userName || "Unknown";
	const dateStr = endTime ? dateFormat(endTime) : "";
	let typeLabel = category[params as keyof typeof category] || params || "Type";
	let actionPhrase = "";
	let entityName = "";
	let entityGuid = "";

	if (operation === "PURGE" || operation === "AUTO_PURGE") {
		try {
			const guidStr = typeof result === "string" ? result.replace("[", "").replace("]", "").split(",")[0]?.trim() : "";
			entityGuid = guidStr ?? "";
			if (entityGuid) entityName = "View entity";
		} catch {
			/* ignore */
		}
		actionPhrase = "operation was performed";
		return { typeLabel: operation === "AUTO_PURGE" ? "Auto purge" : "Purge", entityName, entityGuid, actionPhrase, user, dateStr };
	}
	if (operation === "EXPORT") {
		return { typeLabel: "Export", entityName: "", entityGuid: "", actionPhrase: "operation was performed", user, dateStr };
	}
	if (operation === "IMPORT") {
		return { typeLabel: "Import", entityName: "", entityGuid: "", actionPhrase: "operation was performed", user, dateStr };
	}

	if (operation === "TYPE_DEF_CREATE") {
		actionPhrase = "was created";
	} else if (operation === "TYPE_DEF_UPDATE") {
		actionPhrase = "was updated";
	} else if (operation === "TYPE_DEF_DELETE") {
		actionPhrase = "was deleted";
	} else {
		return { typeLabel: operation || "", entityName: "", entityGuid: "", actionPhrase: "", user, dateStr };
	}

	try {
		const resultObj = typeof result === "string" ? jsonParse(result) : result;
		if (resultObj?.name) {
			entityName = resultObj.name;
			entityGuid = resultObj.guid ?? "";
		} else if (resultObj && typeof resultObj === "object") {
			const paramsKey = params?.split(",")[0]?.trim();
			const arr = paramsKey ? resultObj[paramsKey] : resultObj[Object.keys(resultObj)[0]];
			if (Array.isArray(arr) && arr[0]) {
				entityName = arr[0].name ?? "";
				entityGuid = arr[0].guid ?? "";
			} else if (!Array.isArray(arr) && arr?.name) {
				entityName = arr.name;
				entityGuid = arr.guid ?? "";
			}
		}
	} catch {
		/* ignore */
	}

	return { typeLabel, entityName, entityGuid, actionPhrase, user, dateStr };
};

const getDetailUrl = (
	operation: string | undefined,
	params: string,
	entityName: string,
	entityGuid: string
): string | null => {
	if ((operation === "PURGE" || operation === "AUTO_PURGE") && entityGuid) {
		return `/detailPage/${entityGuid}`;
	}
	if (!entityName && !entityGuid) return null;
	/* Type Created/Updated/Deleted tabs - link to type details */
	const param = params?.split(",")[0]?.trim();
	if (param === "CLASSIFICATION") {
		return `/tag/tagAttribute/${entityName}`;
	}
	if (param === "BUSINESS_METADATA" && entityGuid) {
		return `/administrator/businessMetadata/${entityGuid}`;
	}
	if (["ENTITY", "ENUM", "RELATIONSHIP", "STRUCT"].includes(param ?? "")) {
		return null;
	}
	return `/administrator?tabActive=typeSystem`;
};

const RecentActivity = memo(() => {
	const dashboardRefreshVersion = useAppSelector((state) => state.dashboardRefresh.version);
	const navigate = useNavigate();
	const [activeTab, setActiveTab] = useState(0);
	const [tabData, setTabData] = useState<Record<string, AuditRecord[]>>({
		typeCreated: [],
		typeUpdated: [],
		typeDeleted: [],
		purge: [],
		import: [],
		export: []
	});
	const [loading, setLoading] = useState<Record<string, boolean>>({
		typeCreated: false,
		typeUpdated: false,
		typeDeleted: false,
		purge: false,
		import: false,
		export: false
	});

	const fetchAudits = useCallback(async (tabId: string, operations: string[]) => {
		setLoading((prev) => ({ ...prev, [tabId]: true }));
		try {
			const allResults: AuditRecord[] = [];
			for (const op of operations) {
				const auditFilters = {
					condition: "AND" as const,
					criterion: [
						{
							attributeName: "operation",
							operator: "eq" as const,
							attributeValue: op
						}
					]
				};
				const resp = await getAuditData({
					auditFilters,
					limit: 5,
					offset: 0,
					sortBy: "startTime",
					sortOrder: "DESCENDING"
				});
				const data = (resp as { data?: AuditRecord[] })?.data ?? [];
				if (Array.isArray(data)) allResults.push(...data);
			}
			allResults.sort((a, b) => (b.endTime ?? 0) - (a.endTime ?? 0));
			setTabData((prev) => ({ ...prev, [tabId]: allResults.slice(0, 5) }));
		} catch {
			setTabData((prev) => ({ ...prev, [tabId]: [] }));
		} finally {
			setLoading((prev) => ({ ...prev, [tabId]: false }));
		}
	}, []);

	useEffect(() => {
		AUDIT_TABS.forEach((tab) => {
			fetchAudits(tab.id, [...tab.operations]);
		});
	}, [fetchAudits, dashboardRefreshVersion]);

	const handleViewAll = useCallback(() => {
		navigate("/administrator?tabActive=audit");
	}, [navigate]);

	const handleTabChange = useCallback((_: React.SyntheticEvent, newValue: number) => {
		setActiveTab(newValue);
	}, []);

	const currentTab = AUDIT_TABS[activeTab];
	const records = tabData[currentTab?.id] ?? [];
	const isLoading = loading[currentTab?.id] ?? false;

	const [typeDetailOpen, setTypeDetailOpen] = useState(false);
	const [typeDetailObject, setTypeDetailObject] = useState<Record<string, unknown> | null>(null);

	const handleCloseTypeDetail = useCallback(() => {
		setTypeDetailOpen(false);
		setTypeDetailObject(null);
	}, []);

	const handleOpenTypeDetail = useCallback((obj: Record<string, unknown>) => {
		setTypeDetailObject(obj);
		setTypeDetailOpen(true);
	}, []);

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
				"&:hover": { boxShadow: 4 }
			}}
		>
			<Box sx={{ pb: 2, borderBottom: "1px solid", borderColor: "divider" }}>
				<Stack direction="row" justifyContent="space-between" alignItems="center">
					<Typography sx={{ fontSize: "1rem", fontWeight: 600, color: "#1a1a1a" }}>
						Recent Activity
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
						aria-label="View all audits"
					>
						View All
					</Link>
				</Stack>
			</Box>
			<Stack direction="row" spacing={1} sx={{ mt: 1.5, flexWrap: "wrap", gap: 1 }}>
				{AUDIT_TABS.map((tab, idx) => (
					<Chip
						key={tab.id}
						label={tab.label}
						onClick={(e) => handleTabChange(e as unknown as React.SyntheticEvent, idx)}
						color={activeTab === idx ? "primary" : "default"}
						variant={activeTab === idx ? "filled" : "outlined"}
						sx={{
							fontSize: "0.8125rem",
							height: 32,
							"&.MuiChip-filled": { fontWeight: 600 }
						}}
						aria-pressed={activeTab === idx}
						aria-label={`${tab.label} tab`}
					/>
				))}
			</Stack>
			<Box sx={{ pt: 1 }}>
				{isLoading ? (
					<RecentActivityListSkeleton />
				) : isEmpty(records) ? (
					<Stack alignItems="center" justifyContent="center" height={180}>
						<Typography variant="body2" color="text.secondary">
							No records present
						</Typography>
					</Stack>
				) : (
					<List disablePadding>
						{records.map((record, idx) => {
							const parsed = parseAuditRecord(record);
							const { typeLabel, entityName, entityGuid, actionPhrase, user, dateStr } = parsed;
							const typeDefPayload = extractTypeDefDetailObject(
								record,
								entityName ?? "",
								entityGuid ?? ""
							);
							const detailUrl = getDetailUrl(
								record.operation ?? "",
								record.params ?? "",
								entityName ?? "",
								entityGuid ?? ""
							);

							const openTypeModalIfNeeded = () => {
								if (typeDefPayload) {
									handleOpenTypeDetail(typeDefPayload);
								}
							};

							const handleEntityNameKeyDown = (
								e: React.KeyboardEvent
							) => {
								if (e.key !== "Enter" && e.key !== " ") return;
								e.preventDefault();
								openTypeModalIfNeeded();
							};

							return (
								<ListItem
									key={record.guid ?? idx}
									disablePadding
									sx={{
										py: 1,
										borderBottom: "1px solid",
										borderColor: "divider",
										"&:last-child": { borderBottom: "none" }
									}}
								>
									<Typography component="span" sx={{ fontSize: "0.875rem", color: "#333" }}>
										{entityName ? (
											<>
												{typeDefPayload ? (
													<Link
														component="button"
														type="button"
														onClick={openTypeModalIfNeeded}
														onKeyDown={handleEntityNameKeyDown}
														tabIndex={0}
														aria-label={`Open ${typeLabel} type details for ${entityName}`}
														sx={{
															color: "primary.main",
															textDecoration: "none",
															background: "none",
															border: "none",
															cursor: "pointer",
															padding: 0,
															font: "inherit",
															"&:hover": { textDecoration: "underline" },
														}}
													>
														{entityName}
													</Link>
												) : detailUrl ? (
													<Link
														component={RouterLink}
														to={detailUrl}
														sx={{
															color: "primary.main",
															textDecoration: "none",
															"&:hover": { textDecoration: "underline" }
														}}
													>
														{entityName}
													</Link>
												) : (
													entityName
												)}
												{" "}
												{typeLabel} {actionPhrase} by{" "}
												<Box component="span" sx={{ fontWeight: 700 }}>
													{user}
												</Box>{" "}
												on{" "}
												<Box component="span" sx={{ color: "#6c757d" }}>
													{dateStr}
												</Box>
											</>
										) : (
											<>
												{typeLabel} {actionPhrase} by{" "}
												<Box component="span" sx={{ fontWeight: 700 }}>
													{user}
												</Box>{" "}
												on{" "}
												<Box component="span" sx={{ color: "#6c757d" }}>
													{dateStr}
												</Box>
											</>
										)}
									</Typography>
								</ListItem>
							);
						})}
					</List>
				)}
			</Box>
			<TypeDefAuditDetailModal
				open={typeDetailOpen}
				onClose={handleCloseTypeDetail}
				detailObject={typeDetailObject}
			/>
		</Paper>
	);
});

RecentActivity.displayName = "RecentActivity";

export default RecentActivity;
