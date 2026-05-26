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

import { memo, useCallback, useEffect, useMemo, useState } from "react";
import {
	Paper,
	Stack,
	Typography,
	Box,
	List,
	ListItem,
	Link,
	TextField,
	InputAdornment,
} from "@mui/material";
import SearchIcon from "@mui/icons-material/Search";
import { Link as RouterLink, useNavigate } from "react-router-dom";
import CustomModal from "@components/Modal";
import { numberFormatWithComma } from "@utils/Helper";
import {
	getClassificationTypesInUseCount,
	getUnusedClassificationNames,
} from "@utils/metricsUtils";
import { navigateToTaggedSearch } from "@utils/dashboardSearchUtils";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { fetchClassificationData } from "@redux/slice/typeDefSlices/typedefClassificationSlice";

interface ClassificationCoverageProps {
	/** Defined classification types in the type system (from general.tagCount) */
	classificationTypeDefinitions: number;
	tag: Record<string, unknown> | undefined;
	isLoading?: boolean;
}

const ClassificationCoverage = memo(
	({
		classificationTypeDefinitions,
		tag,
		isLoading,
	}: ClassificationCoverageProps) => {
		const navigate = useNavigate();
		const dispatch = useAppDispatch();
		const { classificationData, loadingClassification } = useAppSelector(
			(state: { classification?: { classificationData?: { classificationDefs?: { name: string }[] } | null; loadingClassification?: boolean } }) =>
				state.classification ?? {}
		);

		useEffect(() => {
			if (!classificationData && !loadingClassification) {
				void dispatch(fetchClassificationData());
			}
		}, [classificationData, loadingClassification, dispatch]);

		const definedNames = useMemo(() => {
			const defs =
				classificationData?.classificationDefs as { name?: string }[] | undefined;
			if (!Array.isArray(defs)) return [];
			return defs.map((d) => d.name).filter((n): n is string => Boolean(n));
		}, [classificationData]);

		const typesInUse = useMemo(
			() => getClassificationTypesInUseCount(tag),
			[tag]
		);
		const unusedNames = useMemo(
			() => getUnusedClassificationNames(definedNames, tag),
			[definedNames, tag]
		);
		const unusedCount = unusedNames.length;

		const typeUsageProgress =
			classificationTypeDefinitions > 0
				? (typesInUse / classificationTypeDefinitions) * 100
				: 0;
		const usagePercentRounded = Math.min(
			100,
			Math.round(Number.isFinite(typeUsageProgress) ? typeUsageProgress : 0)
		);

		const [unusedModalOpen, setUnusedModalOpen] = useState(false);
		const [unusedListSearchQuery, setUnusedListSearchQuery] = useState("");

		const handleClassificationsClick = useCallback(() => {
			navigateToTaggedSearch(navigate);
		}, [navigate]);

		const handleOpenUnusedModal = useCallback(() => {
			if (unusedCount > 0 && definedNames.length > 0) {
				setUnusedListSearchQuery("");
				setUnusedModalOpen(true);
			}
		}, [unusedCount, definedNames.length]);

		const handleCloseUnusedModal = useCallback(() => {
			setUnusedListSearchQuery("");
			setUnusedModalOpen(false);
		}, []);

		const handleUnusedBarSegmentClick = useCallback(
			(e: React.MouseEvent) => {
				e.stopPropagation();
				handleOpenUnusedModal();
			},
			[handleOpenUnusedModal]
		);

		const unusedNamesFiltered = useMemo(() => {
			const q = unusedListSearchQuery.trim().toLowerCase();
			if (!q) return unusedNames;
			return unusedNames.filter((name) => name.toLowerCase().includes(q));
		}, [unusedNames, unusedListSearchQuery]);

		const showUnusedListSearch = unusedNames.length > 5;

		if (isLoading) return null;

		const usedBarPct = Math.min(Math.max(typeUsageProgress, 0), 100);
		const defsReady = definedNames.length > 0 || !!classificationData;

		return (
			<Paper
				elevation={1}
				sx={{
					padding: 2,
					borderRadius: 2,
					minHeight: 200,
					height: "100%",
					boxSizing: "border-box",
					transition: "box-shadow 0.3s ease",
					"&:hover": { boxShadow: 4 },
				}}
			>
				<Box sx={{ pb: 2, borderBottom: "1px solid", borderColor: "divider" }}>
					<Typography sx={{ fontSize: "1rem", fontWeight: 600, color: "#1a1a1a" }}>
						Classification Types In Use
					</Typography>
				</Box>
				<Stack spacing={1.5} sx={{ pt: 2 }}>
					<Stack
						direction="row"
						justifyContent="space-between"
						alignItems="center"
						sx={{ width: "100%" }}
					>
						<Typography
							sx={{
								fontSize: "0.875rem",
								fontWeight: 600,
								color: "#1a1a1a",
							}}
						>
							Classifications in use
						</Typography>
						<Typography
							sx={{
								fontSize: "0.875rem",
								color: "#868e96",
							}}
						>
							{usagePercentRounded}%
						</Typography>
					</Stack>
					<Stack
						direction="row"
						sx={{
							height: 10,
							borderRadius: 5,
							overflow: "hidden",
							width: "100%",
							backgroundColor: "#e9ecef",
						}}
						aria-label="Classification usage: green is in use, grey is not in use"
					>
						{usedBarPct > 0 ? (
							<Box
								component="button"
								type="button"
								onClick={handleClassificationsClick}
								sx={{
									width: `${usedBarPct}%`,
									minWidth: 0,
									border: "none",
									padding: 0,
									cursor: "pointer",
									backgroundColor: "#16a34a",
								}}
								aria-label="Open classification search for types in use"
							/>
						) : null}
						<Box
							component="button"
							type="button"
							onClick={handleUnusedBarSegmentClick}
							disabled={unusedCount === 0 || !defsReady}
							sx={{
								flex: 1,
								minWidth: 0,
								border: "none",
								padding: 0,
								cursor:
									unusedCount > 0 && defsReady ? "pointer" : "default",
								backgroundColor: "#e9ecef",
							}}
							aria-label={
								unusedCount > 0 && defsReady
									? `View ${unusedCount} classification types not in use`
									: "No unused classification types to show"
							}
						/>
					</Stack>
					{defsReady && (
						<Typography
							component="p"
							sx={{
								fontSize: "0.875rem",
								color: "#6c757d",
								m: 0,
							}}
						>
							<strong>Not in use:</strong>{" "}
							{unusedCount > 0 ? (
								<Link
									component="button"
									type="button"
									onClick={handleOpenUnusedModal}
									sx={{
										fontSize: "0.875rem",
										color: "primary.main",
										cursor: "pointer",
										textDecoration: "underline",
										background: "none",
										border: "none",
										padding: 0,
										verticalAlign: "baseline",
										fontFamily: "inherit",
									}}
									aria-label={`Show ${unusedCount} classification types not in use`}
								>
									{numberFormatWithComma(unusedCount)}{" "}
									{unusedCount === 1
										? "classification type"
										: "classification types"}
								</Link>
							) : (
								<>
									{numberFormatWithComma(unusedCount)}{" "}
									{classificationTypeDefinitions > 0
										? unusedCount === 1
											? "classification type"
											: "classification types"
										: ""}
								</>
							)}
						</Typography>
					)}
					{!defsReady && loadingClassification ? (
						<Typography variant="caption" sx={{ color: "#868e96" }}>
							Loading classification definitions…
						</Typography>
					) : null}
					<Typography
						component="button"
						type="button"
						onClick={handleClassificationsClick}
						sx={{
							fontSize: "0.875rem",
							color: "#6c757d",
							background: "none",
							border: "none",
							cursor: "pointer",
							padding: 0,
							textAlign: "left",
							"&:hover": { color: "primary.main", textDecoration: "underline" },
						}}
						aria-label="Open classification search"
					>
						{numberFormatWithComma(typesInUse)} of{" "}
						{numberFormatWithComma(classificationTypeDefinitions)}
						classification types are in use (have at least one entity).
					</Typography>
				</Stack>

				<CustomModal
					open={unusedModalOpen}
					onClose={handleCloseUnusedModal}
					title="Classification types not in use"
					maxWidth="sm"
					button1Label="Close"
					button1Handler={handleCloseUnusedModal}
					button2Label=""
					button2Handler={handleCloseUnusedModal}
					hideButton2={true}
				>
					<Stack spacing={2} sx={{ pt: 0.5 }}>
						<Typography
							variant="body2"
							sx={{ color: "#6c757d", lineHeight: 1.5 }}
						>
							These types are defined in Atlas but have no entity assignments in
							the current metrics snapshot.
						</Typography>
						{showUnusedListSearch ? (
							<TextField
								size="small"
								fullWidth
								placeholder="Search classification name"
								value={unusedListSearchQuery}
								onChange={(e) => setUnusedListSearchQuery(e.target.value)}
								inputProps={{
									"aria-label": "Filter unused classifications by name",
								}}
								InputProps={{
									startAdornment: (
										<InputAdornment position="start">
											<SearchIcon
												sx={{ color: "#868e96", fontSize: "1.25rem" }}
												aria-hidden
											/>
										</InputAdornment>
									),
								}}
							/>
						) : null}
						<Box
							sx={{
								maxHeight: "min(45vh, 320px)",
								overflowY: "auto",
							}}
						>
							{unusedNamesFiltered.length === 0 ? (
								<Typography variant="body2" color="text.secondary" sx={{ py: 2 }}>
									{unusedListSearchQuery.trim()
										? "No classifications match your search."
										: "No unused classifications to list."}
								</Typography>
							) : (
								<List disablePadding>
									{unusedNamesFiltered.map((name) => (
										<ListItem
											key={name}
											disablePadding
											sx={{
												py: 1.25,
												borderBottom: "1px solid",
												borderColor: "divider",
												"&:last-child": { borderBottom: "none" },
											}}
										>
											<Link
												component={RouterLink}
												to={{
													pathname: `/tag/tagAttribute/${encodeURIComponent(name)}`,
													search: new URLSearchParams({ tag: name }).toString(),
												}}
												onClick={handleCloseUnusedModal}
												underline="hover"
												color="primary"
												sx={{
													fontSize: "0.875rem",
													fontWeight: 500,
													cursor: "pointer",
												}}
											>
												{name}
											</Link>
										</ListItem>
									))}
								</List>
							)}
						</Box>
					</Stack>
				</CustomModal>
			</Paper>
		);
	}
);

ClassificationCoverage.displayName = "ClassificationCoverage";

export default ClassificationCoverage;
