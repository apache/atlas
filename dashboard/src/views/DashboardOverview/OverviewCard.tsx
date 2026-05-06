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

import { Paper, Stack, Typography, Box } from "@mui/material";
import { numberFormatWithComma } from "@utils/Helper";
import { useNavigate } from "react-router-dom";
import { navigateToSearch } from "@utils/dashboardSearchUtils";

interface OverviewCardProps {
	entityCount: number;
	tagCount: number;
	isLoading?: boolean;
}

const OverviewCard = ({ entityCount, tagCount, isLoading }: OverviewCardProps) => {
	const navigate = useNavigate();

	const handleEntitiesClick = () => {
		navigateToSearch(navigate, "all_entities");
	};

	const handleClassificationsClick = () => {
		navigateToSearch(navigate, "all_classifications");
	};

	if (isLoading) return null;

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
				"&:hover": { boxShadow: 4 }
			}}
		>
			<Box sx={{ pb: 2, borderBottom: "1px solid", borderColor: "divider" }}>
				<Typography sx={{ fontSize: "1rem", fontWeight: 600, color: "#1a1a1a" }}>
					Overview
				</Typography>
			</Box>
			<Stack direction="column" spacing={3} sx={{ pt: 2 }}>
				<Stack alignItems="flex-start" spacing={0.5}>
					<Typography
						component="button"
						sx={{
							fontSize: "1.75rem",
							fontWeight: 700,
							background: "none",
							border: "none",
							cursor: "pointer",
							padding: 0,
							color: "#1a1a1a",
							textAlign: "left",
							lineHeight: 1.2,
							"&:hover": { textDecoration: "underline", color: "primary.main" }
						}}
						onClick={handleEntitiesClick}
						aria-label="View all entities"
					>
						{numberFormatWithComma(entityCount)}
					</Typography>
					<Typography sx={{ fontSize: "0.875rem", color: "#6c757d", textTransform: "capitalize" }}>
						Entities
					</Typography>
				</Stack>
				<Stack alignItems="flex-start" spacing={0.5}>
					<Typography
						component="button"
						sx={{
							fontSize: "1.75rem",
							fontWeight: 700,
							background: "none",
							border: "none",
							cursor: "pointer",
							padding: 0,
							color: "#1a1a1a",
							textAlign: "left",
							lineHeight: 1.2,
							"&:hover": { textDecoration: "underline", color: "primary.main" }
						}}
						onClick={handleClassificationsClick}
						aria-label="View all classifications"
					>
						{numberFormatWithComma(tagCount)}
					</Typography>
					<Typography sx={{ fontSize: "0.875rem", color: "#6c757d", textTransform: "capitalize" }}>
						Classifications
					</Typography>
				</Stack>
			</Stack>
		</Paper>
	);
};

export default OverviewCard;
