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

import { Stack } from "@mui/material";
import QuickSearch from "@components/GlobalSearch/QuickSearch";
import DashboardOverview from "./DashboardOverview/DashboardOverview";
import { useAppSelector } from "@hooks/reducerHook";

const DashBoard = () => {
	const dashboardRefreshVersion = useAppSelector((state) => state.dashboardRefresh.version);

	return (
		<Stack
			width="100%"
			maxWidth="100%"
			alignItems="stretch"
			justifyContent="flex-start"
			position="relative"
			height="100%"
			flex="1"
			padding={0}
			spacing={2}
			sx={{ boxSizing: "border-box", overflow: "hidden" }}
		>
			<Stack
				direction="row"
				width="100%"
				justifyContent="center"
				sx={{ mb: 2, flexShrink: 0 }}
			>
				<QuickSearch key={dashboardRefreshVersion} />
			</Stack>
			<Stack width="100%" flex={1} sx={{ minWidth: 0 }}>
				<DashboardOverview />
			</Stack>
		</Stack>
	);
};

export default DashBoard;
