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

import { Paper, Stack, Box } from "@mui/material";
import SkeletonLoader from "@components/SkeletonLoader";

const LatestEntitiesSkeleton = () => (
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
				<SkeletonLoader animation="wave" variant="text" count={1} width="60%" height={24} />
				<SkeletonLoader animation="wave" variant="text" count={1} width={60} height={20} />
			</Stack>
		</Box>
		<Stack spacing={1} sx={{ pt: 2 }}>
			{[1, 2, 3, 4, 5, 6, 7].map((i) => (
				<Stack key={i} direction="row" justifyContent="space-between" alignItems="center" sx={{ py: 0.5 }}>
					<SkeletonLoader animation="wave" variant="text" count={1} width={`${70 + (i % 3) * 10}%`} height={20} />
					<SkeletonLoader animation="wave" variant="text" count={1} width={80} height={18} />
				</Stack>
			))}
		</Stack>
	</Paper>
);

export default LatestEntitiesSkeleton;
