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

/** Matches Entity Status Overview donut — single source for dashboard charts */
export const ENTITY_STATUS_DONUT_COLORS = {
	Active: "#10b981",
	Shell: "#f59e0b",
	Deleted: "#ef4444",
} as const;

/** Active primary series / bar fill (aligned with Classification Distribution bars) */
export const CHART_BAR_ACTIVE_BLUE = "#1976d2";

/** Horizontal bar charts: Y-axis title + ticks; keep left tight to reduce card gutter */
export const HORIZONTAL_BAR_CHART_MARGIN = {
	top: 8,
	right: 72,
	left: 44,
	bottom: 48,
} as const;

/** Tighter layout: short classification names — minimize dead space left of Y ticks */
export const CLASSIFICATION_DISTRIBUTION_CHART_MARGIN = {
	top: 8,
	right: 72,
	left: 12,
	bottom: 48,
} as const;
